# Databricks notebook source
# DBTITLE 1,Preview landing data
# MAGIC %sql
# MAGIC select * from finance_zerobus.transaction.gmail_data_raw_noloc

# COMMAND ----------

# DBTITLE 1,Overview
# MAGIC %md
# MAGIC # 01 — Bronze to Silver: Gmail Transaction Alerts (Batch PySpark)
# MAGIC
# MAGIC **Approach:** Regular PySpark batch notebook — runs on interactive or job compute (no pipeline required).
# MAGIC
# MAGIC **Pipeline stage:** Landing (`gmail_data_raw_noloc`) → Silver (`gmail_silver` + `gmail_silver_quarantine`)
# MAGIC
# MAGIC **Responsibilities:**
# MAGIC 1. Batch-read the landing Delta table into a bronze layer with ingestion metadata.
# MAGIC 2. Parse free-text email bodies into structured transaction fields.
# MAGIC 3. Apply data-quality rules: mandatory fields, amount > 0, valid direction.
# MAGIC 4. Split rows into good (`gmail_silver_clean`) and bad (`gmail_silver_quarantine`) with reasons.
# MAGIC 5. Deduplicate into `gmail_silver` via `MERGE` (SCD-1 equivalent, sequenced by `event_time`).
# MAGIC
# MAGIC ### Pipeline vs Batch — side-by-side
# MAGIC | Aspect | Pipeline (`dp`) | This notebook (batch) |
# MAGIC | --- | --- | --- |
# MAGIC | Execution | Runs inside SDP pipeline | Runs on interactive / job compute |
# MAGIC | Read | `spark.readStream.table()` | `spark.read.table()` |
# MAGIC | Write | Implicit (decorator handles it) | Explicit `.write.saveAsTable()` or `MERGE` |
# MAGIC | Dedup | `dp.create_auto_cdc_flow()` | `DeltaTable.merge()` + window dedup |
# MAGIC | Intermediate views | `@dp.temporary_view()` | Plain Python function |

# COMMAND ----------

# DBTITLE 1,Imports and config
# No dlt / dp import needed — just standard PySpark + Delta
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType
from pyspark.sql.window import Window
from delta.tables import DeltaTable

CATALOG = "finance_zerobus"
SCHEMA  = "transaction"
LANDING = f"{CATALOG}.{SCHEMA}.gmail_data_raw_noloc"
TARGET  = f"{CATALOG}.{SCHEMA}"  # all output tables go here

# COMMAND ----------

# DBTITLE 1,Bronze header
# MAGIC %md
# MAGIC ## 1. Bronze — streaming read of landing table
# MAGIC Append-only, adds ingestion metadata. Never fails on content — bad rows are handled at silver.

# COMMAND ----------

# DBTITLE 1,Write bronze table
# Pipeline equivalent:  @dp.table(name="gmail_bronze")
# Batch version:        spark.read → add metadata → .write.saveAsTable()

df_bronze = (
    spark.read.table(LANDING)
         .withColumn("_bronze_ingested_at", F.current_timestamp())
         .withColumn("_source_table", F.lit(LANDING))
)

(df_bronze.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{TARGET}.gmail_bronze")
)

print(f"Bronze: {spark.read.table(f'{TARGET}.gmail_bronze').count():,} rows -> {TARGET}.gmail_bronze")

# COMMAND ----------

# DBTITLE 1,Preview bronze
# MAGIC %sql
# MAGIC select * from finance_zerobus.transaction.gmail_bronze

# COMMAND ----------

# DBTITLE 1,Parser header
# MAGIC %md
# MAGIC ## 2. Parser + DQ flags
# MAGIC
# MAGIC One intermediate view that:
# MAGIC - Normalises whitespace (newlines/tabs → single spaces) — critical for ATM rows.
# MAGIC - Runs regex extraction for amount, direction, txn_type, ref, VPA, card, account, location, balance, local datetime.
# MAGIC - Builds a `failure_reasons` array — empty array = row is clean.

# COMMAND ----------

# DBTITLE 1,Parse and flag
# Pipeline equivalent:  @dp.temporary_view(name="gmail_silver_parsed")
#                       dlt.read_stream("gmail_bronze")
# Batch version:        plain function + spark.read.table()

def parse_and_flag(df_bronze):
    """Parse email text, extract transaction fields, attach DQ failure reasons."""

    RE_AMOUNT_ANCHORED = r"(?i)(?:debited|withdrawal for|spent|purchase of)\s+Rs\.?\s*([0-9,]+\.?[0-9]*)"
    RE_AMOUNT_POSTFIX  = r"(?i)Rs\.?\s*([0-9,]+\.?[0-9]*)\s+(?:has been debited|debited|credited)"
    RE_AMOUNT_GENERIC  = r"(?i)Rs\.?\s*([0-9,]+\.?[0-9]*)"
    RE_REF             = r"(?i)(?:reference number is|ref(?:erence)?\s*(?:no|#)?[:\s]+)\s*([A-Z0-9]+)"
    RE_VPA             = r"(?i)VPA\s+([^\s]+)"
    RE_COUNTERPARTY    = r"(?i)VPA\s+[^\s]+\s+([A-Z][A-Z0-9\s&]+?)(?:\s+on\s+\d)"
    RE_CARD_ENDING     = r"(?i)Card ending\s+(\d{4})"
    RE_CARD_STARS      = r"\*\*\s*(\d{4})"
    RE_ACCOUNT         = r"(?i)from account\s+(\d{3,})"
    RE_CITY            = r"(?i)\bin\s+([A-Z]{3,})\s+at\b"
    RE_PLACE           = r"(?i)\bat\s+([A-Z0-9][A-Z0-9\s&\.\-]+?)\s+on\s+\d"
    RE_BALANCE         = r"(?i)available balance[^0-9]*Rs\.?\s*([0-9,]+\.?[0-9]*)"
    RE_LOCAL_DT        = r"(?i)on\s+(\d{2}-\d{2}-\d{4}\s+\d{2}:\d{2}:\d{2})"

    text = F.regexp_replace(
        F.concat_ws(
            " ",
            F.coalesce(F.col("body"),    F.lit("")),
            F.coalesce(F.col("snippet"), F.lit("")),
            F.coalesce(F.col("subject"), F.lit("")),
        ),
        r"[\s]+", " ",
    )

    df = (
        df_bronze                                       # <-- passed in, not dlt.read_stream()
           .withColumn("_text", text)

           # -- amount (layered fallbacks)
           .withColumn(
               "_amt_raw",
               F.coalesce(
                   F.nullif(F.regexp_extract("_text", RE_AMOUNT_ANCHORED, 1), F.lit("")),
                   F.nullif(F.regexp_extract("_text", RE_AMOUNT_POSTFIX,  1), F.lit("")),
                   F.nullif(F.regexp_extract("_text", RE_AMOUNT_GENERIC,  1), F.lit("")),
               ),
           )
           .withColumn(
               "amount",
               F.when(F.col("_amt_raw").isNotNull(),
                      F.regexp_replace("_amt_raw", ",", "").cast(DecimalType(18, 2)))
                .otherwise(F.lit(None).cast(DecimalType(18, 2))),
           )

           # -- currency
           .withColumn(
               "currency",
               F.when(F.col("_text").rlike(r"(?i)Rs\.?|INR|\u20b9"), F.lit("INR"))
                .when(F.col("_text").rlike(r"\$|USD"),           F.lit("USD"))
                .otherwise(F.lit("UNKNOWN")),
           )

           # -- direction
           .withColumn(
               "direction",
               F.when(F.col("_text").rlike(r"(?i)\bcredited\b|\brefund\b|\breversal\b|\breceived\b"),
                      F.lit("CREDIT"))
                .when(F.col("_text").rlike(r"(?i)\bdebited\b|withdrawal|\bspent\b|\bpurchase\b|debit card"),
                      F.lit("DEBIT"))
                .otherwise(F.lit("UNKNOWN")),
           )

           # -- txn_type
           .withColumn(
               "txn_type",
               F.when(F.col("_text").rlike(r"(?i)ATM\s+withdrawal"), F.lit("ATM_WITHDRAWAL"))
                .when(F.col("_text").rlike(r"(?i)\bUPI\b"),          F.lit("UPI"))
                .when(F.col("_text").rlike(r"(?i)Credit Card"),      F.lit("CREDIT_CARD"))
                .when(F.col("_text").rlike(r"(?i)Debit Card"),       F.lit("DEBIT_CARD"))
                .when(F.col("_text").rlike(r"(?i)\bNEFT\b"),         F.lit("NEFT"))
                .when(F.col("_text").rlike(r"(?i)\bIMPS\b"),         F.lit("IMPS"))
                .when(F.col("_text").rlike(r"(?i)\bRTGS\b"),         F.lit("RTGS"))
                .otherwise(F.lit("OTHER")),
           )

           # -- txn reference
           .withColumn("txn_ref",
                       F.nullif(F.regexp_extract("_text", RE_REF, 1), F.lit("")))

           # -- UPI VPA + counterparty
           .withColumn("vpa",
                       F.nullif(F.regexp_extract("_text", RE_VPA, 1), F.lit("")))
           .withColumn("counterparty",
                       F.trim(F.nullif(F.regexp_extract("_text", RE_COUNTERPARTY, 1), F.lit(""))))

           # -- card / account
           .withColumn(
               "card_last4",
               F.coalesce(
                   F.nullif(F.regexp_extract("_text", RE_CARD_ENDING, 1), F.lit("")),
                   F.nullif(F.regexp_extract("_text", RE_CARD_STARS,  1), F.lit("")),
               ),
           )
           .withColumn("account_last4",
                       F.nullif(F.regexp_extract("_text", RE_ACCOUNT, 1), F.lit("")))

           # -- location (ATM / POS)
           .withColumn("location_city",
                       F.nullif(F.regexp_extract("_text", RE_CITY, 1), F.lit("")))
           .withColumn("location_place",
                       F.trim(F.nullif(F.regexp_extract("_text", RE_PLACE, 1), F.lit(""))))

           # -- post-txn available balance
           .withColumn("_bal_raw",
                       F.nullif(F.regexp_extract("_text", RE_BALANCE, 1), F.lit("")))
           .withColumn(
               "available_balance",
               F.when(F.col("_bal_raw").isNotNull(),
                      F.regexp_replace("_bal_raw", ",", "").cast(DecimalType(18, 2)))
                .otherwise(F.lit(None).cast(DecimalType(18, 2))),
           )

           # -- local txn datetime (body)
           .withColumn("_dt_raw",
                       F.nullif(F.regexp_extract("_text", RE_LOCAL_DT, 1), F.lit("")))
           .withColumn(
               "txn_datetime_local",
               F.expr("try_to_timestamp(_dt_raw, 'dd-MM-yyyy HH:mm:ss')"),
           )

           # -- bank
           .withColumn(
               "bank",
               F.when(F.col("sender").rlike("(?i)hdfc"),  F.lit("HDFC"))
                .when(F.col("sender").rlike("(?i)icici"), F.lit("ICICI"))
                .when(F.col("sender").rlike("(?i)axis"),  F.lit("AXIS"))
                .when(F.col("sender").rlike("(?i)sbi"),   F.lit("SBI"))
                .otherwise(F.lit("OTHER")),
           )
    )

    # -- Build failure_reasons array (empty = clean)
    df = df.withColumn(
        "failure_reasons",
        F.array_compact(F.array(
            F.when(F.col("id").isNull(),
                   F.lit("mandatory_field_missing:id")),
            F.when(F.col("sender").isNull(),
                   F.lit("mandatory_field_missing:sender")),
            F.when(F.col("event_time").isNull(),
                   F.lit("mandatory_field_missing:event_time")),
            F.when(F.col("body").isNull() & F.col("snippet").isNull(),
                   F.lit("mandatory_field_missing:body_and_snippet")),
            F.when(F.col("amount").isNull() | (F.col("amount") <= 0),
                   F.lit("invalid_amount")),
            F.when(~F.col("direction").isin("DEBIT", "CREDIT"),
                   F.lit("invalid_direction")),
        )),
    )

    return df.select(
        "id", "thread_id", "sender", "recipient", "subject",
        "bank", "txn_type", "direction",
        "amount", "currency",
        "txn_ref", "vpa", "counterparty",
        "card_last4", "account_last4",
        "location_city", "location_place",
        "available_balance", "txn_datetime_local",
        "event_time", "ingestion_time",
        "_bronze_ingested_at",
        "body", "snippet",
        "failure_reasons",
        F.current_timestamp().alias("_silver_processed_at"),
    )

# Run the parser on the bronze table we just wrote
df_parsed = parse_and_flag(spark.read.table(f"{TARGET}.gmail_bronze"))
print(f"Parsed: {df_parsed.count():,} rows")
display(df_parsed.limit(15))

# COMMAND ----------

# DBTITLE 1,Silver split header
# MAGIC %md
# MAGIC ## 3. Silver split — clean vs quarantine
# MAGIC
# MAGIC Rows with an empty `failure_reasons` array go to `gmail_silver_clean`.
# MAGIC Rows with at least one failure reason go to `gmail_silver_quarantine`.

# COMMAND ----------

# DBTITLE 1,Write clean and quarantine tables
# Split on DQ flag
df_clean      = df_parsed.filter(F.size("failure_reasons") == 0)
df_quarantine = df_parsed.filter(F.size("failure_reasons") > 0)

# -- Write gmail_silver_clean (overwrite)
(df_clean.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{TARGET}.gmail_silver_clean")
)

# -- Write gmail_silver_quarantine (overwrite)
(df_quarantine.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{TARGET}.gmail_silver_quarantine")
)

clean_ct = spark.read.table(f"{TARGET}.gmail_silver_clean").count()
quar_ct  = spark.read.table(f"{TARGET}.gmail_silver_quarantine").count()
print(f"Silver clean:      {clean_ct:,} rows -> {TARGET}.gmail_silver_clean")
print(f"Silver quarantine: {quar_ct:,} rows  -> {TARGET}.gmail_silver_quarantine")

# COMMAND ----------

# DBTITLE 1,MERGE header
# MAGIC %md
# MAGIC ## 4. Deduplicate & MERGE into `gmail_silver`
# MAGIC
# MAGIC Window-dedup the clean rows by `id` (keep the latest `event_time`), then
# MAGIC MERGE into the final `gmail_silver` table as an SCD-1 upsert.

# COMMAND ----------

# DBTITLE 1,Dedup MERGE into gmail_silver
# ---- Deduplicate clean rows: keep latest event_time per id ----
w = Window.partitionBy("id").orderBy(F.col("event_time").desc())

# Drop columns not in gmail_silver (raw email content + DQ array)
df_deduped = (
    spark.read.table(f"{TARGET}.gmail_silver_clean")
         .drop("body", "snippet", "failure_reasons")
         .withColumn("_rn", F.row_number().over(w))
         .filter(F.col("_rn") == 1)
         .drop("_rn")
)

# ---- MERGE (SCD-1 upsert) into gmail_silver ----
target_table = f"{TARGET}.gmail_silver"

if not spark.catalog.tableExists(target_table):
    # First run — create the table
    df_deduped.write.saveAsTable(target_table)
    print(f"Created {target_table} with {spark.read.table(target_table).count():,} rows")
else:
    dt = DeltaTable.forName(spark, target_table)
    merge_cols = [c for c in df_deduped.columns
                  if c not in ("id", "_silver_processed_at")]

    dt.alias("tgt").merge(
        df_deduped.alias("src"),
        "tgt.id = src.id"
    ).whenMatchedUpdate(
        condition="src.event_time > tgt.event_time",
        set={c: f"src.{c}" for c in merge_cols}
            | {"_silver_processed_at": "src._silver_processed_at"},
    ).whenNotMatchedInsertAll(
    ).execute()

    print(f"Merged into {target_table} — now {spark.read.table(target_table).count():,} rows")

# COMMAND ----------

# DBTITLE 1,Preview gmail_silver
# MAGIC %sql
# MAGIC SELECT * FROM finance_zerobus.transaction.gmail_silver LIMIT 20
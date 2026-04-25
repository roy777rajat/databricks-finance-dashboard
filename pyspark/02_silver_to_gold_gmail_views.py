# Databricks notebook source
# DBTITLE 1,Overview
# MAGIC %md
# MAGIC # 02 — Silver to Gold Views: Consumption Layer (Batch PySpark)
# MAGIC
# MAGIC **Approach:** Regular PySpark batch notebook — runs on interactive or job compute (no pipeline required).
# MAGIC
# MAGIC **Pipeline stage:** Silver → Gold aggregation tables for dashboards & apps.
# MAGIC
# MAGIC Per design, `gmail_data_raw_noloc` remains the gold "source of truth" and is not overwritten.
# MAGIC This notebook builds **gold summary tables** on top of silver for consumption:
# MAGIC
# MAGIC - `gmail_gold_daily_summary`    — daily totals by bank / txn_type / direction
# MAGIC - `gmail_gold_merchant_summary` — per-counterparty roll-up
# MAGIC - `gmail_gold_recon_metrics`    — ingested vs processed vs failed, per day
# MAGIC - `gmail_gold_failure_reasons`  — failure counts grouped by reason
# MAGIC
# MAGIC ### Pipeline vs Batch — what changed
# MAGIC | Pipeline (`@dlt.table`) | This notebook (batch) |
# MAGIC | --- | --- |
# MAGIC | `dlt.read("gmail_silver")` | `spark.read.table(f"{TARGET}.gmail_silver")` |
# MAGIC | `@dlt.table(name="...")` decorator | `df.write.mode("overwrite").saveAsTable()` |
# MAGIC | Framework manages refresh | You re-run the notebook (or schedule via Job) |

# COMMAND ----------

# DBTITLE 1,Imports and config
# No dlt import needed — just standard PySpark
from pyspark.sql import functions as F

CATALOG = "finance_zerobus"
SCHEMA  = "transaction"
LANDING = f"{CATALOG}.{SCHEMA}.gmail_data_raw_noloc"
TARGET  = f"{CATALOG}.{SCHEMA}"

# COMMAND ----------

# DBTITLE 1,Daily summary header
# MAGIC %md
# MAGIC ## 1. Daily summary
# MAGIC Daily debit/credit totals by bank, txn_type, direction, currency.

# COMMAND ----------

# DBTITLE 1,Gold daily summary
# Pipeline equivalent:
#   @dlt.table(name="gmail_gold_daily_summary")
#   def gmail_gold_daily_summary():
#       return dlt.read("gmail_silver").withColumn(...).groupBy(...).agg(...)
#
# Batch version:  spark.read.table() → groupBy → .write.saveAsTable()

df_daily = (
    spark.read.table(f"{TARGET}.gmail_silver")
         .withColumn("txn_date", F.to_date("event_time"))
         .groupBy("txn_date", "bank", "txn_type", "direction", "currency")
         .agg(
             F.count("*").alias("txn_count"),
             F.sum("amount").alias("total_amount"),
             F.avg("amount").alias("avg_amount"),
             F.min("amount").alias("min_amount"),
             F.max("amount").alias("max_amount"),
         )
)

(df_daily.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{TARGET}.gmail_gold_daily_summary")
)

print(f"Daily summary: {spark.read.table(f'{TARGET}.gmail_gold_daily_summary').count():,} rows")
display(spark.read.table(f"{TARGET}.gmail_gold_daily_summary").orderBy(F.col("txn_date").desc()).limit(10))

# COMMAND ----------

# DBTITLE 1,Merchant summary header
# MAGIC %md
# MAGIC ## 2. Merchant / counterparty summary
# MAGIC Per-counterparty transaction roll-up — only rows where counterparty is known.

# COMMAND ----------

# DBTITLE 1,Gold merchant summary
# Pipeline equivalent:
#   @dlt.table(name="gmail_gold_merchant_summary")
#   def gmail_gold_merchant_summary():
#       return dlt.read("gmail_silver").filter(...).groupBy(...).agg(...)
#
# Batch version:  spark.read.table() → filter → groupBy → .write.saveAsTable()

df_merchant = (
    spark.read.table(f"{TARGET}.gmail_silver")
         .filter(F.col("counterparty").isNotNull() & (F.col("counterparty") != ""))
         .groupBy("counterparty", "vpa", "txn_type", "direction", "currency")
         .agg(
             F.count("*").alias("txn_count"),
             F.sum("amount").alias("total_amount"),
             F.min("event_time").alias("first_seen"),
             F.max("event_time").alias("last_seen"),
         )
)

(df_merchant.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{TARGET}.gmail_gold_merchant_summary")
)

print(f"Merchant summary: {spark.read.table(f'{TARGET}.gmail_gold_merchant_summary').count():,} rows")
display(spark.read.table(f"{TARGET}.gmail_gold_merchant_summary").orderBy(F.col("total_amount").desc()).limit(10))

# COMMAND ----------

# DBTITLE 1,Recon metrics header
# MAGIC %md
# MAGIC ## 3. Reconciliation metrics (per day)
# MAGIC
# MAGIC Authoritative counts joined across landing, silver, and quarantine.
# MAGIC Grain = one row per `txn_date`. The invariant:
# MAGIC
# MAGIC ```
# MAGIC ingested = processed + failed + duplicates_dropped
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Gold recon metrics
# Pipeline equivalent:
#   @dlt.table(name="gmail_gold_recon_metrics")
#   def gmail_gold_recon_metrics():
#       ingested = spark.read.table(LANDING)...
#       processed = dlt.read("gmail_silver")...
#       failed = dlt.read("gmail_silver_quarantine")...
#       return ingested.join(processed).join(failed)
#
# Batch version:  all reads via spark.read.table()

# Landing — the "truth" for ingested
ingested = (
    spark.read.table(LANDING)
         .withColumn("txn_date", F.to_date("event_time"))
         .groupBy("txn_date")
         .agg(F.count("*").alias("ingested_count"))
)

# Silver — what made it through DQ + dedup
processed = (
    spark.read.table(f"{TARGET}.gmail_silver")
         .withColumn("txn_date", F.to_date("event_time"))
         .groupBy("txn_date")
         .agg(F.count("*").alias("processed_count"))
)

# Quarantine — DQ failures
failed = (
    spark.read.table(f"{TARGET}.gmail_silver_quarantine")
         .withColumn("txn_date", F.to_date("event_time"))
         .groupBy("txn_date")
         .agg(F.count("*").alias("failed_count"))
)

df_recon = (
    ingested
    .join(processed, "txn_date", "left")
    .join(failed,    "txn_date", "left")
    .na.fill(0, ["processed_count", "failed_count"])
    .withColumn(
        "duplicates_dropped",
        F.greatest(
            F.col("ingested_count") - F.col("processed_count") - F.col("failed_count"),
            F.lit(0),
        ),
    )
    .withColumn(
        "pass_rate_pct",
        F.round(
            100.0 * F.col("processed_count")
            / F.when(F.col("ingested_count") == 0, 1).otherwise(F.col("ingested_count")),
            2,
        ),
    )
    .orderBy(F.col("txn_date").desc())
)

(df_recon.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{TARGET}.gmail_gold_recon_metrics")
)

print(f"Recon metrics: {spark.read.table(f'{TARGET}.gmail_gold_recon_metrics').count():,} rows")
display(spark.read.table(f"{TARGET}.gmail_gold_recon_metrics"))

# COMMAND ----------

# DBTITLE 1,Failure reasons header
# MAGIC %md
# MAGIC ## 4. Failure reasons — grouped view for dashboard bar chart
# MAGIC Each reason from the `failure_reasons` array is exploded and counted separately.

# COMMAND ----------

# DBTITLE 1,Gold failure reasons
# Pipeline equivalent:
#   @dlt.table(name="gmail_gold_failure_reasons")
#   def gmail_gold_failure_reasons():
#       return dlt.read("gmail_silver_quarantine").withColumn("reason", explode(...))
#
# Batch version:  spark.read.table() → explode → groupBy → .write.saveAsTable()

df_failures = (
    spark.read.table(f"{TARGET}.gmail_silver_quarantine")
         .withColumn("reason", F.explode("failure_reasons"))
         .withColumn("txn_date", F.to_date("event_time"))
         .groupBy("txn_date", "reason")
         .agg(F.count("*").alias("failure_count"))
         .orderBy(F.col("txn_date").desc(), F.col("failure_count").desc())
)

(df_failures.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{TARGET}.gmail_gold_failure_reasons")
)

print(f"Failure reasons: {spark.read.table(f'{TARGET}.gmail_gold_failure_reasons').count():,} rows")
display(spark.read.table(f"{TARGET}.gmail_gold_failure_reasons"))

# COMMAND ----------

# DBTITLE 1,Final summary
# —— Summary: all gold tables ——————————————————————————————————————
print("=" * 60)
print("GOLD LAYER — ALL TABLES")
print("=" * 60)
for tbl in ["gmail_gold_daily_summary", "gmail_gold_merchant_summary",
            "gmail_gold_recon_metrics", "gmail_gold_failure_reasons"]:
    cnt = spark.read.table(f"{TARGET}.{tbl}").count()
    print(f"  {TARGET}.{tbl:40s} {cnt:>8,} rows")
print("=" * 60)
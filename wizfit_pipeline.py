# Databricks notebook source
# MAGIC %md
# MAGIC # WizFit Client Progress Pipeline
# MAGIC **Bronze → Silver → Gold medallion pipeline on Databricks (PySpark + Delta Lake)**
# MAGIC
# MAGIC Turns raw, messy coaching data (weekly check-ins, nutrition logs, workout logs) into clean
# MAGIC tables that answer real business questions:
# MAGIC - Which clients are on track toward their goal weight?
# MAGIC - Who is falling off (missed check-ins, low logging) and needs outreach?
# MAGIC - Are clients getting stronger week over week?
# MAGIC
# MAGIC **Before running:** upload the 4 CSVs from `/data` into a Volume
# MAGIC (Catalog → workspace → default → Create → Volume named `wizfit_raw`).

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

CATALOG = "workspace"
SCHEMA = "default"
RAW_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/wizfit_raw"

spark.sql(f"USE {CATALOG}.{SCHEMA}")

def table(name):
    return f"{CATALOG}.{SCHEMA}.{name}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🥉 Bronze — land the raw data as-is
# MAGIC Everything is read as strings so nothing is lost. Each row gets an ingest timestamp
# MAGIC and source file name so issues can be traced back.

# COMMAND ----------

SOURCES = ["clients", "checkins", "nutrition", "workouts"]

for src in SOURCES:
    df = (spark.read
          .option("header", True)
          .option("inferSchema", False)        # keep raw as strings
          .csv(f"{RAW_PATH}/{src}.csv")
          .withColumn("_ingested_at", F.current_timestamp())
          .withColumn("_source_file", F.col("_metadata.file_name")))
    df.write.mode("overwrite").option("overwriteSchema", True).saveAsTable(table(f"bronze_{src}"))
    print(f"bronze_{src}: {df.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🥈 Silver — clean, type, and validate
# MAGIC Data quality problems found in the raw data and how each is fixed:
# MAGIC
# MAGIC | Problem | Fix |
# MAGIC |---|---|
# MAGIC | Client IDs with stray spaces / lowercase (`" wf-003 "`) | `trim` + `upper` |
# MAGIC | Two date formats (`2026-06-04` and `06/04/2026`) | `coalesce` of two parsers |
# MAGIC | Some weights entered in kg instead of lbs | If < 50% of client's start weight, convert ×2.2046 |
# MAGIC | Blank weights | Dropped from weight analysis |
# MAGIC | Duplicate check-in submissions | `dropDuplicates` on client + date |
# MAGIC | Negative calorie entries | Filtered out and counted |
# MAGIC | Exercise names in mixed case (`DEADLIFT`) | Normalized to title case |

# COMMAND ----------

def clean_id(col):
    return F.upper(F.trim(F.col(col)))

def parse_date(col):
    # Route each format to its own parser (safe under ANSI mode, which Databricks enables)
    c = F.trim(F.col(col))
    return (F.when(c.rlike(r"^\d{4}-\d{2}-\d{2}$"), F.to_date(c, "yyyy-MM-dd"))
             .when(c.rlike(r"^\d{2}/\d{2}/\d{4}$"), F.to_date(c, "MM/dd/yyyy")))

# --- clients ---
silver_clients = (spark.table(table("bronze_clients"))
    .select(
        clean_id("client_id").alias("client_id"),
        parse_date("start_date").alias("start_date"),
        F.col("goal"),
        F.col("program"),
        F.col("start_weight_lbs").cast("double").alias("start_weight_lbs"),
        F.col("goal_weight_lbs").cast("double").alias("goal_weight_lbs"),
    ))
silver_clients.write.mode("overwrite").saveAsTable(table("silver_clients"))

# --- check-ins ---
raw_ck = (spark.table(table("bronze_checkins"))
    .select(
        clean_id("client_id").alias("client_id"),
        parse_date("checkin_date").alias("checkin_date"),
        F.col("weight").cast("double").alias("weight_raw"),
        F.col("avg_daily_steps").cast("int").alias("avg_daily_steps"),
        F.col("avg_sleep_hours").cast("double").alias("avg_sleep_hours"),
        F.col("energy_1to10").cast("int").alias("energy_1to10"),
    )
    .dropDuplicates(["client_id", "checkin_date"]))

silver_checkins = (raw_ck
    .join(silver_clients.select("client_id", "start_weight_lbs"), "client_id", "left")
    .withColumn("unit_fixed", F.col("weight_raw") < F.col("start_weight_lbs") * 0.5)
    .withColumn("weight_lbs",
        F.when(F.col("unit_fixed"), F.round(F.col("weight_raw") * 2.2046, 1))
         .otherwise(F.col("weight_raw")))
    .drop("start_weight_lbs"))
silver_checkins.write.mode("overwrite").saveAsTable(table("silver_checkins"))

# --- nutrition ---
raw_nu = (spark.table(table("bronze_nutrition"))
    .select(
        clean_id("client_id").alias("client_id"),
        parse_date("log_date").alias("log_date"),
        F.col("calories").cast("int").alias("calories"),
        F.col("protein_g").cast("int").alias("protein_g"),
        F.col("carbs_g").cast("int").alias("carbs_g"),
        F.col("fat_g").cast("int").alias("fat_g"),
    ))
bad_calories = raw_nu.filter(F.col("calories") <= 0).count()
silver_nutrition = raw_nu.filter(F.col("calories") > 0)
silver_nutrition.write.mode("overwrite").saveAsTable(table("silver_nutrition"))

# --- workouts ---
silver_workouts = (spark.table(table("bronze_workouts"))
    .select(
        clean_id("client_id").alias("client_id"),
        parse_date("workout_date").alias("workout_date"),
        F.initcap(F.trim(F.col("exercise"))).alias("exercise"),
        F.col("sets").cast("int").alias("sets"),
        F.col("reps").cast("int").alias("reps"),
        F.col("load_lbs").cast("double").alias("load_lbs"),
    )
    .withColumn("volume_lbs", F.col("sets") * F.col("reps") * F.col("load_lbs")))
silver_workouts.write.mode("overwrite").saveAsTable(table("silver_workouts"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data quality report

# COMMAND ----------

dq = {
    "duplicate_checkins_removed": spark.table(table("bronze_checkins")).count() - silver_checkins.count(),
    "kg_weights_converted": silver_checkins.filter("unit_fixed").count(),
    "blank_weights": silver_checkins.filter(F.col("weight_lbs").isNull()).count(),
    "negative_calorie_rows_removed": bad_calories,
    "unparsed_dates": silver_checkins.filter(F.col("checkin_date").isNull()).count(),
}
for k, v in dq.items():
    print(f"{k:32s} {v}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🥇 Gold — business-ready tables

# COMMAND ----------

# MAGIC %md
# MAGIC ### `gold_client_progress` — one row per client: are they on track?

# COMMAND ----------

w_first = Window.partitionBy("client_id").orderBy("checkin_date")
w_last = Window.partitionBy("client_id").orderBy(F.col("checkin_date").desc())

valid_ck = silver_checkins.filter(F.col("weight_lbs").isNotNull())
latest = (valid_ck.withColumn("rn", F.row_number().over(w_last))
          .filter("rn = 1")
          .select("client_id", F.col("weight_lbs").alias("current_weight_lbs"),
                  F.col("checkin_date").alias("last_checkin")))

checkin_stats = silver_checkins.groupBy("client_id").agg(
    F.countDistinct("checkin_date").alias("checkins_completed"),
    F.round(F.avg("avg_sleep_hours"), 1).alias("avg_sleep_hours"),
    F.round(F.avg("avg_daily_steps"), 0).alias("avg_daily_steps"),
    F.round(F.avg("energy_1to10"), 1).alias("avg_energy"))

nutrition_stats = silver_nutrition.groupBy("client_id").agg(
    F.countDistinct("log_date").alias("days_logged"),
    F.round(F.avg("calories"), 0).alias("avg_calories"),
    F.round(F.avg("protein_g"), 0).alias("avg_protein_g"))

gold_progress = (silver_clients
    .join(latest, "client_id", "left")
    .join(checkin_stats, "client_id", "left")
    .join(nutrition_stats, "client_id", "left")
    .withColumn("weeks_in_program", F.lit(12))
    .withColumn("lbs_changed", F.round(F.col("current_weight_lbs") - F.col("start_weight_lbs"), 1))
    # % to goal only means something when the goal is a real change (>= 5 lbs).
    # Maintenance/recomp goals made tiny denominators and blew up to 300%+.
    .withColumn("pct_to_goal",
        F.when(F.abs(F.col("start_weight_lbs") - F.col("goal_weight_lbs")) >= 5,
            F.round((F.col("start_weight_lbs") - F.col("current_weight_lbs")) /
                    (F.col("start_weight_lbs") - F.col("goal_weight_lbs")) * 100, 1)))
    .withColumn("checkin_rate_pct", F.round(F.col("checkins_completed") / F.col("weeks_in_program") * 100, 0))
    .withColumn("logging_rate_pct", F.round(F.col("days_logged") / (F.col("weeks_in_program") * 7) * 100, 0))
    .withColumn("status",
        F.when(F.col("checkin_rate_pct") < 75, "🔴 At risk — reach out")
         .when(F.col("logging_rate_pct") < 70, "🟡 Slipping on logging")
         .otherwise("🟢 On track")))

gold_progress.write.mode("overwrite").saveAsTable(table("gold_client_progress"))
display(gold_progress.orderBy("checkin_rate_pct"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### `gold_weekly_trends` — one row per client per week (for charts)

# COMMAND ----------

gold_weekly = (silver_workouts
    .withColumn("week_start", F.date_trunc("week", "workout_date").cast("date"))
    .groupBy("client_id", "week_start")
    .agg(F.countDistinct("workout_date").alias("sessions"),
         F.sum("volume_lbs").alias("total_volume_lbs"))
    .join(silver_checkins
            .withColumn("week_start", F.date_trunc("week", "checkin_date").cast("date"))
            .groupBy("client_id", "week_start")
            .agg(F.avg("weight_lbs").alias("weight_lbs")),
          ["client_id", "week_start"], "full")
    .orderBy("client_id", "week_start"))

gold_weekly.write.mode("overwrite").saveAsTable(table("gold_weekly_trends"))
display(gold_weekly)

# COMMAND ----------

# MAGIC %md
# MAGIC ### `gold_strength_progress` — best lift per exercise, first week vs last week

# COMMAND ----------

# Clients rotate exercises, so a lift often doesn't appear in the first/last calendar week.
# Compare each lift's FIRST session to its LAST session instead.
w_ex = Window.partitionBy("client_id", "exercise")

gold_strength = (silver_workouts
    .withColumn("first_date", F.min("workout_date").over(w_ex))
    .withColumn("last_date", F.max("workout_date").over(w_ex))
    .groupBy("client_id", "exercise")
    .agg(F.max(F.when(F.col("workout_date") == F.col("first_date"), F.col("load_lbs"))).alias("first_session_lbs"),
         F.max(F.when(F.col("workout_date") == F.col("last_date"), F.col("load_lbs"))).alias("last_session_lbs"),
         F.countDistinct("workout_date").alias("sessions"))
    .withColumn("lbs_gained", F.col("last_session_lbs") - F.col("first_session_lbs"))
    .orderBy("client_id", "exercise"))

gold_strength.write.mode("overwrite").option("overwriteSchema", True).saveAsTable(table("gold_strength_progress"))
display(gold_strength)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Analysis — which coaching program keeps clients most consistent?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT program,
# MAGIC        COUNT(*) AS clients,
# MAGIC        ROUND(AVG(checkin_rate_pct), 0) AS avg_checkin_rate,
# MAGIC        ROUND(AVG(lbs_changed), 1) AS avg_lbs_changed
# MAGIC FROM gold_client_progress
# MAGIC GROUP BY program
# MAGIC ORDER BY avg_checkin_rate DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 💪 Your turn — extend it (this is what makes it YOURS in an interview)
# MAGIC 1. ✅ Spark SQL program comparison (done above)
# MAGIC 2. Add a `protein_per_lb` column to `gold_client_progress` (avg protein ÷ current weight).
# MAGIC 3. ✅ Databricks Dashboard on the gold tables (done)
# MAGIC 4. Schedule this notebook as a Lakeflow Job that runs weekly.
# MAGIC 5. Bonus: rewrite the Silver step as Lakeflow Declarative Pipeline with data-quality expectations.

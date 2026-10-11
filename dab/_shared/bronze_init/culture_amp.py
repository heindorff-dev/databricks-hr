# Databricks notebook source
# Culture Amp (engagement) bronze generator: long key-value survey responses, all values as strings.
from pyspark.sql import functions as F

import common as c

dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

SURVEYS = [
    ("sv-2023-q1", "Engagement Pulse Q1 2023", "pulse", "2023-03-01", "2023-03-15"),
    ("sv-2023-q3", "Engagement Survey 2023", "annual", "2023-09-11", "2023-09-29"),
    ("sv-2024-q1", "Engagement Pulse Q1 2024", "pulse", "2024-03-04", "2024-03-18"),
    ("sv-2024-q3", "Engagement Survey 2024", "annual", "2024-09-09", "2024-09-27"),
    ("sv-2025-q1", "Engagement Pulse Q1 2025", "pulse", "2025-03-03", "2025-03-17"),
    ("sv-2025-q3", "Engagement Survey 2025", "annual", "2025-09-08", "2025-09-26"),
    ("sv-2026-q1", "Engagement Pulse Q1 2026", "pulse", "2026-03-02", "2026-03-16"),
    ("sv-2026-q3", "Engagement Survey 2026", "annual", "2026-09-07", "2026-09-25"),
]
QUESTIONS = [
    ("q01", "I would recommend this company as a great place to work", "enps", "scale_0_10"),
    ("q02", "I am proud to work here", "engagement", "likert_5"),
    ("q03", "My manager supports my development", "management", "likert_5"),
    ("q04", "I have the tools and resources to do my job well", "enablement", "likert_5"),
    ("q05", "I see a future for myself here", "retention", "likert_5"),
    ("q06", "Leadership communicates a clear direction", "leadership", "likert_5"),
    ("q07", "I feel included and respected at work", "inclusion", "likert_5"),
    ("q08", "My workload is manageable", "wellbeing", "likert_5"),
    ("q09", "What one thing would improve your experience?", "open", "free_text"),
]
COMMENTS = ["More flexibility please", "Better tooling", "Clearer career paths", "Great team", "Too many meetings", "Improve onboarding"]

# COMMAND ----------

surveys = spark.createDataFrame(SURVEYS, "survey_id string, survey_name string, survey_type string, open_date string, close_date string")
surveys = surveys.withColumn("anonymous", F.lit("true"))
c.write_bronze(spark, catalog, schema, "01_surveys", surveys, "Culture Amp survey definitions. Dates are ISO strings, flags are string booleans.")

questions = spark.createDataFrame(QUESTIONS, "question_id string, question_text string, theme string, scale string")
c.write_bronze(spark, catalog, schema, "01_questions", questions, "Culture Amp question library with theme and response scale.")

# COMMAND ----------

emp = c.employees(spark)
sv = spark.createDataFrame([(s[0], s[3], s[4]) for s in SURVEYS], "survey_id string, open_date string, close_date string")
qs = spark.createDataFrame([(q[0], q[3]) for q in QUESTIONS], "question_id string, scale string")

participants = (
    emp.crossJoin(sv)
    .filter((F.col("hire_date") < F.col("open_date").cast("date")) & (F.col("term_date").isNull() | (F.col("term_date") > F.col("open_date").cast("date"))))
    .withColumn("pkey", F.concat_ws("|", F.col("n").cast("string"), F.col("survey_id")))
    .filter(c.rfrac(F.col("pkey"), "part") < 0.82)
)

base = c.rfrac(F.col("n"), "mood") * 2 + 5.5  # per-person disposition, 5.5-7.5
resp = (
    participants.crossJoin(qs)
    .withColumn("rkey", F.concat_ws("|", F.col("pkey"), F.col("question_id")))
    .withColumn("raw", F.least(F.lit(10.0), F.greatest(F.lit(0.0), base + (c.rfrac(F.col("rkey"), "v") - 0.4) * 5)))
    .select(
        F.md5(F.col("rkey")).alias("response_id"),
        "survey_id",
        "question_id",
        F.md5(F.concat(F.lit("resp"), F.col("n").cast("string"))).alias("respondent_ref"),
        F.concat(F.lit("E"), F.lpad(F.col("n").cast("string"), 6, "0")).alias("employee_ref"),
        F.when(F.col("scale") == "scale_0_10", F.round(F.col("raw")).cast("int").cast("string"))
        .when(F.col("scale") == "likert_5", F.round(F.col("raw") / 2 + 0.5).cast("int").cast("string"))
        .otherwise(c.pick(F.col("rkey"), "txt", COMMENTS))
        .alias("response_value"),
        F.date_format(F.date_add(F.col("open_date").cast("date"), c.rint(F.col("rkey"), "d", 0, 12)), "yyyy-MM-dd").alias("submitted_date"),
    )
)
c.write_bronze(spark, catalog, schema, "01_responses", resp, "Culture Amp long-format survey responses: one row per respondent and question. All values stored as strings.")

# COMMAND ----------

enps = (
    participants.withColumn("rkey", F.concat_ws("|", F.col("pkey"), F.lit("q01")))
    .withColumn("score", F.least(F.lit(10), F.greatest(F.lit(0), F.round(base + (c.rfrac(F.col("rkey"), "v") - 0.4) * 5).cast("int"))))
    .select(
        F.md5(F.concat(F.lit("enps"), F.col("pkey"))).alias("record_id"),
        "survey_id",
        F.concat(F.lit("E"), F.lpad(F.col("n").cast("string"), 6, "0")).alias("employee_ref"),
        F.col("score").cast("string").alias("enps_score"),
        F.when(F.col("score") >= 9, F.lit("promoter")).when(F.col("score") >= 7, F.lit("passive")).otherwise(F.lit("detractor")).alias("category"),
    )
)
c.write_bronze(spark, catalog, schema, "01_enps_scores", enps, "Culture Amp derived eNPS scores per respondent with promoter/passive/detractor category.")

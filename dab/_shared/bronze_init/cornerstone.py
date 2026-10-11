# Databricks notebook source
# Cornerstone OnDemand (learning) bronze generator: PascalCase columns, GUID ids, epoch-second timestamps.
from pyspark.sql import functions as F

import common as c

dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

N_COURSE = 120
N_ENROLL = 60000

PROVIDERS = ["Internal Academy", "LinkedIn Learning", "Coursera", "Udemy Business", "SafetyFirst Ltd"]
CATEGORIES = ["Compliance", "Leadership", "Technical", "Safety", "Soft Skills", "Onboarding", "Security"]
CERTS = ["ISO 27001 Awareness", "Functional Safety (IEC 61508)", "GDPR Practitioner", "First Aid", "Lean Six Sigma Green Belt", "Cloud Practitioner", "PMP"]

# COMMAND ----------

k = F.col("id")
courses = spark.range(1, N_COURSE + 1).select(
    c.guid(k, "course").alias("CourseId"),
    F.concat(F.lit("Course "), F.lpad(k.cast("string"), 3, "0"), F.lit(" - "), c.pick(k, "cat", CATEGORIES)).alias("Title"),
    c.pick(k, "cat", CATEGORIES).alias("Category"),
    c.pick(k, "prov", PROVIDERS).alias("Provider"),
    (c.rint(k, "dur", 1, 16) * 30).cast("int").alias("DurationMinutes"),
    F.when(c.pick(k, "cat", CATEGORIES).isin("Compliance", "Safety", "Security"), F.lit(True)).otherwise(F.lit(False)).alias("IsMandatory"),
    F.when(c.rfrac(k, "act") < 0.92, F.lit("Active")).otherwise(F.lit("Retired")).alias("Status"),
    F.unix_timestamp(c.rdate(k, "cr", "2019-01-01", "2024-12-31").cast("timestamp")).alias("CreatedEpoch"),
)
# Re-published course versions, same CourseId, later modification timestamp
dups = courses.filter(F.pmod(F.hash("CourseId"), F.lit(10)) == 0).withColumn("CreatedEpoch", F.col("CreatedEpoch") + 86400)
courses_all = courses.unionByName(dups)
c.write_bronze(spark, catalog, schema, "01_courses", courses_all, "Cornerstone OnDemand course catalog extract. Some courses are re-published and appear more than once.")

# COMMAND ----------

emp = c.employees(spark)
course_ids = F.array(*[c.guid(F.lit(i), "course") for i in range(1, N_COURSE + 1)])
k = F.col("id")
start = c.rts(k, "st", "2023-01-01", "2026-09-15")
s = c.rfrac(k, "stat")
dur_days = c.rint(k, "dd", 0, 30)
enroll = (
    spark.range(1, N_ENROLL + 1)
    .withColumn("n", c.rint(k, "emp", 1, c.N_EMP))
    .select(
        c.guid(k, "enr").alias("EnrollmentId"),
        F.concat(F.lit("E"), F.lpad(F.col("n").cast("string"), 6, "0")).alias("UserExternalId"),
        F.element_at(course_ids, c.rint(k, "crs", 1, N_COURSE)).alias("CourseId"),
        F.unix_timestamp(start).alias("StartEpoch"),
        F.when(s < 0.78, F.unix_timestamp(start) + dur_days * 86400).alias("CompletionEpoch"),
        F.when(s < 0.78, F.lit("Completed")).when(s < 0.9, F.lit("InProgress")).when(s < 0.96, F.lit("NotStarted")).otherwise(F.lit("Cancelled")).alias("Status"),
        F.when(s < 0.78, c.rint(k, "sc", 55, 100)).alias("Score"),
        F.when(s < 0.78, (c.rint(k, "hr", 1, 16) * 30)).otherwise(F.lit(0)).alias("TimeSpentMinutes"),
    )
)
c.write_bronze(spark, catalog, schema, "01_enrollments", enroll, "Cornerstone OnDemand training transcript: one row per user and course enrollment. Timestamps are epoch seconds.")

# COMMAND ----------

k = F.col("id")
earned = c.rts(k, "er", "2022-01-01", "2026-08-31")
cert = spark.range(1, 12001).select(
    c.guid(k, "cert").alias("CertificationRecordId"),
    F.concat(F.lit("E"), F.lpad(c.rint(k, "emp", 1, c.N_EMP).cast("string"), 6, "0")).alias("UserExternalId"),
    c.pick(k, "cn", CERTS).alias("CertificationName"),
    F.unix_timestamp(earned).alias("EarnedEpoch"),
    F.unix_timestamp(F.add_months(earned.cast("date"), c.rint(k, "val", 12, 36)).cast("timestamp")).alias("ExpiresEpoch"),
    F.when(c.rfrac(k, "rv") < 0.04, F.lit("Revoked")).otherwise(F.lit("Valid")).alias("Status"),
)
c.write_bronze(spark, catalog, schema, "01_certifications", cert, "Cornerstone OnDemand employee certification records. Timestamps are epoch seconds.")

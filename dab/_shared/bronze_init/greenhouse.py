# Databricks notebook source
# Greenhouse (recruiting / ATS) bronze generator: snake_case, ISO-8601 strings, nested structs and arrays.
from pyspark.sql import functions as F

import common as c

dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

N_JOBS, N_CANDIDATES, N_APPLICATIONS, N_OFFERS = 300, 8000, 15000, 3000
STAGES = ["Application Review", "Phone Screen", "Technical Interview", "Onsite Interview", "Offer"]
SOURCES = ["LinkedIn", "Employee Referral", "Company Website", "Indeed", "Agency", "University Event", "Glassdoor"]
REJECTIONS = ["Lacking skills", "Salary expectations", "Culture fit", "Withdrew", "Position filled"]
COMPANIES = ["Acme Robotics", "Nordic Systems", "BlueWave", "Contoso", "Initech", "Globex", "Umbrella Tech", "Freelance"]

# COMMAND ----------

k = F.col("id")
jobs = (
    spark.range(1, N_JOBS + 1)
    .withColumn("opened", c.rts(k, "jo", "2023-01-01", "2026-08-31"))
    .withColumn("is_closed", c.rfrac(k, "jc") < 0.7)
    .select(
        (F.lit(4000000) + k).alias("id"),
        F.element_at(F.array(*[F.lit(j[0]) for j in c.JOB_CODES]), c.rint(k, "jn", 1, c.N_JOB)).alias("name"),
        F.format_string("REQ-%d-%04d", F.year("opened"), k.cast("int")).alias("requisition_id"),
        F.when(F.col("is_closed"), F.lit("closed")).otherwise(c.pick(k, "js", ["open", "open", "draft"])).alias("status"),
        c.iso(F.col("opened")).alias("opened_at"),
        F.when(F.col("is_closed"), c.iso(F.col("opened") + F.expr("make_interval(0, 0, 0, 45)"))).alias("closed_at"),
        F.array(
            F.struct(
                (F.lit(7000) + c.rint(k, "dp", 1, c.N_DEPT)).cast("long").alias("id"),
                c.pick(k, "dn", [d[0] for d in c.DEPARTMENTS]).alias("name"),
            )
        ).alias("departments"),
        F.array(
            F.struct(
                (F.lit(9000) + c.rint(k, "of", 1, c.N_LOC)).cast("long").alias("id"),
                c.pick(k, "on", [l[0] for l in c.LOCATIONS]).alias("name"),
            )
        ).alias("offices"),
        F.struct(
            c.pick(k, "ct", ["full_time", "full_time", "part_time", "contract"]).alias("employment_type"),
            (c.rint(k, "smin", 40, 80) * 1000).cast("double").alias("salary_range_min"),
            (c.rint(k, "smax", 85, 150) * 1000).cast("double").alias("salary_range_max"),
        ).alias("custom_fields"),
    )
)
c.write_bronze(spark, catalog, schema, "01_jobs", jobs, "Greenhouse Harvest API /jobs extract: job requisitions with nested departments, offices and custom fields.")

# COMMAND ----------

def build_candidates(df, activity_shift_days):
    created = c.rts(k, "cc", "2023-01-01", "2026-08-31")
    return df.select(
        (F.lit(100000000) + k).alias("id"),
        c.pick(k, "fn", c.FIRST_NAMES).alias("first_name"),
        c.pick(k, "ln", c.LAST_NAMES).alias("last_name"),
        c.pick(k, "co", COMPANIES).alias("company"),
        F.element_at(F.array(*[F.lit(j[0]) for j in c.JOB_CODES]), c.rint(k, "ti", 1, c.N_JOB)).alias("title"),
        c.iso(created).alias("created_at"),
        c.iso(created + F.expr(f"make_interval(0, 0, 0, {10 + activity_shift_days})")).alias("last_activity"),
        F.array(
            F.struct(
                F.concat(F.lower(c.pick(k, "fn", c.FIRST_NAMES)), F.lit("."), k.cast("string"), F.lit("@mail.example.com")).alias("value"),
                F.lit("personal").alias("type"),
            )
        ).alias("email_addresses"),
        F.when(c.rfrac(k, "tg") > 0.5, F.array(c.pick(k, "t1", ["python", "embedded", "sales", "finance"]), F.lit("referred")))
        .otherwise(F.array(c.pick(k, "t1", ["python", "embedded", "sales", "finance"]))).alias("tags"),
        (c.rfrac(k, "pv") < 0.05).alias("is_private"),
    )


base = spark.range(1, N_CANDIDATES + 1)
candidates = build_candidates(base, 0).unionByName(build_candidates(base.filter(c.rfrac(k, "dup") < 0.02), 40))
c.write_bronze(spark, catalog, schema, "01_candidates", candidates, "Greenhouse Harvest API /candidates extract. Contains re-sent duplicates with later last_activity.")

# COMMAND ----------

r = c.rfrac(k, "st")
applied = c.rts(k, "ap", "2023-01-01", "2026-08-31")
applications = spark.range(1, N_APPLICATIONS + 1).select(
    (F.lit(500000000) + k).alias("id"),
    (F.lit(100000000) + c.rint(k, "cd", 1, N_CANDIDATES)).cast("long").alias("candidate_id"),
    (F.lit(4000000) + c.rint(k, "jb", 1, N_JOBS)).cast("long").alias("job_id"),
    c.iso(applied).alias("applied_at"),
    F.when(r < 0.12, F.lit("hired")).when(r < 0.6, F.lit("rejected")).otherwise(F.lit("active")).alias("status"),
    F.struct(
        c.rint(k, "sg", 1, len(STAGES)).cast("long").alias("id"),
        c.pick(k, "sg", STAGES).alias("name"),
    ).alias("current_stage"),
    F.struct(
        c.rint(k, "so", 1, len(SOURCES)).cast("long").alias("id"),
        c.pick(k, "so", SOURCES).alias("public_name"),
    ).alias("source"),
    F.when(
        r.between(0.12, 0.6),
        F.struct(c.rint(k, "rr", 1, len(REJECTIONS)).cast("long").alias("id"), c.pick(k, "rr", REJECTIONS).alias("name")),
    ).alias("rejection_reason"),
    F.when(r.between(0.12, 0.6), c.iso(applied + F.expr("make_interval(0, 0, 0, 14)"))).alias("rejected_at"),
    c.iso(applied + F.expr("make_interval(0, 0, 0, 20)")).alias("last_activity_at"),
)
c.write_bronze(spark, catalog, schema, "01_applications", applications, "Greenhouse Harvest API /applications extract with nested stage, source and rejection reason.")

# COMMAND ----------

created = c.rts(k, "oc", "2023-02-01", "2026-08-31")
o = c.rfrac(k, "os")
offers = spark.range(1, N_OFFERS + 1).select(
    (F.lit(800000000) + k).alias("id"),
    (F.lit(500000000) + k * 5).alias("application_id"),
    c.rint(k, "ov", 1, 2).alias("version"),
    F.when(o < 0.72, F.lit("accepted")).when(o < 0.85, F.lit("rejected")).when(o < 0.93, F.lit("deprecated")).otherwise(F.lit("unresolved")).alias("offer_status"),
    c.iso(created).alias("created_at"),
    c.iso(created + F.expr("make_interval(0, 0, 0, 1)")).alias("sent_at"),
    F.when(o < 0.93, c.iso(created + F.expr("make_interval(0, 0, 0, 6)"))).alias("resolved_at"),
    F.date_format(F.date_add(created.cast("date"), 45), "yyyy-MM-dd").alias("starts_at"),
    F.struct(
        (c.rint(k, "sal", 45, 140) * 1000).cast("double").alias("salary"),
        c.pick(k, "cur", ["EUR", "SEK", "USD", "GBP"]).alias("currency"),
    ).alias("custom_fields"),
)
c.write_bronze(spark, catalog, schema, "01_offers", offers, "Greenhouse Harvest API /offers extract: offer versions and resolution status.")

# Databricks notebook source
# SuccessFactors (core HR) bronze generator: OData-style camelCase, "/Date(ms)/" strings, Y/N flags.
from pyspark.sql import functions as F

import common as c

dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

SENTINEL = F.lit("/Date(253402214400000)/")  # SuccessFactors uses 9999-12-31 for "open ended"
FX = {"SEK": 11.2, "EUR": 1.0, "GBP": 0.86, "USD": 1.08}

emp = c.employees(spark)
n = F.col("n")

# COMMAND ----------

# per_person: ~3% of persons are re-sent later with a changed last name
def build_person(df, modified_offset, surname_suffix):
    return df.select(
        c.emp_code(n).alias("personIdExternal"),
        F.col("first_name").alias("firstName"),
        F.concat(F.col("last_name"), F.lit(surname_suffix)).alias("lastName"),
        c.odata_date(F.col("birth_date")).alias("dateOfBirth"),
        F.col("gender"),
        F.concat(
            F.lower(F.concat_ws(".", F.col("first_name"), F.col("last_name"))), n.cast("string"), F.lit("@example-corp.com")
        ).alias("emailAddress"),
        F.when(F.col("term_date").isNull(), F.lit("Y")).otherwise(F.lit("N")).alias("isActive"),
        c.odata_date(F.date_add(F.col("hire_date"), F.lit(modified_offset))).alias("lastModifiedDateTime"),
    )


person = build_person(emp, 0, "").unionByName(
    build_person(emp.filter(c.rfrac(n, "dup") < 0.03), 30, "-Lindqvist")
)
c.write_bronze(spark, catalog, schema, "01_per_person", person, "SuccessFactors Person (PerPerson + PerPersonal) raw OData extract. Contains re-sent duplicates.")

# COMMAND ----------

employment = emp.select(
    F.concat(F.lit("u"), n.cast("string")).alias("userId"),
    c.emp_code(n).alias("personIdExternal"),
    c.odata_date(F.col("hire_date")).alias("startDate"),
    F.coalesce(c.odata_date(F.col("term_date")), SENTINEL).alias("endDate"),
    c.odata_date(F.col("hire_date")).alias("originalStartDate"),
    F.when(c.rfrac(n, "rh") < 0.05, F.lit("HIRREH")).otherwise(F.lit("HIRNEW")).alias("eventReason"),
    F.lit("Y").alias("isPrimary"),
    F.when(F.col("term_date").isNotNull(), c.pick(n, "tr", ["RESIGN", "RETIRE", "ENDCON", "DISMIS"])).alias("terminationReason"),
    c.odata_date(F.coalesce(F.col("term_date"), F.col("hire_date"))).alias("lastModifiedOn"),
)
c.write_bronze(spark, catalog, schema, "01_emp_employment", employment, "SuccessFactors EmpEmployment raw OData extract: one row per employment.")

# COMMAND ----------

def build_job(df, seq, start, end, job_idx, reason):
    return df.select(
        F.concat(F.lit("u"), n.cast("string")).alias("userId"),
        F.lit(seq).cast("int").alias("seqNumber") if isinstance(seq, int) else seq.cast("int").alias("seqNumber"),
        c.odata_date(start).alias("startDate"),
        F.coalesce(c.odata_date(end), SENTINEL).alias("endDate"),
        F.format_string("JC%03d", job_idx).alias("jobCode"),
        F.format_string("DEP%04d", F.col("dept_idx")).alias("department"),
        F.format_string("LOC%02d", F.col("loc_idx")).alias("location"),
        F.when(F.col("manager_n").isNotNull(), c.emp_code(F.col("manager_n"))).alias("managerId"),
        F.col("emp_type").alias("employmentType"),
        F.when(F.col("emp_type") == "PT", F.lit("0.6")).otherwise(F.lit("1.0")).alias("fte"),
        F.format_string("POS-%05d", F.pmod(n * 7 + job_idx, F.lit(3000))).alias("position"),
        F.lit(reason).alias("eventReason"),
        c.lookup(c.DEPARTMENTS, "dept_idx", 1).alias("costCenter"),
        c.odata_date(start).alias("lastModifiedOn"),
    )


current_job = build_job(
    emp,
    F.when(F.col("promo_date").isNotNull(), F.lit(2)).otherwise(F.lit(1)),
    F.coalesce(F.col("promo_date"), F.col("hire_date")),
    F.col("term_date"),
    F.col("job_idx"),
    "DATACHG",
).withColumn("eventReason", F.when(F.col("seqNumber") == 2, F.lit("PROMO")).otherwise(F.lit("HIRNEW")))

prior = emp.filter(F.col("promo_date").isNotNull())
prior_job = build_job(prior, 1, F.col("hire_date"), F.date_sub(F.col("promo_date"), 1), F.col("job_idx") - 1, "HIRNEW")

c.write_bronze(spark, catalog, schema, "01_emp_job", current_job.unionByName(prior_job), "SuccessFactors EmpJob raw OData extract: job assignment history per user (seqNumber).")

# COMMAND ----------

def build_comp(df, job_idx, start):
    midpoint = c.lookup(c.JOB_CODES, "job_idx_tmp", 3)
    fx = F.element_at(F.create_map(*[x for k, v in FX.items() for x in (F.lit(k), F.lit(v))]), c.lookup(c.LOCATIONS, "loc_idx", 5))
    amount = F.round(midpoint * (F.lit(0.85) + c.rfrac(n, "sal") * F.lit(0.35)) * fx, 2)
    return df.withColumn("job_idx_tmp", job_idx).select(
        F.concat(F.lit("u"), n.cast("string")).alias("userId"),
        c.odata_date(start).alias("startDate"),
        F.lit("SALARY").alias("payComponent"),
        F.format_string("%.2f", amount).alias("paycompvalue"),
        c.lookup(c.LOCATIONS, "loc_idx", 5).alias("currencyCode"),
        F.lit("ANNUAL").alias("frequency"),
        F.format_string("GR-%d", c.lookup(c.JOB_CODES, "job_idx_tmp", 2)).alias("payGrade"),
        c.odata_date(start).alias("lastModifiedOn"),
    )


comp = build_comp(emp, F.col("job_idx"), F.coalesce(F.col("promo_date"), F.col("hire_date"))).unionByName(
    build_comp(prior, F.col("job_idx") - 1, F.col("hire_date"))
)
c.write_bronze(spark, catalog, schema, "01_emp_compensation", comp, "SuccessFactors EmpPayCompRecurring raw OData extract: recurring salary per user in local currency.")

# COMMAND ----------

dept_rows = [
    (f"DEP{i + 1:04d}", d[0], f"{d[0]} department", "A", d[1], "/Date(1388534400000)/", "/Date(253402214400000)/")
    for i, d in enumerate(c.DEPARTMENTS)
]
dept = spark.createDataFrame(
    dept_rows, "externalCode string, name string, description string, status string, cust_costCenter string, startDate string, endDate string"
)
c.write_bronze(spark, catalog, schema, "01_fo_department", dept, "SuccessFactors FODepartment raw OData extract: department foundation objects.")

loc_rows = [
    (f"LOC{i + 1:02d}", l[0], l[1], l[3], l[4], "A", "/Date(1388534400000)/")
    for i, l in enumerate(c.LOCATIONS)
]
loc = spark.createDataFrame(
    loc_rows, "externalCode string, name string, custCity string, countryCode string, timezone string, status string, startDate string"
)
c.write_bronze(spark, catalog, schema, "01_fo_location", loc, "SuccessFactors FOLocation raw OData extract: office and site locations.")

job_rows = [
    (f"JC{i + 1:03d}", j[0], j[1], f"GR-{j[2]}", str(j[3]), "A")
    for i, j in enumerate(c.JOB_CODES)
]
jobcode = spark.createDataFrame(
    job_rows, "externalCode string, name string, jobFamily string, payGrade string, cust_salaryMidpointEur string, status string"
)
c.write_bronze(spark, catalog, schema, "01_fo_job_code", jobcode, "SuccessFactors FOJobCode raw OData extract: job catalog with pay grade and EUR salary midpoint.")

# Databricks notebook source
# Kronos (time and absence) bronze generator: abbreviated column names, US-style date strings, tinyint flags.
from pyspark.sql import functions as F

import common as c

dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

N_LEAVE = 15000

emp = c.employees(spark)
n = F.col("n")
country = c.lookup(c.LOCATIONS, "loc_idx", 2)

# COMMAND ----------

weeks = (
    spark.range(0, 197)
    .select(F.date_add(F.lit("2023-01-02").cast("date"), (F.col("id") * 7).cast("int")).alias("wk_start"))
    .filter(F.col("wk_start") <= F.lit(c.REF_DATE).cast("date"))
    .withColumn("wk_end", F.date_add("wk_start", 6))
)

key = n * 1000 + F.dayofyear("wk_start") + F.year("wk_start")
pto = F.when(c.rfrac(key, "pto") < 0.1, F.lit(8.0)).otherwise(F.lit(0.0))
base_hrs = F.when(F.col("emp_type") == "PT", F.lit(24.0)).otherwise(F.lit(40.0))

timecards = (
    emp.filter(F.pmod(n, F.lit(5)) < 3)  # hourly population only
    .crossJoin(weeks)
    .filter((F.col("hire_date") <= F.col("wk_end")) & (F.col("term_date").isNull() | (F.col("term_date") >= F.col("wk_start"))))
    .select(
        (n * 1000 + F.datediff("wk_start", F.lit("2023-01-02").cast("date")) / 7).cast("long").alias("tc_id"),
        F.lpad(n.cast("string"), 6, "0").alias("person_num"),
        F.date_format("wk_start", "MM/dd/yyyy").alias("wk_start_dt"),
        (base_hrs - pto).alias("reg_hrs"),
        F.when(c.rfrac(key, "ot") < 0.25, c.rint(key, "oth", 1, 10).cast("double")).otherwise(F.lit(0.0)).alias("ot_hrs"),
        pto.alias("pto_hrs"),
        F.when(country == "GB", F.lit("+00:00")).when(country == "US", F.lit("-05:00")).otherwise(F.lit("+01:00")).alias("tz_off"),
        F.when(c.rfrac(key, "ap") < 0.97, F.lit(1)).otherwise(F.lit(0)).cast("tinyint").alias("approved_flg"),
        F.date_format(F.date_add("wk_end", 2).cast("timestamp"), "MM/dd/yyyy HH:mm:ss").alias("last_upd_ts"),
    )
)
c.write_bronze(spark, catalog, schema, "01_timecards", timecards, "Kronos Workforce Central weekly timecard totals for hourly staff. Dates are US formatted strings.")

# COMMAND ----------

k = F.col("id")
frm = c.rdate(k, "fr", "2023-01-01", "2026-09-15")
days = c.rint(k, "dy", 1, 10)
s = c.rfrac(k, "sc")
leave = spark.range(1, N_LEAVE + 1).select(
    k.alias("req_id"),
    F.lpad(c.rint(k, "pn", 1, c.N_EMP).cast("string"), 6, "0").alias("person_num"),
    c.rint(k, "ty", 1, 5).alias("abs_type_id"),
    F.date_format(frm, "MM/dd/yyyy").alias("from_dt"),
    F.date_format(F.date_add(frm, days - 1), "MM/dd/yyyy").alias("to_dt"),
    days.cast("decimal(5,2)").alias("days_req"),
    F.when(s < 0.8, F.lit("APR")).when(s < 0.9, F.lit("REJ")).when(s < 0.95, F.lit("CXL")).otherwise(F.lit("PND")).alias("status_cd"),
    F.date_sub(frm, 14).cast("timestamp").alias("submitted_ts"),
    F.lpad(c.rint(k, "ap", 1, 60).cast("string"), 6, "0").alias("approver_num"),
)
c.write_bronze(spark, catalog, schema, "01_leave_requests", leave, "Kronos leave request extract. Dates are US formatted strings; person numbers are zero padded.")

# COMMAND ----------

abs_rows = [
    (1, "VAC", "Vacation", 1, 1),
    (2, "SICK", "Sick Leave", 1, 0),
    (3, "PAR", "Parental Leave", 1, 0),
    (4, "UNP", "Unpaid Leave", 0, 0),
    (5, "CMP", "Compensatory Time Off", 1, 1),
]
absence_types = spark.createDataFrame(abs_rows, "abs_type_id int, abs_cd string, abs_nm string, paid_flg tinyint, accrual_flg tinyint")
c.write_bronze(spark, catalog, schema, "01_absence_types", absence_types, "Kronos absence type reference list.")

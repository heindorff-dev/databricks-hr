# Databricks notebook source
# ADP (payroll) bronze generator: UPPER_SNAKE_CASE, amounts as strings, dates as YYYYMMDD integers.
from pyspark.sql import functions as F

import common as c

dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

FX = {"SEK": 11.2, "EUR": 1.0, "GBP": 0.86, "USD": 1.08}


def yyyymmdd(col):
    return F.date_format(col, "yyyyMMdd").cast("int")


emp = c.employees(spark)
n = F.col("n")

# COMMAND ----------

fx = F.element_at(F.create_map(*[x for k, v in FX.items() for x in (F.lit(k), F.lit(v))]), c.lookup(c.LOCATIONS, "loc_idx", 5))
monthly_base = (
    c.lookup(c.JOB_CODES, "job_idx", 3) * (F.lit(0.85) + c.rfrac(n, "sal") * F.lit(0.35)) * fx / F.lit(12.0)
)

stmts = (
    emp.crossJoin(c.months(spark))
    .filter((F.col("hire_date") <= F.col("month_end")) & (F.col("term_date").isNull() | (F.col("term_date") >= F.col("month_start"))))
    .withColumn("country", c.lookup(c.LOCATIONS, "loc_idx", 2))
    .withColumn("ym", F.date_format("month_start", "yyyyMM"))
    .withColumn("gross", monthly_base * (F.lit(0.98) + c.rfrac(n + F.month("month_start"), "gm") * F.lit(0.1)))
    .withColumn("tax", F.col("gross") * (F.lit(0.18) + c.rfrac(n, "tx") * F.lit(0.14)))
    .withColumn("ded", F.col("gross") * F.lit(0.04))
)

pay_statements = stmts.select(
    F.concat(F.lit("S"), n.cast("string"), F.col("ym")).alias("STMT_ID"),
    F.concat(F.lit("PR"), F.col("ym"), F.lit("-"), F.col("country")).alias("PAY_RUN_ID"),
    n.cast("int").alias("ASSOC_FILE_NBR"),
    F.concat(F.lit("G3"), F.upper(F.substring(F.md5(n.cast("string")), 1, 10))).alias("WORKER_ID"),
    F.format_string("%.2f", F.col("gross")).alias("GROSS_PAY_AMT"),
    F.format_string("%.2f", F.col("gross") - F.col("tax") - F.col("ded")).alias("NET_PAY_AMT"),
    F.format_string("%.2f", F.col("tax")).alias("TOTAL_TAX_AMT"),
    F.format_string("%.2f", F.col("ded")).alias("TOTAL_DED_AMT"),
    c.lookup(c.LOCATIONS, "loc_idx", 5).alias("CURR_CD"),
    F.when(F.col("emp_type") == "PT", F.lit("96.00")).otherwise(F.lit("160.00")).alias("HOURS_WORKED"),
    yyyymmdd(F.col("month_end")).alias("PAY_DT"),
)
c.write_bronze(spark, catalog, schema, "01_pay_statements", pay_statements, "ADP Workforce Now pay statement extract: one row per associate per pay run. Amounts are strings in local currency.")

# COMMAND ----------

pay_runs = (
    stmts.groupBy("ym", "country", "month_start", "month_end")
    .agg(F.sum("gross").alias("gross"))
    .select(
        F.concat(F.lit("PR"), F.col("ym"), F.lit("-"), F.col("country")).alias("PAY_RUN_ID"),
        F.concat(F.col("country"), F.lit("-M")).alias("PAY_GROUP_CD"),
        yyyymmdd(F.col("month_start")).alias("PERIOD_START_DT"),
        yyyymmdd(F.col("month_end")).alias("PERIOD_END_DT"),
        yyyymmdd(F.col("month_end")).alias("PAY_DT"),
        F.lit("C").alias("RUN_STATUS_CD"),
        F.format_string("%.2f", F.col("gross")).alias("TOTAL_GROSS_AMT"),
        F.date_format(F.col("month_end").cast("timestamp"), "yyyy-MM-dd HH:mm:ss").alias("CREATED_TS"),
    )
)
c.write_bronze(spark, catalog, schema, "01_pay_runs", pay_runs, "ADP Workforce Now pay run header extract per pay group and month.")

# COMMAND ----------

earning_rows = [
    ("REG", "Regular Salary", "R", "Y", "Y"),
    ("OVT", "Overtime", "O", "Y", "Y"),
    ("BON", "Annual Bonus", "B", "Y", "Y"),
    ("COM", "Sales Commission", "B", "Y", "Y"),
    ("PTO", "Paid Time Off", "R", "Y", "Y"),
    ("SCK", "Sick Pay", "R", "Y", "Y"),
    ("CAR", "Car Allowance", "A", "Y", "Y"),
    ("PHN", "Phone Allowance", "A", "N", "Y"),
    ("OLD", "Legacy Shift Premium", "A", "Y", "N"),
]
earning_codes = spark.createDataFrame(earning_rows, "EARN_CD string, EARN_DESC string, EARN_TYPE_CD string, TAXABLE_FLG string, ACTIVE_FLG string")
c.write_bronze(spark, catalog, schema, "01_earning_codes", earning_codes, "ADP earning code reference list.")

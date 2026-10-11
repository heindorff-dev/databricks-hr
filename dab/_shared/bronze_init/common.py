"""Shared helpers for the HR bronze data generators.

All data is derived deterministically from integer keys using hashes, so every
source system describes the same fictional workforce (employee number ``n``)
while using its own key formats, naming conventions and data types.
"""
from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.column import Column

GOVERNED_TAG = "People and Culture"
N_EMP = 5000
REF_DATE = "2026-09-30"  # "today" for the generated data set
DATA_START = "2023-01-01"

FIRST_NAMES = [
    "Anna", "Lars", "Maria", "Johan", "Sofia", "Erik", "Emma", "Oskar", "Ida", "Nils",
    "Linnea", "Anders", "Elin", "Karl", "Sara", "Mats", "Julia", "Jonas", "Hanna", "Peter",
    "Amira", "Chen", "Priya", "Luca", "Chloe", "Marco", "Ingrid", "Tobias", "Fatima", "Daniel",
]
LAST_NAMES = [
    "Andersson", "Johansson", "Karlsson", "Nilsson", "Eriksson", "Larsson", "Olsson", "Persson",
    "Svensson", "Gustafsson", "Schmidt", "Muller", "Smith", "Jones", "Brown", "de Vries",
    "Jansen", "Rossi", "Dubois", "Nowak", "Khan", "Patel", "Wang", "Garcia", "Hansen",
]
GENDERS = ["M", "F", "X"]

# (name, cost center)
DEPARTMENTS = [
    ("Executive Office", "CC1000"), ("Finance", "CC1100"), ("Accounting", "CC1110"),
    ("People and Culture", "CC1200"), ("Talent Acquisition", "CC1210"), ("IT Operations", "CC1300"),
    ("Information Security", "CC1310"), ("Legal and Compliance", "CC1400"), ("Facilities", "CC1500"),
    ("Sales EMEA", "CC2100"), ("Sales Americas", "CC2200"), ("Sales APAC", "CC2300"),
    ("Marketing", "CC2400"), ("Customer Success", "CC2500"), ("Customer Support", "CC2600"),
    ("Product Management", "CC3100"), ("Software Engineering", "CC3200"), ("Hardware Engineering", "CC3300"),
    ("Firmware", "CC3350"), ("Quality Assurance", "CC3400"), ("Research Lab", "CC3500"),
    ("Manufacturing", "CC4100"), ("Supply Chain", "CC4200"), ("Procurement", "CC4300"),
]
N_DEPT = len(DEPARTMENTS)

# (name, city, country iso2, country iso3, timezone, currency)
LOCATIONS = [
    ("Stockholm HQ", "Stockholm", "SE", "SWE", "Europe/Stockholm", "SEK"),
    ("Gothenburg Site", "Gothenburg", "SE", "SWE", "Europe/Stockholm", "SEK"),
    ("Berlin Office", "Berlin", "DE", "DEU", "Europe/Berlin", "EUR"),
    ("Munich Lab", "Munich", "DE", "DEU", "Europe/Berlin", "EUR"),
    ("Amsterdam Office", "Amsterdam", "NL", "NLD", "Europe/Amsterdam", "EUR"),
    ("London Office", "London", "GB", "GBR", "Europe/London", "GBP"),
    ("Boston Office", "Boston", "US", "USA", "America/New_York", "USD"),
    ("Austin Office", "Austin", "US", "USA", "America/Chicago", "USD"),
    ("Chicago Plant", "Chicago", "US", "USA", "America/Chicago", "USD"),
    ("Remote", "Remote", "SE", "SWE", "Europe/Stockholm", "SEK"),
]
N_LOC = len(LOCATIONS)
COUNTRIES = ["SE", "DE", "NL", "GB", "US"]

# (name, family, grade, annual salary midpoint in EUR)
JOB_CODES = [
    ("Chief Executive Officer", "Executive", 8, 250000), ("VP Engineering", "Executive", 8, 200000),
    ("Director Sales", "Management", 7, 150000), ("Director Finance", "Management", 7, 145000),
    ("Engineering Manager", "Management", 6, 120000), ("Sales Manager", "Management", 6, 105000),
    ("HR Business Partner", "People", 5, 80000), ("Recruiter", "People", 4, 62000),
    ("Payroll Specialist", "People", 4, 58000), ("Staff Software Engineer", "Engineering", 6, 115000),
    ("Senior Software Engineer", "Engineering", 5, 95000), ("Software Engineer", "Engineering", 4, 75000),
    ("Junior Software Engineer", "Engineering", 2, 55000), ("Firmware Engineer", "Engineering", 4, 78000),
    ("Hardware Engineer", "Engineering", 4, 76000), ("Research Scientist", "Research", 5, 92000),
    ("Lab Technician", "Research", 3, 52000), ("QA Engineer", "Engineering", 3, 62000),
    ("Product Manager", "Product", 5, 98000), ("Data Analyst", "Analytics", 4, 68000),
    ("Data Engineer", "Analytics", 5, 90000), ("Accountant", "Finance", 3, 60000),
    ("Financial Controller", "Finance", 6, 110000), ("Account Executive", "Sales", 4, 85000),
    ("Sales Development Rep", "Sales", 2, 54000), ("Marketing Specialist", "Marketing", 3, 58000),
    ("Customer Support Agent", "Support", 2, 46000), ("Customer Success Manager", "Support", 4, 72000),
    ("Production Operator", "Operations", 1, 40000), ("Supply Chain Planner", "Operations", 3, 56000),
]
N_JOB = len(JOB_CODES)


# ---------------------------------------------------------------- randomness
def _h(key: Column, salt: str) -> Column:
    return F.hash(key, F.lit(salt))


def pick(key: Column, salt: str, values) -> Column:
    arr = F.array(*[F.lit(v) for v in values])
    return F.element_at(arr, (F.pmod(_h(key, salt), F.lit(len(values))) + 1).cast("int"))


def rint(key: Column, salt: str, lo: int, hi: int) -> Column:
    return (F.pmod(_h(key, salt), F.lit(hi - lo + 1)) + F.lit(lo)).cast("int")


def rfrac(key: Column, salt: str) -> Column:
    return F.pmod(_h(key, salt), F.lit(1000000)) / F.lit(1000000.0)


def rdate(key: Column, salt: str, start: str, end: str) -> Column:
    days = (date.fromisoformat(end) - date.fromisoformat(start)).days
    return F.date_add(F.lit(start).cast("date"), rint(key, salt, 0, days))


def rts(key: Column, salt: str, start: str, end: str) -> Column:
    """Random timestamp between two dates (second resolution)."""
    d = rdate(key, salt, start, end).cast("timestamp")
    return F.timestamp_seconds(F.unix_timestamp(d) + rint(key, salt + "s", 0, 86399))


def iso(ts: Column) -> Column:
    return F.date_format(ts, "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")


def emp_code(n: Column) -> Column:
    return F.concat(F.lit("E"), F.lpad(n.cast("string"), 6, "0"))


def guid(key: Column, salt: str) -> Column:
    m = F.md5(F.concat(F.lit(salt), key.cast("string")))
    return F.concat_ws("-", m.substr(1, 8), m.substr(9, 4), m.substr(13, 4), m.substr(17, 4), m.substr(21, 12))


def odata_date(date_col: Column) -> Column:
    """SuccessFactors OData date literal, e.g. /Date(1650000000000)/."""
    return F.concat(F.lit("/Date("), (F.unix_timestamp(date_col.cast("timestamp")) * 1000).cast("string"), F.lit(")/"))


# ------------------------------------------------------------------ universe
def employees(spark: SparkSession) -> DataFrame:
    """The fictional workforce shared by every HR source system."""
    n = F.col("n")
    ref = F.lit(REF_DATE).cast("date")
    return (
        spark.range(1, N_EMP + 1).withColumnRenamed("id", "n")
        .withColumn("first_name", pick(n, "fn", FIRST_NAMES))
        .withColumn("last_name", pick(n, "ln", LAST_NAMES))
        .withColumn("gender", pick(n, "gd", GENDERS))
        .withColumn("birth_date", rdate(n, "bd", "1962-01-01", "2003-12-31"))
        .withColumn("hire_date", rdate(n, "hd", "2014-01-01", "2026-06-30"))
        .withColumn("dept_idx", rint(n, "dp", 1, N_DEPT))
        .withColumn("loc_idx", rint(n, "lc", 1, N_LOC))
        .withColumn("job_idx", rint(n, "jb", 1, N_JOB))
        .withColumn("emp_type", pick(n, "et", ["FT", "FT", "FT", "FT", "PT", "CT"]))
        .withColumn("manager_n", F.when(n > 60, rint(n, "mg", 1, 60)))
        .withColumn(
            "term_raw",
            F.when(rfrac(n, "tm") < 0.18, F.date_add(F.col("hire_date"), rint(n, "tl", 120, 1800))),
        )
        .withColumn("term_date", F.when(F.col("term_raw") <= ref, F.col("term_raw")))
        .withColumn(
            "promo_raw",
            F.when(
                (rfrac(n, "pm") < 0.4) & (F.col("job_idx") > 1),
                F.date_add(F.col("hire_date"), rint(n, "pd", 365, 1000)),
            ),
        )
        .withColumn(
            "promo_date",
            F.when(
                (F.col("promo_raw") <= ref)
                & (F.col("term_date").isNull() | (F.col("promo_raw") < F.col("term_date"))),
                F.col("promo_raw"),
            ),
        )
        .drop("term_raw", "promo_raw")
    )


def lookup(values, idx_col: str, pos: int) -> Column:
    """Pick field ``pos`` of the tuple at 1-based ``idx_col`` from a python list of tuples."""
    arr = F.array(*[F.lit(v[pos]) for v in values])
    return F.element_at(arr, F.col(idx_col))


def months(spark: SparkSession) -> DataFrame:
    """One row per calendar month from DATA_START to REF_DATE."""
    return spark.sql(
        f"SELECT explode(sequence(DATE'{DATA_START}', DATE'{REF_DATE}', INTERVAL 1 MONTH)) AS month_start"
    ).withColumn("month_end", F.last_day("month_start"))


# -------------------------------------------------------------------- writes
def write_bronze(spark: SparkSession, catalog: str, schema: str, table: str, df: DataFrame, comment: str) -> None:
    fq = f"`{catalog}`.`{schema}`.`{table}`"
    spark.sql(f"DROP TABLE IF EXISTS {fq}")
    df.withColumn("_ingested_at", F.current_timestamp()).write.format("delta").saveAsTable(
        f"`{catalog}`.`{schema}`.`{table}`"
    )
    safe_comment = comment.replace("'", "''")
    spark.sql(f"COMMENT ON TABLE {fq} IS '{safe_comment}'")
    spark.sql(f"ALTER TABLE {fq} SET TAGS ('{GOVERNED_TAG}')")
    print(f"wrote {fq}: {spark.table(fq).count()} rows")

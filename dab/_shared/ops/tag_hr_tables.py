# Databricks notebook source
# Applies the governed tag to every table, streaming table and materialized view in the given schemas.
GOVERNED_TAG = "People and Culture"

dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schemas", "")
catalog = dbutils.widgets.get("catalog")
schemas = [s.strip() for s in dbutils.widgets.get("schemas").split(",") if s.strip()]

# COMMAND ----------

schema_list = ", ".join(f"'{s}'" for s in schemas)
tables = spark.sql(
    f"""
    SELECT table_schema, table_name, table_type
    FROM `{catalog}`.information_schema.tables
    WHERE table_schema IN ({schema_list})
    """
).collect()

# Pipeline-managed objects may require the object-specific ALTER statement, so fall back to it.
OBJECT_KIND = {"MATERIALIZED_VIEW": "MATERIALIZED VIEW", "STREAMING_TABLE": "STREAMING TABLE"}
failures = []
for t in tables:
    fq = f"`{catalog}`.`{t.table_schema}`.`{t.table_name}`"
    kinds = ["TABLE"]
    if t.table_type in OBJECT_KIND:
        kinds.append(OBJECT_KIND[t.table_type])
    for kind in kinds:
        try:
            spark.sql(f"ALTER {kind} {fq} SET TAGS ('{GOVERNED_TAG}')")
            break
        except Exception as e:  # noqa: BLE001
            last_error = str(e).splitlines()[0]
    else:
        failures.append(f"{fq}: {last_error}")

print(f"tagged {len(tables) - len(failures)} of {len(tables)} objects")

if failures:
    raise RuntimeError(f"{len(failures)} tagging problems:\n" + "\n".join(failures[:100]))

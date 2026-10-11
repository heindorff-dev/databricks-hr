# Databricks notebook source
# Creates the PII mask function and the catalog-level ABAC policy.
# Every column tagged with the governed tag 'pii' is masked for everyone except the exempt group.
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")
dbutils.widgets.text("exempt_group", "grp-hr-sensitive")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
exempt_group = dbutils.widgets.get("exempt_group")
function = f"`{catalog}`.`{schema}`.`mask_pii`"

# COMMAND ----------

# The governed tag must exist before a policy can reference it. Created without allowed values (key-only tag).
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service.tags import TagPolicy

w = WorkspaceClient()
try:
    w.tag_policies.get_tag_policy("pii")
    print("tag policy 'pii' already exists")
except NotFound:
    w.tag_policies.create_tag_policy(
        TagPolicy(tag_key="pii", description="Personal or sensitive data. Columns with this tag are masked by ABAC unless the user is in the exempt group.")
    )
    print("created tag policy 'pii'")

# COMMAND ----------

# A single mask for all PII columns. Returning NULL is valid for every column type (string, decimal, date).
spark.sql(
    f"""
    CREATE OR REPLACE FUNCTION {function}(value STRING)
    RETURNS STRING
    COMMENT 'ABAC column mask for columns tagged pii. Hides the value; the exempt group bypasses it through the policy.'
    RETURN CAST(NULL AS STRING)
    """
)

# COMMAND ----------

spark.sql(
    f"""
    CREATE OR REPLACE POLICY hr_mask_pii
    ON CATALOG `{catalog}`
    COMMENT 'Mask PII columns for everyone except {exempt_group}'
    COLUMN MASK {function}
    TO `account users`
    EXCEPT `{exempt_group}`
    FOR TABLES
    MATCH COLUMNS hasTag('pii') AS pii_col
    ON COLUMN pii_col
    """
)
print(f"policy hr_mask_pii applied on catalog {catalog}")

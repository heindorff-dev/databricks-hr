# HR (People and Culture) – data products

One bundle per gold data product. Each bundle owns a single pipeline (silver streaming tables + gold materialized views) and a single job (bronze → silver → gold).

| Bundle (`dab/…`) | Bronze + silver sources owned | Gold schema |
|---|---|---|
| `infrastructure` | – (schemas, tags) | – |
| `workforce` | `success_factor`, `kronos` | `workforce` |
| `compensation` | `adp` | `compensation` |
| `talent_acquisition` | `greenhouse` | `talent_acquisition` |
| `learning_development` | `cornerstone` | `learning_development` |
| `engagement_retention` | `culture_amp` | `engagement_retention` |

Shared code in `dab/_shared`: schema variables/targets, `bronze_init/` (generators) and `ops/` (table-level tagging).

## Job behaviour

Parameter `init_bronze` (default `false`):
- `true`: regenerate the bronze tables owned by the bundle, then **full refresh** the pipeline (bronze is drop/recreate, which invalidates streaming checkpoints).
- `false`: skip bronze, incremental pipeline refresh.

Silver and gold always run, followed by table and column tagging.

## Deploy order

1. `infrastructure` (schemas, schema tags).
2. `workforce` – owns `success_factor` silver, which other products read.
3. The other bundles, in any order.

First run of every job: `databricks bundle run hr_<product> --params init_bronze=true -t dev`. Run `workforce` first.

## Cross-bundle dependency

`compensation`, `learning_development` and `engagement_retention` read `success_factor` silver tables owned by `workforce`. Refresh `workforce` before them; re-running it with `init_bronze=true` is a full reset of that silver layer.

## PII masking (ABAC)

- Governed tag `pii` (key only) is created as a tag policy by the infrastructure job (`apply_pii_policy`).
- Each bundle owns `<product>/metadata/column_metadata.sql`: plain `SET TAG ON COLUMN` and `COMMENT ON COLUMN` statements for silver and gold PII columns, plus gold column descriptions. It runs as the last job task (`column_metadata`). `tag_hr_tables` only applies the table-level `People and Culture` tag.
- The infrastructure job creates `<governance>.mask_pii` and the catalog policy `hr_mask_pii`: tagged columns are NULL for everyone except `grp-hr-sensitive`.
- The identity that runs the pipelines (your user in dev, the service principal in test/prod) must be in `grp-hr-sensitive`, otherwise silver is read masked and gold gets NULL values.
- To classify a new column, add a `SET TAG ON COLUMN … pii` line to the bundle's `column_metadata.sql`. Bronze is not tagged.

## Not verified

The ABAC policy and tag policy creation on Free Edition, and re-running `column_metadata` (`SET TAG` raises if the tag already exists on the column).

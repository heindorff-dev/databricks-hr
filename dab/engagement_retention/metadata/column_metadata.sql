-- Databricks notebook source
-- Engagement and retention data product: governed column tags and gold column descriptions.
-- Silver column descriptions live in src/silver/*.sql. Gold materialized views only get a table comment there.

USE CATALOG IDENTIFIER(:catalog);

-- COMMAND ----------
-- PII tags on silver

USE SCHEMA IDENTIFIER(:culture_amp_silver_schema);

SET TAG ON COLUMN `02_responses`.textValue pii;

-- COMMAND ----------
-- Gold: descriptions (no PII columns)

USE SCHEMA IDENTIFIER(:engagement_retention_gold_schema);

COMMENT ON COLUMN `03_agg_enps_by_department`.surveyId IS 'Identifier of the survey.';
COMMENT ON COLUMN `03_agg_enps_by_department`.surveyName IS 'Name of the survey.';
COMMENT ON COLUMN `03_agg_enps_by_department`.surveyCloseDate IS 'Date the survey closed.';
COMMENT ON COLUMN `03_agg_enps_by_department`.departmentCode IS 'Department code, references the department dimension.';
COMMENT ON COLUMN `03_agg_enps_by_department`.departmentName IS 'Name of the department.';
COMMENT ON COLUMN `03_agg_enps_by_department`.respondentCount IS 'Number of respondents. Groups below 5 are suppressed.';
COMMENT ON COLUMN `03_agg_enps_by_department`.promoterCount IS 'Respondents scoring 9-10.';
COMMENT ON COLUMN `03_agg_enps_by_department`.passiveCount IS 'Respondents scoring 7-8.';
COMMENT ON COLUMN `03_agg_enps_by_department`.detractorCount IS 'Respondents scoring 0-6.';
COMMENT ON COLUMN `03_agg_enps_by_department`.enpsScore IS 'Employee Net Promoter Score: percent promoters minus percent detractors.';
COMMENT ON COLUMN `03_agg_enps_by_department`.avgScore IS 'Average answer on the 0-10 scale.';

COMMENT ON COLUMN `03_fact_attrition_monthly`.reportingMonthEnd IS 'Last day of the reporting month.';
COMMENT ON COLUMN `03_fact_attrition_monthly`.departmentCode IS 'Department code, references the department dimension.';
COMMENT ON COLUMN `03_fact_attrition_monthly`.departmentName IS 'Name of the department.';
COMMENT ON COLUMN `03_fact_attrition_monthly`.headcountInMonth IS 'Employees employed at any time in the month.';
COMMENT ON COLUMN `03_fact_attrition_monthly`.leavers IS 'Employees whose employment ended in the month.';
COMMENT ON COLUMN `03_fact_attrition_monthly`.voluntaryLeavers IS 'Leavers with reason resignation or retirement.';
COMMENT ON COLUMN `03_fact_attrition_monthly`.involuntaryLeavers IS 'Leavers with any other reason.';
COMMENT ON COLUMN `03_fact_attrition_monthly`.monthlyAttritionRate IS 'Leavers divided by headcount in the month.';

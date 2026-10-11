-- Databricks notebook source
-- Talent acquisition data product: governed column tags and gold column descriptions.
-- Silver column descriptions live in src/silver/*.sql. Gold materialized views only get a table comment there.

USE CATALOG IDENTIFIER(:catalog);

-- COMMAND ----------
-- PII tags on silver

USE SCHEMA IDENTIFIER(:greenhouse_silver_schema);

SET TAG ON COLUMN `02_candidates`.firstName pii;
SET TAG ON COLUMN `02_candidates`.lastName pii;
SET TAG ON COLUMN `02_candidates`.emailAddress pii;
SET TAG ON COLUMN `02_candidates`.currentCompany pii;
SET TAG ON COLUMN `02_applications`.rejectionReason pii;
SET TAG ON COLUMN `02_offers`.offeredSalary pii;

-- COMMAND ----------
-- Gold: descriptions (no PII columns)

USE SCHEMA IDENTIFIER(:talent_acquisition_gold_schema);

COMMENT ON COLUMN `03_fact_recruiting_funnel`.jobId IS 'Identifier of the job requisition.';
COMMENT ON COLUMN `03_fact_recruiting_funnel`.requisitionId IS 'Requisition identifier of the job.';
COMMENT ON COLUMN `03_fact_recruiting_funnel`.jobTitle IS 'Title of the job.';
COMMENT ON COLUMN `03_fact_recruiting_funnel`.departmentName IS 'Name of the department the job belongs to.';
COMMENT ON COLUMN `03_fact_recruiting_funnel`.officeName IS 'Name of the office the job is located in.';
COMMENT ON COLUMN `03_fact_recruiting_funnel`.employmentType IS 'Employment type of the job.';
COMMENT ON COLUMN `03_fact_recruiting_funnel`.jobStatus IS 'Status of the job requisition.';
COMMENT ON COLUMN `03_fact_recruiting_funnel`.openedAt IS 'Timestamp the job was opened.';
COMMENT ON COLUMN `03_fact_recruiting_funnel`.closedAt IS 'Timestamp the job was closed. Null while open.';
COMMENT ON COLUMN `03_fact_recruiting_funnel`.applicationCount IS 'Number of applications.';
COMMENT ON COLUMN `03_fact_recruiting_funnel`.activeCount IS 'Applications currently active.';
COMMENT ON COLUMN `03_fact_recruiting_funnel`.rejectedCount IS 'Number of rejected applications.';
COMMENT ON COLUMN `03_fact_recruiting_funnel`.hiredCount IS 'Number of applications that resulted in a hire.';
COMMENT ON COLUMN `03_fact_recruiting_funnel`.offerCount IS 'Applications with an offer.';
COMMENT ON COLUMN `03_fact_recruiting_funnel`.acceptedOfferCount IS 'Offers that were accepted.';
COMMENT ON COLUMN `03_fact_recruiting_funnel`.avgDaysToOfferResponse IS 'Average days between sending an offer and its resolution.';

COMMENT ON COLUMN `03_agg_source_effectiveness`.applicationQuarter IS 'First day of the quarter the application was made.';
COMMENT ON COLUMN `03_agg_source_effectiveness`.sourceName IS 'Name of the sourcing channel the candidate came from.';
COMMENT ON COLUMN `03_agg_source_effectiveness`.applicationCount IS 'Number of applications.';
COMMENT ON COLUMN `03_agg_source_effectiveness`.hiredCount IS 'Number of applications that resulted in a hire.';
COMMENT ON COLUMN `03_agg_source_effectiveness`.rejectedCount IS 'Number of rejected applications.';
COMMENT ON COLUMN `03_agg_source_effectiveness`.hireRate IS 'Hired applications divided by all applications.';

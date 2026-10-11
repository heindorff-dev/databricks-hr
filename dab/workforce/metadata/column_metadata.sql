-- Databricks notebook source
-- Workforce data product: governed column tags and gold column descriptions.
-- Silver column descriptions live in src/silver/*.sql. Gold materialized views only get a table comment there.

USE CATALOG IDENTIFIER(:catalog);

-- COMMAND ----------
-- PII tags on silver

USE SCHEMA IDENTIFIER(:success_factor_silver_schema);

SET TAG ON COLUMN `02_per_person`.firstName pii;
SET TAG ON COLUMN `02_per_person`.lastName pii;
SET TAG ON COLUMN `02_per_person`.dateOfBirth pii;
SET TAG ON COLUMN `02_per_person`.gender pii;
SET TAG ON COLUMN `02_per_person`.emailAddress pii;
SET TAG ON COLUMN `02_emp_employment`.terminationReasonCode pii;
SET TAG ON COLUMN `02_emp_compensation`.annualAmountLocal pii;

-- COMMAND ----------
-- Gold: descriptions and PII tags

USE SCHEMA IDENTIFIER(:workforce_gold_schema);

COMMENT ON COLUMN `03_dim_employee`.employeeId IS 'Conformed employee identifier.';
COMMENT ON COLUMN `03_dim_employee`.firstName IS 'Given name of the person.';
COMMENT ON COLUMN `03_dim_employee`.lastName IS 'Family name of the person.';
COMMENT ON COLUMN `03_dim_employee`.fullName IS 'First and last name combined.';
COMMENT ON COLUMN `03_dim_employee`.emailAddress IS 'Corporate email address of the person.';
COMMENT ON COLUMN `03_dim_employee`.gender IS 'Gender code as recorded in SuccessFactors (M, F or X).';
COMMENT ON COLUMN `03_dim_employee`.dateOfBirth IS 'Date of birth of the person.';
COMMENT ON COLUMN `03_dim_employee`.ageAtReference IS 'Age in whole years at the reference date 2026-09-30.';
COMMENT ON COLUMN `03_dim_employee`.hireDate IS 'Date the employee was hired.';
COMMENT ON COLUMN `03_dim_employee`.terminationDate IS 'Date the employment ended. Null for active employees.';
COMMENT ON COLUMN `03_dim_employee`.terminationReasonCode IS 'Reason code for the termination (RESIGN, RETIRE, ENDCON, DISMIS). Null when not terminated.';
COMMENT ON COLUMN `03_dim_employee`.isActive IS 'True when the employee has no termination date.';
COMMENT ON COLUMN `03_dim_employee`.jobCode IS 'Job code of the current job.';
COMMENT ON COLUMN `03_dim_employee`.jobTitle IS 'Title of the current job.';
COMMENT ON COLUMN `03_dim_employee`.jobFamily IS 'Job family of the current job.';
COMMENT ON COLUMN `03_dim_employee`.payGrade IS 'Pay grade of the current job.';
COMMENT ON COLUMN `03_dim_employee`.employmentType IS 'Employment type, for example full time or part time.';
COMMENT ON COLUMN `03_dim_employee`.fteRatio IS 'Full-time equivalent ratio of the current job (1.0 = full time).';
COMMENT ON COLUMN `03_dim_employee`.departmentCode IS 'Department code of the current job.';
COMMENT ON COLUMN `03_dim_employee`.departmentName IS 'Name of the department.';
COMMENT ON COLUMN `03_dim_employee`.costCenter IS 'Cost center the employee is charged to.';
COMMENT ON COLUMN `03_dim_employee`.locationCode IS 'Location code of the current job.';
COMMENT ON COLUMN `03_dim_employee`.locationName IS 'Name of the office or site.';
COMMENT ON COLUMN `03_dim_employee`.city IS 'City where the location is situated.';
COMMENT ON COLUMN `03_dim_employee`.countryCode IS 'ISO 3166 alpha-3 country code.';
COMMENT ON COLUMN `03_dim_employee`.managerEmployeeId IS 'Conformed employee identifier of the line manager. Null for top managers.';
COMMENT ON COLUMN `03_dim_employee`.managerName IS 'First and last name of the line manager.';

SET TAG ON COLUMN `03_dim_employee`.firstName pii;
SET TAG ON COLUMN `03_dim_employee`.lastName pii;
SET TAG ON COLUMN `03_dim_employee`.fullName pii;
SET TAG ON COLUMN `03_dim_employee`.emailAddress pii;
SET TAG ON COLUMN `03_dim_employee`.gender pii;
SET TAG ON COLUMN `03_dim_employee`.dateOfBirth pii;
SET TAG ON COLUMN `03_dim_employee`.ageAtReference pii;
SET TAG ON COLUMN `03_dim_employee`.terminationReasonCode pii;
SET TAG ON COLUMN `03_dim_employee`.managerName pii;

COMMENT ON COLUMN `03_dim_department`.departmentCode IS 'Department code.';
COMMENT ON COLUMN `03_dim_department`.departmentName IS 'Name of the department.';
COMMENT ON COLUMN `03_dim_department`.costCenter IS 'Cost center of the department.';
COMMENT ON COLUMN `03_dim_department`.isActive IS 'True when the department is active.';
COMMENT ON COLUMN `03_dim_department`.currentHeadcount IS 'Number of active employees whose current job is in the department.';

COMMENT ON COLUMN `03_fact_headcount_monthly`.reportingMonthEnd IS 'Last day of the reporting month.';
COMMENT ON COLUMN `03_fact_headcount_monthly`.departmentCode IS 'Department code, references the department dimension.';
COMMENT ON COLUMN `03_fact_headcount_monthly`.locationCode IS 'Location code of the job.';
COMMENT ON COLUMN `03_fact_headcount_monthly`.employmentType IS 'Employment type, for example full time or part time.';
COMMENT ON COLUMN `03_fact_headcount_monthly`.headcount IS 'Employees employed at the end of the month.';
COMMENT ON COLUMN `03_fact_headcount_monthly`.fteTotal IS 'Sum of FTE ratios of the employees employed at the end of the month.';
COMMENT ON COLUMN `03_fact_headcount_monthly`.hires IS 'Employees hired in the month.';
COMMENT ON COLUMN `03_fact_headcount_monthly`.terminations IS 'Employees whose employment ended in the month.';

COMMENT ON COLUMN `03_agg_absence_by_department`.absenceMonth IS 'First day of the month the absence started.';
COMMENT ON COLUMN `03_agg_absence_by_department`.departmentCode IS 'Department code, references the department dimension.';
COMMENT ON COLUMN `03_agg_absence_by_department`.absenceName IS 'Name of the absence type.';
COMMENT ON COLUMN `03_agg_absence_by_department`.isPaid IS 'True when the absence type is paid.';
COMMENT ON COLUMN `03_agg_absence_by_department`.requestCount IS 'Number of approved leave requests.';
COMMENT ON COLUMN `03_agg_absence_by_department`.absenceDays IS 'Total approved absence days.';
COMMENT ON COLUMN `03_agg_absence_by_department`.employeesAbsent IS 'Distinct employees with approved absence.';

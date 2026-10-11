-- Databricks notebook source
-- Compensation data product: governed column tags and gold column descriptions.
-- Silver column descriptions live in src/silver/*.sql. Gold materialized views only get a table comment there.

USE CATALOG IDENTIFIER(:catalog);

-- COMMAND ----------
-- PII tags on silver

USE SCHEMA IDENTIFIER(:adp_silver_schema);

SET TAG ON COLUMN `02_pay_statements`.grossPayAmount pii;
SET TAG ON COLUMN `02_pay_statements`.netPayAmount pii;
SET TAG ON COLUMN `02_pay_statements`.totalTaxAmount pii;
SET TAG ON COLUMN `02_pay_statements`.totalDeductionAmount pii;

-- COMMAND ----------
-- Gold: descriptions and PII tags

USE SCHEMA IDENTIFIER(:compensation_gold_schema);

COMMENT ON COLUMN `03_fact_payroll_monthly`.payMonth IS 'First day of the pay month.';
COMMENT ON COLUMN `03_fact_payroll_monthly`.employeeId IS 'Conformed employee identifier.';
COMMENT ON COLUMN `03_fact_payroll_monthly`.departmentCode IS 'Current department code of the employee.';
COMMENT ON COLUMN `03_fact_payroll_monthly`.locationCode IS 'Current location code of the employee.';
COMMENT ON COLUMN `03_fact_payroll_monthly`.currencyCode IS 'ISO 4217 currency code of the local amounts.';
COMMENT ON COLUMN `03_fact_payroll_monthly`.grossPayLocal IS 'Gross pay for the month in local currency.';
COMMENT ON COLUMN `03_fact_payroll_monthly`.netPayLocal IS 'Net pay for the month in local currency.';
COMMENT ON COLUMN `03_fact_payroll_monthly`.grossPayEur IS 'Gross pay for the month converted to EUR.';
COMMENT ON COLUMN `03_fact_payroll_monthly`.netPayEur IS 'Net pay for the month converted to EUR.';
COMMENT ON COLUMN `03_fact_payroll_monthly`.taxEur IS 'Tax withheld for the month converted to EUR.';
COMMENT ON COLUMN `03_fact_payroll_monthly`.deductionsEur IS 'Other deductions for the month converted to EUR.';
COMMENT ON COLUMN `03_fact_payroll_monthly`.hoursWorked IS 'Hours worked in the pay period.';

SET TAG ON COLUMN `03_fact_payroll_monthly`.grossPayLocal pii;
SET TAG ON COLUMN `03_fact_payroll_monthly`.netPayLocal pii;
SET TAG ON COLUMN `03_fact_payroll_monthly`.grossPayEur pii;
SET TAG ON COLUMN `03_fact_payroll_monthly`.netPayEur pii;
SET TAG ON COLUMN `03_fact_payroll_monthly`.taxEur pii;
SET TAG ON COLUMN `03_fact_payroll_monthly`.deductionsEur pii;

COMMENT ON COLUMN `03_agg_labor_cost_by_department`.payMonth IS 'First day of the pay month.';
COMMENT ON COLUMN `03_agg_labor_cost_by_department`.departmentCode IS 'Department code, references the department dimension.';
COMMENT ON COLUMN `03_agg_labor_cost_by_department`.departmentName IS 'Name of the department.';
COMMENT ON COLUMN `03_agg_labor_cost_by_department`.paidEmployees IS 'Distinct employees paid in the month.';
COMMENT ON COLUMN `03_agg_labor_cost_by_department`.grossLaborCostEur IS 'Total gross pay in EUR.';
COMMENT ON COLUMN `03_agg_labor_cost_by_department`.avgGrossPerEmployeeEur IS 'Average gross pay per paid employee in EUR.';

COMMENT ON COLUMN `03_agg_comp_ratio_by_grade`.payGrade IS 'Pay grade.';
COMMENT ON COLUMN `03_agg_comp_ratio_by_grade`.employeeCount IS 'Active employees with a salary in the pay grade.';
COMMENT ON COLUMN `03_agg_comp_ratio_by_grade`.avgSalaryEur IS 'Average annual salary in EUR.';
COMMENT ON COLUMN `03_agg_comp_ratio_by_grade`.avgMidpointEur IS 'Average salary midpoint of the grade in EUR.';
COMMENT ON COLUMN `03_agg_comp_ratio_by_grade`.avgCompaRatio IS 'Average compa-ratio (salary divided by grade midpoint).';
COMMENT ON COLUMN `03_agg_comp_ratio_by_grade`.minCompaRatio IS 'Lowest compa-ratio in the grade.';
COMMENT ON COLUMN `03_agg_comp_ratio_by_grade`.maxCompaRatio IS 'Highest compa-ratio in the grade.';
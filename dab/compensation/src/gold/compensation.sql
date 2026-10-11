-- Gold data product: Compensation. Materialized views over the ADP and SuccessFactors silver tables.
-- All monetary KPIs are converted to EUR with fixed reference rates (EUR per 1 unit of local currency = 1 / rate).

-- ---------------------------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW `${catalog}`.`${compensation_gold_schema}`.`03_fact_payroll_monthly`
COMMENT 'Monthly payroll per employee in local currency and EUR, enriched with department. Grain: pay month x employee. Contains sensitive compensation data.'
AS
WITH fx AS (
  SELECT * FROM VALUES ('EUR', 1.0), ('SEK', 11.2), ('GBP', 0.86), ('USD', 1.08) AS t(currencyCode, unitsPerEur)
),
current_job AS (
  SELECT * FROM `${catalog}`.`${success_factor_silver_schema}`.`02_emp_job`
  QUALIFY row_number() OVER (PARTITION BY employeeId ORDER BY sequenceNumber DESC) = 1
)
SELECT
  date_trunc('MONTH', s.payDate) AS payMonth,
  s.employeeId,
  j.departmentCode,
  j.locationCode,
  s.currencyCode,
  s.grossPayAmount AS grossPayLocal,
  s.netPayAmount AS netPayLocal,
  round(s.grossPayAmount / fx.unitsPerEur, 2) AS grossPayEur,
  round(s.netPayAmount / fx.unitsPerEur, 2) AS netPayEur,
  round(s.totalTaxAmount / fx.unitsPerEur, 2) AS taxEur,
  round(s.totalDeductionAmount / fx.unitsPerEur, 2) AS deductionsEur,
  s.hoursWorked
FROM `${catalog}`.`${adp_silver_schema}`.`02_pay_statements` s
JOIN fx ON fx.currencyCode = s.currencyCode
LEFT JOIN current_job j ON j.employeeId = s.employeeId;

-- ---------------------------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW `${catalog}`.`${compensation_gold_schema}`.`03_agg_labor_cost_by_department`
COMMENT 'Monthly gross labor cost in EUR per department with paid headcount and average cost per employee. Grain: pay month x department.'
AS
SELECT
  f.payMonth,
  f.departmentCode,
  d.departmentName,
  count(DISTINCT f.employeeId) AS paidEmployees,
  round(sum(f.grossPayEur), 2) AS grossLaborCostEur,
  round(sum(f.grossPayEur) / count(DISTINCT f.employeeId), 2) AS avgGrossPerEmployeeEur
FROM `${catalog}`.`${compensation_gold_schema}`.`03_fact_payroll_monthly` f
LEFT JOIN `${catalog}`.`${success_factor_silver_schema}`.`02_fo_department` d ON d.departmentCode = f.departmentCode
GROUP BY f.payMonth, f.departmentCode, d.departmentName;

-- ---------------------------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW `${catalog}`.`${compensation_gold_schema}`.`03_agg_comp_ratio_by_grade`
COMMENT 'Compa-ratio (current annual salary in EUR divided by grade midpoint) per pay grade for active employees. Grain: pay grade.'
AS
WITH fx AS (
  SELECT * FROM VALUES ('EUR', 1.0), ('SEK', 11.2), ('GBP', 0.86), ('USD', 1.08) AS t(currencyCode, unitsPerEur)
),
current_comp AS (
  SELECT * FROM `${catalog}`.`${success_factor_silver_schema}`.`02_emp_compensation`
  WHERE payComponent = 'SALARY'
  QUALIFY row_number() OVER (PARTITION BY employeeId ORDER BY effectiveDate DESC) = 1
),
current_job AS (
  SELECT * FROM `${catalog}`.`${success_factor_silver_schema}`.`02_emp_job`
  QUALIFY row_number() OVER (PARTITION BY employeeId ORDER BY sequenceNumber DESC) = 1
)
SELECT
  jc.payGrade,
  count(*) AS employeeCount,
  round(avg(c.annualAmountLocal / fx.unitsPerEur), 2) AS avgSalaryEur,
  round(avg(jc.salaryMidpointEur), 2) AS avgMidpointEur,
  round(avg((c.annualAmountLocal / fx.unitsPerEur) / jc.salaryMidpointEur), 3) AS avgCompaRatio,
  round(min((c.annualAmountLocal / fx.unitsPerEur) / jc.salaryMidpointEur), 3) AS minCompaRatio,
  round(max((c.annualAmountLocal / fx.unitsPerEur) / jc.salaryMidpointEur), 3) AS maxCompaRatio
FROM current_comp c
JOIN fx ON fx.currencyCode = c.currencyCode
JOIN current_job j ON j.employeeId = c.employeeId
JOIN `${catalog}`.`${success_factor_silver_schema}`.`02_emp_employment` e ON e.employeeId = c.employeeId AND e.terminationDate IS NULL
JOIN `${catalog}`.`${success_factor_silver_schema}`.`02_fo_job_code` jc ON jc.jobCode = j.jobCode
GROUP BY jc.payGrade;

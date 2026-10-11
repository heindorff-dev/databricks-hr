-- Gold data product: Engagement and retention. Materialized views over Culture Amp and SuccessFactors silver tables.

-- ---------------------------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW `${catalog}`.`${engagement_retention_gold_schema}`.`03_agg_enps_by_department`
COMMENT 'Employee Net Promoter Score per survey and department. Groups with fewer than 5 respondents are suppressed to protect anonymity. Grain: survey x department.'
AS
WITH current_job AS (
  SELECT * FROM `${catalog}`.`${success_factor_silver_schema}`.`02_emp_job`
  QUALIFY row_number() OVER (PARTITION BY employeeId ORDER BY sequenceNumber DESC) = 1
)
SELECT
  s.surveyId,
  s.surveyName,
  s.closeDate AS surveyCloseDate,
  j.departmentCode,
  d.departmentName,
  count(*) AS respondentCount,
  count_if(n.enpsCategory = 'promoter') AS promoterCount,
  count_if(n.enpsCategory = 'passive') AS passiveCount,
  count_if(n.enpsCategory = 'detractor') AS detractorCount,
  round(100.0 * (count_if(n.enpsCategory = 'promoter') - count_if(n.enpsCategory = 'detractor')) / count(*), 1) AS enpsScore,
  round(avg(n.enpsScore), 2) AS avgScore
FROM `${catalog}`.`${culture_amp_silver_schema}`.`02_enps_scores` n
JOIN `${catalog}`.`${culture_amp_silver_schema}`.`02_surveys` s ON s.surveyId = n.surveyId
JOIN current_job j ON j.employeeId = n.employeeId
LEFT JOIN `${catalog}`.`${success_factor_silver_schema}`.`02_fo_department` d ON d.departmentCode = j.departmentCode
GROUP BY s.surveyId, s.surveyName, s.closeDate, j.departmentCode, d.departmentName
HAVING count(*) >= 5;

-- ---------------------------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW `${catalog}`.`${engagement_retention_gold_schema}`.`03_fact_attrition_monthly`
COMMENT 'Monthly attrition per department split by voluntary and involuntary leavers with average headcount and attrition rate. Grain: month x department.'
AS
WITH month_spine AS (
  SELECT last_day(m) AS monthEnd, m AS monthStart
  FROM (SELECT explode(sequence(DATE'2023-01-01', DATE'2026-09-01', INTERVAL 1 MONTH)) AS m)
),
employee_month AS (
  SELECT
    s.monthEnd, s.monthStart, e.employeeId, e.hireDate, e.terminationDate, e.terminationReasonCode,
    least(s.monthEnd, coalesce(e.terminationDate, s.monthEnd)) AS refDate
  FROM month_spine s
  JOIN `${catalog}`.`${success_factor_silver_schema}`.`02_emp_employment` e
    ON e.hireDate <= s.monthEnd AND (e.terminationDate IS NULL OR e.terminationDate >= s.monthStart)
),
monthly AS (
  SELECT
    em.monthEnd,
    j.departmentCode,
    count(*) AS headcountInMonth,
    count_if(em.terminationDate BETWEEN em.monthStart AND em.monthEnd) AS leavers,
    count_if(em.terminationDate BETWEEN em.monthStart AND em.monthEnd AND em.terminationReasonCode IN ('RESIGN', 'RETIRE')) AS voluntaryLeavers,
    count_if(em.terminationDate BETWEEN em.monthStart AND em.monthEnd AND em.terminationReasonCode NOT IN ('RESIGN', 'RETIRE')) AS involuntaryLeavers
  FROM employee_month em
  JOIN `${catalog}`.`${success_factor_silver_schema}`.`02_emp_job` j
    ON j.employeeId = em.employeeId
   AND j.effectiveStartDate <= em.refDate
   AND coalesce(j.effectiveEndDate, DATE'9999-12-31') >= em.refDate
  GROUP BY em.monthEnd, j.departmentCode
)
SELECT
  m.monthEnd AS reportingMonthEnd,
  m.departmentCode,
  d.departmentName,
  m.headcountInMonth,
  m.leavers,
  m.voluntaryLeavers,
  m.involuntaryLeavers,
  round(m.leavers / m.headcountInMonth, 4) AS monthlyAttritionRate
FROM monthly m
LEFT JOIN `${catalog}`.`${success_factor_silver_schema}`.`02_fo_department` d ON d.departmentCode = m.departmentCode;

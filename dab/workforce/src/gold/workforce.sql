-- Gold data product: Workforce. Materialized views over the SuccessFactors and Kronos silver tables.
-- Written to the workforce gold schema; dimensional model plus pre-aggregated absence KPIs.

-- ---------------------------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW `${catalog}`.`${workforce_gold_schema}`.`03_dim_employee`
COMMENT 'Employee dimension: one row per employee with current job, department, location and employment status. Contains personal data (names, email, birth date): restrict access accordingly.'
AS
WITH current_job AS (
  SELECT * FROM `${catalog}`.`${success_factor_silver_schema}`.`02_emp_job`
  QUALIFY row_number() OVER (PARTITION BY employeeId ORDER BY sequenceNumber DESC) = 1
)
SELECT
  p.employeeId,
  p.firstName,
  p.lastName,
  concat_ws(' ', p.firstName, p.lastName) AS fullName,
  p.emailAddress,
  p.gender,
  p.dateOfBirth,
  floor(months_between(DATE'2026-09-30', p.dateOfBirth) / 12) AS ageAtReference,
  e.hireDate,
  e.terminationDate,
  e.terminationReasonCode,
  e.terminationDate IS NULL AS isActive,
  j.jobCode,
  jc.jobTitle,
  jc.jobFamily,
  jc.payGrade,
  j.employmentType,
  j.fteRatio,
  j.departmentCode,
  d.departmentName,
  j.costCenter,
  j.locationCode,
  l.locationName,
  l.city,
  l.countryCode,
  j.managerEmployeeId,
  concat_ws(' ', m.firstName, m.lastName) AS managerName
FROM `${catalog}`.`${success_factor_silver_schema}`.`02_per_person` p
JOIN `${catalog}`.`${success_factor_silver_schema}`.`02_emp_employment` e ON e.employeeId = p.employeeId
LEFT JOIN current_job j ON j.employeeId = p.employeeId
LEFT JOIN `${catalog}`.`${success_factor_silver_schema}`.`02_fo_job_code` jc ON jc.jobCode = j.jobCode
LEFT JOIN `${catalog}`.`${success_factor_silver_schema}`.`02_fo_department` d ON d.departmentCode = j.departmentCode
LEFT JOIN `${catalog}`.`${success_factor_silver_schema}`.`02_fo_location` l ON l.locationCode = j.locationCode
LEFT JOIN `${catalog}`.`${success_factor_silver_schema}`.`02_per_person` m ON m.employeeId = j.managerEmployeeId;

-- ---------------------------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW `${catalog}`.`${workforce_gold_schema}`.`03_dim_department`
COMMENT 'Department dimension: one row per department with cost center and current headcount.'
AS
WITH current_job AS (
  SELECT * FROM `${catalog}`.`${success_factor_silver_schema}`.`02_emp_job`
  QUALIFY row_number() OVER (PARTITION BY employeeId ORDER BY sequenceNumber DESC) = 1
),
active_headcount AS (
  SELECT j.departmentCode, count(*) AS headcount
  FROM current_job j
  JOIN `${catalog}`.`${success_factor_silver_schema}`.`02_emp_employment` e ON e.employeeId = j.employeeId
  WHERE e.terminationDate IS NULL
  GROUP BY j.departmentCode
)
SELECT
  d.departmentCode,
  d.departmentName,
  d.costCenter,
  d.isActive,
  coalesce(h.headcount, 0) AS currentHeadcount
FROM `${catalog}`.`${success_factor_silver_schema}`.`02_fo_department` d
LEFT JOIN active_headcount h ON h.departmentCode = d.departmentCode;

-- ---------------------------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW `${catalog}`.`${workforce_gold_schema}`.`03_fact_headcount_monthly`
COMMENT 'Monthly headcount snapshot per department, location and employment type with hires, terminations and FTE. Grain: month end x department x location x employment type.'
AS
WITH month_spine AS (
  SELECT last_day(m) AS monthEnd, m AS monthStart
  FROM (SELECT explode(sequence(DATE'2023-01-01', DATE'2026-09-01', INTERVAL 1 MONTH)) AS m)
),
employee_month AS (
  SELECT
    s.monthEnd, s.monthStart, e.employeeId, e.hireDate, e.terminationDate,
    least(s.monthEnd, coalesce(e.terminationDate, s.monthEnd)) AS refDate
  FROM month_spine s
  JOIN `${catalog}`.`${success_factor_silver_schema}`.`02_emp_employment` e
    ON e.hireDate <= s.monthEnd AND (e.terminationDate IS NULL OR e.terminationDate >= s.monthStart)
)
SELECT
  em.monthEnd AS reportingMonthEnd,
  j.departmentCode,
  j.locationCode,
  j.employmentType,
  count_if(em.terminationDate IS NULL OR em.terminationDate > em.monthEnd) AS headcount,
  sum(CASE WHEN em.terminationDate IS NULL OR em.terminationDate > em.monthEnd THEN j.fteRatio ELSE 0 END) AS fteTotal,
  count_if(em.hireDate >= em.monthStart) AS hires,
  count_if(em.terminationDate BETWEEN em.monthStart AND em.monthEnd) AS terminations
FROM employee_month em
JOIN `${catalog}`.`${success_factor_silver_schema}`.`02_emp_job` j
  ON j.employeeId = em.employeeId
 AND j.effectiveStartDate <= em.refDate
 AND coalesce(j.effectiveEndDate, DATE'9999-12-31') >= em.refDate
GROUP BY em.monthEnd, j.departmentCode, j.locationCode, j.employmentType;

-- ---------------------------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW `${catalog}`.`${workforce_gold_schema}`.`03_agg_absence_by_department`
COMMENT 'Approved leave per month and department from Kronos: requests, absence days and absence type mix. Grain: month x department x absence type.'
AS
WITH current_job AS (
  SELECT * FROM `${catalog}`.`${success_factor_silver_schema}`.`02_emp_job`
  QUALIFY row_number() OVER (PARTITION BY employeeId ORDER BY sequenceNumber DESC) = 1
)
SELECT
  date_trunc('MONTH', r.startDate) AS absenceMonth,
  j.departmentCode,
  t.absenceName,
  t.isPaid,
  count(*) AS requestCount,
  sum(r.requestedDays) AS absenceDays,
  count(DISTINCT r.employeeId) AS employeesAbsent
FROM `${catalog}`.`${kronos_silver_schema}`.`02_leave_requests` r
JOIN `${catalog}`.`${kronos_silver_schema}`.`02_absence_types` t ON t.absenceTypeId = r.absenceTypeId
JOIN current_job j ON j.employeeId = r.employeeId
WHERE r.statusCode = 'APR'
GROUP BY date_trunc('MONTH', r.startDate), j.departmentCode, t.absenceName, t.isPaid;

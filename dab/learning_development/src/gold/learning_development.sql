-- Gold data product: Learning and development. Materialized views over the Cornerstone silver tables.

-- ---------------------------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW `${catalog}`.`${learning_development_gold_schema}`.`03_fact_training_completion`
COMMENT 'Training enrollments and completions per course and month with completion rate and average score. Grain: enrollment month x course.'
AS
SELECT
  date_trunc('MONTH', e.startedAt) AS enrollmentMonth,
  c.courseId,
  c.courseTitle,
  c.category,
  c.provider,
  c.isMandatory,
  count(*) AS enrollmentCount,
  count_if(e.status = 'Completed') AS completedCount,
  count_if(e.status = 'Cancelled') AS cancelledCount,
  round(count_if(e.status = 'Completed') / count(*), 4) AS completionRate,
  round(avg(e.score), 1) AS avgScore,
  round(sum(e.timeSpentMinutes) / 60.0, 1) AS learningHours
FROM `${catalog}`.`${cornerstone_silver_schema}`.`02_enrollments` e
JOIN `${catalog}`.`${cornerstone_silver_schema}`.`02_courses` c ON c.courseId = e.courseId
GROUP BY date_trunc('MONTH', e.startedAt), c.courseId, c.courseTitle, c.category, c.provider, c.isMandatory;

-- ---------------------------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW `${catalog}`.`${learning_development_gold_schema}`.`03_agg_employee_learning_hours`
COMMENT 'Learning summary per employee: completed courses, learning hours, mandatory completions and valid certifications. Grain: one row per employee.'
AS
WITH learning AS (
  SELECT
    e.employeeId,
    count_if(e.status = 'Completed') AS completedCourses,
    count_if(e.status = 'Completed' AND c.isMandatory) AS completedMandatoryCourses,
    round(sum(e.timeSpentMinutes) / 60.0, 1) AS learningHours
  FROM `${catalog}`.`${cornerstone_silver_schema}`.`02_enrollments` e
  JOIN `${catalog}`.`${cornerstone_silver_schema}`.`02_courses` c ON c.courseId = e.courseId
  GROUP BY e.employeeId
),
certs AS (
  SELECT employeeId, count_if(status = 'Valid') AS validCertifications
  FROM `${catalog}`.`${cornerstone_silver_schema}`.`02_certifications`
  GROUP BY employeeId
)
SELECT
  emp.employeeId,
  emp.hireDate,
  coalesce(l.completedCourses, 0) AS completedCourses,
  coalesce(l.completedMandatoryCourses, 0) AS completedMandatoryCourses,
  coalesce(l.learningHours, 0) AS learningHours,
  coalesce(c.validCertifications, 0) AS validCertifications
FROM `${catalog}`.`${success_factor_silver_schema}`.`02_emp_employment` emp
LEFT JOIN learning l ON l.employeeId = emp.employeeId
LEFT JOIN certs c ON c.employeeId = emp.employeeId
WHERE emp.terminationDate IS NULL;

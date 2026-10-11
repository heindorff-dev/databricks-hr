-- Databricks notebook source
-- Learning and development data product: gold column descriptions (no PII columns).
-- Silver column descriptions live in src/silver/*.sql. Gold materialized views only get a table comment there.

USE CATALOG IDENTIFIER(:catalog);
USE SCHEMA IDENTIFIER(:learning_development_gold_schema);

-- COMMAND ----------

COMMENT ON COLUMN `03_fact_training_completion`.enrollmentMonth IS 'First day of the month the enrollment started.';
COMMENT ON COLUMN `03_fact_training_completion`.courseId IS 'Identifier of the course.';
COMMENT ON COLUMN `03_fact_training_completion`.courseTitle IS 'Title of the course.';
COMMENT ON COLUMN `03_fact_training_completion`.category IS 'Course category.';
COMMENT ON COLUMN `03_fact_training_completion`.provider IS 'Course provider.';
COMMENT ON COLUMN `03_fact_training_completion`.isMandatory IS 'True when the course is mandatory.';
COMMENT ON COLUMN `03_fact_training_completion`.enrollmentCount IS 'Number of enrollments.';
COMMENT ON COLUMN `03_fact_training_completion`.completedCount IS 'Enrollments with status Completed.';
COMMENT ON COLUMN `03_fact_training_completion`.cancelledCount IS 'Enrollments with status Cancelled.';
COMMENT ON COLUMN `03_fact_training_completion`.completionRate IS 'Completed enrollments divided by all enrollments.';
COMMENT ON COLUMN `03_fact_training_completion`.avgScore IS 'Average assessment score (0-100).';
COMMENT ON COLUMN `03_fact_training_completion`.learningHours IS 'Total time spent learning in hours.';

COMMENT ON COLUMN `03_agg_employee_learning_hours`.employeeId IS 'Conformed employee identifier.';
COMMENT ON COLUMN `03_agg_employee_learning_hours`.hireDate IS 'Date the employee was hired.';
COMMENT ON COLUMN `03_agg_employee_learning_hours`.completedCourses IS 'Courses completed by the employee.';
COMMENT ON COLUMN `03_agg_employee_learning_hours`.completedMandatoryCourses IS 'Mandatory courses completed by the employee.';
COMMENT ON COLUMN `03_agg_employee_learning_hours`.learningHours IS 'Total time spent learning in hours.';
COMMENT ON COLUMN `03_agg_employee_learning_hours`.validCertifications IS 'Certifications with status Valid.';

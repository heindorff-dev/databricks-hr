-- Silver layer for Cornerstone OnDemand (learning). Streaming tables read from the bronze Delta tables.
-- Bronze uses PascalCase, GUID identifiers and epoch-second timestamps; silver conforms them to typed camelCase columns.

-- ---------------------------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE `${catalog}`.`${cornerstone_silver_schema}`.`02_courses` (
  courseId STRING COMMENT 'Cornerstone course identifier (GUID).',
  courseTitle STRING COMMENT 'Title of the course.',
  category STRING COMMENT 'Course category, for example Compliance, Leadership or Technical.',
  provider STRING COMMENT 'Provider that delivers the course content.',
  durationMinutes INT COMMENT 'Expected duration of the course in minutes.',
  isMandatory BOOLEAN COMMENT 'True when the course is mandatory training.',
  status STRING COMMENT 'Course status: Active or Retired.',
  createdAt TIMESTAMP COMMENT 'Timestamp of the latest published version of the course.',
  ingestedAt TIMESTAMP COMMENT 'Timestamp when the row was written to bronze.',
  CONSTRAINT valid_course_id EXPECT (courseId IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Current-state course catalog from Cornerstone, de-duplicated on courseId using the latest publish timestamp (SCD type 1).';

CREATE TEMPORARY VIEW courses_conformed AS
SELECT
  CAST(CourseId AS STRING) AS courseId,
  CAST(Title AS STRING) AS courseTitle,
  CAST(Category AS STRING) AS category,
  CAST(Provider AS STRING) AS provider,
  CAST(DurationMinutes AS INT) AS durationMinutes,
  CAST(IsMandatory AS BOOLEAN) AS isMandatory,
  CAST(Status AS STRING) AS status,
  timestamp_seconds(CreatedEpoch) AS createdAt,
  CAST(_ingested_at AS TIMESTAMP) AS ingestedAt
FROM STREAM(`${catalog}`.`${cornerstone_bronze_schema}`.`01_courses`);

CREATE FLOW courses_cdc AS AUTO CDC INTO `${catalog}`.`${cornerstone_silver_schema}`.`02_courses`
FROM STREAM(courses_conformed)
KEYS (courseId)
SEQUENCE BY createdAt
STORED AS SCD TYPE 1;

-- ---------------------------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE `${catalog}`.`${cornerstone_silver_schema}`.`02_enrollments` (
  enrollmentId STRING COMMENT 'Cornerstone enrollment identifier (GUID).',
  employeeId STRING COMMENT 'Conformed employee identifier (E plus six digits) of the learner.',
  courseId STRING COMMENT 'Course the learner is enrolled in, references the courses table.',
  startedAt TIMESTAMP COMMENT 'Timestamp when the learner started the enrollment.',
  completedAt TIMESTAMP COMMENT 'Timestamp when the course was completed. Null unless completed.',
  status STRING COMMENT 'Enrollment status: Completed, InProgress, NotStarted or Cancelled.',
  score INT COMMENT 'Assessment score between 0 and 100. Null when not completed.',
  timeSpentMinutes INT COMMENT 'Minutes the learner spent on the course.',
  ingestedAt TIMESTAMP COMMENT 'Timestamp when the row was written to bronze.',
  CONSTRAINT valid_keys EXPECT (enrollmentId IS NOT NULL AND employeeId IS NOT NULL AND courseId IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Training transcript from Cornerstone: one row per learner and course enrollment.'
AS SELECT
  CAST(EnrollmentId AS STRING) AS enrollmentId,
  CAST(UserExternalId AS STRING) AS employeeId,
  CAST(CourseId AS STRING) AS courseId,
  timestamp_seconds(StartEpoch) AS startedAt,
  timestamp_seconds(CompletionEpoch) AS completedAt,
  CAST(Status AS STRING) AS status,
  CAST(Score AS INT) AS score,
  CAST(TimeSpentMinutes AS INT) AS timeSpentMinutes,
  CAST(_ingested_at AS TIMESTAMP) AS ingestedAt
FROM STREAM(`${catalog}`.`${cornerstone_bronze_schema}`.`01_enrollments`);

-- ---------------------------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE `${catalog}`.`${cornerstone_silver_schema}`.`02_certifications` (
  certificationRecordId STRING COMMENT 'Cornerstone certification record identifier (GUID).',
  employeeId STRING COMMENT 'Conformed employee identifier (E plus six digits) of the certificate holder.',
  certificationName STRING COMMENT 'Name of the certification.',
  earnedAt TIMESTAMP COMMENT 'Timestamp when the certification was earned.',
  expiresAt TIMESTAMP COMMENT 'Timestamp when the certification expires.',
  status STRING COMMENT 'Certification status: Valid or Revoked.',
  ingestedAt TIMESTAMP COMMENT 'Timestamp when the row was written to bronze.',
  CONSTRAINT valid_keys EXPECT (certificationRecordId IS NOT NULL AND employeeId IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Employee certification records from Cornerstone with typed timestamps.'
AS SELECT
  CAST(CertificationRecordId AS STRING) AS certificationRecordId,
  CAST(UserExternalId AS STRING) AS employeeId,
  CAST(CertificationName AS STRING) AS certificationName,
  timestamp_seconds(EarnedEpoch) AS earnedAt,
  timestamp_seconds(ExpiresEpoch) AS expiresAt,
  CAST(Status AS STRING) AS status,
  CAST(_ingested_at AS TIMESTAMP) AS ingestedAt
FROM STREAM(`${catalog}`.`${cornerstone_bronze_schema}`.`01_certifications`);

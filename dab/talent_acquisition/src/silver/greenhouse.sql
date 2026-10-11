-- Silver layer for Greenhouse (recruiting / ATS). Streaming tables read from the bronze Delta tables.
-- Bronze is snake_case with ISO-8601 strings and nested structs; silver flattens to typed camelCase columns.

-- ---------------------------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE `${catalog}`.`${greenhouse_silver_schema}`.`02_jobs` (
  jobId BIGINT COMMENT 'Greenhouse job identifier.',
  jobTitle STRING COMMENT 'Title of the job the requisition is opened for.',
  requisitionId STRING COMMENT 'Requisition number (REQ-year-sequence).',
  status STRING COMMENT 'Job status: open, closed or draft.',
  openedAt TIMESTAMP COMMENT 'Timestamp when the requisition was opened.',
  closedAt TIMESTAMP COMMENT 'Timestamp when the requisition was closed. Null while open.',
  departmentName STRING COMMENT 'Name of the primary department the job belongs to.',
  officeName STRING COMMENT 'Name of the primary office the job is located in.',
  employmentType STRING COMMENT 'Employment type from the custom fields: full_time, part_time or contract.',
  salaryRangeMin DECIMAL(12,2) COMMENT 'Lower bound of the advertised annual salary range.',
  salaryRangeMax DECIMAL(12,2) COMMENT 'Upper bound of the advertised annual salary range.',
  ingestedAt TIMESTAMP COMMENT 'Timestamp when the row was written to bronze.',
  CONSTRAINT valid_job_id EXPECT (jobId IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Job requisitions from Greenhouse with nested department, office and custom fields flattened.'
AS SELECT
  CAST(id AS BIGINT) AS jobId,
  CAST(name AS STRING) AS jobTitle,
  CAST(requisition_id AS STRING) AS requisitionId,
  CAST(status AS STRING) AS status,
  try_cast(opened_at AS TIMESTAMP) AS openedAt,
  try_cast(closed_at AS TIMESTAMP) AS closedAt,
  CAST(departments[0].name AS STRING) AS departmentName,
  CAST(offices[0].name AS STRING) AS officeName,
  CAST(custom_fields.employment_type AS STRING) AS employmentType,
  CAST(custom_fields.salary_range_min AS DECIMAL(12,2)) AS salaryRangeMin,
  CAST(custom_fields.salary_range_max AS DECIMAL(12,2)) AS salaryRangeMax,
  CAST(_ingested_at AS TIMESTAMP) AS ingestedAt
FROM STREAM(`${catalog}`.`${greenhouse_bronze_schema}`.`01_jobs`);

-- ---------------------------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE `${catalog}`.`${greenhouse_silver_schema}`.`02_candidates` (
  candidateId BIGINT COMMENT 'Greenhouse candidate identifier.',
  firstName STRING COMMENT 'Given name of the candidate.',
  lastName STRING COMMENT 'Family name of the candidate.',
  currentCompany STRING COMMENT 'Current employer of the candidate.',
  currentTitle STRING COMMENT 'Current job title of the candidate.',
  emailAddress STRING COMMENT 'Primary email address of the candidate.',
  tags ARRAY<STRING> COMMENT 'Free form tags attached to the candidate.',
  isPrivate BOOLEAN COMMENT 'True when the candidate profile is restricted to the recruiting team.',
  createdAt TIMESTAMP COMMENT 'Timestamp when the candidate was created.',
  lastActivityAt TIMESTAMP COMMENT 'Timestamp of the latest activity on the candidate.',
  ingestedAt TIMESTAMP COMMENT 'Timestamp when the row was written to bronze.',
  CONSTRAINT valid_candidate_id EXPECT (candidateId IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Current-state candidate profiles from Greenhouse, de-duplicated on candidateId using the latest activity (SCD type 1).';

CREATE TEMPORARY VIEW candidates_conformed AS
SELECT
  CAST(id AS BIGINT) AS candidateId,
  CAST(first_name AS STRING) AS firstName,
  CAST(last_name AS STRING) AS lastName,
  CAST(company AS STRING) AS currentCompany,
  CAST(title AS STRING) AS currentTitle,
  CAST(email_addresses[0].value AS STRING) AS emailAddress,
  CAST(tags AS ARRAY<STRING>) AS tags,
  CAST(is_private AS BOOLEAN) AS isPrivate,
  try_cast(created_at AS TIMESTAMP) AS createdAt,
  try_cast(last_activity AS TIMESTAMP) AS lastActivityAt,
  CAST(_ingested_at AS TIMESTAMP) AS ingestedAt
FROM STREAM(`${catalog}`.`${greenhouse_bronze_schema}`.`01_candidates`);

CREATE FLOW candidates_cdc AS AUTO CDC INTO `${catalog}`.`${greenhouse_silver_schema}`.`02_candidates`
FROM STREAM(candidates_conformed)
KEYS (candidateId)
SEQUENCE BY lastActivityAt
STORED AS SCD TYPE 1;

-- ---------------------------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE `${catalog}`.`${greenhouse_silver_schema}`.`02_applications` (
  applicationId BIGINT COMMENT 'Greenhouse application identifier.',
  candidateId BIGINT COMMENT 'Candidate who applied, references the candidates table.',
  jobId BIGINT COMMENT 'Job applied for, references the jobs table.',
  appliedAt TIMESTAMP COMMENT 'Timestamp when the application was submitted.',
  status STRING COMMENT 'Application status: active, rejected or hired.',
  currentStageName STRING COMMENT 'Name of the interview stage the application is currently in.',
  sourceName STRING COMMENT 'Public name of the sourcing channel the candidate came from.',
  rejectionReason STRING COMMENT 'Reason for rejection. Null unless the application was rejected.',
  rejectedAt TIMESTAMP COMMENT 'Timestamp when the application was rejected. Null unless rejected.',
  lastActivityAt TIMESTAMP COMMENT 'Timestamp of the latest activity on the application.',
  ingestedAt TIMESTAMP COMMENT 'Timestamp when the row was written to bronze.',
  CONSTRAINT valid_keys EXPECT (applicationId IS NOT NULL AND candidateId IS NOT NULL AND jobId IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Job applications from Greenhouse with nested stage, source and rejection reason flattened.'
AS SELECT
  CAST(id AS BIGINT) AS applicationId,
  CAST(candidate_id AS BIGINT) AS candidateId,
  CAST(job_id AS BIGINT) AS jobId,
  try_cast(applied_at AS TIMESTAMP) AS appliedAt,
  CAST(status AS STRING) AS status,
  CAST(current_stage.name AS STRING) AS currentStageName,
  CAST(source.public_name AS STRING) AS sourceName,
  CAST(rejection_reason.name AS STRING) AS rejectionReason,
  try_cast(rejected_at AS TIMESTAMP) AS rejectedAt,
  try_cast(last_activity_at AS TIMESTAMP) AS lastActivityAt,
  CAST(_ingested_at AS TIMESTAMP) AS ingestedAt
FROM STREAM(`${catalog}`.`${greenhouse_bronze_schema}`.`01_applications`);

-- ---------------------------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE `${catalog}`.`${greenhouse_silver_schema}`.`02_offers` (
  offerId BIGINT COMMENT 'Greenhouse offer identifier.',
  applicationId BIGINT COMMENT 'Application the offer belongs to, references the applications table.',
  versionNumber INT COMMENT 'Version of the offer. Higher means a revised offer.',
  offerStatus STRING COMMENT 'Offer status: accepted, rejected, deprecated or unresolved.',
  createdAt TIMESTAMP COMMENT 'Timestamp when the offer was drafted.',
  sentAt TIMESTAMP COMMENT 'Timestamp when the offer was sent to the candidate.',
  resolvedAt TIMESTAMP COMMENT 'Timestamp when the candidate responded. Null while unresolved.',
  plannedStartDate DATE COMMENT 'Planned first working day of the candidate.',
  offeredSalary DECIMAL(12,2) COMMENT 'Offered annual salary in the offer currency.',
  currencyCode STRING COMMENT 'ISO 4217 currency code of the offered salary.',
  ingestedAt TIMESTAMP COMMENT 'Timestamp when the row was written to bronze.',
  CONSTRAINT valid_keys EXPECT (offerId IS NOT NULL AND applicationId IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Offer versions from Greenhouse with parsed timestamps and flattened salary custom fields.'
AS SELECT
  CAST(id AS BIGINT) AS offerId,
  CAST(application_id AS BIGINT) AS applicationId,
  CAST(version AS INT) AS versionNumber,
  CAST(offer_status AS STRING) AS offerStatus,
  try_cast(created_at AS TIMESTAMP) AS createdAt,
  try_cast(sent_at AS TIMESTAMP) AS sentAt,
  try_cast(resolved_at AS TIMESTAMP) AS resolvedAt,
  try_cast(starts_at AS DATE) AS plannedStartDate,
  CAST(custom_fields.salary AS DECIMAL(12,2)) AS offeredSalary,
  CAST(custom_fields.currency AS STRING) AS currencyCode,
  CAST(_ingested_at AS TIMESTAMP) AS ingestedAt
FROM STREAM(`${catalog}`.`${greenhouse_bronze_schema}`.`01_offers`);

-- Silver layer for SAP SuccessFactors (core HR). Streaming tables read from the bronze Delta tables.
-- Bronze columns are camelCase OData fields with "/Date(ms)/" strings; silver conforms them to typed camelCase columns.

-- ---------------------------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE `${catalog}`.`${success_factor_silver_schema}`.`02_per_person` (
  employeeId STRING COMMENT 'Conformed employee identifier (E plus six digits), shared across all HR source systems.',
  firstName STRING COMMENT 'Given name of the person.',
  lastName STRING COMMENT 'Family name of the person. Latest value after de-duplication of re-sent records.',
  dateOfBirth DATE COMMENT 'Date of birth of the person.',
  gender STRING COMMENT 'Gender code as recorded in SuccessFactors (M, F or X).',
  emailAddress STRING COMMENT 'Corporate email address of the person.',
  isActive BOOLEAN COMMENT 'True when the person currently has an active employment.',
  lastModifiedAt TIMESTAMP COMMENT 'Timestamp of the last change to the person record in the source system.',
  ingestedAt TIMESTAMP COMMENT 'Timestamp when the row was written to bronze.',
  CONSTRAINT valid_employee_id EXPECT (employeeId IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Current-state person master data from SuccessFactors, de-duplicated on employeeId using the latest source modification (SCD type 1).';

CREATE TEMPORARY VIEW per_person_conformed AS
SELECT
  CAST(personIdExternal AS STRING) AS employeeId,
  CAST(firstName AS STRING) AS firstName,
  CAST(lastName AS STRING) AS lastName,
  CAST(timestamp_millis(try_cast(regexp_extract(dateOfBirth, '[(](-?[0-9]+)[)]', 1) AS BIGINT)) AS DATE) AS dateOfBirth,
  CAST(gender AS STRING) AS gender,
  CAST(emailAddress AS STRING) AS emailAddress,
  CAST(isActive = 'Y' AS BOOLEAN) AS isActive,
  timestamp_millis(try_cast(regexp_extract(lastModifiedDateTime, '[(](-?[0-9]+)[)]', 1) AS BIGINT)) AS lastModifiedAt,
  CAST(_ingested_at AS TIMESTAMP) AS ingestedAt
FROM STREAM(`${catalog}`.`${success_factor_bronze_schema}`.`01_per_person`);

CREATE FLOW per_person_cdc AS AUTO CDC INTO `${catalog}`.`${success_factor_silver_schema}`.`02_per_person`
FROM STREAM(per_person_conformed)
KEYS (employeeId)
SEQUENCE BY lastModifiedAt
STORED AS SCD TYPE 1;

-- ---------------------------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE `${catalog}`.`${success_factor_silver_schema}`.`02_emp_employment` (
  userId STRING COMMENT 'SuccessFactors internal user identifier (u plus employee number).',
  employeeId STRING COMMENT 'Conformed employee identifier (E plus six digits), shared across all HR source systems.',
  hireDate DATE COMMENT 'Start date of the employment.',
  terminationDate DATE COMMENT 'End date of the employment. Null while the employment is open ended.',
  originalHireDate DATE COMMENT 'First ever hire date of the person, used for tenure calculations.',
  hireReasonCode STRING COMMENT 'Event reason for the hire (HIRNEW new hire, HIRREH rehire).',
  isPrimary BOOLEAN COMMENT 'True when this is the primary employment of the person.',
  terminationReasonCode STRING COMMENT 'Reason code for the termination (RESIGN, RETIRE, ENDCON, DISMIS). Null when not terminated.',
  lastModifiedAt TIMESTAMP COMMENT 'Timestamp of the last change to the employment record in the source system.',
  ingestedAt TIMESTAMP COMMENT 'Timestamp when the row was written to bronze.',
  CONSTRAINT valid_employee_id EXPECT (employeeId IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_hire_date EXPECT (hireDate IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Employment records from SuccessFactors with parsed dates. One row per employment of a person.'
AS SELECT
  CAST(userId AS STRING) AS userId,
  CAST(personIdExternal AS STRING) AS employeeId,
  CAST(timestamp_millis(try_cast(regexp_extract(startDate, '[(](-?[0-9]+)[)]', 1) AS BIGINT)) AS DATE) AS hireDate,
  CASE WHEN try_cast(regexp_extract(endDate, '[(](-?[0-9]+)[)]', 1) AS BIGINT) < 253402214400000
       THEN CAST(timestamp_millis(try_cast(regexp_extract(endDate, '[(](-?[0-9]+)[)]', 1) AS BIGINT)) AS DATE) END AS terminationDate,
  CAST(timestamp_millis(try_cast(regexp_extract(originalStartDate, '[(](-?[0-9]+)[)]', 1) AS BIGINT)) AS DATE) AS originalHireDate,
  CAST(eventReason AS STRING) AS hireReasonCode,
  CAST(isPrimary = 'Y' AS BOOLEAN) AS isPrimary,
  CAST(terminationReason AS STRING) AS terminationReasonCode,
  timestamp_millis(try_cast(regexp_extract(lastModifiedOn, '[(](-?[0-9]+)[)]', 1) AS BIGINT)) AS lastModifiedAt,
  CAST(_ingested_at AS TIMESTAMP) AS ingestedAt
FROM STREAM(`${catalog}`.`${success_factor_bronze_schema}`.`01_emp_employment`);

-- ---------------------------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE `${catalog}`.`${success_factor_silver_schema}`.`02_emp_job` (
  userId STRING COMMENT 'SuccessFactors internal user identifier (u plus employee number).',
  employeeId STRING COMMENT 'Conformed employee identifier (E plus six digits), shared across all HR source systems.',
  sequenceNumber INT COMMENT 'Sequence of the job assignment within the user history. Higher is more recent.',
  effectiveStartDate DATE COMMENT 'First day the job assignment is valid.',
  effectiveEndDate DATE COMMENT 'Last day the job assignment is valid. Null when open ended.',
  jobCode STRING COMMENT 'Job code, references the job code catalog.',
  departmentCode STRING COMMENT 'Department code, references the department catalog.',
  locationCode STRING COMMENT 'Location code, references the location catalog.',
  managerEmployeeId STRING COMMENT 'Conformed employee identifier of the line manager. Null for top managers.',
  employmentType STRING COMMENT 'Employment type: FT full time, PT part time, CT contractor.',
  fteRatio DECIMAL(3,2) COMMENT 'Full time equivalent ratio between 0 and 1.',
  positionCode STRING COMMENT 'Position identifier in the organisation structure.',
  eventReasonCode STRING COMMENT 'Reason for the job change (HIRNEW hire, PROMO promotion).',
  costCenter STRING COMMENT 'Cost center the employee is booked on.',
  lastModifiedAt TIMESTAMP COMMENT 'Timestamp of the last change to the job record in the source system.',
  ingestedAt TIMESTAMP COMMENT 'Timestamp when the row was written to bronze.',
  CONSTRAINT valid_user EXPECT (userId IS NOT NULL AND effectiveStartDate IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Job assignment history from SuccessFactors: one row per user and sequence number with typed effective dates.'
AS SELECT
  CAST(userId AS STRING) AS userId,
  CONCAT('E', lpad(substr(userId, 2), 6, '0')) AS employeeId,
  CAST(seqNumber AS INT) AS sequenceNumber,
  CAST(timestamp_millis(try_cast(regexp_extract(startDate, '[(](-?[0-9]+)[)]', 1) AS BIGINT)) AS DATE) AS effectiveStartDate,
  CASE WHEN try_cast(regexp_extract(endDate, '[(](-?[0-9]+)[)]', 1) AS BIGINT) < 253402214400000
       THEN CAST(timestamp_millis(try_cast(regexp_extract(endDate, '[(](-?[0-9]+)[)]', 1) AS BIGINT)) AS DATE) END AS effectiveEndDate,
  CAST(jobCode AS STRING) AS jobCode,
  CAST(department AS STRING) AS departmentCode,
  CAST(location AS STRING) AS locationCode,
  CAST(managerId AS STRING) AS managerEmployeeId,
  CAST(employmentType AS STRING) AS employmentType,
  CAST(try_cast(fte AS DECIMAL(3,2)) AS DECIMAL(3,2)) AS fteRatio,
  CAST(position AS STRING) AS positionCode,
  CAST(eventReason AS STRING) AS eventReasonCode,
  CAST(costCenter AS STRING) AS costCenter,
  timestamp_millis(try_cast(regexp_extract(lastModifiedOn, '[(](-?[0-9]+)[)]', 1) AS BIGINT)) AS lastModifiedAt,
  CAST(_ingested_at AS TIMESTAMP) AS ingestedAt
FROM STREAM(`${catalog}`.`${success_factor_bronze_schema}`.`01_emp_job`);

-- ---------------------------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE `${catalog}`.`${success_factor_silver_schema}`.`02_emp_compensation` (
  userId STRING COMMENT 'SuccessFactors internal user identifier (u plus employee number).',
  employeeId STRING COMMENT 'Conformed employee identifier (E plus six digits), shared across all HR source systems.',
  effectiveDate DATE COMMENT 'Date from which the pay component value applies.',
  payComponent STRING COMMENT 'Pay component code, for example SALARY.',
  annualAmountLocal DECIMAL(14,2) COMMENT 'Annual amount in the local currency given by currencyCode.',
  currencyCode STRING COMMENT 'ISO 4217 currency code of the amount.',
  payFrequency STRING COMMENT 'Frequency the amount is expressed in, for example ANNUAL.',
  payGrade STRING COMMENT 'Pay grade of the employee at the time of the pay record.',
  lastModifiedAt TIMESTAMP COMMENT 'Timestamp of the last change to the pay record in the source system.',
  ingestedAt TIMESTAMP COMMENT 'Timestamp when the row was written to bronze.',
  CONSTRAINT valid_amount EXPECT (annualAmountLocal IS NOT NULL AND annualAmountLocal > 0) ON VIOLATION DROP ROW
)
COMMENT 'Recurring pay component records from SuccessFactors with amounts parsed to decimals in local currency.'
AS SELECT
  CAST(userId AS STRING) AS userId,
  CONCAT('E', lpad(substr(userId, 2), 6, '0')) AS employeeId,
  CAST(timestamp_millis(try_cast(regexp_extract(startDate, '[(](-?[0-9]+)[)]', 1) AS BIGINT)) AS DATE) AS effectiveDate,
  CAST(payComponent AS STRING) AS payComponent,
  CAST(try_cast(paycompvalue AS DECIMAL(14,2)) AS DECIMAL(14,2)) AS annualAmountLocal,
  CAST(currencyCode AS STRING) AS currencyCode,
  CAST(frequency AS STRING) AS payFrequency,
  CAST(payGrade AS STRING) AS payGrade,
  timestamp_millis(try_cast(regexp_extract(lastModifiedOn, '[(](-?[0-9]+)[)]', 1) AS BIGINT)) AS lastModifiedAt,
  CAST(_ingested_at AS TIMESTAMP) AS ingestedAt
FROM STREAM(`${catalog}`.`${success_factor_bronze_schema}`.`01_emp_compensation`);

-- ---------------------------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE `${catalog}`.`${success_factor_silver_schema}`.`02_fo_department` (
  departmentCode STRING COMMENT 'Department code (DEP plus four digits).',
  departmentName STRING COMMENT 'Name of the department.',
  description STRING COMMENT 'Free text description of the department.',
  isActive BOOLEAN COMMENT 'True when the department is active.',
  costCenter STRING COMMENT 'Cost center that belongs to the department.',
  validFromDate DATE COMMENT 'Date from which the department is valid.',
  ingestedAt TIMESTAMP COMMENT 'Timestamp when the row was written to bronze.',
  CONSTRAINT valid_code EXPECT (departmentCode IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Department foundation objects from SuccessFactors.'
AS SELECT
  CAST(externalCode AS STRING) AS departmentCode,
  CAST(name AS STRING) AS departmentName,
  CAST(description AS STRING) AS description,
  CAST(status = 'A' AS BOOLEAN) AS isActive,
  CAST(cust_costCenter AS STRING) AS costCenter,
  CAST(timestamp_millis(try_cast(regexp_extract(startDate, '[(](-?[0-9]+)[)]', 1) AS BIGINT)) AS DATE) AS validFromDate,
  CAST(_ingested_at AS TIMESTAMP) AS ingestedAt
FROM STREAM(`${catalog}`.`${success_factor_bronze_schema}`.`01_fo_department`);

-- ---------------------------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE `${catalog}`.`${success_factor_silver_schema}`.`02_fo_location` (
  locationCode STRING COMMENT 'Location code (LOC plus two digits).',
  locationName STRING COMMENT 'Name of the office or site.',
  city STRING COMMENT 'City where the location is situated.',
  countryCode STRING COMMENT 'ISO 3166 alpha-3 country code.',
  timeZone STRING COMMENT 'IANA time zone of the location.',
  isActive BOOLEAN COMMENT 'True when the location is active.',
  validFromDate DATE COMMENT 'Date from which the location is valid.',
  ingestedAt TIMESTAMP COMMENT 'Timestamp when the row was written to bronze.',
  CONSTRAINT valid_code EXPECT (locationCode IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Office and site locations from SuccessFactors.'
AS SELECT
  CAST(externalCode AS STRING) AS locationCode,
  CAST(name AS STRING) AS locationName,
  CAST(custCity AS STRING) AS city,
  CAST(countryCode AS STRING) AS countryCode,
  CAST(timezone AS STRING) AS timeZone,
  CAST(status = 'A' AS BOOLEAN) AS isActive,
  CAST(timestamp_millis(try_cast(regexp_extract(startDate, '[(](-?[0-9]+)[)]', 1) AS BIGINT)) AS DATE) AS validFromDate,
  CAST(_ingested_at AS TIMESTAMP) AS ingestedAt
FROM STREAM(`${catalog}`.`${success_factor_bronze_schema}`.`01_fo_location`);

-- ---------------------------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE `${catalog}`.`${success_factor_silver_schema}`.`02_fo_job_code` (
  jobCode STRING COMMENT 'Job code (JC plus three digits).',
  jobTitle STRING COMMENT 'Title of the job.',
  jobFamily STRING COMMENT 'Job family the job belongs to.',
  payGrade STRING COMMENT 'Pay grade of the job (GR-n).',
  salaryMidpointEur DECIMAL(14,2) COMMENT 'Annual salary midpoint of the pay grade expressed in EUR.',
  isActive BOOLEAN COMMENT 'True when the job code is active.',
  ingestedAt TIMESTAMP COMMENT 'Timestamp when the row was written to bronze.',
  CONSTRAINT valid_code EXPECT (jobCode IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Job catalog from SuccessFactors with pay grade and EUR salary midpoint.'
AS SELECT
  CAST(externalCode AS STRING) AS jobCode,
  CAST(name AS STRING) AS jobTitle,
  CAST(jobFamily AS STRING) AS jobFamily,
  CAST(payGrade AS STRING) AS payGrade,
  CAST(try_cast(cust_salaryMidpointEur AS DECIMAL(14,2)) AS DECIMAL(14,2)) AS salaryMidpointEur,
  CAST(status = 'A' AS BOOLEAN) AS isActive,
  CAST(_ingested_at AS TIMESTAMP) AS ingestedAt
FROM STREAM(`${catalog}`.`${success_factor_bronze_schema}`.`01_fo_job_code`);

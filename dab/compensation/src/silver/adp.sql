-- Silver layer for ADP Workforce Now (payroll). Streaming tables read from the bronze Delta tables.
-- Bronze uses UPPER_SNAKE_CASE, YYYYMMDD integer dates and string amounts; silver conforms them to typed camelCase columns.

-- ---------------------------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE `${catalog}`.`${adp_silver_schema}`.`02_pay_runs` (
  payRunId STRING COMMENT 'Pay run identifier (PR, year and month, country code).',
  payGroupCode STRING COMMENT 'Pay group the run was executed for (country code plus -M for monthly).',
  periodStartDate DATE COMMENT 'First day of the pay period.',
  periodEndDate DATE COMMENT 'Last day of the pay period.',
  payDate DATE COMMENT 'Date the employees are paid.',
  runStatusCode STRING COMMENT 'Status of the pay run (C completed).',
  totalGrossAmountLocal DECIMAL(18,2) COMMENT 'Total gross pay of the run in local currency.',
  createdAt TIMESTAMP COMMENT 'Timestamp when the pay run was created in ADP.',
  ingestedAt TIMESTAMP COMMENT 'Timestamp when the row was written to bronze.',
  CONSTRAINT valid_run EXPECT (payRunId IS NOT NULL AND payDate IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Pay run headers from ADP per pay group and month with typed dates and amounts.'
AS SELECT
  CAST(PAY_RUN_ID AS STRING) AS payRunId,
  CAST(PAY_GROUP_CD AS STRING) AS payGroupCode,
  try_to_date(CAST(PERIOD_START_DT AS STRING), 'yyyyMMdd') AS periodStartDate,
  try_to_date(CAST(PERIOD_END_DT AS STRING), 'yyyyMMdd') AS periodEndDate,
  try_to_date(CAST(PAY_DT AS STRING), 'yyyyMMdd') AS payDate,
  CAST(RUN_STATUS_CD AS STRING) AS runStatusCode,
  CAST(try_cast(TOTAL_GROSS_AMT AS DECIMAL(18,2)) AS DECIMAL(18,2)) AS totalGrossAmountLocal,
  try_cast(CREATED_TS AS TIMESTAMP) AS createdAt,
  CAST(_ingested_at AS TIMESTAMP) AS ingestedAt
FROM STREAM(`${catalog}`.`${adp_bronze_schema}`.`01_pay_runs`);

-- ---------------------------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE `${catalog}`.`${adp_silver_schema}`.`02_pay_statements` (
  statementId STRING COMMENT 'Pay statement identifier.',
  payRunId STRING COMMENT 'Pay run the statement belongs to, references the pay runs table.',
  employeeId STRING COMMENT 'Conformed employee identifier (E plus six digits), derived from the ADP associate file number.',
  adpWorkerId STRING COMMENT 'ADP internal worker identifier.',
  payDate DATE COMMENT 'Date the employee is paid.',
  grossPayAmount DECIMAL(14,2) COMMENT 'Gross pay for the period in local currency.',
  netPayAmount DECIMAL(14,2) COMMENT 'Net pay after tax and deductions in local currency.',
  totalTaxAmount DECIMAL(14,2) COMMENT 'Total tax withheld in local currency.',
  totalDeductionAmount DECIMAL(14,2) COMMENT 'Total other deductions in local currency.',
  currencyCode STRING COMMENT 'ISO 4217 currency code of all amounts on the statement.',
  hoursWorked DECIMAL(6,2) COMMENT 'Paid hours in the period.',
  ingestedAt TIMESTAMP COMMENT 'Timestamp when the row was written to bronze.',
  CONSTRAINT valid_keys EXPECT (statementId IS NOT NULL AND employeeId IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_gross EXPECT (grossPayAmount IS NOT NULL AND grossPayAmount >= 0) ON VIOLATION DROP ROW
)
COMMENT 'Pay statements from ADP: one row per employee and pay run with typed amounts in local currency.'
AS SELECT
  CAST(STMT_ID AS STRING) AS statementId,
  CAST(PAY_RUN_ID AS STRING) AS payRunId,
  CONCAT('E', lpad(CAST(ASSOC_FILE_NBR AS STRING), 6, '0')) AS employeeId,
  CAST(WORKER_ID AS STRING) AS adpWorkerId,
  try_to_date(CAST(PAY_DT AS STRING), 'yyyyMMdd') AS payDate,
  CAST(try_cast(GROSS_PAY_AMT AS DECIMAL(14,2)) AS DECIMAL(14,2)) AS grossPayAmount,
  CAST(try_cast(NET_PAY_AMT AS DECIMAL(14,2)) AS DECIMAL(14,2)) AS netPayAmount,
  CAST(try_cast(TOTAL_TAX_AMT AS DECIMAL(14,2)) AS DECIMAL(14,2)) AS totalTaxAmount,
  CAST(try_cast(TOTAL_DED_AMT AS DECIMAL(14,2)) AS DECIMAL(14,2)) AS totalDeductionAmount,
  CAST(CURR_CD AS STRING) AS currencyCode,
  CAST(try_cast(HOURS_WORKED AS DECIMAL(6,2)) AS DECIMAL(6,2)) AS hoursWorked,
  CAST(_ingested_at AS TIMESTAMP) AS ingestedAt
FROM STREAM(`${catalog}`.`${adp_bronze_schema}`.`01_pay_statements`);

-- ---------------------------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE `${catalog}`.`${adp_silver_schema}`.`02_earning_codes` (
  earningCode STRING COMMENT 'Earning code, for example REG or BON.',
  earningDescription STRING COMMENT 'Description of the earning type.',
  earningTypeCode STRING COMMENT 'Earning category: R regular, O overtime, B bonus, A allowance.',
  isTaxable BOOLEAN COMMENT 'True when the earning is taxable.',
  isActive BOOLEAN COMMENT 'True when the earning code can still be used.',
  ingestedAt TIMESTAMP COMMENT 'Timestamp when the row was written to bronze.',
  CONSTRAINT valid_code EXPECT (earningCode IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Earning code reference list from ADP.'
AS SELECT
  CAST(EARN_CD AS STRING) AS earningCode,
  CAST(EARN_DESC AS STRING) AS earningDescription,
  CAST(EARN_TYPE_CD AS STRING) AS earningTypeCode,
  CAST(TAXABLE_FLG = 'Y' AS BOOLEAN) AS isTaxable,
  CAST(ACTIVE_FLG = 'Y' AS BOOLEAN) AS isActive,
  CAST(_ingested_at AS TIMESTAMP) AS ingestedAt
FROM STREAM(`${catalog}`.`${adp_bronze_schema}`.`01_earning_codes`);

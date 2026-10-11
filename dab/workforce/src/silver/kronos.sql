-- Silver layer for Kronos (time and absence). Streaming tables read from the bronze Delta tables.
-- Bronze uses abbreviated names, US formatted date strings and tinyint flags; silver conforms them to typed camelCase columns.

-- ---------------------------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE `${catalog}`.`${kronos_silver_schema}`.`02_timecards` (
  timecardId BIGINT COMMENT 'Kronos timecard identifier.',
  employeeId STRING COMMENT 'Conformed employee identifier (E plus six digits), derived from the Kronos person number.',
  weekStartDate DATE COMMENT 'Monday of the week the timecard covers.',
  regularHours DECIMAL(5,2) COMMENT 'Regular hours worked in the week.',
  overtimeHours DECIMAL(5,2) COMMENT 'Overtime hours worked in the week.',
  paidTimeOffHours DECIMAL(5,2) COMMENT 'Paid time off hours taken in the week.',
  utcOffset STRING COMMENT 'UTC offset of the employee time zone, for example +01:00.',
  isApproved BOOLEAN COMMENT 'True when the manager has approved the timecard.',
  lastUpdatedAt TIMESTAMP COMMENT 'Timestamp of the last update in Kronos.',
  ingestedAt TIMESTAMP COMMENT 'Timestamp when the row was written to bronze.',
  CONSTRAINT valid_keys EXPECT (timecardId IS NOT NULL AND employeeId IS NOT NULL AND weekStartDate IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_hours EXPECT (regularHours >= 0 AND overtimeHours >= 0) ON VIOLATION DROP ROW
)
COMMENT 'Weekly timecard totals from Kronos for hourly staff with typed dates and hours.'
AS SELECT
  CAST(tc_id AS BIGINT) AS timecardId,
  CONCAT('E', lpad(person_num, 6, '0')) AS employeeId,
  try_to_date(wk_start_dt, 'MM/dd/yyyy') AS weekStartDate,
  CAST(reg_hrs AS DECIMAL(5,2)) AS regularHours,
  CAST(ot_hrs AS DECIMAL(5,2)) AS overtimeHours,
  CAST(pto_hrs AS DECIMAL(5,2)) AS paidTimeOffHours,
  CAST(tz_off AS STRING) AS utcOffset,
  CAST(approved_flg = 1 AS BOOLEAN) AS isApproved,
  try_to_timestamp(last_upd_ts, 'MM/dd/yyyy HH:mm:ss') AS lastUpdatedAt,
  CAST(_ingested_at AS TIMESTAMP) AS ingestedAt
FROM STREAM(`${catalog}`.`${kronos_bronze_schema}`.`01_timecards`);

-- ---------------------------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE `${catalog}`.`${kronos_silver_schema}`.`02_leave_requests` (
  leaveRequestId BIGINT COMMENT 'Kronos leave request identifier.',
  employeeId STRING COMMENT 'Conformed employee identifier of the requester.',
  absenceTypeId INT COMMENT 'Absence type, references the absence types table.',
  startDate DATE COMMENT 'First day of the requested leave.',
  endDate DATE COMMENT 'Last day of the requested leave.',
  requestedDays DECIMAL(5,2) COMMENT 'Number of days requested.',
  statusCode STRING COMMENT 'Request status: APR approved, REJ rejected, CXL cancelled, PND pending.',
  submittedAt TIMESTAMP COMMENT 'Timestamp when the request was submitted.',
  approverEmployeeId STRING COMMENT 'Conformed employee identifier of the approver.',
  ingestedAt TIMESTAMP COMMENT 'Timestamp when the row was written to bronze.',
  CONSTRAINT valid_keys EXPECT (leaveRequestId IS NOT NULL AND employeeId IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_dates EXPECT (startDate IS NOT NULL AND endDate >= startDate) ON VIOLATION DROP ROW
)
COMMENT 'Leave requests from Kronos with typed dates and conformed employee identifiers.'
AS SELECT
  CAST(req_id AS BIGINT) AS leaveRequestId,
  CONCAT('E', lpad(person_num, 6, '0')) AS employeeId,
  CAST(abs_type_id AS INT) AS absenceTypeId,
  try_to_date(from_dt, 'MM/dd/yyyy') AS startDate,
  try_to_date(to_dt, 'MM/dd/yyyy') AS endDate,
  CAST(days_req AS DECIMAL(5,2)) AS requestedDays,
  CAST(status_cd AS STRING) AS statusCode,
  CAST(submitted_ts AS TIMESTAMP) AS submittedAt,
  CONCAT('E', lpad(approver_num, 6, '0')) AS approverEmployeeId,
  CAST(_ingested_at AS TIMESTAMP) AS ingestedAt
FROM STREAM(`${catalog}`.`${kronos_bronze_schema}`.`01_leave_requests`);

-- ---------------------------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE `${catalog}`.`${kronos_silver_schema}`.`02_absence_types` (
  absenceTypeId INT COMMENT 'Absence type identifier.',
  absenceCode STRING COMMENT 'Short code of the absence type.',
  absenceName STRING COMMENT 'Name of the absence type.',
  isPaid BOOLEAN COMMENT 'True when the absence is paid.',
  isAccrued BOOLEAN COMMENT 'True when the absence draws from an accrued balance.',
  ingestedAt TIMESTAMP COMMENT 'Timestamp when the row was written to bronze.',
  CONSTRAINT valid_id EXPECT (absenceTypeId IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Absence type reference list from Kronos.'
AS SELECT
  CAST(abs_type_id AS INT) AS absenceTypeId,
  CAST(abs_cd AS STRING) AS absenceCode,
  CAST(abs_nm AS STRING) AS absenceName,
  CAST(paid_flg = 1 AS BOOLEAN) AS isPaid,
  CAST(accrual_flg = 1 AS BOOLEAN) AS isAccrued,
  CAST(_ingested_at AS TIMESTAMP) AS ingestedAt
FROM STREAM(`${catalog}`.`${kronos_bronze_schema}`.`01_absence_types`);

-- Silver layer for Culture Amp (engagement). Streaming tables read from the bronze Delta tables.
-- Bronze is snake_case with all values as strings and a long key-value response format; silver conforms to typed camelCase columns.

-- ---------------------------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE `${catalog}`.`${culture_amp_silver_schema}`.`02_surveys` (
  surveyId STRING COMMENT 'Culture Amp survey identifier.',
  surveyName STRING COMMENT 'Name of the survey.',
  surveyType STRING COMMENT 'Survey type: pulse or annual.',
  openDate DATE COMMENT 'Date the survey opened for responses.',
  closeDate DATE COMMENT 'Date the survey closed.',
  isAnonymous BOOLEAN COMMENT 'True when responses are collected anonymously.',
  ingestedAt TIMESTAMP COMMENT 'Timestamp when the row was written to bronze.',
  CONSTRAINT valid_id EXPECT (surveyId IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Engagement survey definitions from Culture Amp.'
AS SELECT
  CAST(survey_id AS STRING) AS surveyId,
  CAST(survey_name AS STRING) AS surveyName,
  CAST(survey_type AS STRING) AS surveyType,
  try_to_date(open_date, 'yyyy-MM-dd') AS openDate,
  try_to_date(close_date, 'yyyy-MM-dd') AS closeDate,
  CAST(try_cast(anonymous AS BOOLEAN) AS BOOLEAN) AS isAnonymous,
  CAST(_ingested_at AS TIMESTAMP) AS ingestedAt
FROM STREAM(`${catalog}`.`${culture_amp_bronze_schema}`.`01_surveys`);

-- ---------------------------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE `${catalog}`.`${culture_amp_silver_schema}`.`02_questions` (
  questionId STRING COMMENT 'Culture Amp question identifier.',
  questionText STRING COMMENT 'Text of the question as shown to respondents.',
  theme STRING COMMENT 'Engagement theme the question measures, for example enps or wellbeing.',
  responseScale STRING COMMENT 'Response scale: scale_0_10, likert_5 or free_text.',
  ingestedAt TIMESTAMP COMMENT 'Timestamp when the row was written to bronze.',
  CONSTRAINT valid_id EXPECT (questionId IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Question library from Culture Amp with theme and response scale.'
AS SELECT
  CAST(question_id AS STRING) AS questionId,
  CAST(question_text AS STRING) AS questionText,
  CAST(theme AS STRING) AS theme,
  CAST(scale AS STRING) AS responseScale,
  CAST(_ingested_at AS TIMESTAMP) AS ingestedAt
FROM STREAM(`${catalog}`.`${culture_amp_bronze_schema}`.`01_questions`);

-- ---------------------------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE `${catalog}`.`${culture_amp_silver_schema}`.`02_responses` (
  responseId STRING COMMENT 'Unique identifier of the response.',
  surveyId STRING COMMENT 'Survey the response belongs to, references the surveys table.',
  questionId STRING COMMENT 'Question that was answered, references the questions table.',
  respondentRef STRING COMMENT 'Pseudonymised respondent reference, stable across surveys.',
  employeeId STRING COMMENT 'Conformed employee identifier of the respondent. Restricted: use only in aggregates.',
  numericValue INT COMMENT 'Numeric answer for scale questions. Null for free text answers.',
  textValue STRING COMMENT 'Free text answer. Null for scale questions.',
  submittedDate DATE COMMENT 'Date the response was submitted.',
  ingestedAt TIMESTAMP COMMENT 'Timestamp when the row was written to bronze.',
  CONSTRAINT valid_keys EXPECT (responseId IS NOT NULL AND surveyId IS NOT NULL AND questionId IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Survey responses from Culture Amp: one row per respondent and question, with string values split into numeric and text.'
AS SELECT
  CAST(response_id AS STRING) AS responseId,
  CAST(survey_id AS STRING) AS surveyId,
  CAST(question_id AS STRING) AS questionId,
  CAST(respondent_ref AS STRING) AS respondentRef,
  CAST(employee_ref AS STRING) AS employeeId,
  try_cast(response_value AS INT) AS numericValue,
  CASE WHEN try_cast(response_value AS INT) IS NULL THEN response_value END AS textValue,
  try_to_date(submitted_date, 'yyyy-MM-dd') AS submittedDate,
  CAST(_ingested_at AS TIMESTAMP) AS ingestedAt
FROM STREAM(`${catalog}`.`${culture_amp_bronze_schema}`.`01_responses`);

-- ---------------------------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE `${catalog}`.`${culture_amp_silver_schema}`.`02_enps_scores` (
  recordId STRING COMMENT 'Unique identifier of the eNPS record.',
  surveyId STRING COMMENT 'Survey the score was collected in, references the surveys table.',
  employeeId STRING COMMENT 'Conformed employee identifier of the respondent. Restricted: use only in aggregates.',
  enpsScore INT COMMENT 'Employee Net Promoter Score answer between 0 and 10.',
  enpsCategory STRING COMMENT 'Category derived from the score: promoter (9-10), passive (7-8) or detractor (0-6).',
  ingestedAt TIMESTAMP COMMENT 'Timestamp when the row was written to bronze.',
  CONSTRAINT valid_score EXPECT (enpsScore BETWEEN 0 AND 10) ON VIOLATION DROP ROW
)
COMMENT 'Employee Net Promoter Score per respondent and survey from Culture Amp.'
AS SELECT
  CAST(record_id AS STRING) AS recordId,
  CAST(survey_id AS STRING) AS surveyId,
  CAST(employee_ref AS STRING) AS employeeId,
  try_cast(enps_score AS INT) AS enpsScore,
  CAST(category AS STRING) AS enpsCategory,
  CAST(_ingested_at AS TIMESTAMP) AS ingestedAt
FROM STREAM(`${catalog}`.`${culture_amp_bronze_schema}`.`01_enps_scores`);

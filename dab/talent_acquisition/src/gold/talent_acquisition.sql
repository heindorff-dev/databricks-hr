-- Gold data product: Talent acquisition. Materialized views over the Greenhouse silver tables.

-- ---------------------------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW `${catalog}`.`${talent_acquisition_gold_schema}`.`03_fact_recruiting_funnel`
COMMENT 'Recruiting funnel per job requisition: applications, rejections, hires and offers. Grain: one row per job.'
AS
WITH app AS (
  SELECT
    jobId,
    count(*) AS applicationCount,
    count_if(status = 'active') AS activeCount,
    count_if(status = 'rejected') AS rejectedCount,
    count_if(status = 'hired') AS hiredCount
  FROM `${catalog}`.`${greenhouse_silver_schema}`.`02_applications`
  GROUP BY jobId
),
off AS (
  SELECT
    a.jobId,
    count(DISTINCT o.applicationId) AS offerCount,
    count_if(o.offerStatus = 'accepted') AS acceptedOfferCount,
    avg(datediff(o.resolvedAt, o.sentAt)) AS avgDaysToOfferResponse
  FROM `${catalog}`.`${greenhouse_silver_schema}`.`02_offers` o
  JOIN `${catalog}`.`${greenhouse_silver_schema}`.`02_applications` a ON a.applicationId = o.applicationId
  GROUP BY a.jobId
)
SELECT
  j.jobId,
  j.requisitionId,
  j.jobTitle,
  j.departmentName,
  j.officeName,
  j.employmentType,
  j.status AS jobStatus,
  j.openedAt,
  j.closedAt,
  coalesce(app.applicationCount, 0) AS applicationCount,
  coalesce(app.activeCount, 0) AS activeCount,
  coalesce(app.rejectedCount, 0) AS rejectedCount,
  coalesce(app.hiredCount, 0) AS hiredCount,
  coalesce(off.offerCount, 0) AS offerCount,
  coalesce(off.acceptedOfferCount, 0) AS acceptedOfferCount,
  round(off.avgDaysToOfferResponse, 1) AS avgDaysToOfferResponse
FROM `${catalog}`.`${greenhouse_silver_schema}`.`02_jobs` j
LEFT JOIN app ON app.jobId = j.jobId
LEFT JOIN off ON off.jobId = j.jobId;

-- ---------------------------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW `${catalog}`.`${talent_acquisition_gold_schema}`.`03_agg_source_effectiveness`
COMMENT 'Candidate source effectiveness per quarter: applications, hires and hire rate per sourcing channel. Grain: quarter x source.'
AS
SELECT
  date_trunc('QUARTER', appliedAt) AS applicationQuarter,
  sourceName,
  count(*) AS applicationCount,
  count_if(status = 'hired') AS hiredCount,
  count_if(status = 'rejected') AS rejectedCount,
  round(count_if(status = 'hired') / count(*), 4) AS hireRate
FROM `${catalog}`.`${greenhouse_silver_schema}`.`02_applications`
GROUP BY date_trunc('QUARTER', appliedAt), sourceName;

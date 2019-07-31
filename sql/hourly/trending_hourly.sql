-- STEP 1: Retrieve core data --
-- 7743986 rows --
DROP TABLE IF EXISTS central_insights_sandbox.trending_hourly_step1;
CREATE TABLE central_insights_sandbox.trending_hourly_step1 AS

SELECT DATE_TRUNC('hr', CAST(event_datetime_min AS timestamp)) AS datehour, destination, top_level_editorial_object AS tleo, COUNT(DISTINCT play_id)
FROM audience.audience_activity_daily_summary_enriched
WHERE destination IN ('PS_IPLAYER', 'PS_SOUNDS')
AND playback_time_total >= 3
AND clip_id = 'null'
GROUP BY 1, 2, 3;

-- STEP 2: Assign TLEO to programme metadata --
-- 3258609 rows --
DROP TABLE IF EXISTS central_insights_sandbox.trending_hourly_step2;
CREATE TABLE central_insights_sandbox.trending_hourly_step2 AS
SELECT

CASE
WHEN brand_title != 'null' THEN brand_title
WHEN series_title != 'null' THEN series_title
WHEN episode_title != 'null' THEN episode_title
WHEN presentation_title != 'null' THEN presentation_title
ELSE clip_title
END AS tleo,
pips_genre_level_1_names

FROM prez.scv_vmb
WHERE brand_title IN (SELECT tleo FROM central_insights_sandbox.trending_hourly_step1)
OR series_title IN (SELECT tleo FROM central_insights_sandbox.trending_hourly_step1)
OR episode_title IN (SELECT tleo FROM central_insights_sandbox.trending_hourly_step1)
OR presentation_title IN (SELECT tleo FROM central_insights_sandbox.trending_hourly_step1)
OR clip_title IN (SELECT tleo FROM central_insights_sandbox.trending_hourly_step1);

-- STEP 3: Pick most common genre for each TLEO --
-- 645421 rows --
-- 13.5 seconds --
DROP TABLE IF EXISTS central_insights_sandbox.trending_hourly_step3;
CREATE TABLE central_insights_sandbox.trending_hourly_step3 AS

SELECT DISTINCT a.*,
FIRST_VALUE(b.pips_genre_level_1_names IGNORE NULLS)
OVER (PARTITION BY b.tleo ORDER BY COUNT(pips_genre_level_1_names) DESC ROWS BETWEEN unbounded preceding AND unbounded following) AS genre
FROM central_insights_sandbox.trending_hourly_step1 AS a
LEFT JOIN central_insights_sandbox.trending_hourly_step2 AS b ON a.tleo = b.tleo
GROUP BY 1, 2, 3, 4, b.pips_genre_level_1_names, b.tleo;

-- STEP 4: Join on to dates table so we have a value each day for every brand, even if it is 0 (essentially a cross join) --
-- 1305348 rows --
DROP TABLE IF EXISTS central_insights_sandbox.trending_hourly_step4;
CREATE TABLE central_insights_sandbox.trending_hourly_step4 AS

WITH b AS (
  SELECT DISTINCT destination, tleo, genre
  FROM central_insights_sandbox.trending_hourly_step3
)

SELECT DISTINCT a.datehour, b.destination, b.tleo, b.genre
FROM central_insights_sandbox.trending_hourly_step1 AS a, b;

-- STEP 5: Retrieve streams --
-- 1305348 rows --
DROP TABLE IF EXISTS central_insights_sandbox.trending_hourly_step5;
CREATE TABLE central_insights_sandbox.trending_hourly_step5 AS

SELECT a.*,

CASE
WHEN count < 10 THEN NULL
ELSE count
END AS streams

FROM central_insights_sandbox.trending_hourly_step4 AS a
LEFT JOIN central_insights_sandbox.trending_hourly_step3 AS b
ON a.datehour = b.datehour
AND a.destination = b.destination
AND a.tleo = b.tleo
AND a.genre = b.genre;

-- STEP 6 --
-- 1305348 rows --
DROP TABLE IF EXISTS central_insights_sandbox.trending_hourly_step6;
CREATE TABLE central_insights_sandbox.trending_hourly_step6 AS

SELECT datehour, destination, tleo,

CASE
WHEN CHARINDEX(';', genre) > 0
THEN LEFT(genre, CHARINDEX(';', genre) - 1)
ELSE genre
END AS genre,

streams,

COUNT(streams) OVER (PARTITION BY destination, tleo, genre ORDER BY datehour
ROWS BETWEEN 168 PRECEDING AND 1 PRECEDING) AS non_nulls,

AVG(CAST(streams AS float)) OVER (PARTITION BY destination, tleo, genre ORDER BY datehour
ROWS BETWEEN 672 PRECEDING AND 1 PRECEDING) AS brand_avg,

STDDEV_SAMP(CAST(streams AS float)) OVER (PARTITION BY destination, tleo, genre ORDER BY datehour
ROWS BETWEEN 672 PRECEDING AND 1 PRECEDING) AS brand_sd,

AVG(CAST(streams AS float)) OVER (PARTITION BY datehour) AS overall_avg,
STDDEV_SAMP(CAST(streams AS float)) OVER (PARTITION BY datehour) AS overall_sd,

CASE
WHEN brand_sd IS NULL OR brand_sd = 0 OR non_nulls < 160 -- At least 95% of hours --
THEN (streams - overall_avg) / overall_sd
ELSE (streams - brand_avg) / brand_sd
END AS trend_score

FROM central_insights_sandbox.trending_hourly_step5;

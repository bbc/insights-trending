/*
	Title: TRENDING
	Description:  To identify shows that are suddenly popular with respect to previous performance
  Dependency Scripts: None
  Dependency Tables(s): audience.audience_activity_daily_summary_enriched
	Output Table(s): central_insights_sandbox.TRENDING
*/


BEGIN; --Starts a transaction in redshift

  -- Specify the default schema for this transaction
  -- This allows us to change only this and test the script in another schema
  SET LOCAL search_path = 'central_insights_sandbox';

  /*
    ===============================
    Start Recording History Record
    ===============================
    Tracking the history of when run, who by and how long it took
   */
  -- Record start running of segmentation to temp table (locking issues)
  DROP TABLE IF EXISTS TEMP_TRENDING_History;
  CREATE TABLE IF NOT EXISTS TEMP_TRENDING_History (
    Script VARCHAR(1000),
    Target_Table VARCHAR(1000),
    Updated_By   VARCHAR(1000),
    Updated_Date DATE,
    Started      TIMESTAMP,
    Finished     TIMESTAMP,
    STATUS       VARCHAR(100)
  );

  INSERT INTO TEMP_TRENDING_History VALUES (
    'TRENDING', --TODO: update
    'TRENDING', --TODO: update
    CURRENT_USER,
    NULL,
    GETDATE(), -- track start time
    NULL,
    'RUNNING'
  );

  /*
    ===============================
    Logic
    ===============================
    Put the logic of your script here
    Be sure to include short comments to what each step is doing
    You may also which to break the logic up into blocks with larger comment blocks like this one
    Think about the following:
    - Large joins
        - These are slow, perhaps select into a temp table first e.g. joining with csactivityview
    - Dropping tables
        - This is not rolled back if the script fails (unlike delete)
        - Also it will fail if a user other than the table owner tries to run
    - Temporary tables
        - It's a good idea to use real TEMPORARY tables if you only need the table during the script
   */

  /*
     ===============================
     PART 1: OVERALL TRENDING
     ===============================
  */

   -- STEP 1: Core data --
   DROP TABLE IF EXISTS central_insights_sandbox.trending_step1;
   CREATE TABLE central_insights_sandbox.trending_step1 AS

   SELECT date_of_event AS date, destination, top_level_editorial_object AS tleo, COUNT(DISTINCT play_id)
   FROM audience.audience_activity_daily_summary_enriched
   WHERE destination IN ('PS_IPLAYER', 'PS_SOUNDS')
   AND playback_time_total >= 3
   AND clip_id = 'null'
   AND date BETWEEN DATEADD('day', -28, CURRENT_DATE)::date AND DATEADD('day', -1, CURRENT_DATE)::date
   GROUP BY 1, 2, 3;

   -- STEP 2: Assign TLEO to programme metadata --
   DROP TABLE IF EXISTS central_insights_sandbox.trending_step2;
   CREATE TABLE central_insights_sandbox.trending_step2 AS
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
   WHERE brand_title IN (SELECT tleo FROM central_insights_sandbox.trending_step1)
   OR series_title IN (SELECT tleo FROM central_insights_sandbox.trending_step1)
   OR episode_title IN (SELECT tleo FROM central_insights_sandbox.trending_step1)
   OR presentation_title IN (SELECT tleo FROM central_insights_sandbox.trending_step1)
   OR clip_title IN (SELECT tleo FROM central_insights_sandbox.trending_step1);

   -- STEP 3: Pick most common genre for each TLEO --
   DROP TABLE IF EXISTS central_insights_sandbox.trending_step3;
   CREATE TABLE central_insights_sandbox.trending_step3 AS

   SELECT DISTINCT a.*,
   FIRST_VALUE(b.pips_genre_level_1_names IGNORE NULLS)
   OVER (PARTITION BY b.tleo ORDER BY COUNT(pips_genre_level_1_names) DESC ROWS BETWEEN unbounded preceding AND unbounded following) AS genre
   FROM central_insights_sandbox.trending_step1 AS a
   LEFT JOIN central_insights_sandbox.trending_step2 AS b ON a.tleo = b.tleo
   GROUP BY 1, 2, 3, 4, b.pips_genre_level_1_names, b.tleo;

   -- STEP 4: Join on to dates table so we have a value each day for every brand, even if it is 0 (essentially a cross join) --
   DROP TABLE IF EXISTS central_insights_sandbox.trending_step4;
   CREATE TABLE central_insights_sandbox.trending_step4 AS

   WITH b AS (
     SELECT DISTINCT destination, tleo, genre
     FROM central_insights_sandbox.trending_step3
   )

   SELECT DISTINCT a.date, b.destination, b.tleo, b.genre
   FROM central_insights_sandbox.trending_step1 AS a, b;

   -- STEP 5: Retrieve streams --
   DROP TABLE IF EXISTS central_insights_sandbox.trending_step5;
   CREATE TABLE central_insights_sandbox.trending_step5 AS

   SELECT a.*,

   CASE
   WHEN count < 100 THEN NULL
   ELSE count
   END AS streams

   FROM central_insights_sandbox.trending_step4 AS a
   LEFT JOIN central_insights_sandbox.trending_step3 AS b
   ON a.date = b.date
   AND a.destination = b.destination
   AND a.tleo = b.tleo
   AND a.genre = b.genre;

   -- STEP 6 --
   DROP TABLE IF EXISTS central_insights_sandbox.trending_step6;
   CREATE TABLE central_insights_sandbox.trending_step6 AS

   SELECT date, destination, tleo,

   CASE
   WHEN CHARINDEX(';', genre) > 0
   THEN LEFT(genre, CHARINDEX(';', genre) - 1)
   ELSE genre
   END AS genre,

   streams,

   COUNT(streams) OVER (PARTITION BY destination, tleo, genre ORDER BY date
   ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING) AS non_nulls,

   AVG(CAST(streams AS float)) OVER (PARTITION BY destination, tleo, genre ORDER BY date
   ROWS BETWEEN 28 PRECEDING AND 1 PRECEDING) AS brand_avg28,

   STDDEV_SAMP(CAST(streams AS float)) OVER (PARTITION BY destination, tleo, genre ORDER BY date
   ROWS BETWEEN 28 PRECEDING AND 1 PRECEDING) AS brand_sd28,

   AVG(CAST(streams AS float)) OVER (PARTITION BY date) AS overall_avg28,
   STDDEV_SAMP(CAST(streams AS float)) OVER (PARTITION BY date) AS overall_sd28,

   CASE
   WHEN brand_sd28 IS NULL OR brand_sd28 = 0 OR non_nulls < 7
   THEN (streams - overall_avg28) / overall_sd28
   ELSE (streams - brand_avg28) / brand_sd28
   END AS trend_score

   FROM central_insights_sandbox.trending_step5;

   -- STEP 7 --
   DROP TABLE IF EXISTS central_insights_sandbox.trending_step7;
   CREATE TABLE central_insights_sandbox.trending_step7 AS

   SELECT * FROM central_insights_sandbox.trending_step6
   WHERE date = DATEADD('day', -1, CURRENT_DATE)::date;

   /*
      ===============================
      PART 2: ON-DEMAND TRENDING
      ===============================
   */

   -- STEP 1: Core data --
   DROP TABLE IF EXISTS central_insights_sandbox.trending_ondemand_step1;
   CREATE TABLE central_insights_sandbox.trending_ondemand_step1 AS

   SELECT date_of_event AS date, destination, top_level_editorial_object AS tleo, COUNT(DISTINCT play_id)
   FROM audience.audience_activity_daily_summary_enriched
   WHERE destination IN ('PS_IPLAYER', 'PS_SOUNDS')
   AND playback_time_total >= 3
   AND clip_id = 'null'
   AND date BETWEEN DATEADD('day', -28, CURRENT_DATE)::date AND DATEADD('day', -1, CURRENT_DATE)::date
   AND broadcast_type = 'Clip'
   GROUP BY 1, 2, 3;

   -- STEP 2: Assign TLEO to programme metadata --
   DROP TABLE IF EXISTS central_insights_sandbox.trending_ondemand_step2;
   CREATE TABLE central_insights_sandbox.trending_ondemand_step2 AS
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
   WHERE brand_title IN (SELECT tleo FROM central_insights_sandbox.trending_ondemand_step1)
   OR series_title IN (SELECT tleo FROM central_insights_sandbox.trending_ondemand_step1)
   OR episode_title IN (SELECT tleo FROM central_insights_sandbox.trending_ondemand_step1)
   OR presentation_title IN (SELECT tleo FROM central_insights_sandbox.trending_ondemand_step1)
   OR clip_title IN (SELECT tleo FROM central_insights_sandbox.trending_ondemand_step1);

   -- STEP 3: Pick most common genre for each TLEO --
   DROP TABLE IF EXISTS central_insights_sandbox.trending_ondemand_step3;
   CREATE TABLE central_insights_sandbox.trending_ondemand_step3 AS

   SELECT DISTINCT a.*,
   FIRST_VALUE(b.pips_genre_level_1_names IGNORE NULLS)
   OVER (PARTITION BY b.tleo ORDER BY COUNT(pips_genre_level_1_names) DESC ROWS BETWEEN unbounded preceding AND unbounded following) AS genre
   FROM central_insights_sandbox.trending_ondemand_step1 AS a
   LEFT JOIN central_insights_sandbox.trending_ondemand_step2 AS b ON a.tleo = b.tleo
   GROUP BY 1, 2, 3, 4, b.pips_genre_level_1_names, b.tleo;

   -- STEP 4: Join on to dates table so we have a value each day for every brand, even if it is 0 (essentially a cross join) --
   DROP TABLE IF EXISTS central_insights_sandbox.trending_ondemand_step4;
   CREATE TABLE central_insights_sandbox.trending_ondemand_step4 AS

   WITH b AS (
     SELECT DISTINCT destination, tleo, genre
     FROM central_insights_sandbox.trending_ondemand_step3
   )

   SELECT DISTINCT a.date, b.destination, b.tleo, b.genre
   FROM central_insights_sandbox.trending_ondemand_step1 AS a, b;

   -- STEP 5: Retrieve streams --
   DROP TABLE IF EXISTS central_insights_sandbox.trending_ondemand_step5;
   CREATE TABLE central_insights_sandbox.trending_ondemand_step5 AS

   SELECT a.*,

   CASE
   WHEN count < 100 THEN NULL
   ELSE count
   END AS streams

   FROM central_insights_sandbox.trending_ondemand_step4 AS a
   LEFT JOIN central_insights_sandbox.trending_ondemand_step3 AS b
   ON a.date = b.date
   AND a.destination = b.destination
   AND a.tleo = b.tleo
   AND a.genre = b.genre;

   -- STEP 6 --
   DROP TABLE IF EXISTS central_insights_sandbox.trending_ondemand_step6;
   CREATE TABLE central_insights_sandbox.trending_ondemand_step6 AS

   SELECT date, destination, tleo,

   CASE
   WHEN CHARINDEX(';', genre) > 0
   THEN LEFT(genre, CHARINDEX(';', genre) - 1)
   ELSE genre
   END AS genre,

   streams,

   COUNT(streams) OVER (PARTITION BY destination, tleo, genre ORDER BY date
   ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING) AS non_nulls,

   AVG(CAST(streams AS float)) OVER (PARTITION BY destination, tleo, genre ORDER BY date
   ROWS BETWEEN 28 PRECEDING AND 1 PRECEDING) AS brand_avg28,

   STDDEV_SAMP(CAST(streams AS float)) OVER (PARTITION BY destination, tleo, genre ORDER BY date
   ROWS BETWEEN 28 PRECEDING AND 1 PRECEDING) AS brand_sd28,

   AVG(CAST(streams AS float)) OVER (PARTITION BY date) AS overall_avg28,
   STDDEV_SAMP(CAST(streams AS float)) OVER (PARTITION BY date) AS overall_sd28,

   CASE
   WHEN brand_sd28 IS NULL OR brand_sd28 = 0 OR non_nulls < 7
   THEN (streams - overall_avg28) / overall_sd28
   ELSE (streams - brand_avg28) / brand_sd28
   END AS trend_score

   FROM central_insights_sandbox.trending_ondemand_step5;

   -- STEP 7 --
   DROP TABLE IF EXISTS central_insights_sandbox.trending_ondemand_step7;
   CREATE TABLE central_insights_sandbox.trending_ondemand_step7 AS

   SELECT * FROM central_insights_sandbox.trending_ondemand_step6
   WHERE date = DATEADD('day', -1, CURRENT_DATE)::date;

   /*
      ===============================
      PART 3: YOUTH TRENDING
      ===============================
   */

   -- STEP 1: Core data --
   DROP TABLE IF EXISTS central_insights_sandbox.trending_young_step1;
   CREATE TABLE central_insights_sandbox.trending_young_step1 AS

   SELECT date_of_event AS date, destination, top_level_editorial_object AS tleo, COUNT(DISTINCT play_id)
   FROM audience.audience_activity_daily_summary_enriched
   WHERE destination IN ('PS_IPLAYER', 'PS_SOUNDS')
   AND playback_time_total >= 3
   AND clip_id = 'null'
   AND date BETWEEN DATEADD('day', -28, CURRENT_DATE)::date AND DATEADD('day', -1, CURRENT_DATE)::date
   AND age_range IN ('16-19', '20-24')
   GROUP BY 1, 2, 3;

   -- STEP 2: Assign TLEO to programme metadata --
   DROP TABLE IF EXISTS central_insights_sandbox.trending_young_step2;
   CREATE TABLE central_insights_sandbox.trending_young_step2 AS
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
   WHERE brand_title IN (SELECT tleo FROM central_insights_sandbox.trending_young_step1)
   OR series_title IN (SELECT tleo FROM central_insights_sandbox.trending_young_step1)
   OR episode_title IN (SELECT tleo FROM central_insights_sandbox.trending_young_step1)
   OR presentation_title IN (SELECT tleo FROM central_insights_sandbox.trending_young_step1)
   OR clip_title IN (SELECT tleo FROM central_insights_sandbox.trending_young_step1);

   -- STEP 3: Pick most common genre for each TLEO --
   DROP TABLE IF EXISTS central_insights_sandbox.trending_young_step3;
   CREATE TABLE central_insights_sandbox.trending_young_step3 AS

   SELECT DISTINCT a.*,
   FIRST_VALUE(b.pips_genre_level_1_names IGNORE NULLS)
   OVER (PARTITION BY b.tleo ORDER BY COUNT(pips_genre_level_1_names) DESC ROWS BETWEEN unbounded preceding AND unbounded following) AS genre
   FROM central_insights_sandbox.trending_young_step1 AS a
   LEFT JOIN central_insights_sandbox.trending_young_step2 AS b ON a.tleo = b.tleo
   GROUP BY 1, 2, 3, 4, b.pips_genre_level_1_names, b.tleo;

   -- STEP 4: Join on to dates table so we have a value each day for every brand, even if it is 0 (essentially a cross join) --
   DROP TABLE IF EXISTS central_insights_sandbox.trending_young_step4;
   CREATE TABLE central_insights_sandbox.trending_young_step4 AS

   WITH b AS (
     SELECT DISTINCT destination, tleo, genre
     FROM central_insights_sandbox.trending_young_step3
   )

   SELECT DISTINCT a.date, b.destination, b.tleo, b.genre
   FROM central_insights_sandbox.trending_young_step1 AS a, b;

   -- STEP 5: Retrieve streams --
   DROP TABLE IF EXISTS central_insights_sandbox.trending_young_step5;
   CREATE TABLE central_insights_sandbox.trending_young_step5 AS

   SELECT a.*,

   CASE
   WHEN count < 100 THEN NULL
   ELSE count
   END AS streams

   FROM central_insights_sandbox.trending_young_step4 AS a
   LEFT JOIN central_insights_sandbox.trending_young_step3 AS b
   ON a.date = b.date
   AND a.destination = b.destination
   AND a.tleo = b.tleo
   AND a.genre = b.genre;

   -- STEP 6 --
   DROP TABLE IF EXISTS central_insights_sandbox.trending_young_step6;
   CREATE TABLE central_insights_sandbox.trending_young_step6 AS

   SELECT date, destination, tleo,

   CASE
   WHEN CHARINDEX(';', genre) > 0
   THEN LEFT(genre, CHARINDEX(';', genre) - 1)
   ELSE genre
   END AS genre,

   streams,

   COUNT(streams) OVER (PARTITION BY destination, tleo, genre ORDER BY date
   ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING) AS non_nulls,

   AVG(CAST(streams AS float)) OVER (PARTITION BY destination, tleo, genre ORDER BY date
   ROWS BETWEEN 28 PRECEDING AND 1 PRECEDING) AS brand_avg28,

   STDDEV_SAMP(CAST(streams AS float)) OVER (PARTITION BY destination, tleo, genre ORDER BY date
   ROWS BETWEEN 28 PRECEDING AND 1 PRECEDING) AS brand_sd28,

   AVG(CAST(streams AS float)) OVER (PARTITION BY date) AS overall_avg28,
   STDDEV_SAMP(CAST(streams AS float)) OVER (PARTITION BY date) AS overall_sd28,

   CASE
   WHEN brand_sd28 IS NULL OR brand_sd28 = 0 OR non_nulls < 7
   THEN (streams - overall_avg28) / overall_sd28
   ELSE (streams - brand_avg28) / brand_sd28
   END AS trend_score

   FROM central_insights_sandbox.trending_young_step5;

   -- STEP 7 --
   DROP TABLE IF EXISTS central_insights_sandbox.trending_young_step7;
   CREATE TABLE central_insights_sandbox.trending_young_step7 AS

   SELECT * FROM central_insights_sandbox.trending_young_step6
   WHERE date = DATEADD('day', -1, CURRENT_DATE)::date;

   /*
      ===============================
      PART 4: FINAL JOIN
      ===============================
   */

   DROP TABLE IF EXISTS central_insights_sandbox.TRENDING_temp;
   CREATE TABLE central_insights_sandbox.TRENDING_temp AS
   SELECT a.date, a.destination, a.tleo, a.genre, a.streams, a.trend_score,
   b.streams AS streams_young, b.trend_score AS trend_score_young,
   c.streams AS streams_ondemand, c.trend_score AS trend_score_ondemand

   FROM central_insights_sandbox.trending_step7 AS a
   LEFT JOIN central_insights_sandbox.trending_young_step7 AS b
   ON a.date = b.date
   AND a.destination = b.destination
   AND a.tleo = b.tleo
   AND a.genre = b.genre
   LEFT JOIN central_insights_sandbox.trending_ondemand_step7 AS c
   ON b.date = c.date
   AND b.destination = c.destination
   AND b.tleo = c.tleo
   AND b.genre = c.genre;


   INSERT INTO central_insights_sandbox.TRENDING
   SELECT * FROM central_insights_sandbox.TRENDING_temp

  /*
    ============================
    Clean up temp tables
    ============================
    Any tables you created which are not part of the desired output should be dropped
  */

  /*
    ============================
    Add grants
    ============================
    By default tables are only accessible to the creator.
    You can grant the following:
    - SELECT
    - INSERT
    - UPDATE
    - DELETE
    - REFERENCES
    - ALL
  */
  -- example grant
  GRANT ALL ON TRENDING TO GROUP central_insights;
  GRANT ALL ON TRENDING TO GROUP central_insights;


  /*
    ============================
    Sort out history
    ============================
    Finalise the history record
  */
  -- Record completed
  UPDATE TEMP_TRENDING_History
  SET
    Updated_Date = GETDATE(),
    Finished     = GETDATE(),
    STATUS       = 'FINISHED'
  WHERE Script = 'TRENDING'
    AND STATUS = 'RUNNING'
    AND Updated_By = CURRENT_USER;

  -- COPY history into a permanent table
  -- IMPORTANT: Make sure nothing else is accessing this table when the script is RUNNING
  --            redshift doesn't cope well with concurrent access
  CREATE TABLE IF NOT EXISTS TRENDING_History (
    Script       VARCHAR(1000),
    Target_Table VARCHAR(1000),
    Updated_By   VARCHAR(1000),
    Updated_Date DATE,
    Started      TIMESTAMP,
    Finished     TIMESTAMP,
    STATUS       VARCHAR(100)
  );
  INSERT INTO TRENDING_History
  SELECT * FROM TRENDING_History;

COMMIT; --Ends the transaction

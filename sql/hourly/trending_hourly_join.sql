-- 1305348 --
DROP TABLE IF EXISTS central_insights_sandbox.TRENDING_HOURLY;
CREATE TABLE central_insights_sandbox.TRENDING_HOURLY AS
SELECT a.datehour, a.destination, a.tleo, a.genre, a.streams, a.trend_score,
b.streams AS streams_young, b.trend_score AS trend_score_young,
c.streams AS streams_ondemand, c.trend_score AS trend_score_ondemand

FROM central_insights_sandbox.trending_hourly_step6 AS a
LEFT JOIN central_insights_sandbox.trending_hourly_young_step6 AS b
ON a.datehour = b.datehour
AND a.destination = b.destination
AND a.tleo = b.tleo
AND a.genre = b.genre
LEFT JOIN central_insights_sandbox.trending_hourly_ondemand_step6 AS c
ON b.datehour = c.datehour
AND b.destination = c.destination
AND b.tleo = c.tleo
AND b.genre = c.genre;

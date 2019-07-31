-- 1305348 --
DROP TABLE IF EXISTS central_insights_sandbox.TRENDING;
CREATE TABLE central_insights_sandbox.TRENDING AS
SELECT a.date, a.destination, a.tleo, a.genre, a.streams, a.trend_score,
b.streams AS streams_young, b.trend_score AS trend_score_young,
c.streams AS streams_ondemand, c.trend_score AS trend_score_ondemand

FROM central_insights_sandbox.trending_step6 AS a
LEFT JOIN central_insights_sandbox.trending_young_step6 AS b
ON a.date = b.date
AND a.destination = b.destination
AND a.tleo = b.tleo
AND a.genre = b.genre
LEFT JOIN central_insights_sandbox.trending_ondemand_step6 AS c
ON b.date = c.date
AND b.destination = c.destination
AND b.tleo = c.tleo
AND b.genre = c.genre;

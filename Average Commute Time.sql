SELECT 
commuter_id,
FLOOR(AVG(TIMESTAMPDIFF(Minute, start_dt, end_dt))) AS avg_commuter_time,
(SELECT FLOOR(AVG(TIMESTAMPDIFF(Minute, start_dt, end_dt))) FROM rides WHERE CITY = "NY") AS avg_time 
FROM rides
WHERE CITY = "NY"
GROUP BY commuter_id

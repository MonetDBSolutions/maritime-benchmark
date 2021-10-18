-- Simpler queries
-- Get number of different vessels in dynamic messages
SELECT count(*) 
FROM
	(SELECT DISTINCT mmsi 
	FROM ais_data.dynamic_ships) AS distinct_vessels;

-- Get number of messages per ship in all navigation datasets
SELECT mmsi, count(*) as vessel_count
FROM
	(SELECT mmsi FROM ais_data.dynamic_sar
	UNION ALL
	SELECT mmsi FROM ais_data.dynamic_aton
	UNION ALL
	SELECT sourcemmsi FROM ais_data.static_ships
	UNION ALL
	SELECT mmsi FROM ais_data.dynamic_ships) AS navigation_union
GROUP BY mmsi
ORDER BY vessel_count DESC;

-- Get average and max speed per day
SELECT EXTRACT(DAY FROM t) as t_day, EXTRACT(MONTH FROM t) as t_month, AVG(speed) as avg_speed, MAX(speed) as max_speed, count(*) as day_messages
FROM ais_data.dynamic_ships
WHERE speed <> 0
GROUP BY EXTRACT(DAY FROM t), EXTRACT(MONTH FROM t);
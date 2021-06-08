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

-- GEOM queries


-- Book queries
-- Get ships within 500 m of a port and the duration of their stay
SELECT port_name, mmsi, shipname, min_t, max_t, max_t - min_t as dur
FROM (
	SELECT libelle_po as port_name, mmsi, min(q2.t) as min_t, max(q2.t) as max_t
	FROM sys.brittany_ports as q1 -- ports location
	INNER JOIN ais_data.dynamic_ships as q2 -- ship AIS positions
		ON 
			q2.speed = 0 -- not moving
			AND q2.t >= str_to_timestamp('2016-01-01 00:00:00','%Y-%m-%d %H:%M:%S')
			AND q2.t < str_to_timestamp('2016-01-02 00:00:00','%Y-%m-%d %H:%M:%S') -- during Jan 1,2016
			AND sys.st_dwithin(q1.geom,q2.geom,500) -- ships within 500m of the port
	GROUP BY libelle_po, mmsi
	) AS q3
LEFT JOIN ais_data.static_ships as q4 -- ship names
ON q3.mmsi = q4.sourcemmsi;

-- Get ship info
CREATE FUNCTION get_vessel_info (mmsi integer) RETURNS text
BEGIN
	DECLARE vessel_imo integer;
	DECLARE vessel_name text;

	SELECT DISTINCT shipname, imo
	INTO vessel_name, vessel_imo
	FROM ais_data.static_ships
	WHERE sourcemmsi = mmsi;

	RETURN vessel_name;
END;

-- Voronoi tesselation
CREATE TABLE d a t a _ a n a l y s i s . p o r t s _ v o r o n o i AS
SELECT p o r _ i d as por t_id , l i b e l l e _ p o as port_name , geom3035 ,
voronoi_zone3035
FROM c o n t e x t _ d a t a . p o r t s
LEFT JOIN (
SELECT (ST_Dump ( ST_VoronoiPolygons ( ST_Col l e c t ( geom3035 ) ) ) ) . geom
as voronoi_zone3035
FROM c o n t e x t _ d a t a . p o r t s ) as vp
ON ( ST_Within ( p o r t s . geom3035 , vp . v o ronoi_zone3035 ) ) ;

CREATE TABLE d a t a _ a n a l y s i s . n o n _ m o v i n g _ p o s i t i o n s AS
SELECT id , mmsi , t , q1 . geom3035 , p o r t _ i d , port_name ,
ST_Distance ( q1 . geom3035 , p o r t s _ v o r o n o i . geom3035 ) as p o r t _ d i s t
FROM (
SELECT ∗
FROM a i s _ d a t a . dynamic_ships
WHERE speed =0 −− non moving ship p o s i t i o n s only
) as q1
LEFT JOIN d a t a _ a n a l y s i s . p o r t s _ v o r o n o i
ON ST_Within ( q1 . geom3035 , p o r t s _ v o r o n o i . voronoi_zone3035 ) ; −− s h i p s in
v o r o n o i a r e a




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

-- Get ship info (doesn't work still)
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

-- Get ships that have fished for longer than 15 minutes in 22/01/2016 (a fishing vessel fishes at a speed between 2.5 and 3.5 knots)
SELECT mmsi, min_t, max_t, duration_s
FROM (
	SELECT mmsi, MIN(t) AS min_t, MAX(t) AS max_t, MAX(t) - MIN(t) AS duration_s
	FROM (
		SELECT ROW_NUMBER() OVER (PARTITION BY mmsi ORDER BY rn) - rn AS grp, speed, mmsi, t
	    FROM  (
	    	SELECT mmsi, rn, speed, t FROM ( 
				SELECT mmsi, speed, t, ROW_NUMBER() OVER (PARTITION BY mmsi ORDER BY t) AS rn 
				FROM ais_data.dynamic_ships
				WHERE t >= str_to_timestamp('22-01-2016 00:00', '%d-%m-%Y %H:%M') 
				AND t < str_to_timestamp('23-01-2016 00:00', '%d-%m-%Y %H:%M')
				AND mmsi < 999999999 -- invalid mmsi
			) AS q1
		WHERE speed >= 2.5 AND speed <= 3.5
		) AS q2
	) AS q3
	GROUP BY mmsi, grp
	ORDER  BY mmsi, grp
) as q4
WHERE q4.duration_s > INTERVAL '15' SECOND
ORDER BY mmsi;

-- Same as last query, but group by ship, add the fishing time and enrich with ship metadata
SELECT DISTINCT mmsi, shipname, total_duration_s
FROM (
	SELECT mmsi, SUM(duration_s) as total_duration_s
	FROM (
		SELECT mmsi, MIN(t) AS min_t, MAX(t) AS max_t, MAX(t) - MIN(t) AS duration_s
		FROM (
			SELECT ROW_NUMBER() OVER (PARTITION BY mmsi ORDER BY rn) - rn AS grp, speed, mmsi, t
		    FROM  (
		    	SELECT mmsi, rn, speed, t FROM ( 
					SELECT mmsi, speed, t, ROW_NUMBER() OVER (PARTITION BY mmsi ORDER BY t) AS rn 
					FROM ais_data.dynamic_ships
					WHERE t >= str_to_timestamp('22-01-2016 00:00', '%d-%m-%Y %H:%M') 
					AND t < str_to_timestamp('23-01-2016 00:00', '%d-%m-%Y %H:%M')
					AND mmsi < 999999999 -- invalid mmsi
				) AS q1
			WHERE speed >= 2.5 AND speed <= 3.5
			) AS q2
		) AS q3
		GROUP BY mmsi, grp
		ORDER  BY mmsi, grp
	) as q4
	WHERE q4.duration_s > INTERVAL '15' SECOND
	GROUP BY mmsi
) as q5
LEFT JOIN ais_data.static_ships as q6
ON q5.mmsi = q6.sourcemmsi;

-- Detect vessel stops
-- Vessel movement segments
CREATE TABLE segments AS
SELECT mmsi, t1, t2, speed1, speed2, p1, p2, 
	st_makeline(p1,p2) as segment, 
	st_distance(p1,p2) as distance,
	(t2-t1) as duration_s,
	(st_distance(p1,p2) / (extract(epoch from (t2-t1)/1000))) as speed_m_s
FROM (
	SELECT mmsi, LEAD(mmsi) OVER (ORDER BY mmsi, t) as mmsi2,
		t AS t1, LEAD(t) OVER (ORDER BY mmsi, t) as t2,
		speed AS speed1, LEAD(speed) OVER (ORDER BY mmsi, t) as speed2,
		geom AS p1, LEAD(geom) OVER (ORDER BY mmsi, t) as p2
	FROM ais_data.dynamic_ships
) as q1
WHERE mmsi = mmsi2 
AND t1 < t2; -- The lead function returns the top timestamp where the t is the last one

CREATE TABLE stop_begin AS
SELECT mmsi, t2 AS t_begin FROM segments
WHERE speed1 > 0.1 AND speed2 <= 0.1;

CREATE TABLE stop_end AS
SELECT mmsi, t1 AS t_end FROM segments
WHERE speed1 <= 0.1 AND speed2 > 0.1;

CREATE TABLE stops AS
SELECT q1.mmsi, q1.t_begin, q1.t_end, (q1.t_end - q1.t_begin) as duration_s
FROM (
	SELECT DISTINCT stop_begin.mmsi, t_begin, FIRST_VALUE(t_end) OVER (PARTITION by stop_begin.mmsi, t_begin ORDER BY t_end) as t_end
	FROM stop_begin
	INNER JOIN stop_end
	ON stop_begin.mmsi = stop_end.mmsi AND t_begin <= t_end
) AS q1;	

select mmsi, stop_count, total_time from 
(select mmsi, count(*) as stop_count, sum(duration_s) as total_time from stops group by mmsi) as q1 
order by total_time;

-- Calculate stop centroid (doesn't work still)
ALTER TABLE stops ADD COLUMN centroid Point;
ALTER TABLE stops ADD COLUMN n_pos int;

UPDATE stops SET (centroid,n_pos) = (
	SELECT st_centroid(st_collect(geom)), count(*) as nb_pos
	FROM ais_data.dynamic_ships 
	WHERE mmsi = stops.mmsi 
	AND t >= stops.t_begin 
	AND t <= stops.t_end);

ALTER TABLE stops ADD COLUMN avg_dist_centroid numeric;
ALTER TABLE stops ADD COLUMN max_dist_centroid numeric;

UPDATE stops SET (avg_dist_centroid,max_dist_centroid) = (
	SELECT avg(d), max(d) FROM (
		SELECT st_distance(centroid,geom) as d
		FROM ais_data.dynamic_ships
		WHERE mmsi = stops.mmsi AND t >= stops.t_begin AND t <= stops.t_end) AS q1
	);

SELECT count(*)
FROM stops
WHERE duration_s >= (5*60)
AND n_pos > 5
AND avg_dist_centroid <= 10;

-- Vessel tracks between stops
CREATE TABLE tracks AS
SELECT q1.mmsi, 
	q1.cid as start_cid, q3.cid as end_cid,
	q1.t_end as t_start, q3.t_begin as t_end,
	EXTRACT(EPOCH FROM (q3.t_begin - q1.t_end)/1000) as duration_s
FROM 
	(SELECT mmsi, cid, stop_end
	FROM stops
	WHERE cid IS NOT NULL) as q1
	INNER JOIN LATERAL (
		SELECT q2.cid q2.t_begin FROM stops as q2
		WHERE q2.cid IS NOT NULL
		AND q1.mmsi = q2.mmsi2
		AND q2.t_begin > q1.stop_end
		ORDER BY q2.t_begin LIMIT 1) as q3
	ON true;


ALTER TABLE tracks ADD COLUMN track geom(LineString, 3035);
UPDATE tracks SET track = (
	SELECT st_makeline(geom) FROM (
		SELECT geom FROM ais_data.dynamic_ships
		WHERE mmsi = tracks.mmsi AND t >= tracks.t_start AND t <= tracks.stop_end
		ORDER BY t
	) as q1
);

CREATE TABLE d a t a _ a n a l y s i s . graph_edges AS
SELECT
s t a r t _ c i d , −− from node
end_cid , −− t o node
n b _ t r a c k s , −− number of t r a c k s
q1 . nb_ships , −− number of d i f f e r e n t s h i p s
q1 . a v g _ d u r a t i o n , −− a v e r a g e t r a n s i t t ime
q1 . a vg_length , −− a v e r a g e t r a n s i t l e n g t h
q1 . min_length , −− minimum t r a n s i t l e n g t h
s t _ m a k e l i n e ( c1 . c e n t r o i d , c2 . c e n t r o i d ) as s t r a i g h t _ e d g e −− edge
FROM (
SELECT
s t a r t _ c i d , −− from node
end_cid , −− t o node
count ( ∗ ) as n b _ t r a j , −− number of v e s s e l t r a j e c t o r i e s
count ( d i s t i n c t mmsi ) as nb_ships , −− number of unique s h i p s
avg ( d u r a t i o n _ s ) as a v g _ d u r a t i o n , −− a v e r a g e t r a j e c t o r y
d u r a t i o n
avg ( s t _ l e n g t h ( t r a c k ) ) as avg_length , −− a v e r a g e l e n g t h
min ( s t _ l e n g t h ( t r a c k ) ) as m i n _ l e n g t h −− s h o r t e s t l e n g t h
FROM d a t a _ a n a l y s i s . t r a c k s
GROUP BY s t a r t _ c i d , end_cid ) as q1
LEFT JOIN d a t a _ a n a l y s i s . c l u s t e r s _ s t o p s _ h u l l s as c1
ON ( c1 . cid=q1 . s t a r t _ c i d )
LEFT JOIN d a t a _ a n a l y s i s . c l u s t e r s _ s t o p s _ h u l l s as c2
ON ( c2 . c id =q1 . end_cid ) ;



-- Voronoi tesselation Queries (do we want to support voronoi polygons?)
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




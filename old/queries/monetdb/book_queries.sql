-- Book queries
-- Get ships within 500 m of a port and the duration of their stay
SELECT DISTINCT port_name, mmsi, shipname, min_t, max_t, max_t - min_t as dur
FROM (
	SELECT libelle_po as port_name, mmsi, min(q2.t) as min_t, max(q2.t) as max_t
	FROM sys.britanny_ports as q1 -- ports location
	INNER JOIN ais_data.dynamic_ships as q2 -- ship AIS positions
		ON 
			q2.speed = 0 -- not moving
			AND q2.t >= str_to_timestamp('2016-01-01 00:00:00','%Y-%m-%d %H:%M:%S')
			AND q2.t < str_to_timestamp('2016-01-02 00:00:00','%Y-%m-%d %H:%M:%S') -- during Jan 1,2016
			AND ST_DWithinGeographic(q1.geom,q2.geom,500) -- ships within 500m of the port
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
	(st_distancegeographic(p1,p2) / (extract(epoch from (t2-t1)/1000))) as speed_m_s
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
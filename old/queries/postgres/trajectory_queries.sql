CREATE TABLE segments AS
SELECT mmsi, t1, t2, speed1, speed2, p1, p2, 
	st_makeline(p1,p2) as segment, 
	st_distance(p1,p2) as distance,
	(t2-t1) as duration_s,
	(st_distance(p1::Geography,p2::Geography) / (extract(epoch from (t2-t1)/1000))) as speed_m_s
FROM (
	SELECT mmsi, LEAD(mmsi) OVER (ORDER BY mmsi, t) as mmsi2,
		t AS t1, LEAD(t) OVER (ORDER BY mmsi, t) as t2,
		speed AS speed1, LEAD(speed) OVER (ORDER BY mmsi, t) as speed2,
		geom AS p1, LEAD(geom) OVER (ORDER BY mmsi, t) as p2
	FROM ais_data.dynamic_ships
) as q1
WHERE mmsi = mmsi2 
AND t1 < t2;

CREATE TABLE trajectories AS 
SELECT mmsi, st_collect(segment) as trajectory
FROM segments
GROUP BY mmsi;

CREATE TABLE segments_10k AS 
SELECT * FROM segments
ORDER BY mmsi
LIMIT 10000;

CREATE TABLE trajectories_10k AS
SELECT mmsi, st_collect(segment) as trajectory
FROM segments_10k
GROUP BY mmsi;
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
AND t1 < t2;

set optimizer = 'sequential_pipe';
CREATE TABLE trajectories AS 
SELECT mmsi, st_collect(segment) as trajectory
FROM segments
GROUP BY mmsi;

set schema = comparison;
set optimizer = 'sequential_pipe';

CREATE TABLE comparison.segments_10k AS 
SELECT * FROM segments LIMIT 10000;

CREATE TABLE comparison.trajectories_10k AS
SELECT mmsi, st_collect(segment) as trajectory
FROM segments_10k
GROUP BY mmsi;

set optimizer = 'sequential_pipe';
SELECT mmsi, st_collect(segment) as trajectory
FROM comparison.segments_10k
GROUP BY mmsi;

select distinct st_geometrytype(polygonwkb) from st_dump((select st_collect(geom) from fishing_areas group by value));


select * from st_dump((select st_collect(segment) from segments where mmsi = 923166));

set optimizer = 'sequential_pipe';
select st_collect(segment) from segments where mmsi <= 37100300 group by mmsi;

select st_collect(segment) from segments group by mmsi;

select count(*) from st_dump((select st_collect(segment) from segments where mmsi = 37100300 group by mmsi));
select count(*) from segments where mmsi = 37100300;

select st_collect(segment) from segments where mmsi = 37100300 group by mmsi;
select segment from segments where mmsi = 37100300 limit 10;

select count(*) from st_dump((select st_collect(segment) from segments where mmsi = 923166 group by mmsi));
select count(*) from segments where mmsi = 923166;

select * from (select mmsi, count(segment) as seg from segments group by mmsi) as s where s.seg > 10 order by s.seg desc;

select st_collect(segment) from segments where mmsi = 241253000 or mmsi = 538004127 or mmsi = 477845600 group by mmsi;
select segment from segments where mmsi = 241253000 limit 10;

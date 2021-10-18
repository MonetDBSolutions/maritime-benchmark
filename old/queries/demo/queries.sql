select * from sys.tables where schema_id = 8323;

-- Make a trajectory for a certain ship (st_collect)
-- Aggregate individual positions into segments, by joining concurrent positions into a line
CREATE TABLE vesselai.trajectory_segments AS
SELECT mmsi, t1, t2, p1, p2, speed1, speed2, 
  st_makeline(p1,p2) as segment
FROM (
  SELECT mmsi, LEAD(mmsi) OVER (ORDER BY mmsi, t) as mmsi2,
    t AS t1, LEAD(t) OVER (ORDER BY mmsi, t) as t2,
    speed AS speed1, LEAD(speed) OVER (ORDER BY mmsi, t) as speed2,
    geom AS p1, LEAD(geom) OVER (ORDER BY mmsi, t) as p2
  FROM vesselai.ships
) as q1
WHERE mmsi = mmsi2;

-- Collect the ship position segments into a full trajectory
CREATE TABLE vesselai.trajectory AS
SELECT mmsi, st_collect(segment) as geom
FROM vesselai.trajectory_segments
GROUP BY mmsi;

-- Collect only the points
CREATE TABLE vesselai.trajectory_points AS
SELECT mmsi, st_collect(geom) as geom
FROM (SELECT mmsi, geom, t FROM vesselai.ships ORDER BY mmsi,t) as ordered_ships
GROUP BY mmsi;

-- Actual queries
-- Minimal distance between a ship's trajectory and a port location
SELECT q1.mmsi as ship, q2.gid as port, st_distancegeographic(q1.geom,q2.geom) as distance_meters
FROM vesselai.trajectory as q1
JOIN vesselai.ports as q2
ON q1.mmsi = 226105000
ORDER BY st_distancegeographic(q1.geom,q2.geom);

-- Distance between two ships, given a certain timestamp and tolerance interval
SELECT q1.t as t1, q2.t as t2, st_distancegeographic(q1.geom,q2.geom) as distance, q1.mmsi as ship1, q2.mmsi as ship2
FROM vesselai.ships as q1
JOIN vesselai.ships as q2
ON q1.mmsi = 228813000 AND q2.mmsi = 227730220 
AND q1.t > '2015-10-05 15:00:00' AND q1.t < '2015-10-05 15:10:00'  
AND (epoch(q2.t) >= (epoch(q1.t) - 10) AND epoch(q2.t) <= epoch(q1.t))
ORDER BY q1.t;

-- All ships which passed by a certain distance to a port
SELECT q1.mmsi as ship, q2.gid as port, st_distancegeographic(q1.geom,q2.geom) as distance
FROM vesselai.trajectory as q1
JOIN vesselai.ports as q2
ON st_dwithingeographic(q1.geom,q2.geom,500);
-- MMSI 228813000 teleports around for the first values, leading to low distance values

-- Get ships within 500 m of a port and the duration of their stay
SELECT libelle_po as port_name, mmsi, min(q2.t) as min_t, max(q2.t) as max_t, max(q2.t) - min(q2.t) as dur, min(st_distancegeographic(q1.geom,q2.geom)) as dist
  FROM vesselai.ports as q1 -- ports location
  INNER JOIN vesselai.ships as q2 -- ship AIS positions
    ON 
      q2.speed = 0 -- not moving
      AND ST_DWithinGeographic(q1.geom,q2.geom,500) -- ships within 500m of the port
  GROUP BY libelle_po, mmsi;

-- Detecting stops
CREATE TABLE vesselai.stop_begin AS
SELECT mmsi, t2 AS t_begin FROM vesselai.trajectory_segments
WHERE speed1 > 0.1 AND speed2 <= 0.1;

CREATE TABLE vesselai.stop_end AS
SELECT mmsi, t1 AS t_end FROM vesselai.trajectory_segments
WHERE speed1 <= 0.1 AND speed2 > 0.1;

CREATE TABLE vesselai.stops AS
SELECT q1.mmsi, q1.t_begin, q1.t_end, (q1.t_end - q1.t_begin) as duration_s
FROM (
  SELECT DISTINCT stop_begin.mmsi, t_begin, FIRST_VALUE(t_end) OVER (PARTITION by stop_begin.mmsi, t_begin ORDER BY t_end) as t_end
  FROM vesselai.stop_begin
  INNER JOIN vesselai.stop_end
  ON stop_begin.mmsi = stop_end.mmsi AND t_begin <= t_end
) AS q1
WHERE q1.t_end <> q1.t_begin;

select mmsi, stop_count, total_time from 
(select mmsi, count(*) as stop_count, sum(duration_s) as total_time from stops group by mmsi) as q1 
order by total_time;

--Getting vessels which are fishing
SELECT DISTINCT mmsi, total_duration_s
FROM (
  SELECT mmsi, SUM(duration_s) as total_duration_s
  FROM (
    SELECT mmsi, MIN(t) AS min_t, MAX(t) AS max_t, MAX(t) - MIN(t) AS duration_s
    FROM (
      SELECT ROW_NUMBER() OVER (PARTITION BY mmsi ORDER BY rn) - rn AS grp, speed, mmsi, t
        FROM  (
          SELECT mmsi, rn, speed, t FROM ( 
          SELECT mmsi, speed, t, ROW_NUMBER() OVER (PARTITION BY mmsi ORDER BY t) AS rn 
          FROM vesselai.ships
          WHERE mmsi < 999999999 -- invalid mmsi
        ) AS q1
      WHERE speed >= 2.5 AND speed <= 3.5
      ) AS q2
    ) AS q3
    GROUP BY mmsi, grp
    ORDER  BY mmsi, grp
  ) as q4
  WHERE q4.duration_s > INTERVAL '15' SECOND
  GROUP BY mmsi
) as q5;










-- Exclusive economic zones visited by a vessel on their trajectory
SELECT q1.mmsi as ship, q2.gid as zone_id
FROM vesselai.trajectory as q1
JOIN vesselai.eez as q2
ON st_intersectsgeographic(q1.geom,q2.geom)
WHERE q1.mmsi = ;


-- Query 1: Trajectory segments
CREATE TABLE trajectory_segments AS
  SELECT mmsi, t1, t2, p1, p2, speed1, speed2, st_makeline(p1,p2) as segment
  FROM (
    SELECT mmsi, LEAD(mmsi) OVER (ORDER BY mmsi, t) as mmsi2,
           t AS t1, LEAD(t) OVER (ORDER BY mmsi, t) as t2,
           speed AS speed1, LEAD(speed) OVER (ORDER BY mmsi, t) as speed2,
           geom AS p1, LEAD(geom) OVER (ORDER BY mmsi, t) as p2
    FROM ais_dynamic) as q1
  WHERE mmsi = mmsi2;
-- Query 2: Aggregate trajectories
CREATE TABLE trajectory AS
  SELECT mmsi, st_collect(segment) as geom
  FROM trajectory_segments
  GROUP BY mmsi;
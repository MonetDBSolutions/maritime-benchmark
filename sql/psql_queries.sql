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
-- Query 3: Distance between ships and ports (Point-Point distance)
CREATE TABLE ship_port_distance AS
  SELECT mmsi, libelle_po as port, q2.gml_id as port_id, q1.t as position_time,
         st_distance(q1.geom::Geography,q2.geom::Geography,false) as distance
  FROM ais_dynamic as q1
  JOIN brittany_ports as q2
  ON TRUE;
-- Query 4: Distance between ship trajectories and ports (Line-Point distance)
CREATE TABLE trajectory_port_distance AS
  SELECT mmsi, libelle_po as port, q2.gml_id as port_id,
         st_distance(q1.geom::Geography,q2.geom::Geography,false) as distance
  FROM trajectory as q1
  JOIN brittany_ports as q2
  ON TRUE;
-- Query 5: Trajectories within a certain distance from wpi_ports in France (Line-Point DWithin)
CREATE TABLE trajectory_close_france AS
  SELECT mmsi, q2.index_no as port_id,
         st_distance(q1.geom::Geography,q2.geom::Geography,false) as distance
  FROM trajectory as q1
  JOIN wpi_ports as q2
  ON q2.country = 'FR' AND st_dwithin(q1.geom::Geography,q2.geom::Geography,50000,false);
-- Query 6: Distance between ship trajectories within 5000 meters of one another (Line-Line DWithin)
CREATE TABLE close_trajectories AS
  SELECT q1.mmsi as mmsi1, q2.mmsi as mmsi2,
         st_distance(q1.geom::Geography,q2.geom::Geography,false) as distance
  FROM trajectory as q1
  JOIN trajectory as q2
  ON q1.mmsi < q2.mmsi AND st_distance(q1.geom::Geography,q2.geom::Geography,false) <= 5000;
-- Query 7: FAO areas intersected by ship trajectories (Line-Polygon DIntersects)
CREATE TABLE fao_trajectory_intersect AS
  SELECT mmsi, fid::INTEGER as fao_id,
        st_intersects(q1.geom::Geography,q2.geom::Geography) as intersects
  FROM trajectory as q1
  JOIN fao_areas as q2
  ON TRUE
  AND q2.f_division IN ('27.8.a','27.8.b','27.8.d','27.7.d','27.7.e','27.7.h','27.7.f','27.7.g','27.7.j');
-- Query 8: FAO areas intersected by fishing interdiction area (Polygon-Polygon DIntersects)
CREATE TABLE fao_fishing_intersect AS
  SELECT q1.gid as fishing_id, q2.fid::INTEGER as fao_id,
        st_intersects(q1.geom::Geography,q2.geom::Geography) as intersects
  FROM fishing_areas as q1
  JOIN fao_areas as q2
  ON TRUE
  AND q2.f_division IN ('27.8.a','27.8.b','27.8.d','27.7.d','27.7.e','27.7.h','27.7.f','27.7.g','27.7.j')
  AND q1.gid < 10;
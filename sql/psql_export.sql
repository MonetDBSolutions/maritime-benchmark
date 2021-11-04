-- Query 1
COPY
  (SELECT mmsi, t1, t2, upper(substring(st_asbinary(p1)::text from 3)), upper(substring(st_asbinary(p2)::text from 3)),
    speed1, speed2,  upper(substring(st_asbinary(segment)::text from 3))
  FROM trajectory_segments
  ORDER BY mmsi, t1)
TO '%OUT%/Q1_trajectory_segments%SF%.csv'
WITH (FORMAT CSV);
-- Query 2
COPY
  (SELECT mmsi, upper(substring(st_asbinary(geom)::text from 3)) FROM trajectory
  ORDER BY mmsi)
TO '%OUT%/Q2_trajectory%SF%.csv'
WITH (FORMAT CSV);
-- Query 3
COPY
  (SELECT * FROM ship_port_distance
  ORDER BY mmsi, position_time, port_id)
TO '%OUT%/Q3_ship_port_distance%SF%.csv'
WITH (FORMAT CSV);
-- Query 4
COPY
  (SELECT * FROM trajectory_port_distance
  ORDER BY mmsi, port_id)
TO '%OUT%/Q4_trajectory_port_distance%SF%.csv'
WITH (FORMAT CSV);
-- Query 5
COPY
  (SELECT * FROM trajectory_close_france
  ORDER BY mmsi, port_id)
TO '%OUT%/Q5_trajectory_close_france%SF%.csv'
WITH (FORMAT CSV);
-- Query 6
COPY
  (SELECT * FROM close_trajectories
  ORDER BY mmsi1, mmsi2)
TO '%OUT%/Q6_close_trajectories%SF%.csv'
WITH (FORMAT CSV);
-- Query 7
COPY
  (SELECT * FROM fao_trajectory_intersect
  ORDER BY mmsi, fao_id)
TO '%OUT%/Q7_fao_trajectory_intersect%SF%.csv'
WITH (FORMAT CSV);
-- Query 8
COPY
  (SELECT fishing_id, fao_id FROM fao_fishing_intersect
  ORDER BY fishing_id,fao_id)
TO '%OUT%/Q8_fao_fishing_intersect%SF%.csv'
WITH (FORMAT CSV);
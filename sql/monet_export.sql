-- Query 1
COPY
  SELECT mmsi, t1, t2, st_asbinary(p1), st_asbinary(p2), speed1, speed2, st_asbinary(segment)
  FROM trajectory_segments
  ORDER BY mmsi, t1
INTO '%OUT%/Q1_trajectory_segments%SF%.csv'
USING DELIMITERS ',' , '\n' , '';
-- Query 2
COPY
  SELECT mmsi, st_asbinary(geom) FROM trajectory
  ORDER BY mmsi
INTO '%OUT%/Q2_trajectory%SF%.csv'
USING DELIMITERS ',' , '\n' , '';
-- Query 3
COPY
  SELECT * FROM ship_port_distance
  ORDER BY mmsi, position_time, port_id
INTO '%OUT%/Q3_ship_port_distance%SF%.csv'
USING DELIMITERS ',' , '\n' , '';
-- Query 4
COPY
  SELECT * FROM trajectory_port_distance
  ORDER BY mmsi, port_id
INTO '%OUT%/Q4_trajectory_port_distance%SF%.csv'
USING DELIMITERS ',' , '\n' , '';
-- Query 5
COPY
  SELECT * FROM trajectory_close_france
  ORDER BY mmsi, port_id
INTO '%OUT%/Q5_trajectory_close_france%SF%.csv'
USING DELIMITERS ',' , '\n' , '';
-- Query 6
COPY
  SELECT * FROM close_trajectories
  ORDER BY mmsi1, mmsi2
INTO '%OUT%/Q6_close_trajectories%SF%.csv'
USING DELIMITERS ',' , '\n' , '';
-- Query 7
COPY
  SELECT * FROM fao_trajectory_intersect
  ORDER BY mmsi, fao_id
INTO '%OUT%/Q7_fao_trajectory_intersect%SF%.csv'
USING DELIMITERS ',' , '\n' , '';
-- Query 8
COPY
  SELECT * FROM fao_fishing_intersect
  ORDER BY fishing_id, fao_id
INTO '%OUT%/Q8_fao_fishing_intersect%SF%.csv'
USING DELIMITERS ',' , '\n' , '';
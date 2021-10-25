-- Query 1
COPY
  SELECT * FROM trajectory_segments
  ORDER BY mmsi, t1
INTO '%OUT%/trajectory_segments%SF%.csv'
USING DELIMITERS ',' , '\n' , '\"';
-- Query 2
COPY
  SELECT * FROM trajectory
  ORDER BY mmsi
INTO '%OUT%/trajectory%SF%.csv'
USING DELIMITERS ',' , '\n' , '\"';
-- Query 3
COPY
  SELECT * FROM ship_port_distance
  ORDER BY mmsi, position_time, port
INTO '%OUT%/ship_port_distance%SF%.csv'
USING DELIMITERS ',' , '\n' , '\"';
-- Query 4
COPY
  SELECT * FROM trajectory_port_distance
  ORDER BY mmsi, port
INTO '%OUT%/trajectory_port_distance%SF%.csv'
USING DELIMITERS ',' , '\n' , '\"';
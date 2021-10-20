-- Query 1
COPY
  SELECT * FROM trajectory_segments
  ORDER BY mmsi, t1
INTO '%OUT%/monet/trajectory_segments.csv'
USING DELIMITERS ',' , '\n' , '\"';
-- Query 2
COPY
  SELECT * FROM trajectory
  ORDER BY mmsi
INTO '%OUT%/monet/trajectory.csv'
USING DELIMITERS ',' , '\n' , '\"';
-- Query 3
COPY
  SELECT * FROM ship_port_distance
  ORDER BY mmsi, position_time, port
INTO '%OUT%/monet/ship_port_distance.csv'
USING DELIMITERS ',' , '\n' , '\"';
-- Query 4
COPY
  SELECT * FROM trajectory_port_distance
  ORDER BY mmsi, port
INTO '%OUT%/monet/trajectory_port_distance.csv'
USING DELIMITERS ',' , '\n' , '\"';
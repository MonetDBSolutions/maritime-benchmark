-- Query 1
COPY
  (SELECT * FROM trajectory_segments
  ORDER BY mmsi, t1)
TO '%OUT%/psql/trajectory_segments.csv'
WITH (FORMAT CSV);
-- Query 2
COPY
  (SELECT * FROM trajectory
  ORDER BY mmsi)
TO '%OUT%/psql/trajectory.csv'
WITH (FORMAT CSV);
-- Query 3
COPY
  (SELECT * FROM ship_port_distance
  ORDER BY mmsi, position_time, port)
TO '%OUT%/psql/ship_port_distance.csv'
WITH (FORMAT CSV);
-- Query 4
COPY
  (SELECT * FROM trajectory_port_distance
  ORDER BY mmsi, port)
TO '%OUT%/psql/trajectory_port_distance.csv'
WITH (FORMAT CSV);
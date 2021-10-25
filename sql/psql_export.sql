-- Query 1
COPY
  (SELECT * FROM trajectory_segments
  ORDER BY mmsi, t1)
TO '%OUT%/trajectory_segments%SF%.csv'
WITH (FORMAT CSV);
-- Query 2
COPY
  (SELECT * FROM trajectory
  ORDER BY mmsi)
TO '%OUT%/trajectory%SF%.csv'
WITH (FORMAT CSV);
-- Query 3
COPY
  (SELECT * FROM ship_port_distance
  ORDER BY mmsi, position_time, port)
TO '%OUT%/ship_port_distance%SF%.csv'
WITH (FORMAT CSV);
-- Query 4
COPY
  (SELECT * FROM trajectory_port_distance
  ORDER BY mmsi, port)
TO '%OUT%/trajectory_port_distance%SF%.csv'
WITH (FORMAT CSV);
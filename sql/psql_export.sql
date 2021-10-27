-- Query 1
COPY
  (SELECT mmsi, t1, t2, upper(substring(st_asbinary(p1)::text from 3)), upper(substring(st_asbinary(p2)::text from 3)),
    speed1, speed2,  upper(substring(st_asbinary(segment)::text from 3))
  FROM trajectory_segments
  ORDER BY mmsi, t1)
TO '%OUT%/trajectory_segments%SF%.csv'
WITH (FORMAT CSV);
-- Query 2
COPY
  (SELECT mmsi, upper(substring(st_asbinary(geom)::text from 3)) FROM trajectory
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
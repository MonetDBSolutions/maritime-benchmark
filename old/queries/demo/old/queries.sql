-- Distance - st_distancegeographic()
-- Distance between ships and brittany ports on the 1st of October 2015
SELECT q1.mmsi as ship, q2.gml_id as port, st_distancegeographic(q1.geom,q2.geom) as distance
FROM ais_data.trajectories_01_10_2015 as q1
INNER JOIN geographic_features.brittany_ports as q2
ON TRUE
ORDER BY st_distancegeographic(q1.geom,q2.geom) desc;

-- Distance within - st_dwithingeographic()
-- Ships that were within 1000 meters from a port on the 1st of October 2015
SELECT q1.mmsi as ship, q2.gml_id as port, st_distancegeographic(q1.geom,q2.geom) as distance
FROM ais_data.trajectories_01_10_2015 as q1
INNER JOIN geographic_features.brittany_ports as q2
ON TRUE
WHERE st_dwithingeographic(q1.geom,q2.geom, 1000);

-- Ships that were within 2500 meters of the european coastline on the 1st of October 2015
SELECT q1.mmsi as ship, q2.gid as gid, st_distancegeographic(q1.geom,q2.geom) as distance
FROM ais_data.trajectories_01_10_2015 as q1
INNER JOIN geographic_features.fishing_areas as q2
ON TRUE
WHERE st_dwithingeographic(q1.geom,q2.geom, 2500);

-- Intersects - st_intersectsgeographic()


-- Covers - st_coversgeographic() -> Only polygons and lines

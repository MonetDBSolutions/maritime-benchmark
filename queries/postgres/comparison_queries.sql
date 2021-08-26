CREATE SCHEMA IF NOT EXISTS comparison;

-- Distance between dynamic_sar points and brittany_ports points (1013652 tuples)
-- Sphere distance
DROP TABLE IF EXISTS comparison.distance_sar_brittany;
CREATE TABLE comparison.distance_sar_brittany AS
SELECT mmsi, libelle_po as port_name, q2.t, st_distance(q1.geom::Geography,q2.geom::Geography, false) as distance
FROM ports.ports_of_brittany as q1
INNER JOIN ais_data.dynamic_sar as q2
ON TRUE
ORDER BY mmsi, libelle_po, ts;
COPY comparison.distance_sar_brittany TO '/Users/bernardo/Monet/VesselAI/Prototype/Comparisons/Distance/distance_sar_brittany_postgres.csv' DELIMITER ',';

-- Ellipsis distance
DROP TABLE IF EXISTS comparison.distance_sar_brittany_ellipsis;
CREATE TABLE comparison.distance_sar_brittany_ellipsis AS
SELECT mmsi, libelle_po as port_name, q2.t, st_distance(q1.geom::Geography,q2.geom::Geography) as distance
FROM ports.ports_of_brittany as q1
INNER JOIN ais_data.dynamic_sar as q2
ON TRUE
ORDER BY mmsi, libelle_po, ts;
COPY comparison.distance_sar_brittany_ellipsis TO '/Users/bernardo/Monet/VesselAI/Prototype/Comparisons/Distance/distance_sar_brittany_postgres_ellipsis.csv' DELIMITER ',';


-- Distance between dynamic_sar point and fishing_areas polygons () -> Taking too long!
DROP TABLE IF EXISTS comparison.distance_sar_fishing;

CREATE TABLE comparison.distance_sar_fishing AS
SELECT mmsi, name, q2.t, st_distance(q1.geom::Geography,q2.geom::Geography, false) as distance
FROM geographic_features.fishing_areas_eu as q1
INNER JOIN ais_data.dynamic_sar as q2
ON TRUE
ORDER BY mmsi, name, ts;

COPY comparison.distance_sar_fishing TO '/Users/bernardo/Monet/VesselAI/Prototype/Comparisons/Distance/distance_sar_fishing_postgres.csv' DELIMITER ',';

-- Distance between brittany_ports points and fishing_areas polygons ()
DROP TABLE IF EXISTS comparison.distance_brittany_fishing;

CREATE TABLE comparison.distance_brittany_fishing AS
SELECT libelle_po, q1.gid, st_distance(q1.geom::Geography,q2.geom::Geography, false) as distance
FROM geographic_features.fishing_areas_eu as q1
INNER JOIN ports.ports_of_brittany as q2
ON TRUE
ORDER BY q1.gid, q2.gid;

COPY comparison.distance_brittany_fishing TO '/Users/bernardo/Monet/VesselAI/Prototype/Comparisons/Distance/distance_brittany_fishing_postgres.csv' DELIMITER ',';

SELECT st_distance(q1.geom::Geography,q2.geom::Geography, false) as distance
FROM geographic_features.fishing_areas_eu as q1
INNER JOIN ports.ports_of_brittany as q2
ON TRUE
WHERE q2.libelle_po='Île de Molène' AND q1.gid=206;

-- Distance between fao_areas polygons and fishing_areas polygons ()
DROP TABLE IF EXISTS comparison.distance_fao_fishing;

CREATE TABLE comparison.distance_fao_fishing AS
SELECT q1.gid as gid1, name, q2.gid as gid2, st_distance(q1.geom::Geography,q2.geom::Geography, false) as distance
FROM geographic_features.fishing_areas_eu as q1
INNER JOIN geographic_features.fao_areas as q2
ON TRUE
ORDER BY q2.gid, q1.gid;

COPY comparison.distance_fao_fishing TO '/Users/bernardo/Monet/VesselAI/Prototype/Comparisons/Distance/distance_fao_fishing_postgres.csv' DELIMITER ',';


-- Stopped ships within 500m of a port of brittany during Jan 1,2016 (13 resulting tuples) -> TODO Need to analyse these!
CREATE TABLE comparison.anchored_ships_brittany AS
SELECT DISTINCT port_name, mmsi, shipname, min_t, max_t, max_t - min_t as dur
FROM (
	SELECT libelle_po as port_name, mmsi, min(q2.t) as min_t, max(q2.t) as max_t
	FROM ports.ports_of_brittany as q1 -- ports location
	INNER JOIN ais_data.dynamic_ships as q2 -- ship AIS positions
		ON 
			q2.speed = 0 -- not moving
			AND q2.t >= '2016-01-01 00:00:00'
			AND q2.t < '2016-01-02 00:00:00' -- during Jan 1,2016
			AND ST_DWithin(q1.geom::Geography,q2.geom::Geography,500) -- ships within 500m of the port
	GROUP BY libelle_po, mmsi
	) AS q3
LEFT JOIN ais_data.static_ships as q4 -- ship names
ON q3.mmsi = q4.sourcemmsi;



-- Get distance between ships and ports (tests for distancegeographic function)
SELECT port_name, 
	q1.latitude as port_lat, 
	q1.longitude as port_lon, mmsi as ship_mmsi, 
	q2.lat as ship_lat, 
	q2.lon as ship_lon, 
	st_distance(q1.geom::geography,q2.geom::geography) as distance
FROM ais_data.dynamic_ships as q2
INNER JOIN (
	SELECT * from ports.wpi_ports
	LIMIT 1
) as q1
ON true
LIMIT 50;

CREATE TABLE distance_wpi AS
	SELECT port_name, q1.latitude as port_lat, q1.longitude as port_lon, mmsi as ship_mmsi, q2.lat as ship_lat, q2.lon as ship_lon, st_distance(q1.geom::geography,q2.geom::geography) as distance
	FROM (
		SELECT * FROM ais_data.dynamic_ships
		ORDER BY mmsi, ts
		LIMIT 50
	) as q2
	INNER JOIN (
		SELECT * from ports.wpi_ports
		LIMIT 1
	) as q1
	ON true;

COPY distance_wpi TO '/Users/bernardo/Monet/VesselAI/Prototype/distance_wpi_postgres.csv' DELIMITER ',' CSV HEADER;

CREATE TABLE distance_wpi_sphere AS
	SELECT port_name, q1.latitude as port_lat, q1.longitude as port_lon, mmsi as ship_mmsi, q2.lat as ship_lat, q2.lon as ship_lon, st_distance(q1.geom::geography,q2.geom::geography, false) as distance
	FROM (
		SELECT * FROM ais_data.dynamic_ships
		ORDER BY mmsi, ts
		LIMIT 50
	) as q2
	INNER JOIN (
		SELECT * from ports.wpi_ports
		LIMIT 1
	) as q1
	ON true;

COPY distance_wpi_sphere TO '/Users/bernardo/Monet/VesselAI/Prototype/distance_wpi_postgres_sphere.csv' DELIMITER ',' CSV HEADER;

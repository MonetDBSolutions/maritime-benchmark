CREATE SCHEMA IF NOT EXISTS comparison;

-- Distance between dynamic_sar points and brittany_ports points (1013652 tuples)
DROP TABLE IF EXISTS comparison.distance_sar_brittany;

CREATE TABLE comparison.distance_sar_brittany AS
SELECT mmsi, libelle_po as port_name, q2.t, st_distancegeographic(q1.geom,q2.geom) as distance
FROM sys.brittany_ports as q1
INNER JOIN ais_data.dynamic_sar as q2
ON TRUE
ORDER BY mmsi, libelle_po, ts;

COPY SELECT * FROM comparison.distance_sar_brittany INTO '/Users/bernardo/Monet/VesselAI/Prototype/Comparisons/Distance/distance_sar_brittany_monet.csv' USING DELIMITERS ',' , '\n' , '"';

-- Distance between dynamic_sar point and fishing_areas polygons
DROP TABLE IF EXISTS comparison.distance_sar_fishing;

CREATE TABLE comparison.distance_sar_fishing AS
SELECT mmsi, name, q2.t, st_distancegeographic(q1.geom,q2.geom) as distance
FROM sys.fishing_areas as q1
INNER JOIN ais_data.dynamic_sar as q2
ON TRUE
ORDER BY mmsi, name, ts;

COPY SELECT * FROM comparison.distance_sar_fishing INTO '/Users/bernardo/Monet/VesselAI/Prototype/Comparisons/Distance/distance_sar_fishing_monet.csv' USING DELIMITERS ',' , '\n' , '"';

-- Distance between brittany_ports points and fishing_areas polygons
DROP TABLE IF EXISTS comparison.distance_brittany_fishing;

CREATE TABLE comparison.distance_brittany_fishing AS
SELECT libelle_po, name, q2.gid, st_distancegeographic(q1.geom,q2.geom) as distance
FROM sys.fishing_areas as q1
INNER JOIN sys.brittany_ports as q2
ON TRUE
ORDER BY q2.gid, name;

COPY SELECT * FROM comparison.distance_brittany_fishing INTO '/Users/bernardo/Monet/VesselAI/Prototype/Comparisons/Distance/distance_brittany_fishing_monet.csv' USING DELIMITERS ',' , '\n' , '"';

-- Distance between fao_areas polygons and fishing_areas polygons
DROP TABLE IF EXISTS comparison.distance_fao_fishing;

CREATE TABLE comparison.distance_fao_fishing AS
SELECT q1.gid as gid1, name, q2.gid as gid2, st_distancegeographic(q1.geom,q2.geom) as distance
FROM sys.fishing_areas as q1
INNER JOIN sys.fao_areas as q2
ON TRUE
ORDER BY q2.gid, q1.gid;

COPY SELECT * FROM comparison.distance_fao_fishing INTO '/Users/bernardo/Monet/VesselAI/Prototype/Comparisons/Distance/distance_fao_fishing_monet.csv' USING DELIMITERS ',' , '\n' , '"';

-- Stopped ships within 500m of a port of brittany during Jan 1,2016 
CREATE TABLE comparison.anchored_ships_brittany AS
SELECT DISTINCT port_name, mmsi, shipname, min_t, max_t, max_t - min_t as dur
FROM (
	SELECT libelle_po as port_name, mmsi, min(q2.t) as min_t, max(q2.t) as max_t
	FROM sys.brittany_ports as q1 -- ports location
	INNER JOIN ais_data.dynamic_ships as q2 -- ship AIS positions
		ON 
			q2.speed = 0 -- not moving
			AND q2.t >= str_to_timestamp('2016-01-01 00:00:00','%Y-%m-%d %H:%M:%S')
			AND q2.t < str_to_timestamp('2016-01-02 00:00:00','%Y-%m-%d %H:%M:%S') -- during Jan 1,2016
			AND ST_DWithinGeographic(q1.geom,q2.geom,500) -- ships within 500m of the port
	GROUP BY libelle_po, mmsi
	) AS q3
LEFT JOIN ais_data.static_ships as q4 -- ship names
ON q3.mmsi = q4.sourcemmsi;

-- Get distance between ships and ports (tests for distancegeographic function)
SELECT port_name, q1.latitude as port_lat, q1.longitude as port_lon, mmsi as ship_mmsi, q2.lat as ship_lat, q2.lon as ship_lon, st_distancegeographic(q1.geom,q2.geom_4326) as distance
FROM ais_data.dynamic_ships as q2
INNER JOIN sys.test_wpi_ports as q1 -- First port for wpi_ports
ON true
LIMIT 50;

CREATE TABLE sys.test_wpi_ports AS
	SELECT * FROM wpi_ports
	LIMIT 1;

CREATE TABLE sys.test_dynamic_ships AS
	SELECT * FROM ais_data.dynamic_ships
	ORDER BY mmsi, t
	LIMIT 50;

CREATE TABLE distance_wpi AS
	SELECT port_name, q1.latitude as port_lat, q1.longitude as port_lon, mmsi as ship_mmsi, q2.lat as ship_lat, q2.lon as ship_lon, st_distancegeographic(q1.geom,q2.geom_4326) as distance
	FROM sys.test_dynamic_ships as q2
	INNER JOIN sys.test_wpi_ports as q1 -- First port for wpi_ports
	ON true;

COPY SELECT * FROM distance_wpi INTO '/Users/bernardo/Monet/VesselAI/Prototype/distance_wpi_monet.csv' USING DELIMITERS ',' , '\n' , '"';
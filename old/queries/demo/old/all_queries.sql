-- LOADING --
-- Loading CSV data
CREATE SCHEMA ais_data;
CREATE TABLE ais_data.dynamic_ships(
  id bigint AUTO_INCREMENT,
  mmsi integer,
  status integer,
  turn double precision,
  speed double precision,
  course double precision,
  heading integer,
  lon double precision,
  lat double precision,
  ts bigint,
  CONSTRAINT dynamic_ships_pkey PRIMARY KEY (id)
);

COPY OFFSET 2 INTO ais_data.dynamic_ships(mmsi,status,turn,speed,course,heading,lon,lat,ts) 
FROM '/ubuntu/data/AIS_Data/nari_dynamic.csv' (mmsi,status,turn,speed,course,heading,lon,lat,ts) 
DELIMITERS ',','\n','"' NULL AS '';

ALTER TABLE ais_data.dynamic_ships ADD COLUMN geom Geometry;
UPDATE ais_data.dynamic_ships SET geom = st_setsrid(st_point(lon,lat),4326);
ALTER TABLE ais_data.dynamic_ships ADD COLUMN t Timestamp;
UPDATE ais_data.dynamic_ships SET t = epoch(cast(ts as int));

-- Loading Shapefile data
CREATE SCHEMA geographic_features;
-- Points
call shpload('/ubuntu/data/Ports_of_Brittany/brittany_ports.shp','geographic_features','brittany_ports');
call shpload('/ubuntu/data/SeaDataNet_Port_Index/seadatanet_fishing_ports.shp','geographic_features','seadata_ports');
-- Lines
call shpload('/ubuntu/data/European_Coastline/europe_coastline.shp','geographic_features','europe_coastline');
-- Polygons
call shpload('/ubuntu/data/Europe_Fishing_Areas/v_recode_fish_area_clean.shp','geographic_features','fishing_areas');

-- Creating trajectories (1st of October 2015)
CREATE TABLE ais_data.segments_01_10_2015 AS
SELECT mmsi, t1, t2, p1, p2, 
  st_makeline(p1,p2) as segment
FROM (
  SELECT mmsi, LEAD(mmsi) OVER (ORDER BY mmsi, t) as mmsi2,
    t AS t1, LEAD(t) OVER (ORDER BY mmsi, t) as t2,
    geom AS p1, LEAD(geom) OVER (ORDER BY mmsi, t) as p2
  FROM ais_data.dynamic_ships
) as q1
WHERE mmsi = mmsi2 
AND t1 > '2015-09-30 23:59:59' AND t2 <= '2015-10-01 23:59:59' 
AND t1 < t2;

set optimizer = 'sequential_pipe';
CREATE TABLE ais_data.trajectories_01_10_2015 AS 
(
  SELECT mmsi, st_collect(segment) as geom, max(t2) as last_message 
  FROM ais_data.segments_01_10_2015 
  GROUP BY mmsi
);

CREATE TABLE ais_data.collect_01_10_2015 AS 
(
  SELECT mmsi, st_collect(geom) as geom, max(t) as last_message 
  FROM ais_data.dynamic_ships 
  WHERE t > '2015-09-30 23:59:59' AND t <= '2015-10-01 23:59:59' 
  GROUP BY mmsi
);

-- QUERIES --

-- Distance - st_distancegeographic()
-- Between first 10 trajectories of October 2015 and the first 10 britanny ports
CREATE TABLE ais_data.trajectories_10_2015_first10 AS SELECT * from ais_data.trajectories_10_2015 order by mmsi limit 10;
CREATE TABLE geographic_features.brittany_ports_first10 AS SELECT * from geographic_features.brittany_ports order by gid limit 10;

SELECT q1.mmsi as ship, q2.gid as port, st_distancegeographic(q1.geom,q2.geom) as distance
FROM ais_data.trajectories_10_2015_first10 as q1
INNER JOIN geographic_features.brittany_ports_first10 as q2
ON TRUE
ORDER BY st_distancegeographic(q1.geom,q2.geom) desc;

-- Between first 10 trajectories of October 2015 and britanny ports
SELECT q1.mmsi as ship, q2.gid as port, st_distancegeographic(q1.geom,q2.geom) as distance
FROM ais_data.trajectories_10_2015_first10 as q1
INNER JOIN geographic_features.brittany_ports as q2
ON TRUE
ORDER BY st_distancegeographic(q1.geom,q2.geom) desc;

-- Between all trajectories and britanny ports
--430014 tuples
--sql:0.093 opt:0.400 run:1837064.292 clk:1837071.603 ms

-- Distance within - st_dwithingeographic()
SELECT 

-- Intersects - st_intersectsgeographic()


-- Covers - st_coversgeographic() -> Only polygons and lines

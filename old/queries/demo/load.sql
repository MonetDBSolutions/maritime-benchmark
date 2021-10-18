CREATE SCHEMA vesselai;
-- Loading CSV data
CREATE TABLE vesselai.ships_all(
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

COPY OFFSET 2 INTO vesselai.ships_all(mmsi,status,turn,speed,course,heading,lon,lat,ts) 
FROM '/home/ubuntu/data/nari_dynamic.csv' (mmsi,status,turn,speed,course,heading,lon,lat,ts) 
--FROM '/Users/bernardo/Monet/VesselAI/Prototype/Datasets/Maritime Integrated Dataset/Navigation/[P1] AIS Data/nari_dynamic.csv' (mmsi,status,turn,speed,course,heading,lon,lat,ts) 
DELIMITERS ',','\n','"' NULL AS '';


ALTER TABLE vesselai.ships_all ADD COLUMN t Timestamp;
UPDATE vesselai.ships_all SET t = epoch(cast(ts as int));
ALTER TABLE vesselai.ships_all ADD COLUMN geom Geometry;
UPDATE vesselai.ships_all SET geom = st_setsrid(st_point(lon,lat),4326);

-- Loading Shapefile data
call shpload('/home/ubuntu/data/port.shp','vesselai','ports');
--call shpload('/Users/bernardo/Monet/VesselAI/Prototype/Datasets/Maritime Integrated Dataset/Geographic/[C1] Ports of Brittany/port.shp','vesselai','ports');
call shpload('/home/ubuntu/data/eez.shp','vesselai','eez');
--call shpload('/Users/bernardo/Monet/VesselAI/Prototype/Datasets/Maritime Integrated Dataset/Geographic/[C2] World EEZ/eez.shp','vesselai','eez');

-- Filter the ammount of data
-- Date -> October 5th
-- Chosen ships (around 1k messages) -> 227860000, 227730220, 226105000, 228022900, 228813000, 304655000, 226177000, 226179000, 228186700, 227941000
CREATE TABLE vesselai.ships AS 
    (SELECT * 
    FROM vesselai.ships_all 
    WHERE mmsi IN (227860000, 227730220, 226105000, 228022900, 228813000, 304655000, 226177000, 226179000, 228186700, 227941000)
    AND EXTRACT(MONTH from t) = 10 AND EXTRACT(DAY from t) = 5);
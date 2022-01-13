DROP TABLE IF EXISTS ais_data.dynamic_sar;

CREATE TABLE ais_data.dynamic_sar (
  id bigint AUTO_INCREMENT,
  mmsi integer,
  altitude smallint,
  speed double precision,
  course double precision,
  lon double precision,
  lat double precision,
  ts bigint,
  CONSTRAINT dynamic_sar_pkey PRIMARY KEY (id) 
);

COPY OFFSET 2 INTO ais_data.dynamic_sar (mmsi,altitude,speed,course,lon,lat,ts) 
FROM '/path/to/data/[P1] AIS Data/nari_dynamic_sar.csv' (mmsi,altitude,speed,course,lon,lat,ts) 
DELIMITERS ',';

#GEOM COLUMN
ALTER TABLE ais_data.dynamic_sar ADD COLUMN geom Geometry;
UPDATE ais_data.dynamic_sar SET geom = st_setsrid(st_point(lon,lat),4326);

#Timestamp Column
ALTER TABLE ais_data.dynamic_sar ADD COLUMN t Timestamp;
UPDATE ais_data.dynamic_sar SET t = epoch(cast(ts as int));


DROP TABLE IF EXISTS ais_data.dynamic_aton;

CREATE TABLE ais_data.dynamic_aton(
  id bigint AUTO_INCREMENT,
  mmsi integer,
  typeofaid smallint,
  aidsname text,
  virtual boolean,
  vrt_tmp char(1),
  lon double precision,
  lat double precision,
  ts bigint,
  CONSTRAINT dynamic_aton_pkey PRIMARY KEY (id) 
);

COPY OFFSET 2 INTO ais_data.dynamic_aton (mmsi,typeofaid,aidsname,vrt_tmp,lon,lat,ts) 
FROM '/path/to/data/[P1] AIS Data/nari_dynamic_aton.csv' (mmsi,typeofaid,aidsname,vrt_tmp,lon,lat,t) 
DELIMITERS ',';
UPDATE ais_data.dynamic_aton SET virtual = (SELECT CASE WHEN vrt_tmp = 'f' THEN FALSE ELSE TRUE END);

#GEOM COLUMN
ALTER TABLE ais_data.dynamic_aton ADD COLUMN geom Geometry;
UPDATE ais_data.dynamic_aton SET geom = st_setsrid(st_point(lon,lat),4326);

#Timestamp Column
ALTER TABLE ais_data.dynamic_aton ADD COLUMN t Timestamp;
UPDATE ais_data.dynamic_aton SET t = epoch(cast(ts as int));


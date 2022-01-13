DROP TABLE IF EXISTS ais_data.dynamic_ships;

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
FROM '/path/to/data/[P1] AIS Data/nari_dynamic.csv' (mmsi,status,turn,speed,course,heading,lon,lat,ts) 
DELIMITERS ',','\n','"' NULL AS '';

#GEOM COLUMN
ALTER TABLE ais_data.dynamic_ships ADD COLUMN geom Geometry;
UPDATE ais_data.dynamic_ships SET geom = st_setsrid(st_point(lon,lat),4326);

#Timestamp Column
ALTER TABLE ais_data.dynamic_ships ADD COLUMN t Timestamp;
UPDATE ais_data.dynamic_ships SET t = epoch(cast(ts as int));


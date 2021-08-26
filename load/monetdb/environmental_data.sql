#
# === Environmental Data  ===
#

DROP TABLE IF EXISTS environment_data.observations;
DROP TABLE IF EXISTS environment_data.wind_direction;
DROP TABLE IF EXISTS environment_data.weather_station;
DROP TABLE IF EXISTS  environment_data.oc_october;
DROP TABLE IF EXISTS  environment_data.oc_november;
DROP TABLE IF EXISTS  environment_data.oc_december;
DROP TABLE IF EXISTS  environment_data.oc_january;
DROP TABLE IF EXISTS  environment_data.oc_february;
DROP TABLE IF EXISTS  environment_data.oc_march;
DROP SCHEMA IF EXISTS environment_data;

CREATE SCHEMA IF NOT EXISTS environment_data;

#
# === Ocean conditions ===
#

---------------- OCTOBER --------------------
CREATE TABLE environment_data.oc_october
(
  lon double precision,
  lat double precision,
  dpt double precision,
  wlv double precision,
  hs double precision,
  lm double precision,
  dir double precision,
  ts bigint
);

COPY OFFSET 2 INTO environment_data.oc_october (lon, lat, dpt, wlv,hs,lm,dir,ts)
FROM '/path/to/data/[E1] Ocean Conditions/oc_october.csv'
DELIMITERS ',','\n','"' NULL AS '';

#GEOM COLUMN
ALTER TABLE environment_data.oc_october ADD COLUMN geom Geometry;
UPDATE environment_data.oc_october SET geom = st_setsrid(st_point(lon,lat),4326);

---------------- NOVEMBER --------------------
CREATE TABLE environment_data.oc_november
(
  lon double precision,
  lat double precision,
  dpt double precision,
  wlv double precision,
  hs double precision,
  lm double precision,
  dir double precision,
  ts bigint
);

COPY OFFSET 2 INTO environment_data.oc_november (lon, lat, dpt, wlv,hs,lm,dir,ts)
FROM '/path/to/data/[E1] Ocean Conditions/oc_november.csv'
DELIMITERS ',','\n','"' NULL AS '';

#GEOM COLUMN
ALTER TABLE environment_data.oc_november ADD COLUMN geom Geometry;
UPDATE environment_data.oc_november SET geom = st_setsrid(st_point(lon,lat),4326);

---------------- DECEMBER --------------------
CREATE TABLE environment_data.oc_december
(
  lon double precision,
  lat double precision,
  dpt double precision,
  wlv double precision,
  hs double precision,
  lm double precision,
  dir double precision,
  ts bigint
);

COPY OFFSET 2 INTO environment_data.oc_december (lon, lat, dpt, wlv,hs,lm,dir,ts)
FROM '/path/to/data/[E1] Ocean Conditions/oc_december.csv'
DELIMITERS ',','\n','"' NULL AS '';

#GEOM COLUMN
ALTER TABLE environment_data.oc_december ADD COLUMN geom Geometry;
UPDATE environment_data.oc_december SET geom = st_setsrid(st_point(lon,lat),4326);

---------------- JANUARY --------------------

CREATE TABLE environment_data.oc_january
(
  lon double precision,
  lat double precision,
  dpt double precision,
  wlv double precision,
  hs double precision,
  lm double precision,
  dir double precision,
  ts bigint
);

COPY OFFSET 2 INTO environment_data.oc_january (lon, lat, dpt, wlv,hs,lm,dir,ts)
FROM '/path/to/data/[E1] Ocean Conditions/oc_january.csv'
DELIMITERS ',','\n','"' NULL AS '';

#GEOM COLUMN
ALTER TABLE environment_data.oc_january ADD COLUMN geom Geometry;
UPDATE environment_data.oc_january SET geom = st_setsrid(st_point(lon,lat),4326);

---------------- FEBRUARY --------------------

CREATE TABLE environment_data.oc_february
(
  lon double precision,
  lat double precision,
  dpt double precision,
  wlv double precision,
  hs double precision,
  lm double precision,
  dir double precision,
  ts bigint
);

COPY OFFSET 2 INTO environment_data.oc_february (lon, lat, dpt, wlv,hs,lm,dir,ts)
FROM '/path/to/data/[E1] Ocean Conditions/oc_february.csv'
DELIMITERS ',','\n','"' NULL AS '';

#GEOM COLUMN
ALTER TABLE environment_data.oc_february ADD COLUMN geom Geometry;
UPDATE environment_data.oc_february SET geom = st_setsrid(st_point(lon,lat),4326);

---------------- MARCH --------------------

CREATE TABLE environment_data.oc_march
(
  lon double precision,
  lat double precision,
  dpt double precision,
  wlv double precision,
  hs double precision,
  lm double precision,
  dir double precision,
  ts bigint
);

COPY OFFSET 2 INTO environment_data.oc_march (lon, lat, dpt, wlv,hs,lm,dir,ts)
FROM '/path/to/data/[E1] Ocean Conditions/oc_march.csv'
DELIMITERS ',','\n','"' NULL AS '';

#GEOM COLUMN
ALTER TABLE environment_data.oc_march ADD COLUMN geom Geometry;
UPDATE environment_data.oc_march SET geom = st_setsrid(st_point(lon,lat),4326);

#
# === Weather data ===
#
CREATE TABLE environment_data.wind_direction
(
  id_wind_direction integer,
  dd_num double precision,
  dd_plaintext text,
  dd_shorttext character varying(3),
  CONSTRAINT wind_direction_pkey PRIMARY KEY (id_wind_direction)  
);

COPY OFFSET 2 INTO environment_data.wind_direction(id_wind_direction, dd_num, dd_plaintext, dd_shorttext)
FROM '/path/to/data/[E2] Weather Conditions/table_windDirection.csv'
DELIMITERS ',','\n','"';

CREATE TABLE environment_data.weather_station
(
  id_station integer,
  station_name text,
  latitude double precision,
  longitude double precision,
  elevation double precision,
  CONSTRAINT wheather_station_pkey PRIMARY KEY (id_station)  
);

COPY OFFSET 2 INTO environment_data.weather_station(id_station, station_name, latitude, longitude, elevation)
FROM '/path/to/data/[E2] Weather Conditions/table_weatherStation.csv'
DELIMITERS ',';

#GEOM COLUMN
ALTER TABLE environment_data.weather_station ADD COLUMN geom Geometry;
UPDATE environment_data.weather_station SET geom = st_setsrid(st_point(longitude,latitude),4326);

UPDATE sys.fao_areas SET geom = st_setsrid(geom,4326);

CREATE TABLE environment_data.observations
(
  id bigint AUTO_INCREMENT, 
  id_station integer,
  local_time integer,
  T double precision,
  Tn double precision,
  Tx double precision,
  P double precision,
  U integer,
  id_windDirection integer,
  Ff integer,
  ff10 integer, 
  ff3 integer,
  VV double precision,
  Td double precision,
  RRR double precision,
  tR integer,
  CONSTRAINT observations_pkey PRIMARY KEY (id),    
  CONSTRAINT station_fk FOREIGN KEY (id_station)     
      REFERENCES environment_data.weather_station (id_station)     
      ON DELETE CASCADE    
      ON UPDATE CASCADE    ,    
  CONSTRAINT wind_direction_fk FOREIGN KEY (id_windDirection)     
      REFERENCES environment_data.wind_direction (id_wind_direction)     
      ON DELETE CASCADE    
      ON UPDATE CASCADE    
  
 );

COPY OFFSET 2 INTO environment_data.observations (id_station, local_time, T, Tn, Tx, P, U, id_windDirection, Ff, ff10, ff3, VV, Td, RRR, tR)
FROM '/path/to/data/[E2] Weather Conditions/table_wheatherObservation.csv' (id_station, local_time, T, Tn, Tx, P, U, id_windDirection, Ff, ff10, ff3, VV, Td, RRR, tR)
DELIMITERS ',';

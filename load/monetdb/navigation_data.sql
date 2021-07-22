#
#	=== Navigation Data (Naval Academy receiver) ===
#

DROP TABLE IF EXISTS ais_data.dynamic_sar;
DROP TABLE IF EXISTS ais_data.dynamic_aton;
DROP TABLE IF EXISTS ais_data.static_ships;
DROP TABLE IF EXISTS ais_data.dynamic_ships;
DROP SCHEMA IF EXISTS ais_data CASCADE;

CREATE SCHEMA IF NOT EXISTS ais_data;

-- SAR
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

-- ATON
CREATE TABLE ais_data.dynamic_aton(
  id bigint AUTO_INCREMENT,
  mmsi integer,
  typeofaid smallint,
  aidsname text,
  virtual boolean,
  lon double precision,
  lat double precision,
  ts bigint,
  CONSTRAINT dynamic_aton_pkey PRIMARY KEY (id) 
);

COPY OFFSET 2 INTO ais_data.dynamic_aton (mmsi,typeofaid,aidsname,virtual,lon,lat,ts) 
FROM '/path/to/data/[P1] AIS Data/nari_dynamic_aton.csv' (mmsi,typeofaid,aidsname,virtual,lon,lat,t) 
DELIMITERS ',';

#GEOM COLUMN
ALTER TABLE ais_data.dynamic_aton ADD COLUMN geom Geometry;
UPDATE ais_data.dynamic_aton SET geom = st_setsrid(st_point(lon,lat),4326);

#Timestamp Column
ALTER TABLE ais_data.dynamic_aton ADD COLUMN t Timestamp;
UPDATE ais_data.dynamic_aton SET t = epoch(cast(ts as int));

-- STATIC SHIPS
CREATE TABLE ais_data.static_ships(
  id bigint AUTO_INCREMENT,
  sourcemmsi integer,
  imo integer,
  callsign text,
  shipname text,
  shiptype integer,
  to_bow integer,
  to_stern integer,
  to_starboard integer,
  to_port integer,
  eta text,
  draught double precision,
  destination text,
  mothershipmmsi integer,
  ts bigint,
  CONSTRAINT static_ships_pkey PRIMARY KEY (id)
);

COPY OFFSET 2 INTO ais_data.static_ships(sourcemmsi,imo,callsign,shipname,shiptype,to_bow,to_stern,to_starboard,to_port,eta,draught,destination,mothershipmmsi,ts) 
FROM '/path/to/data/[P1] AIS Data/nari_static.csv' (sourcemmsi,imo,callsign,shipname,shiptype,to_bow,to_stern,to_starboard,to_port,eta,draught,destination,mothershipmmsi,ts) 
DELIMITERS ',','\n','"' NULL AS '';

#Timestamp Column
ALTER TABLE ais_data.static_ships ADD COLUMN t Timestamp;
UPDATE ais_data.static_ships SET t = epoch(cast(ts as int));

-- DYNAMIC SHIPS 
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

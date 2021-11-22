-- -------------------------------------
-- Dataset 10.5281/zenodo.1167595
-- Naval Academy, France
-- Licence CC-BY-NC-SA-4.0
-- -------------------------------------

 -- [P1] AIS Data --
DROP SCHEMA IF EXISTS ais_data CASCADE;
DROP TABLE IF EXISTS ais_data.dynamic_srbm;
DROP TABLE IF EXISTS ais_data.dynamic_sar;
DROP TABLE IF EXISTS ais_data.dynamic_aton;
DROP TABLE IF EXISTS ais_data.static_ships;
DROP TABLE IF EXISTS ais_data.dynamic_ships;

DROP SEQUENCE IF EXISTS ais_data.dynamic_srbm_id_seq;
DROP SEQUENCE IF EXISTS ais_data.dynamic_sar_id_seq;
DROP SEQUENCE IF EXISTS ais_data.dynamic_aton_id_seq;
DROP SEQUENCE IF EXISTS ais_data.static_ships_id_seq;
DROP SEQUENCE IF EXISTS ais_data.dynamic_ships_id_seq;

CREATE SCHEMA IF NOT EXISTS ais_data;


-- SAR
CREATE SEQUENCE ais_data.dynamic_sar_id_seq
  INCREMENT 1
  MINVALUE 1
  MAXVALUE 9223372036854775807
  START 1
  CACHE 1;

 
CREATE TABLE ais_data.dynamic_sar
(
  id bigint NOT NULL DEFAULT nextval('ais_data.dynamic_sar_id_seq'::regclass),
  mmsi integer,
  altitude smallint,
  speed double precision,
  course double precision,
  lon double precision,
  lat double precision,
  ts bigint,
  CONSTRAINT dynamic_sar_pkey PRIMARY KEY (id) 
)
WITH (
  OIDS=FALSE
);

COPY ais_data.dynamic_sar(
  mmsi,altitude,speed,course,lon,lat,ts
  )
  FROM '/path/to/data/[P1] AIS Data/nari_dynamic_sar.csv'
delimiter ',' csv HEADER;

ALTER TABLE ais_data.dynamic_sar ADD COLUMN geom geometry(Point,4326);
UPDATE ais_data.dynamic_sar SET geom = ST_SetSRID(ST_MakePoint(lon, lat),4326);

ALTER TABLE ais_data.dynamic_sar ADD COLUMN t timestamp without time zone;
UPDATE ais_data.dynamic_sar SET t = to_timestamp(ts);

-- ATON
CREATE SEQUENCE ais_data.dynamic_aton_id_seq
  INCREMENT 1
  MINVALUE 1
  MAXVALUE 9223372036854775807
  START 1
  CACHE 1;
   
CREATE TABLE ais_data.dynamic_aton
(
  id bigint NOT NULL DEFAULT nextval('ais_data.dynamic_aton_id_seq'::regclass),
  mmsi integer,
  typeofaid smallint,
  aidsname text,
  virtual boolean,
  lon double precision,
  lat double precision,
  ts bigint,
  CONSTRAINT dynamic_aton_pkey PRIMARY KEY (id) 
)
WITH (
  OIDS=FALSE
);

COPY ais_data.dynamic_aton(
  mmsi,typeofaid,aidsname,virtual,lon,lat,ts
  )
  FROM '/path/to/data/[P1] AIS Data/nari_dynamic_aton.csv'
delimiter ',' csv HEADER;

ALTER TABLE ais_data.dynamic_aton ADD COLUMN geom geometry(Point,4326);
UPDATE ais_data.dynamic_aton SET geom = ST_SetSRID(ST_MakePoint(lon, lat),4326);

-- STATIC SHIPS
CREATE SEQUENCE ais_data.static_ships_id_seq
  INCREMENT 1
  MINVALUE 1
  MAXVALUE 9223372036854775807
  START 1
  CACHE 1;
  
CREATE TABLE ais_data.static_ships(
  id bigint NOT NULL DEFAULT nextval('ais_data.static_ships_id_seq'::regclass),
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
)
WITH (
  OIDS=FALSE
);

COPY ais_data.static_ships(
  sourcemmsi,imo,callsign,shipname,shiptype,to_bow,to_stern,to_starboard,to_port,eta,draught,destination,mothershipmmsi,ts
  )
  FROM '/path/to/data/[P1] AIS Data/nari_static.csv'
delimiter ',' csv HEADER;



-- DYNAMIC SHIPS 
CREATE SEQUENCE ais_data.dynamic_ships_id_seq
  INCREMENT 1
  MINVALUE 1
  MAXVALUE 9223372036854775807
  START 1
  CACHE 1;

  
CREATE TABLE ais_data.dynamic_ships(
  id bigint NOT NULL DEFAULT nextval('ais_data.dynamic_ships_id_seq'::regclass),
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
)
WITH (
  OIDS=FALSE
);

COPY ais_data.dynamic_ships(
  mmsi,status,turn,speed,course,heading,lon,lat,ts
  )
  FROM '/path/to/data/[P1] AIS Data/nari_dynamic.csv'
delimiter ',' csv HEADER;

ALTER TABLE ais_data.dynamic_ships ADD COLUMN geom geometry(Point,4326);
UPDATE ais_data.dynamic_ships SET geom = ST_SetSRID(ST_MakePoint(lon, lat),4326);

ALTER TABLE ais_data.dynamic_ships ADD COLUMN t timestamp without time zone;
UPDATE ais_data.dynamic_ships SET t = to_timestamp(ts);


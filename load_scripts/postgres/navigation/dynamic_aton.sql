DROP TABLE IF EXISTS ais_data.dynamic_aton;
DROP SEQUENCE IF EXISTS ais_data.dynamic_aton_id_seq;

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
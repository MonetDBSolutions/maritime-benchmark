DROP TABLE IF EXISTS ais_data.dynamic_sar;
DROP SEQUENCE IF EXISTS ais_data.dynamic_sar_id_seq;

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
DROP TABLE IF EXISTS ais_data.dynamic_ships;
DROP SEQUENCE IF EXISTS ais_data.dynamic_ships_id_seq;

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
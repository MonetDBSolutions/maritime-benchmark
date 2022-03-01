-- -------------------------------------
-- Dataset 10.5281/zenodo.1167595
-- Naval Academy, France
-- Licence CC-BY-NC-SA-4.0
-- -------------------------------------

-- [E2] WEATHER CONDITIONS --
DROP SCHEMA IF EXISTS environment_data CASCADE;
CREATE SCHEMA IF NOT EXISTS environment_data;

DROP TABLE IF EXISTS environment_data.observations;
DROP TABLE IF EXISTS environment_data.wind_direction;
DROP TABLE IF EXISTS environment_data.weather_station;


CREATE TABLE environment_data.wind_direction
(
  id_wind_direction integer,
  dd_num double precision,
  dd_plaintext text,
  dd_shorttext character varying(3),
  CONSTRAINT wind_direction_pkey PRIMARY KEY (id_wind_direction)
)
WITH (
  OIDS=FALSE
);

COPY environment_data.wind_direction(
  id_wind_direction, dd_num, dd_plaintext, dd_shorttext)
  FROM '/path/to/data/[E2] Weather Conditions/table_windDirection.csv'
delimiter ',' csv header;

CREATE TABLE environment_data.weather_station
(
  id_station integer,
  station_name text,
  latitude double precision,
  longitude double precision,
  elevation double precision,
  CONSTRAINT wheather_station_pkey PRIMARY KEY (id_station)
)
WITH (
  OIDS=FALSE
);

COPY environment_data.weather_station(
  id_station, station_name, latitude, longitude, elevation)
  FROM '/path/to/data/[E2] Weather Conditions/table_weatherStation.csv'
delimiter ',' csv header;

ALTER TABLE environment_data.weather_station ADD COLUMN geom geometry(Geometry,4326);
UPDATE environment_data.weather_station SET geom = ST_SetSRID(ST_MakePoint(longitude,latitude),4326);



CREATE SEQUENCE environment_data.observations_id_seq
  INCREMENT 1
  MINVALUE 1
  MAXVALUE 9223372036854775807
  START 1
  CACHE 1;

CREATE TABLE environment_data.observations
(
  id bigint NOT NULL DEFAULT nextval('environment_data.observations_id_seq'::regclass),
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

 )
WITH (
  OIDS=FALSE
);

COPY environment_data.observations(
  id_station, local_time, T, Tn, Tx, P, U, id_windDirection, Ff, ff10, ff3, VV, Td, RRR, tR)
  FROM '/path/to/data/[E2] Weather Conditions/table_wheatherObservation.csv'
delimiter ',' csv header;
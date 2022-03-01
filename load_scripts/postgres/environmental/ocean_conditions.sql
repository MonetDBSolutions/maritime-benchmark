-- -------------------------------------
-- Dataset 10.5281/zenodo.1167595
-- Naval Academy, France
-- Licence CC-BY-NC-SA-4.0
-- -------------------------------------

CREATE SCHEMA IF NOT EXISTS environment_data;

---------------- OCTOBER --------------------
DROP TABLE IF EXISTS  environment_data.oc_october;
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
)
WITH (
  OIDS=FALSE
);

COPY environment_data.oc_october (lon, lat, dpt, wlv,hs,lm,dir,ts)
FROM '/path/to/data/[E1] Ocean Conditions/oc_october.csv'
delimiter ',' csv header;

---------------- NOVEMBER --------------------
DROP TABLE IF EXISTS  environment_data.oc_november;

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
)
WITH (
  OIDS=FALSE
);

COPY environment_data.oc_november (lon, lat, dpt, wlv,hs,lm,dir,ts)
FROM '/path/to/data/[E1] Ocean Conditions/oc_november.csv'
delimiter ',' csv header;

---------------- DECEMBER --------------------
DROP TABLE IF EXISTS  environment_data.oc_december;
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
)
WITH (
  OIDS=FALSE
);

COPY environment_data.oc_december (lon, lat, dpt, wlv,hs,lm,dir,ts)
FROM '/path/to/data/[E1] Ocean Conditions/oc_december.csv'
delimiter ',' csv header;

---------------- JANUARY --------------------
DROP TABLE IF EXISTS  environment_data.oc_january;

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
)
WITH (
  OIDS=FALSE
);

COPY environment_data.oc_january (lon, lat, dpt, wlv,hs,lm,dir,ts)
FROM '/path/to/data/[E1] Ocean Conditions/oc_january.csv'
delimiter ',' csv header;




---------------- FEBRUARY --------------------
DROP TABLE IF EXISTS  environment_data.oc_february;

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
)
WITH (
  OIDS=FALSE
);


COPY environment_data.oc_february (lon, lat, dpt, wlv,hs,lm,dir,ts)
FROM '/path/to/data/[E1] Ocean Conditions/oc_february.csv'
delimiter ',' csv header;


---------------- MARCH --------------------
DROP TABLE IF EXISTS  environment_data.oc_march;

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
)
WITH (
  OIDS=FALSE
);

COPY environment_data.oc_march (lon, lat, dpt, wlv,hs,lm,dir,ts)
FROM '/path/to/data/[E1] Ocean Conditions/oc_march.csv'
delimiter ',' csv header;

----------- Spatialisation of tables  -----------

ALTER TABLE environment_data.oc_october ADD COLUMN geom geometry(Point,4326);
UPDATE environment_data.oc_october SET geom = ST_SetSRID(ST_MakePoint(lon, lat),4326);

ALTER TABLE environment_data.oc_november ADD COLUMN geom geometry(Point,4326);
UPDATE environment_data.oc_november SET geom = ST_SetSRID(ST_MakePoint(lon, lat),4326);

ALTER TABLE environment_data.oc_december ADD COLUMN geom geometry(Point,4326);
UPDATE environment_data.oc_december SET geom = ST_SetSRID(ST_MakePoint(lon, lat),4326);

ALTER TABLE environment_data.oc_january ADD COLUMN geom geometry(Point,4326);
UPDATE environment_data.oc_january SET geom = ST_SetSRID(ST_MakePoint(lon, lat),4326);

ALTER TABLE environment_data.oc_february ADD COLUMN geom geometry(Point,4326);
UPDATE environment_data.oc_february SET geom = ST_SetSRID(ST_MakePoint(lon, lat),4326);

ALTER TABLE environment_data.oc_march ADD COLUMN geom geometry(Point,4326);
UPDATE environment_data.oc_march SET geom = ST_SetSRID(ST_MakePoint(lon, lat),4326);

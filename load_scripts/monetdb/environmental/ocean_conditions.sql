DROP TABLE IF EXISTS environment_data.oc_october;
DROP TABLE IF EXISTS environment_data.oc_november;
DROP TABLE IF EXISTS environment_data.oc_december;
DROP TABLE IF EXISTS environment_data.oc_january;
DROP TABLE IF EXISTS environment_data.oc_february;
DROP TABLE IF EXISTS environment_data.oc_march;

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


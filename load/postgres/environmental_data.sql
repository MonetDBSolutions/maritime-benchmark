-- -------------------------------------
-- Dataset 10.5281/zenodo.1167595
-- Naval Academy, France
-- Licence CC-BY-NC-SA-4.0
-- -------------------------------------

CREATE SCHEMA IF NOT EXISTS ocean_condition;

---------------- OCTOBER --------------------
DROP TABLE IF EXISTS  ocean_condition.oc_october;
CREATE TABLE ocean_condition.oc_october
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

COPY ocean_condition.oc_october (lon, lat, dpt, wlv,hs,lm,dir,ts)
FROM '/path/to/data/[E1] Ocean Conditions/oc_october.csv'
delimiter ',' csv header;

---------------- NOVEMBER --------------------
DROP TABLE IF EXISTS  ocean_condition.oc_november;

CREATE TABLE ocean_condition.oc_november
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

COPY ocean_condition.oc_november (lon, lat, dpt, wlv,hs,lm,dir,ts)
FROM '/path/to/data/[E1] Ocean Conditions/oc_november.csv'
delimiter ',' csv header;

---------------- DECEMBER --------------------
DROP TABLE IF EXISTS  ocean_condition.oc_december;
CREATE TABLE ocean_condition.oc_december
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

COPY ocean_condition.oc_december (lon, lat, dpt, wlv,hs,lm,dir,ts)
FROM '/path/to/data/[E1] Ocean Conditions/oc_december.csv'
delimiter ',' csv header;

---------------- JANUARY --------------------
DROP TABLE IF EXISTS  ocean_condition.oc_january;

CREATE TABLE ocean_condition.oc_january
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

COPY ocean_condition.oc_january (lon, lat, dpt, wlv,hs,lm,dir,ts)
FROM '/path/to/data/[E1] Ocean Conditions/oc_january.csv'
delimiter ',' csv header;



  
---------------- FEBRUARY --------------------
DROP TABLE IF EXISTS  ocean_condition.oc_february;

CREATE TABLE ocean_condition.oc_february
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


COPY ocean_condition.oc_february (lon, lat, dpt, wlv,hs,lm,dir,ts)
FROM '/path/to/data/[E1] Ocean Conditions/oc_february.csv'
delimiter ',' csv header;


---------------- MARCH --------------------
DROP TABLE IF EXISTS  ocean_condition.oc_march;

CREATE TABLE ocean_condition.oc_march
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
  
COPY ocean_condition.oc_march (lon, lat, dpt, wlv,hs,lm,dir,ts)
FROM '/path/to/data/[E1] Ocean Conditions/oc_march.csv'
delimiter ',' csv header;

----------- Spatialisation of tables  ----------- 

ALTER TABLE ocean_condition.oc_october ADD COLUMN geom geometry(Point,4326);
UPDATE ocean_condition.oc_october SET geom = ST_SetSRID(ST_MakePoint(lon, lat),4326);

ALTER TABLE ocean_condition.oc_november ADD COLUMN geom geometry(Point,4326);
UPDATE ocean_condition.oc_november SET geom = ST_SetSRID(ST_MakePoint(lon, lat),4326);

ALTER TABLE ocean_condition.oc_december ADD COLUMN geom geometry(Point,4326);
UPDATE ocean_condition.oc_december SET geom = ST_SetSRID(ST_MakePoint(lon, lat),4326);

ALTER TABLE ocean_condition.oc_january ADD COLUMN geom geometry(Point,4326);
UPDATE ocean_condition.oc_january SET geom = ST_SetSRID(ST_MakePoint(lon, lat),4326);

ALTER TABLE ocean_condition.oc_february ADD COLUMN geom geometry(Point,4326);
UPDATE ocean_condition.oc_february SET geom = ST_SetSRID(ST_MakePoint(lon, lat),4326);

ALTER TABLE ocean_condition.oc_march ADD COLUMN geom geometry(Point,4326);
UPDATE ocean_condition.oc_march SET geom = ST_SetSRID(ST_MakePoint(lon, lat),4326);


-- -------------------------------------
-- Dataset 10.5281/zenodo.1167595
-- Naval Academy, France
-- Licence CC-BY-NC-SA-4.0
-- -------------------------------------

-- [E2] WEATHER CONDITIONS -- 
--DROP SCHEMA IF EXISTS wheather_conditions CASCADE;
CREATE SCHEMA IF NOT EXISTS wheather_conditions;

DROP TABLE IF EXISTS wheather_conditions.observations;
DROP TABLE IF EXISTS wheather_conditions.wind_direction;
DROP TABLE IF EXISTS wheather_conditions.weather_station;


CREATE TABLE wheather_conditions.wind_direction
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

COPY wheather_conditions.wind_direction(
  id_wind_direction, dd_num, dd_plaintext, dd_shorttext)
  FROM '/path/to/data/[E2] Weather Conditions/table_windDirection.csv'
delimiter ',' csv header;

CREATE TABLE wheather_conditions.weather_station
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

COPY wheather_conditions.weather_station(
  id_station, station_name, latitude, longitude, elevation)
  FROM '/path/to/data/[E2] Weather Conditions/table_weatherStation.csv'
delimiter ',' csv header;

ALTER TABLE wheather_conditions.weather_station ADD COLUMN geom geometry(Geometry,4326);
UPDATE wheather_conditions.weather_station SET geom = ST_SetSRID(ST_MakePoint(longitude,latitude),4326);
  


CREATE SEQUENCE wheather_conditions.observations_id_seq
  INCREMENT 1
  MINVALUE 1
  MAXVALUE 9223372036854775807
  START 1
  CACHE 1;
  
CREATE TABLE wheather_conditions.observations
(
  id bigint NOT NULL DEFAULT nextval('wheather_conditions.observations_id_seq'::regclass), 
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
      REFERENCES wheather_conditions.weather_station (id_station)     
      ON DELETE CASCADE    
      ON UPDATE CASCADE    ,    
  CONSTRAINT wind_direction_fk FOREIGN KEY (id_windDirection)     
      REFERENCES wheather_conditions.wind_direction (id_wind_direction)     
      ON DELETE CASCADE    
      ON UPDATE CASCADE    
  
 )
WITH (
  OIDS=FALSE
);

COPY wheather_conditions.observations(
  id_station, local_time, T, Tn, Tx, P, U, id_windDirection, Ff, ff10, ff3, VV, Td, RRR, tR)
  FROM '/path/to/data/[E2] Weather Conditions/table_wheatherObservation.csv'
delimiter ',' csv header;

-- -------------------------------------
-- Dataset 10.5281/zenodo.1167595
-- Naval Academy, France
-- Licence CC-BY-NC-SA-4.0
-- -------------------------------------

-- NATURA 2000 ---

CREATE SCHEMA IF NOT EXISTS natura2000;

DROP TABLE IF EXISTS natura2000.directives_species;
DROP TABLE IF EXISTS natura2000.contacts;
DROP TABLE IF EXISTS natura2000.bioregion;
DROP TABLE IF EXISTS natura2000.designation_status;
DROP TABLE IF EXISTS natura2000.impact;
DROP TABLE IF EXISTS natura2000.management;
DROP TABLE IF EXISTS natura2000.habitat_class;
DROP TABLE IF EXISTS natura2000.habitat;
DROP TABLE IF EXISTS natura2000.other_species;
DROP TABLE IF EXISTS natura2000.species;
DROP TABLE IF EXISTS natura2000.sites;

DROP SEQUENCE IF EXISTS natura2000.directives_species_id_seq;
DROP SEQUENCE IF EXISTS natura2000.contacts_id_seq;
DROP SEQUENCE IF EXISTS natura2000.bioregion_id_seq;
DROP SEQUENCE IF EXISTS natura2000.designation_status_id_seq;
DROP SEQUENCE IF EXISTS natura2000.impact_id_seq;
DROP SEQUENCE IF EXISTS natura2000.management_id_seq;
DROP SEQUENCE IF EXISTS natura2000.habitat_class_id_seq;
DROP SEQUENCE IF EXISTS natura2000.habitat_id_seq;
DROP SEQUENCE IF EXISTS natura2000.other_species_id_seq;
DROP SEQUENCE IF EXISTS natura2000.species_id_seq;
DROP SEQUENCE IF EXISTS natura2000.sites_id_seq;


CREATE SEQUENCE natura2000.directives_species_id_seq
  INCREMENT 1
  MINVALUE 1
  MAXVALUE 9223372036854775807
  START 1
  CACHE 1;
  
CREATE TABLE natura2000.directives_species
  (
  id bigint NOT NULL DEFAULT nextval('natura2000.directives_species_id_seq'::regclass), 
  directive text,
  species_name text,
  annexii text,
  annexii1 text,
  annexii2 text,
  annexiii1 text,
  annexiii2 text,
  annexiv text,
  annexv text,
  spbcax1 text,
  CONSTRAINT directive_pkey PRIMARY KEY (id)    
  )
WITH (
  OIDS=FALSE
);

COPY natura2000.directives_species(
  directive,species_name,annexii,annexii1,annexii2,annexiii1,annexiii2,annexiv,annexv,spbcax1)
  FROM '/path/to/data/[C5] Marine Protected Areas (EEA Natura 2000)/descriptive data/DIRECTIVESPECIES.csv'
delimiter ',' csv header;


CREATE SEQUENCE natura2000.contacts_id_seq
  INCREMENT 1
  MINVALUE 1
  MAXVALUE 9223372036854775807
  START 1
  CACHE 1;
  
CREATE TABLE natura2000.contacts
  (
  id bigint NOT NULL DEFAULT nextval('natura2000.contacts_id_seq'::regclass), 
  "sitecode" text,
  "name" text,
  "email" text,
  "address_unstructured" text,
  "adminunit" text,
  "thoroughfare" text,
  "designator" text,
  "postcode" text,
  "postname" text,
  "address" text,
  "locatorname" text,
  CONSTRAINT contacts_pkey PRIMARY KEY (id)    
  )
WITH (
  OIDS=FALSE
);

COPY natura2000.contacts(
  "sitecode","name","email","address_unstructured","adminunit","thoroughfare","designator","postcode","postname","address","locatorname")
  FROM '/path/to/data/[C5] Marine Protected Areas (EEA Natura 2000)/descriptive data/CONTACTS.csv'
delimiter ',' csv header;
   

CREATE SEQUENCE natura2000.bioregion_id_seq
  INCREMENT 1
  MINVALUE 1
  MAXVALUE 9223372036854775807
  START 1
  CACHE 1;
  
CREATE TABLE natura2000.bioregion
  (
  id bigint NOT NULL DEFAULT nextval('natura2000.bioregion_id_seq'::regclass), 
  "sitecode" text,
  "biogefraphicreg" text,
  "percentage" double precision,
  CONSTRAINT bioregion_pkey PRIMARY KEY (id)    
  )
WITH (
  OIDS=FALSE
);

COPY natura2000.bioregion(
  "sitecode","biogefraphicreg","percentage")
  FROM '/path/to/data/[C5] Marine Protected Areas (EEA Natura 2000)/descriptive data/BIOREGION.csv'
delimiter ',' csv header;



CREATE SEQUENCE natura2000.designation_status_id_seq
  INCREMENT 1
  MINVALUE 1
  MAXVALUE 9223372036854775807
  START 1
  CACHE 1;
  
CREATE TABLE natura2000.designation_status
  (
  id bigint NOT NULL DEFAULT nextval('natura2000.designation_status_id_seq'::regclass),   
  "sitecode" text,
  "designationcode" text,
  "designatedsitename" text,
  "overlapcode" text,
  "overlapperc" double precision,
  CONSTRAINT designation_status_pkey PRIMARY KEY (id)    
  )
WITH (
  OIDS=FALSE
);

COPY natura2000.designation_status(
  "sitecode","designationcode","designatedsitename","overlapcode","overlapperc")
  FROM '/path/to/data/[C5] Marine Protected Areas (EEA Natura 2000)/descriptive data/DESIGNATIONSTATUS.csv'
delimiter ',' csv header;



CREATE SEQUENCE natura2000.impact_id_seq
  INCREMENT 1
  MINVALUE 1
  MAXVALUE 9223372036854775807
  START 1
  CACHE 1;
  
CREATE TABLE natura2000.impact
  (
  id bigint NOT NULL DEFAULT nextval('natura2000.impact_id_seq'::regclass),  
  "sitecode" text,"impactcode" text,"description" text,"intensity" text,"pollutioncode" text,"occurrence" text,"impact_type" text,
  CONSTRAINT impact_pkey PRIMARY KEY (id)    
  )
WITH (
  OIDS=FALSE
);

COPY natura2000.impact(
  "sitecode","impactcode","description","intensity","pollutioncode","occurrence","impact_type")
  FROM '/path/to/data/[C5] Marine Protected Areas (EEA Natura 2000)/descriptive data/IMPACT.csv'
delimiter ',' csv header;



CREATE SEQUENCE natura2000.habitat_class_id_seq
  INCREMENT 1
  MINVALUE 1
  MAXVALUE 9223372036854775807
  START 1
  CACHE 1;
  
CREATE TABLE natura2000.habitat_class
  (
  id bigint NOT NULL DEFAULT nextval('natura2000.habitat_class_id_seq'::regclass),
  "sitecode" text,
  "habitatcode" text,
  "percentagecover" double precision,
  "description" text,
  CONSTRAINT habitat_class_pkey PRIMARY KEY (id)    
  )
WITH (
  OIDS=FALSE
);

COPY natura2000.habitat_class(
  "sitecode","habitatcode","percentagecover","description")
  FROM '/path/to/data/[C5] Marine Protected Areas (EEA Natura 2000)/descriptive data/HABITATCLASS.csv'
delimiter ',' csv header;


CREATE SEQUENCE natura2000.habitats_id_seq
  INCREMENT 1
  MINVALUE 1
  MAXVALUE 9223372036854775807
  START 1
  CACHE 1;
  
CREATE TABLE natura2000.habitats
  (
  id bigint NOT NULL DEFAULT nextval('natura2000.habitats_id_seq'::regclass),
  "sitecode" text,
  "habitatcode" text,
  "description" text,
  "habitat_priority" text,
  "priority_form_habitat_type" text,
  "non_presence_in_site" text,
  "cover_ha"  double precision,
  "caves" text,
  "representativity" text,
  "relsurface" text,
  "conservation" text,
  "global_assesment" text,
  "dataquality" text,
  "percentage_cover"  double precision,
  CONSTRAINT habitats_pkey PRIMARY KEY (id)    
  )
WITH (
  OIDS=FALSE
);

COPY natura2000.habitats(
  "sitecode","habitatcode","description","habitat_priority","priority_form_habitat_type","non_presence_in_site","cover_ha","caves","representativity","relsurface","conservation","global_assesment","dataquality","percentage_cover")
  FROM '/path/to/data/[C5] Marine Protected Areas (EEA Natura 2000)/descriptive data/HABITATS.csv'
delimiter ',' csv header;



CREATE SEQUENCE natura2000.other_species_id_seq
  INCREMENT 1
  MINVALUE 1
  MAXVALUE 9223372036854775807
  START 1
  CACHE 1;
  
  
CREATE TABLE natura2000.other_species
  (
  id bigint NOT NULL DEFAULT nextval('natura2000.other_species_id_seq'::regclass),
  "country_code" text,
  "sitecode" text,
  "speciesgroup" text,
  "speciesname" text,
  "speciescode" text,
  "motivation" text,
  "sensitive" text,
  "nonpresenceinsite" text,
  "lowerbound" text,
  "upperbound" text,
  "counting_unit" text,
  "abundance_category" text,
  CONSTRAINT other_species_pkey PRIMARY KEY (id)    
  )
WITH (
  OIDS=FALSE
);

COPY natura2000.other_species(
  "country_code","sitecode","speciesgroup","speciesname","speciescode","motivation","sensitive","nonpresenceinsite","lowerbound","upperbound","counting_unit","abundance_category")
  FROM '/path/to/data/[C5] Marine Protected Areas (EEA Natura 2000)/descriptive data/OTHERSPECIES.csv'
delimiter ',' csv header;



CREATE SEQUENCE natura2000.species_id_seq
  INCREMENT 1
  MINVALUE 1
  MAXVALUE 9223372036854775807
  START 1
  CACHE 1;
  
  
CREATE TABLE natura2000.species
  (
  id bigint NOT NULL DEFAULT nextval('natura2000.species_id_seq'::regclass),
  "country_code" text,
  "sitecode" text,
  "speciesname" text,
  "speciescode" text,
  "ref_spgroup" text,
  "spgroup" text,
  "sensitive" text,
  "nonpresenceinsite" text,
  "population_type" text,
  "lowerbound" integer,
  "upperbound" integer,
  "counting_unit" text,
  "abundance_category" text,
  "dataquality" text,
  "population" text,
  "conservation" text,
  "isolation" text,
  "global" text,
  CONSTRAINT species_pkey PRIMARY KEY (id)    
  )
WITH (
  OIDS=FALSE
);

COPY natura2000.species(
  "country_code","sitecode","speciesname","speciescode","ref_spgroup","spgroup","sensitive","nonpresenceinsite","population_type","lowerbound","upperbound","counting_unit","abundance_category","dataquality","population","conservation","isolation","global")
  FROM '/path/to/data/[C5] Marine Protected Areas (EEA Natura 2000)/descriptive data/SPECIES.csv'
delimiter ',' csv header;



CREATE SEQUENCE natura2000.sites_id_seq
  INCREMENT 1
  MINVALUE 1
  MAXVALUE 9223372036854775807
  START 1
  CACHE 1;
  
  
CREATE TABLE natura2000.sites
  (
  id bigint NOT NULL DEFAULT nextval('natura2000.sites_id_seq'::regclass),
  "country_code" text,
  "sitecode" text,
  "sitename" text,
  "sitetype" text,
  "date_compilation" date,
  "date_update" date,
  "date_spa" date,
  "spa_legal_reference" text,
  "date_prop_sci" date,
  "date_conf_sci" date,
  "date_sac" date,
  "sac_legal_reference" text,
  "explanations" text,
  "areaha" double precision,
  "lengthkm"  double precision,
  "marine_area_percentage"  double precision,
  "longitude" double precision,
  "latitude" double precision,
  "documentation" text,
  "quality" text,
  "designation" text,
  "othercharact" text,
  CONSTRAINT sites_pkey PRIMARY KEY (id)    
  )
WITH (
  OIDS=FALSE
);

ALTER TABLE natura2000.sites ADD COLUMN geom geometry(Geometry,4326);
UPDATE natura2000.sites SET geom = ST_SetSRID(ST_MakePoint(longitude,latitude),4326);

SET datestyle = "ISO, DMY";
COPY natura2000.sites(
  "country_code","sitecode","sitename","sitetype","date_compilation","date_update","date_spa","spa_legal_reference","date_prop_sci","date_conf_sci","date_sac","sac_legal_reference","explanations","areaha","lengthkm","marine_area_percentage","longitude","latitude","documentation","quality","designation","othercharact")
  FROM '/path/to/data/[C5] Marine Protected Areas (EEA Natura 2000)/descriptive data/NATURA2000SITES.csv'
delimiter ',' csv header;
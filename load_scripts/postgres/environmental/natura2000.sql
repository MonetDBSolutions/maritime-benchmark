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
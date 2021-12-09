-- -------------------------------------
-- Dataset 10.5281/zenodo.1167595
-- Naval Academy, France
-- Licence CC-BY-NC-SA-4.0
-- -------------------------------------

-- P1] AIS Status, Codes and Types --
DROP SCHEMA IF EXISTS ais_status_codes_types CASCADE;
DROP TABLE IF EXISTS ais_status_codes_types.aton;
DROP TABLE IF EXISTS ais_status_codes_types.mmsi_country_codes;
DROP TABLE IF EXISTS ais_status_codes_types.navigational_status;
DROP TABLE IF EXISTS ais_status_codes_types.ship_types;
DROP SEQUENCE IF EXISTS ais_status_codes_types.ship_types_detailed;

CREATE SCHEMA IF NOT EXISTS ais_status_codes_types;
  
CREATE TABLE ais_status_codes_types.aton
  (
  nature text,
  id_code integer,
  definition text,
  CONSTRAINT aton_code_pkey PRIMARY KEY (id_code)    
  )
WITH (
  OIDS=FALSE
);

COPY ais_status_codes_types.aton(
  nature, id_code, definition)
  FROM '/Users/bernardo/Monet/Geo/maritime-import/data/[P1] AIS Status, Codes and Types/aton.csv'
delimiter ';' csv header;

  
CREATE TABLE ais_status_codes_types.mmsi_country_codes
  (
  country_code integer,
  country text,
  CONSTRAINT country_code_pkey PRIMARY KEY (country_code)    
  )
WITH (
  OIDS=FALSE
);

COPY ais_status_codes_types.mmsi_country_codes(
  country_code, country)
  FROM '/Users/bernardo/Monet/Geo/maritime-import/data/[P1] AIS Status, Codes and Types/MMSI Country Codes.csv'
delimiter ',' csv ;


  
CREATE TABLE ais_status_codes_types.navigational_status
  (
  id_status integer,
  definition text,
  CONSTRAINT id_status_pkey PRIMARY KEY (id_status)    
  )
WITH (
  OIDS=FALSE
);

COPY ais_status_codes_types.navigational_status(
  id_status, definition)
  FROM '/Users/bernardo/Monet/Geo/maritime-import/data/[P1] AIS Status, Codes and Types/Navigational Status.csv'
delimiter ';' csv;


  
CREATE TABLE ais_status_codes_types.ship_types
  (
  id_shiptype integer,
  shiptype_min integer,
  shiptype_max integer,
  type_name text,
  ais_type_summary text,
  CONSTRAINT id_ship_types_pkey PRIMARY KEY (id_shiptype)    
  )
WITH (
  OIDS=FALSE
);

COPY ais_status_codes_types.ship_types(
  id_shiptype,shiptype_min,shiptype_max,type_name,ais_type_summary
  )
  FROM '/Users/bernardo/Monet/Geo/maritime-import/data/[P1] AIS Status, Codes and Types/Ship Types List.csv'
delimiter ',' csv header;

  
  
CREATE TABLE ais_status_codes_types.ship_types_detailed
  (
  id_detailedtype integer,
  detailed_type text,
  id_shiptype integer,
  CONSTRAINT id_detailedtype_pkey PRIMARY KEY (id_detailedtype), 
  CONSTRAINT ship_type_fk FOREIGN KEY (id_shiptype)     
      REFERENCES ais_status_codes_types.ship_types (id_shiptype)     
      ON DELETE CASCADE    
      ON UPDATE CASCADE       
  )
WITH (
  OIDS=FALSE
);

COPY ais_status_codes_types.ship_types_detailed(
  id_detailedtype,detailed_type,id_shiptype
  )
  FROM '/Users/bernardo/Monet/Geo/maritime-import/data/[P1] AIS Status, Codes and Types/Ship Types Detailed List.csv'
delimiter ',' csv header;

-- -------------------------------------
-- Dataset 10.5281/zenodo.1167595
-- Naval Academy, France
-- Licence CC-BY-NC-SA-4.0
-- -------------------------------------

-- [C6] ANFR Vessel List --

DROP SCHEMA IF EXISTS vesselregister CASCADE;
DROP TABLE IF EXISTS vesselregister.anfr_vessel_list;
DROP SEQUENCE IF EXISTS vesselregister.anfr_vessel_list_id_seq;

CREATE SCHEMA IF NOT EXISTS vesselregister;

CREATE SEQUENCE vesselregister.anfr_vessel_list_id_seq
  INCREMENT 1
  MINVALUE 1
  MAXVALUE 9223372036854775807
  START 1
  CACHE 1;
  
CREATE TABLE vesselregister.anfr_vessel_list
(
  id bigint NOT NULL DEFAULT nextval('vesselregister.anfr_vessel_list_id_seq'::regclass),
  maritime_area text,
  registration_number text,
  imo_number text,
  ship_name text,
  callsign text,
  mmsi integer,
  shiptype text,
  length double precision,
  tonnage double precision,
  tonnage_unit text,
  materiel_onboard text,
  atis_code text,
  radio_license_status text,
  date_first_license text,
  date_inactivity_license text,
  CONSTRAINT anfr_pkey PRIMARY KEY (id)
)
WITH (
  OIDS=FALSE
);

COPY vesselregister.anfr_vessel_list (maritime_area, registration_number, imo_number, ship_name, callsign, mmsi, shiptype, length, tonnage, tonnage_unit, 
 materiel_onboard, atis_code, radio_license_status, date_first_license, date_inactivity_license)
FROM '/Users/bernardo/Monet/Geo/maritime-import/data/[C6] ANFR Vessel List/anfr.csv' 
delimiter ';' csv header;

-- -------------------------------------
-- Dataset 10.5281/zenodo.1167595
-- Naval Academy, France
-- Licence CC-BY-NC-SA-4.0
-- -------------------------------------

-- [C6] EU Fishing Vessels --

DROP TABLE IF EXISTS vesselregister.eu_fishingvessels;
DROP TABLE IF EXISTS vesselregister.eu_eventcode_details;
DROP TABLE IF EXISTS vesselregister.eu_geartypecode_details;
DROP SEQUENCE IF EXISTS vesselregister.eu_fishingvessels_id_seq;

CREATE SEQUENCE vesselregister.eu_fishingvessels_id_seq
  INCREMENT 1
  MINVALUE 1
  MAXVALUE 9223372036854775807
  START 1
  CACHE 1;

CREATE TABLE vesselregister.eu_fishingvessels
(
  id bigint NOT NULL DEFAULT nextval('vesselregister.eu_fishingvessels_id_seq'::regclass),  
  countrycode character varying(3),
  cfr character varying(12),
  eventcode character varying(3),
  eventstartdate integer,
  eventenddate integer,
  licenseind character varying(1),
  registrationnbr character varying(15),
  extmarking character varying(15),
  vesselname text,
  portcode character varying(10),
  portname character varying(30),
  ircscode character varying(1),
  ircs character varying(10),
  vmscode character varying(1),
  gearmaincode character varying(3),
  gearseccode character varying(3),
  loa real,
  lbp real,
  tonref real,
  tongt real,
  tonoth real,
  tongts real,
  powermain real,
  poweraux real,
  hullmaterial smallint,
  comyear smallint,
  commonth smallint,
  comday smallint,
  segment character varying(10),
  expcountry character varying(3),
  exptype character varying(2),
  publicaidcode character varying(2),
  decisiondate integer,
  decisionsegcode character varying(15),
  constructionyear smallint,
  constructionplace text,
  CONSTRAINT eu_fishingvessels_pkey PRIMARY KEY (id)
)
WITH (
  OIDS=FALSE
);

COPY vesselregister.eu_fishingvessels(countrycode,
  cfr, eventcode, eventstartdate, eventenddate, licenseind, registrationnbr, extmarking, vesselname, portcode, portname, ircscode, ircs, vmscode, gearmaincode,
  gearseccode, loa, lbp, tonref, tongt, tonoth, tongts, powermain, poweraux, hullmaterial, comyear, commonth, comday, segment, expcountry, exptype, publicaidcode,
  decisiondate, decisionsegcode, constructionyear, constructionplace)
FROM '/Users/bernardo/Monet/Geo/maritime-import/data/[C6] EU Fishing Vessels/EuropeanVesselRegister.csv'
delimiter ';' csv header;
  
CREATE TABLE vesselregister.eu_eventcode_details
(
  eventcode character varying(3),
  details text,
  CONSTRAINT eventcode_pkey PRIMARY KEY (eventcode)
)
WITH (
  OIDS=FALSE
);
  
COPY vesselregister.eu_eventcode_details(eventcode,details)
FROM '/Users/bernardo/Monet/Geo/maritime-import/data/[C6] EU Fishing Vessels/table_event_codes.csv' 
delimiter ',' QUOTE '"' csv header;


CREATE TABLE vesselregister.eu_geartypecode_details
(
  gearmaincode character varying(3),
  detail1 text,
  detail2 text,
  CONSTRAINT geartypecode_pkey PRIMARY KEY (gearmaincode)
)
WITH (
  OIDS=FALSE
);
  
COPY vesselregister.eu_geartypecode_details(gearmaincode, detail1, detail2)
FROM '/Users/bernardo/Monet/Geo/maritime-import/data/[C6] EU Fishing Vessels/table_gear_type_code.csv' 
delimiter ',' QUOTE '"' csv header;


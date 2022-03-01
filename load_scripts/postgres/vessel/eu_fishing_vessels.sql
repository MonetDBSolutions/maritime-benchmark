-- -------------------------------------
-- Dataset 10.5281/zenodo.1167595
-- Naval Academy, France
-- Licence CC-BY-NC-SA-4.0
-- -------------------------------------

-- [C6] EU Fishing Vessels --

DROP TABLE IF EXISTS vessel_data.eu_fishingvessels;
DROP TABLE IF EXISTS vessel_data.eu_eventcode_details;
DROP TABLE IF EXISTS vessel_data.eu_geartypecode_details;
DROP SEQUENCE IF EXISTS vessel_data.eu_fishingvessels_id_seq;

CREATE SEQUENCE vessel_data.eu_fishingvessels_id_seq
  INCREMENT 1
  MINVALUE 1
  MAXVALUE 9223372036854775807
  START 1
  CACHE 1;

CREATE TABLE vessel_data.eu_fishingvessels
(
  id bigint NOT NULL DEFAULT nextval('vessel_data.eu_fishingvessels_id_seq'::regclass),
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

COPY vessel_data.eu_fishingvessels(countrycode,
  cfr, eventcode, eventstartdate, eventenddate, licenseind, registrationnbr, extmarking, vesselname, portcode, portname, ircscode, ircs, vmscode, gearmaincode,
  gearseccode, loa, lbp, tonref, tongt, tonoth, tongts, powermain, poweraux, hullmaterial, comyear, commonth, comday, segment, expcountry, exptype, publicaidcode,
  decisiondate, decisionsegcode, constructionyear, constructionplace)
FROM '/path/to/data/[C6] EU Fishing Vessels/EuropeanVesselRegister.csv'
delimiter ';' csv header;

CREATE TABLE vessel_data.eu_eventcode_details
(
  eventcode character varying(3),
  details text,
  CONSTRAINT eventcode_pkey PRIMARY KEY (eventcode)
)
WITH (
  OIDS=FALSE
);

COPY vessel_data.eu_eventcode_details(eventcode,details)
FROM '/path/to/data/[C6] EU Fishing Vessels/table_event_codes.csv'
delimiter ',' QUOTE '"' csv header;


CREATE TABLE vessel_data.eu_geartypecode_details
(
  gearmaincode character varying(3),
  detail1 text,
  detail2 text,
  CONSTRAINT geartypecode_pkey PRIMARY KEY (gearmaincode)
)
WITH (
  OIDS=FALSE
);

COPY vessel_data.eu_geartypecode_details(gearmaincode, detail1, detail2)
FROM '/path/to/data/[C6] EU Fishing Vessels/table_gear_type_code.csv'
delimiter ',' QUOTE '"' csv header;
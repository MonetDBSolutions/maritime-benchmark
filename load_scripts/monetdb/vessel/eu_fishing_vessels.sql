DROP TABLE IF EXISTS vessel_data.eu_fishingvessels;
DROP TABLE IF EXISTS vessel_data.eu_eventcode_details;
DROP TABLE IF EXISTS vessel_data.eu_geartypecode_details;

CREATE TABLE vessel_data.eu_fishingvessels
(
  id bigint AUTO_INCREMENT,  
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
);

COPY OFFSET 2 INTO vessel_data.eu_fishingvessels (countrycode, cfr, 
    eventcode, eventstartdate, eventenddate, licenseind, registrationnbr, 
    extmarking, vesselname, portcode, portname, ircscode, ircs, vmscode, 
    gearmaincode,gearseccode, loa, lbp, tonref, tongt, tonoth, tongts, 
    powermain, poweraux, hullmaterial, comyear, commonth, comday, 
    segment, expcountry, exptype, publicaidcode,decisiondate, 
    decisionsegcode, constructionyear, constructionplace)
FROM '/path/to/data/[C6] EU Fishing Vessels/EuropeanVesselRegister.csv' (countrycode, cfr,
    eventcode, eventstartdate, eventenddate, licenseind, registrationnbr, 
    extmarking, vesselname, portcode, portname, ircscode, ircs, vmscode, 
    gearmaincode,gearseccode, loa, lbp, tonref, tongt, tonoth, tongts, 
    powermain, poweraux, hullmaterial, comyear, commonth, comday, 
    segment, expcountry, exptype, publicaidcode,decisiondate, 
    decisionsegcode, constructionyear, constructionplace)
DELIMITERS ';','\n','"' NULL AS '';

CREATE TABLE vessel_data.eu_eventcode_details
(
  eventcode character varying(3),
  details text,
  CONSTRAINT eventcode_pkey PRIMARY KEY (eventcode)
);

COPY OFFSET 2 INTO vessel_data.eu_eventcode_details (eventcode,details)
FROM '/path/to/data/[C6] EU Fishing Vessels/table_event_codes.csv' 
DELIMITERS ',','\n','"' NULL AS '';


CREATE TABLE vessel_data.eu_geartypecode_details
(
  gearmaincode character varying(3),
  detail1 text,
  detail2 text,
  CONSTRAINT geartypecode_pkey PRIMARY KEY (gearmaincode)
);

COPY OFFSET 2 INTO vessel_data.eu_geartypecode_details(gearmaincode, detail1, detail2)
FROM '/path/to/data/[C6] EU Fishing Vessels/table_gear_type_code.csv' 
DELIMITERS ',','\n','"' NULL AS '';


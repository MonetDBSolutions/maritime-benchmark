#
#	=== Vessel Data ===
#

DROP SCHEMA IF EXISTS vessel_data CASCADE;
DROP TABLE IF EXISTS vessel_data.aton;
DROP TABLE IF EXISTS vessel_data.mmsi_country_codes;
DROP TABLE IF EXISTS vessel_data.navigational_status;
DROP TABLE IF EXISTS vessel_data.ship_types_detailed;
DROP TABLE IF EXISTS vessel_data.ship_types;

CREATE SCHEMA IF NOT EXISTS vessel_data;
  
CREATE TABLE vessel_data.aton (
  nature text,
  id_code integer,
  definition text,
  CONSTRAINT aton_code_pkey PRIMARY KEY (id_code)    
);

COPY OFFSET 2 INTO vessel_data.aton(nature, id_code, definition) 
FROM '/Users/bernardo/Monet/Geo/maritime-import/data/[P1] AIS Status, Codes and Types/aton.csv' 
DELIMITERS ';';

CREATE TABLE vessel_data.mmsi_country_codes(
  country_code integer,
  country text,
  CONSTRAINT country_code_pkey PRIMARY KEY (country_code)    
);

COPY INTO vessel_data.mmsi_country_codes(country_code, country) 
FROM '/Users/bernardo/Monet/Geo/maritime-import/data/[P1] AIS Status, Codes and Types/MMSI Country Codes.csv'
DELIMITERS ',','\n','"' NULL AS '';
  
CREATE TABLE vessel_data.navigational_status (
  id_status integer,
  definition text,
  CONSTRAINT id_status_pkey PRIMARY KEY (id_status)    
);

COPY INTO vessel_data.navigational_status(id_status, definition) 
FROM '/Users/bernardo/Monet/Geo/maritime-import/data/[P1] AIS Status, Codes and Types/Navigational Status.csv'
DELIMITERS ';';
  
CREATE TABLE vessel_data.ship_types (
  id_shiptype integer,
  shiptype_min integer,
  shiptype_max integer,
  type_name text,
  ais_type_summary text,
  CONSTRAINT id_ship_types_pkey PRIMARY KEY (id_shiptype)    
);

COPY OFFSET 2 INTO vessel_data.ship_types(id_shiptype,shiptype_min,shiptype_max,type_name,ais_type_summary) 
FROM '/Users/bernardo/Monet/Geo/maritime-import/data/[P1] AIS Status, Codes and Types/Ship Types List.csv'
DELIMITERS ',';
  
CREATE TABLE vessel_data.ship_types_detailed (
  id_detailedtype integer,
  detailed_type text,
  id_shiptype integer,
  CONSTRAINT id_detailedtype_pkey PRIMARY KEY (id_detailedtype), 
  CONSTRAINT ship_type_fk FOREIGN KEY (id_shiptype)     
      REFERENCES vessel_data.ship_types (id_shiptype)     
      ON DELETE CASCADE    
      ON UPDATE CASCADE       
);

COPY OFFSET 2 INTO vessel_data.ship_types_detailed (id_detailedtype,detailed_type,id_shiptype) 
FROM '/Users/bernardo/Monet/Geo/maritime-import/data/[P1] AIS Status, Codes and Types/Ship Types Detailed List.csv'
DELIMITERS ',';

#
#	=== Vessel Data (ANFR Vessel List) ===
#

DROP TABLE IF EXISTS vessel_data.anfr_vessel_list;

CREATE TABLE vessel_data.anfr_vessel_list
(
  id bigint AUTO_INCREMENT,
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
);

COPY OFFSET 2 INTO vessel_data.anfr_vessel_list (maritime_area, registration_number, imo_number, ship_name, callsign, mmsi, shiptype, length, tonnage, tonnage_unit, materiel_onboard, atis_code, radio_license_status, date_first_license, date_inactivity_license)
FROM '/Users/bernardo/Monet/Geo/maritime-import/data/[C6] ANFR Vessel List/anfr.csv' (maritime_area, registration_number, imo_number, ship_name, callsign, mmsi, shiptype, length, tonnage, tonnage_unit, materiel_onboard, atis_code, radio_license_status, date_first_license, date_inactivity_license)
DELIMITERS ';','\n','"' NULL AS '';


#
#	=== Vessel Data (EU Fishing Vessels) ===
#

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

COPY OFFSET 2 INTO vessel_data.eu_fishingvessels (countrycode,cfr, eventcode, eventstartdate, eventenddate, licenseind, registrationnbr, extmarking, vesselname, portcode, portname, ircscode, ircs, vmscode, gearmaincode,gearseccode, loa, lbp, tonref, tongt, tonoth, tongts, powermain, poweraux, hullmaterial, comyear, commonth, comday, segment, expcountry, exptype, publicaidcode,decisiondate, decisionsegcode, constructionyear, constructionplace)
FROM '/Users/bernardo/Monet/Geo/maritime-import/data/[C6] EU Fishing Vessels/EuropeanVesselRegister.csv' (countrycode,cfr, eventcode, eventstartdate, eventenddate, licenseind, registrationnbr, extmarking, vesselname, portcode, portname, ircscode, ircs, vmscode, gearmaincode,gearseccode, loa, lbp, tonref, tongt, tonoth, tongts, powermain, poweraux, hullmaterial, comyear, commonth, comday, segment, expcountry, exptype, publicaidcode,decisiondate, decisionsegcode, constructionyear, constructionplace)
DELIMITERS ';','\n','"' NULL AS '';

CREATE TABLE vessel_data.eu_eventcode_details
(
  eventcode character varying(3),
  details text,
  CONSTRAINT eventcode_pkey PRIMARY KEY (eventcode)
);

COPY OFFSET 2 INTO vessel_data.eu_eventcode_details (eventcode,details)
FROM '/Users/bernardo/Monet/Geo/maritime-import/data/[C6] EU Fishing Vessels/table_event_codes.csv' 
DELIMITERS ',','\n','"' NULL AS '';


CREATE TABLE vessel_data.eu_geartypecode_details
(
  gearmaincode character varying(3),
  detail1 text,
  detail2 text,
  CONSTRAINT geartypecode_pkey PRIMARY KEY (gearmaincode)
);

COPY OFFSET 2 INTO vessel_data.eu_geartypecode_details(gearmaincode, detail1, detail2)
FROM '/Users/bernardo/Monet/Geo/maritime-import/data/[C6] EU Fishing Vessels/table_gear_type_code.csv' 
DELIMITERS ',','\n','"' NULL AS '';


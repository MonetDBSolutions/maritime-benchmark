-- -------------------------------------
-- Dataset 10.5281/zenodo.1167595
-- Naval Academy, France
-- Licence CC-BY-NC-SA-4.0
-- -------------------------------------

-- [C6] ANFR Vessel List --
DROP TABLE IF EXISTS vessel_data.anfr_vessel_list;
DROP SEQUENCE IF EXISTS vessel_data.anfr_vessel_list_id_seq;

CREATE SEQUENCE vessel_data.anfr_vessel_list_id_seq
  INCREMENT 1
  MINVALUE 1
  MAXVALUE 9223372036854775807
  START 1
  CACHE 1;

CREATE TABLE vessel_data.anfr_vessel_list
(
  id bigint NOT NULL DEFAULT nextval('vessel_data.anfr_vessel_list_id_seq'::regclass),
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

COPY vessel_data.anfr_vessel_list (maritime_area, registration_number, imo_number, ship_name, callsign, mmsi, shiptype, length, tonnage, tonnage_unit,
 materiel_onboard, atis_code, radio_license_status, date_first_license, date_inactivity_license)
FROM '/path/to/data/[C6] ANFR Vessel List/anfr.csv'
delimiter ';' csv header;
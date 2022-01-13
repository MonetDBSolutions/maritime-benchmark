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

COPY OFFSET 2 INTO vessel_data.anfr_vessel_list (maritime_area, 
    registration_number, imo_number, ship_name, callsign, mmsi,
    shiptype, length, tonnage, tonnage_unit, materiel_onboard, 
    atis_code, radio_license_status, date_first_license, 
    date_inactivity_license)
FROM '/path/to/data/[C6] ANFR Vessel List/anfr.csv' (maritime_area, 
    registration_number, imo_number, ship_name, callsign, mmsi, 
    shiptype, length, tonnage, tonnage_unit, materiel_onboard, 
    atis_code, radio_license_status, date_first_license, 
    date_inactivity_license)
DELIMITERS ';','\n','"' NULL AS '';


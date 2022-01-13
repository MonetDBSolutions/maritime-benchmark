DROP TABLE IF EXISTS vessel_data.mmsi_country_codes;

CREATE TABLE vessel_data.mmsi_country_codes(
  country_code integer,
  country text,
  CONSTRAINT country_code_pkey PRIMARY KEY (country_code)    
);

COPY INTO vessel_data.mmsi_country_codes(country_code, country) 
FROM '/path/to/data/[P1] AIS Status, Codes and Types/MMSI Country Codes.csv'
DELIMITERS ',','\n','"' NULL AS '';


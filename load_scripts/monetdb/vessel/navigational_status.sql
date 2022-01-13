DROP TABLE IF EXISTS vessel_data.navigational_status;

CREATE TABLE vessel_data.navigational_status (
  id_status integer,
  definition text,
  CONSTRAINT id_status_pkey PRIMARY KEY (id_status)    
);

COPY INTO vessel_data.navigational_status(id_status, definition) 
FROM '/path/to/data/[P1] AIS Status, Codes and Types/Navigational Status.csv'
DELIMITERS ';';


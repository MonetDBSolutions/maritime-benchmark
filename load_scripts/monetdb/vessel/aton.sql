DROP TABLE IF EXISTS vessel_data.aton;

CREATE TABLE vessel_data.aton (
  nature text,
  id_code integer,
  definition text,
  CONSTRAINT aton_code_pkey PRIMARY KEY (id_code)    
);

COPY OFFSET 2 INTO vessel_data.aton(nature, id_code, definition) 
FROM '/path/to/data/[P1] AIS Status, Codes and Types/ATON.csv'
DELIMITERS ';';


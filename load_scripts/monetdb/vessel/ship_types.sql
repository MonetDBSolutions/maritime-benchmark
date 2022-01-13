DROP TABLE IF EXISTS vessel_data.ship_types;

CREATE TABLE vessel_data.ship_types (
  id_shiptype integer,
  shiptype_min integer,
  shiptype_max integer,
  type_name text,
  ais_type_summary text,
  CONSTRAINT id_ship_types_pkey PRIMARY KEY (id_shiptype)    
);

COPY OFFSET 2 INTO vessel_data.ship_types(id_shiptype,shiptype_min,shiptype_max,type_name,ais_type_summary) 
FROM '/path/to/data/[P1] AIS Status, Codes and Types/Ship Types List.csv'
DELIMITERS ',';

DROP TABLE IF EXISTS vessel_data.ship_types_detailed;

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
FROM '/path/to/data/[P1] AIS Status, Codes and Types/Ship Types Detailed List.csv'
DELIMITERS ',';


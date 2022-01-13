DROP TABLE IF EXISTS ais_data.static_ships;

CREATE TABLE ais_data.static_ships(
  id bigint AUTO_INCREMENT,
  sourcemmsi integer,
  imo integer,
  callsign text,
  shipname text,
  shiptype integer,
  to_bow integer,
  to_stern integer,
  to_starboard integer,
  to_port integer,
  eta text,
  draught double precision,
  destination text,
  mothershipmmsi integer,
  ts bigint,
  CONSTRAINT static_ships_pkey PRIMARY KEY (id)
);

COPY OFFSET 2 INTO ais_data.static_ships(sourcemmsi,imo,callsign,shipname,shiptype,to_bow,to_stern,to_starboard,to_port,eta,draught,destination,mothershipmmsi,ts) 
FROM '/path/to/data/[P1] AIS Data/nari_static.csv' (sourcemmsi,imo,callsign,shipname,shiptype,to_bow,to_stern,to_starboard,to_port,eta,draught,destination,mothershipmmsi,ts) 
DELIMITERS ',','\n','"' NULL AS '' BEST EFFORT;

#Timestamp Column
ALTER TABLE ais_data.static_ships ADD COLUMN t Timestamp;
UPDATE ais_data.static_ships SET t = epoch(cast(ts as int));


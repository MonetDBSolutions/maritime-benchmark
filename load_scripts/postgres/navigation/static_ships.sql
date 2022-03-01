DROP TABLE IF EXISTS ais_data.static_ships;
DROP SEQUENCE IF EXISTS ais_data.static_ships_id_seq;

-- STATIC SHIPS
CREATE SEQUENCE ais_data.static_ships_id_seq
  INCREMENT 1
  MINVALUE 1
  MAXVALUE 9223372036854775807
  START 1
  CACHE 1;

CREATE TABLE ais_data.static_ships(
  id bigint NOT NULL DEFAULT nextval('ais_data.static_ships_id_seq'::regclass),
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
)
WITH (
  OIDS=FALSE
);

COPY ais_data.static_ships(
  sourcemmsi,imo,callsign,shipname,shiptype,to_bow,to_stern,to_starboard,to_port,eta,draught,destination,mothershipmmsi,ts
  )
  FROM '/path/to/data/[P1] AIS Data/nari_static.csv'
delimiter ',' csv HEADER;
CREATE SCHEMA IF NOT EXISTS  bench_geo;
SET SCHEMA 'bench_geo';
CREATE EXTENSION IF NOT EXISTS postgis WITH SCHEMA bench_geo;
CREATE TABLE ais_dynamic (id SERIAL PRIMARY KEY, mmsi integer, status integer, turn double precision, speed double precision, course double precision, heading integer, lon double precision, lat double precision, ts bigint, t timestamp without time zone, geom geometry(Point,4326)) WITH (OIDS=FALSE);
CREATE TABLE ais_static(id SERIAL PRIMARY KEY, sourcemmsi integer, imo integer, callsign text, shipname text, shiptype integer, to_bow integer, to_stern integer, to_starboard integer, to_port integer, eta text, draught double precision, destination text, mothershipmmsi integer, ts bigint, t timestamp without time zone) WITH (OIDS=FALSE);
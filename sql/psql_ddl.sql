CREATE SCHEMA IF NOT EXISTS  bench_geo;
SET SCHEMA 'bench_geo';
ALTER DATABASE marine SET search_path = bench_geo, public;
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE TABLE IF NOT EXISTS ais_dynamic (id SERIAL PRIMARY KEY, mmsi integer, status integer, turn double precision, speed double precision, course double precision, heading integer, lon double precision, lat double precision, ts bigint, t timestamp without time zone, geom geometry(Point,4326)) WITH (OIDS=FALSE);
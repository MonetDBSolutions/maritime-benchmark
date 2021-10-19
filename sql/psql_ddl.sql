CREATE SCHEMA IF NOT EXISTS  bench_geo;
SET SCHEMA 'bench_geo';
CREATE EXTENSION IF NOT EXISTS postgis WITH SCHEMA bench_geo;
CREATE TABLE IF NOT EXISTS ais_dynamic (id SERIAL PRIMARY KEY, mmsi integer, status integer, turn double precision, speed double precision, course double precision, heading integer, lon double precision, lat double precision, ts bigint, t timestamp without time zone, geom geometry(Point,4326)) WITH (OIDS=FALSE);
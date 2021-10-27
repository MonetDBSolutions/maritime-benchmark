CALL sys.querylog_enable();
CREATE SCHEMA IF NOT EXISTS bench_geo;
SET SCHEMA = bench_geo;
SET CURRENT_TIMEZONE = INTERVAL '122' MINUTE;
CREATE TABLE IF NOT EXISTS ais_dynamic (id bigint AUTO_INCREMENT, mmsi integer, status integer, turn double precision, speed double precision, course double precision, heading integer, lon double precision, lat double precision, ts bigint, t Timestamp, geom Geometry, CONSTRAINT dynamic_ships_pkey PRIMARY KEY (id));
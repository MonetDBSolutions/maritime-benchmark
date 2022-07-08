CREATE TABLE IF NOT EXISTS ais_data.dynamic_ships (id bigint AUTO_INCREMENT, mmsi integer, status integer, turn double precision, speed double precision, course double precision, heading integer, lon double precision, lat double precision, ts bigint, t Timestamp, geom Geometry, geom3035 Geometry, mbr mbr, CONSTRAINT dynamic_ships_pkey PRIMARY KEY (id));
COPY INTO ais_data.dynamic_ships (mmsi,status,turn,speed,course,heading,lon,lat,ts, geom, geom3035) FROM '/Users/bernardo/Monet/Geo/dynamic_ships_psql.csv' (mmsi,status,turn,speed,course,heading,lon,lat,ts, geom, geom3035) USING DELIMITERS '\t', E'\n', '"' NULL AS '';
UPDATE ais_data.dynamic_ships SET t = epoch(cast(ts as bigint));
UPDATE ais_data.dynamic_ships SET mbr = mbr(geom3035);
UPDATE ais_data.dynamic_ships SET geom = st_setsrid(geom,4326);
UPDATE ais_data.dynamic_ships SET geom3035 = st_setsrid(geom3035,3035);
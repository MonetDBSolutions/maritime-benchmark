#!/bin/bash

db_name="maritime"

if [ $# -eq 1 ]; then
	db_name=$1
fi

echo "Connecting to '$db_name' database"

psql -d $db_name -c 'CREATE EXTENSION postgis';

sql_load=("navigation_data.sql" "vessel_data.sql" "environmental_data.sql")

for s in ${sql_load[@]}; do
	echo "Loading $s"
	psql -d $db_name -f $PWD/load_scripts/postgres/$s
done

echo "Loading Shapefile data"

# Ports Locations
psql -d $db_name  -c "CREATE SCHEMA IF NOT EXISTS ports;"
shp2pgsql -I -s 4326 '/Users/bernardo/Monet/Geo/maritime-import/data/[C1] Ports of Brittany/port.shp' ports.ports_of_brittany | psql  -d $db_name;
shp2pgsql -I -s 4326 '/Users/bernardo/Monet/Geo/maritime-import/data/[C1] SeaDataNet Port Index/Fishing Ports.shp' ports.fishing_ports | psql  -d $db_name;
shp2pgsql -I -s 4326 '/Users/bernardo/Monet/Geo/maritime-import/data/[C1] World Port Index/wpi.shp' ports.wpi_ports | psql  -d $db_name; 


# Geographic regions
psql -d $db_name  -c "CREATE SCHEMA IF NOT EXISTS geographic_features;"
shp2pgsql -I -s 3035 '/Users/bernardo/Monet/Geo/maritime-import/data/[C2] European Coastline/Europe Coastline.shp' geographic_features.europe_coastline | psql  -d $db_name;
shp2pgsql -I -s 3035 '/Users/bernardo/Monet/Geo/maritime-import/data/[C2] European Coastline/Europe Coastline (Polygone).shp' geographic_features.europe_coastline_polygone | psql  -d $db_name;
shp2pgsql -I -s 4258 '/Users/bernardo/Monet/Geo/maritime-import/data/[C2] European Maritime Boundaries/mbeulsiv1.shp' geographic_features.europe_maritime_bounderies | psql  -d $db_name;
shp2pgsql -I -s 4326 '/Users/bernardo/Monet/Geo/maritime-import/data/[C2] IHO World Seas/world_seas_iho_v2.shp' geographic_features.world_seas | psql  -d $db_name;
shp2pgsql -I -s 4326 '/Users/bernardo/Monet/Geo/maritime-import/data/[C2] World EEZ/eez.shp' geographic_features.world_eez | psql  -d $db_name;
shp2pgsql -I -s 4326 '/Users/bernardo/Monet/Geo/maritime-import/data/[C2] World EEZ/eez_boundaries.shp' geographic_features.world_eez_boundaries | psql  -d $db_name;
shp2pgsql -I -s 4326 '/Users/bernardo/Monet/Geo/maritime-import/data/[C4] FAO Maritime Areas/fao_areas.shp' geographic_features.fao_areas | psql  -d $db_name;
shp2pgsql -I -s 4326 '/Users/bernardo/Monet/Geo/maritime-import/data/[C4] Fishing Areas (European commission)/v_recode_fish_area_clean.shp' geographic_features.fishing_areas_eu | psql  -d $db_name;
shp2pgsql -I -s 4326 '/Users/bernardo/Monet/Geo/maritime-import/data/[C5] Fishing Constraints/fishing_interdiction.shp' geographic_features.fishing_constraints | psql  -d $db_name;


# AIS Receiver used to capture data
psql -d $db_name  -c "CREATE SCHEMA IF NOT EXISTS Receiver;"
shp2pgsql -I -s 4326 '/Users/bernardo/Monet/Geo/maritime-import/data/[P1] Brest Receiver/receptor.shp' Receiver.ais_receiver | psql  -d $db_name;
shp2pgsql -I -s 4326 '/Users/bernardo/Monet/Geo/maritime-import/data/[P1] Brest Receiver/Theoretical Coverage.shp' Receiver.ais_receiver_coverage | psql  -d $db_name;

# Natura 
psql -d $db_name  -c "CREATE SCHEMA IF NOT EXISTS natura2000;"
shp2pgsql -I -s 3035 '/Users/bernardo/Monet/Geo/maritime-import/data/[C5] Marine Protected Areas (EEA Natura 2000)/spatial data/Natura2000_end2015.shp' natura2000.spatialfeatures | psql  -d $db_name;


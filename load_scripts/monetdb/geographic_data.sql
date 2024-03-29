#
# === Geographic Data ===
#

DROP SCHEMA IF EXISTS geographic_data CASCADE;
CREATE SCHEMA geographic_data;

call shpload('/path/to/data/[C1] Ports of Brittany/port.shp','geographic_data','brittany_ports');
call shpload('/path/to/data/[C1] SeaDataNet Port Index/Fishing Ports.shp','geographic_data','seadata_ports');
call shpload('/path/to/data/[C2] European Coastline/Europe Coastline.shp','geographic_data','europe_coastline');
call shpload('/path/to/data/[C2] European Coastline/Europe Coastline (Polygone).shp','geographic_data','europe_coastline_polygon');
call shpload('/path/to/data/[C4] FAO Maritime Areas/FAO_AREAS.shp','geographic_data','fao_areas');
call shpload('/path/to/data/[C4] Fishing Areas (European commission)/v_recode_fish_area_clean.shp','geographic_data','fishing_areas');
call shpload('/path/to/data/[C5] Fishing Constraints/fishing_interdiction.shp','geographic_data','fishing_interdiction');
call shpload('/path/to/data/[P1] Brest Receiver/Receptor.shp','geographic_data','brest_receptor');
call shpload('/path/to/data/[P1] Brest Receiver/Theoretical Coverage.shp','geographic_data','brest_coverage');
call shpload('/path/to/data/[C2] European Maritime Boundaries/MBEULSIV1.shp','geographic_data','europe_borders');
call shpload('/path/to/data/[C5] Marine Protected Areas (EEA Natura 2000)/spatial data/Natura2000_end2015.shp','geographic_data','natura_protected_areas');
call shpload('/path/to/data/[C1] World Port Index/WPI.shp','geographic_data','wpi_ports');
call shpload('/path/to/data/[C2] IHO World Seas/World_Seas_IHO_v2.shp','geographic_data','world_seas');
call shpload('/path/to/data/[C2] World EEZ/eez.shp','geographic_data','world_eez');
call shpload('/path/to/data/[C2] World EEZ/eez_boundaries.shp','geographic_data','world_eez_boundaries');

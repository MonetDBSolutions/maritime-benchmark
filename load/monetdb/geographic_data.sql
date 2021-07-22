#
# === Geographic Data ===
#
DROP TABLE IF EXISTS brittany_ports;
DROP TABLE IF EXISTS seadata_ports;
DROP TABLE IF EXISTS europe_coastline;
DROP TABLE IF EXISTS fao_areas;
DROP TABLE IF EXISTS fishing_areas;
DROP TABLE IF EXISTS fishing_interdiction;
DROP TABLE IF EXISTS brest_receptor;
DROP TABLE IF EXISTS brest_coverage;
DROP TABLE IF EXISTS wpi_ports;
DROP TABLE IF EXISTS europe_borders;
DROP TABLE IF EXISTS natura_protected_areas;

call shpload('/path/to/data/[C1] Ports of Brittany/brittany_ports.shp','brittany_ports');
call shpload('/path/to/data/[C1] SeaDataNet Port Index/seadatanet_fishing_ports.shp','seadata_ports');
call shpload('/path/to/data/[C2] European Coastline/europe_coastline.shp','europe_coastline');
call shpload('/path/to/data/[C4] FAO Maritime Areas/FAO_AREAS.shp','fao_areas');
call shpload('/path/to/data/[C4] Fishing Areas (European commission)/v_recode_fish_area_clean.shp','fishing_areas');
call shpload('/path/to/data/[C5] Fishing Constraints/fishing_interdiction.shp','fishing_interdiction');
call shpload('/path/to/data/[P1] Brest Receiver/Receptor.shp','brest_receptor');
call shpload('/path/to/data/[P1] Brest Receiver/theoretical_coverage.shp','brest_coverage');
call shpload('/path/to/data/[C2] European Maritime Boundaries/MBEULSIV1.shp','europe_borders');
call shpload('/path/to/data/[C5] Marine Protected Areas (EEA Natura 2000)/spatial data/Natura2000_end2015.shp','natura_protected_areas');
call shpload('/path/to/data/[C1] World Port Index/WPI.shp','wpi_ports');
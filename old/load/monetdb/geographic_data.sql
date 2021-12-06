#
# === Geographic Data ===
#
DROP SCHEMA IF EXISTS geographic_data CASCADE;
CREATE SCHEMA geographic_data;

call shpload('/path/to/data/[C1] Ports of Brittany/port.shp','geographic_data','brittany_ports');

call shpload('/path/to/data/[C1] SeaDataNet Port Index/fishing_ports.shp','geographic_data','seadata_ports');

call shpload('/path/to/data/[C2] European Coastline/europe_coastline.shp','geographic_data','europe_coastline');

call shpload('/path/to/data/[C4] FAO Maritime Areas/fao_areas.shp','geographic_data','fao_areas');

call shpload('/path/to/data/[C4] Fishing Areas (European commission)/v_recode_fish_area_clean.shp','geographic_data','fishing_areas');

call shpload('/path/to/data/[C5] Fishing Constraints/fishing_interdiction.shp','geographic_data','fishing_interdiction');

call shpload('/path/to/data/[P1] Brest Receiver/receptor.shp','geographic_data','brest_receptor');

call shpload('/path/to/data/[P1] Brest Receiver/theoretical_coverage.shp','geographic_data','brest_coverage');

call shpload('/path/to/data/[C2] European Maritime Boundaries/mbeulsiv1.shp','geographic_data','europe_borders');

call shpload('/path/to/data/[C5] Marine Protected Areas (EEA Natura 2000)/spatial_data/Natura2000_end2015.shp','geographic_data','natura_protected_areas');

call shpload('/path/to/data/[C1] World Port Index/wpi.shp','geographic_data','wpi_ports');

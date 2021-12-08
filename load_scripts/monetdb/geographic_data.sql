#
# === Geographic Data ===
#

DROP SCHEMA IF EXISTS geographic_data CASCADE;
CREATE SCHEMA geographic_data;

call shpload('/Users/bernardo/Monet/Geo/maritime-import/data/[C1] Ports of Brittany/port.shp','geographic_data','brittany_ports');
call shpload('/Users/bernardo/Monet/Geo/maritime-import/data/[C1] SeaDataNet Port Index/Fishing Ports.shp','geographic_data','seadata_ports');
call shpload('/Users/bernardo/Monet/Geo/maritime-import/data/[C2] European Coastline/Europe Coastline.shp','geographic_data','europe_coastline');
call shpload('/Users/bernardo/Monet/Geo/maritime-import/data/[C2] European Coastline/Europe Coastline (Polygone).shp','geographic_data','europe_coastline_polygon');
call shpload('/Users/bernardo/Monet/Geo/maritime-import/data/[C4] FAO Maritime Areas/fao_areas.shp','geographic_data','fao_areas');
call shpload('/Users/bernardo/Monet/Geo/maritime-import/data/[C4] Fishing Areas (European commission)/v_recode_fish_area_clean.shp','geographic_data','fishing_areas');
call shpload('/Users/bernardo/Monet/Geo/maritime-import/data/[C5] Fishing Constraints/fishing_interdiction.shp','geographic_data','fishing_interdiction');
call shpload('/Users/bernardo/Monet/Geo/maritime-import/data/[P1] Brest Receiver/receptor.shp','geographic_data','brest_receptor');
call shpload('/Users/bernardo/Monet/Geo/maritime-import/data/[P1] Brest Receiver/Theoretical Coverage.shp','geographic_data','brest_coverage');
call shpload('/Users/bernardo/Monet/Geo/maritime-import/data/[C2] European Maritime Boundaries/mbeulsiv1.shp','geographic_data','europe_borders');
call shpload('/Users/bernardo/Monet/Geo/maritime-import/data/[C5] Marine Protected Areas (EEA Natura 2000)/spatial data/Natura2000_end2015.shp','geographic_data','natura_protected_areas');
call shpload('/Users/bernardo/Monet/Geo/maritime-import/data/[C1] World Port Index/wpi.shp','geographic_data','wpi_ports');
call shpload('/Users/bernardo/Monet/Geo/maritime-import/data/[C2] IHO World Seas/world_seas_iho_v2.shp','geographic_data','world_seas');
call shpload('/Users/bernardo/Monet/Geo/maritime-import/data/[C2] World EEZ/eez.shp','geographic_data','world_eez');
call shpload('/Users/bernardo/Monet/Geo/maritime-import/data/[C2] World EEZ/eez_boundaries.shp','geographic_data','world_eez_boundaries');

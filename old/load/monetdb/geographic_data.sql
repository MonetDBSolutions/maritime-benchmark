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

call shpload('/path/to/data/[C1] Ports of Brittany/brittany_ports.shp','sys','brittany_ports');
call shpload('/path/to/data/[C1] SeaDataNet Port Index/seadatanet_fishing_ports.shp','sys','seadata_ports');
call shpload('/path/to/data/[C2] European Coastline/europe_coastline.shp','sys','europe_coastline');
call shpload('/path/to/data/[C4] FAO Maritime Areas/FAO_AREAS.shp','sys','fao_areas');
call shpload('/path/to/data/[C4] Fishing Areas (European commission)/v_recode_fish_area_clean.shp','sys','fishing_areas');
call shpload('/path/to/data/[C5] Fishing Constraints/fishing_interdiction.shp','sys','fishing_interdiction');
call shpload('/path/to/data/[P1] Brest Receiver/Receptor.shp','sys','brest_receptor');
call shpload('/path/to/data/[P1] Brest Receiver/theoretical_coverage.shp','sys','brest_coverage');
call shpload('/path/to/data/[C2] European Maritime Boundaries/MBEULSIV1.shp','sys','europe_borders');
call shpload('/path/to/data/[C5] Marine Protected Areas (EEA Natura 2000)/spatial data/Natura2000_end2015.shp','sys','natura_protected_areas');
call shpload('/path/to/data/[C1] World Port Index/WPI.shp','sys','wpi_ports');


call shpload('/Users/bernardo/Monet/VesselAI/Prototype/Datasets/Maritime Integrated Dataset/Geographic/[C1] Ports of Brittany/brittany_ports.shp','sys','BrittanyPorts');
call shpload('/Users/bernardo/Monet/VesselAI/Prototype/Datasets/Maritime Integrated Dataset/Geographic/[C4] FAO Maritime Areas/FAO_AREAS.shp','sys','fao_areas');

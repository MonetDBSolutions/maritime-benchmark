DROP TABLE IF EXISTS geographic_data.brittany_ports;
call shpload('/path/to/data/[C1] Ports of Brittany/port.shp','geographic_data','brittany_ports');

DROP TABLE IF EXISTS geographic_data.fao_areas;
call shpload('/path/to/data/[C4] FAO Maritime Areas/FAO_AREAS.shp','geographic_data','fao_areas');

DROP TABLE IF EXISTS geographic_data.fishing_areas;
call shpload('/path/to/data/[C4] Fishing Areas (European commission)/v_recode_fish_area_clean.shp','geographic_data','fishing_areas');

DROP TABLE IF EXISTS geographic_data.wpi_ports;
call shpload('/path/to/data/[C1] World Port Index/WPI.shp','geographic_data','wpi_ports');


#
# === Geographic Data ===
#

DROP TABLE IF EXISTS sys.files;
DROP TABLE IF EXISTS sys.shapefiles;
DROP TABLE IF EXISTS sys.shapefiles_dbf;
DROP TABLE IF EXISTS brittany_ports;
DROP TABLE IF EXISTS seadatanet_fishing_ports;
DROP TABLE IF EXISTS WPI;
DROP TABLE IF EXISTS europe_coastline;
DROP TABLE IF EXISTS MBEULSIV1;
DROP TABLE IF EXISTS FAO_AREAS;
DROP TABLE IF EXISTS v_recode_fish_area_clean;
DROP TABLE IF EXISTS fishing_interdiction;
DROP TABLE IF EXISTS Receptor;
DROP TABLE IF EXISTS theoretical_coverage;
DROP TABLE IF EXISTS Natura2000_end2015;

#Create tables beforehand, as they don't exist by default (and there needs to be one on sys schema)
create table sys.files (
  id int,
  path clob
  );
create table sys.shapefiles (
  shapefileid int,
  fileid int,
  mbb geometry,
  srid int,
  datatable clob
  );
create table sys.shapefiles_dbf (
  shapefileid int,
  attr clob,
  datatype clob
  );

# Ports Locations
call sys.shpattach('/path/to/data/[C1] Ports of Brittany/brittany_ports.shp');
call sys.shpattach('/path/to/data/[C1] SeaDataNet Port Index/seadatanet_fishing_ports.shp');
call sys.shpattach('/path/to/data/[C1] World Port Index/WPI.shp');

# Geographic regions
call sys.shpattach('/path/to/data/[C2] European Coastline/europe_coastline.shp');
#call sys.shpattach('/path/to/data/[C2] European Coastline/Europe Coastline (Polygone).shp');
call sys.shpattach('/path/to/data/[C2] European Maritime Boundaries/MBEULSIV1.shp');
#call sys.shpattach('/path/to/data/[C2] IHO World Seas/World_Seas_IHO_v2.shp.shp');
#shp2pgsql -I -s 4326 '../../[C2] World EEZ/eez.shp' geographic_features.world_eez | psql -U postgres -d doi105281zenodo1167595;
#shp2pgsql -I -s 4326 '../../[C2] World EEZ/eez_boundaries.shp' geographic_features.world_eez_boundaries | psql -U postgres -d doi105281zenodo1167595;
call sys.shpattach('/path/to/data/[C4] FAO Maritime Areas/FAO_AREAS.shp');
call sys.shpattach('/path/to/data/[C4] Fishing Areas (European commission)/v_recode_fish_area_clean.shp');
call sys.shpattach('/path/to/data/[C5] Fishing Constraints/fishing_interdiction.shp');


# AIS Receiver used to capture data
call sys.shpattach('/path/to/data/[P1] Brest Receiver/Receptor.shp');
call sys.shpattach('/path/to/data/[P1] Brest Receiver/theoretical_coverage.shp');

# Natura 
call sys.shpattach('/path/to/data/[C5] Marine Protected Areas (EEA Natura 2000)/spatial data/Natura2000_end2015.shp');



#Actually load the data (it crashes it we load multiple files at once)
call sys.shpload(1);
#call sys.shpload(2);
#call sys.shpload(3);
#call sys.shpload(4);
#call sys.shpload(5);
#call sys.shpload(6);
#call sys.shpload(7);
#call sys.shpload(8);
#call sys.shpload(9);
#call sys.shpload(10);
#call sys.shpload(11);


#Add geometry columns
ALTER TABLE sys.brittany_ports ADD COLUMN geom_p Point;
UPDATE sys.brittany_ports SET geom_p = st_point(por_x,por_y);




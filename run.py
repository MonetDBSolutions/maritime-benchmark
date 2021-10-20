#!/usr/bin/env python3
import pymonetdb
import argparse
import sys
import os
from timeit import default_timer as timer
import psycopg2
import subprocess
import logging
from psycopg2.extras import MinTimeLoggingConnection

#TODO: Time from database server in Postgres -> How do we parse the log to get query duration info?

#TODO: Start with simpler queries, move on to more meaningful queries
#TODO: Format SQL queries; separating them with split on ;

#TODO: Unify the methods, change Exception to general exception
#TODO: Sanitize: Save query output results to file
#TODO: Read from CSV header; Create table from columns of CSV
#TODO: Integrate the server startup (for Monet and Postgres)


parser = argparse.ArgumentParser(
    description='Geom benchmark (MonetDB Geo vs PostGIS)',
    epilog='''
    This program loads and executes the Geographic data benchmark on MonetDB and PostGIS.
    ''')
#Program arguments
parser.add_argument('--data', type=str, help='Absolute path to the dataset directory', required = True, default=None)
parser.add_argument('--system', type=str, help='Database system to benchmark (default is monetdb)', default='monetdb')
parser.add_argument('--database', type=str, help='Database to connect to (default is marine)', default='marine')
parser.add_argument('--scale', type=float, help='Benchmark scale factor (currently only values < 1 allowed)', default=0)

#Switch (bool) arguments
parser.add_argument('--debug', help='Turn on debugging log', dest='debug', action='store_true')
parser.add_argument('--no-debug', help='Turn off debugging log (default is off)', dest='debug', action='store_false')
parser.set_defaults(debug=False)
parser.add_argument('--load', help='Turn on loading the data', dest='load', action='store_true')
parser.add_argument('--no-load', help='Turn off loading the data (default is on)', dest='load', action='store_false')
parser.set_defaults(load=True)
parser.add_argument('--query', help='Turn on querying the data', dest='query', action='store_true')
parser.add_argument('--no-query', help='Turn off querying the data (default is on)', dest='query', action='store_false')
parser.set_defaults(query=True)
parser.add_argument('--drop', help='Turn on dropping the data after execution', dest='drop', action='store_true')
parser.add_argument('--no-drop', help='Turn off dropping the data after execution (default is on)', dest='drop', action='store_false')
parser.set_defaults(drop=True)

load_tables = {
  "csv_tables":
          [
            {
            "tablename":"ais_dynamic",
            "filename":"ais_dynamic",
            "columns":"mmsi,status,turn,speed,course,heading,lon,lat,ts",
            "timestamp":"ts",
            "geom":"lon,lat",
            "scalable":"true"
            }
          ],
  "shape_tables":
          [
            {
            "tablename":"brittany_ports",
            "filename":"brittany_ports.shp"
            }
          ]
}

def configure_logger():
    #TODO Add option to log to file
    if args.debug:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s: %(levelname)s: %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

#Create a new CSV file with a subset of data from an input CSV, given a scale factor (only < 1 SF allowed)
#If the file already exists, use it. We currently don't delete the file after execution
def scale_csv(input_name, scale):
    if scale > 1:
        logger.warning("Scale factor must be less than 1, using original csv (SF 1)")
        return f'{data_dir}/{input_name}.csv'
    output_file_name = f'{data_dir}/{input_name}_SF_{scale}.csv'
    if os.path.isfile(output_file_name):
        logger.debug(f"Found already existing csv {output_file_name}")
        return output_file_name
    input_file_name = f'{data_dir}/{input_name}.csv'
    with open(input_file_name, 'r') as input_file, open(output_file_name, 'w+') as output_file:
        try:
            input_lines = input_file.readlines()
            records_scaled = int(len(input_lines) * scale)
            output_file.writelines(input_lines[0:records_scaled])
            logger.debug(f"Number of records in scaled dataset '{input_name}': {records_scaled}")
        except IOError as msg:
            logger.warning("cutcsv() operation failed, using original csv (SF 1)")
            logger.exception(msg)
            return input_file_name
    return output_file_name

def get_last_exec_time_monet(cur):
    try:
        cur.execute(f"select extract(epoch from t) "
                    f"from (select max(stop)-max(start) as t from querylog_history) "
                    f"as a;")
    except pymonetdb.DatabaseError as msg:
        logger.exception(msg)
        return -1
    result = cur.fetchone()
    return result[0]

def create_schema_monet(cur):
    filename = os.getcwd() + "/sql/monet_ddl.sql"
    try:
        f = open(filename, "r")
        geo_ddl = f.read().splitlines()
    except IOError as msg:
        logger.exception(msg)
        sys.exit()
    logger.debug("Creating schema")
    for q in geo_ddl:
        logger.debug(f"Executing query '{q}'")
        try:
            cur.execute(q)
        except pymonetdb.DatabaseError as msg:
            logger.exception(msg)
            continue

def load_shp_monet(cur):
    total_time = 0
    for csv_t in load_tables["shape_tables"]:
        query = f'call shpload(\'{data_dir}/{csv_t["filename"]}\',\'bench_geo\',\'{csv_t["tablename"]}\');'
        logger.debug(f"Executing query '{query}'")
        start = timer()
        try:
            cur.execute(query)
        except pymonetdb.DatabaseError as msg:
            logger.exception(msg)
            continue
        end = timer() - start
        load_time = get_last_exec_time_monet(cur)
        total_time += load_time
        logger.info("CLIENT: Loaded %s in %6.3f seconds" % (csv_t["tablename"],end))
        logger.info("Loaded %s in %6.3f seconds" % (csv_t["tablename"],load_time))
    return total_time

#TODO: Should I count the last execution time everytime a query is executed or keep the counting queries strategy?
def load_csv_monet(cur):
    total_time = 0
    for csv_t in load_tables["csv_tables"]:
        if "scalable" in csv_t and args.scale != 0:
            filename = scale_csv(csv_t["filename"], args.scale)
        else:
            filename = f'{data_dir}/{csv_t["filename"]}.csv'

        query = f'COPY OFFSET 2 INTO {csv_t["tablename"]} ({csv_t["columns"]}) FROM \'{filename}\' \
        ({csv_t["columns"]}) DELIMITERS \',\',\'\\n\',\'\"\' NULL AS \'\';'
        logger.debug(f"Executing query '{query}'")
        start = timer()
        try:
            cur.execute(query)
        except pymonetdb.DatabaseError as msg:
            logger.exception(msg)
            continue
        load_time = get_last_exec_time_monet(cur)

        if "timestamp" in csv_t:
            try:
                cur.execute(f'UPDATE {csv_t["tablename"]} SET t = epoch(cast({csv_t["timestamp"]} as int));')
            except pymonetdb.DatabaseError as msg:
                logger.exception(msg)
                continue
            load_time += get_last_exec_time_monet(cur)
        if "geom" in csv_t:
            try:
                cur.execute(f'UPDATE {csv_t["tablename"]} SET geom = ST_SetSRID(ST_MakePoint({csv_t["geom"]}),4326);')
            except pymonetdb.DatabaseError as msg:
                logger.exception(msg)
                continue
            load_time += get_last_exec_time_monet(cur)
        end = timer() - start
        logger.info("CLIENT: Loaded %s_%s in %6.3f seconds" % (csv_t["tablename"], args.scale, end))
        total_time += load_time
        if "scalable" in csv_t and args.scale > 0:
            logger.info("Loaded %s_%s in %6.3f seconds" % (csv_t["tablename"],args.scale,load_time))
        else:
            logger.info("Loaded %s in %6.3f seconds" % (filename, load_time))
    return total_time

def load_data_monet(cur):
    logger.debug("Loading data")
    total_time = load_csv_monet(cur)
    total_time += load_shp_monet(cur)
    logger.info("All loads in %6.3f seconds" % total_time)

def run_queries_monet(cur):
    try:
        filename = os.getcwd() + "/sql/monet_queries.sql"
        f = open(filename, "r")
    except IOError as msg:
        logger.exception(msg)
        sys.exit()
    geo_queries = f.read().split(";")
    logger.debug("Running queries")

    total_time = 0
    query_id = 1
    for q in geo_queries:
        if not q:
            continue
        logger.debug(f"Executing query '{q}'")
        start = timer()
        try:
            cur.execute(q)
        except pymonetdb.DatabaseError as msg:
            logger.exception(msg)
            continue
        end = timer() - start
        logger.info("CLIENT: Executed query %s in %6.3f seconds" % (query_id,end))
        query_time = get_last_exec_time_monet(cur)
        total_time += query_time
        logger.info("Executed query %s in %6.3f seconds" % (query_id, query_time))
        query_id +=1
    logger.info("Executed all queries in %6.3f seconds" % total_time)

def drop_schema_monet(cur):
    logger.debug("Dropping schema")
    cur.execute("SET SCHEMA = sys;")
    cur.execute("DROP SCHEMA bench_geo cascade;")

def enable_query_history(cur):
    logger.debug("Enabling query history")
    try:
        cur.execute("call sys.querylog_enable();")
    except pymonetdb.DatabaseError as msg:
        logger.exception(msg)

def benchmark_monet():
    conn = pymonetdb.connect(args.database, autocommit=True)
    if not conn:
        logger.error(f'Could not access the database {args.database}')
        sys.exit()
    logger.debug(f'MonetDB: Connected to database {args.database}')

    cur = conn.cursor()
    enable_query_history(cur)
    if args.load:
        create_schema_monet(cur)
        load_data_monet(cur)
    if args.query:
        run_queries_monet(cur)
    if args.drop:
        drop_schema_monet(cur)
    conn.close()

def create_schema_psql(cur):
    filename = os.getcwd() + "/sql/psql_ddl.sql"
    try:
        f = open(filename, "r")
        geo_ddl = f.read().splitlines()
    except IOError as msg:
        logger.exception(msg)
        sys.exit()
    logger.debug("Creating schema")
    for q in geo_ddl:
        logger.debug(f"Executing query '{q}'")
        try:
            cur.execute(q)
        except psycopg2.DatabaseError as msg:
            logger.exception(msg)
            continue

def load_csv_psql(cur):
    total_time = 0
    for csv_t in load_tables["csv_tables"]:
        if "scalable" in csv_t and args.scale != 0:
            filename = scale_csv(csv_t["filename"], args.scale)
        else:
            filename = f'{data_dir}/{csv_t["filename"]}.csv'
        #TODO Change to psycopg copy_from() method?
        query = f'COPY {csv_t["tablename"]} ({csv_t["columns"]}) FROM \'{filename}\' delimiter \',\' csv HEADER;'
        logger.debug(f"Executing query '{query}'")
        start = timer()
        try:
            cur.execute(query)
        except psycopg2.DatabaseError as msg:
            logger.exception(msg)
            continue

        if "timestamp" in csv_t:
            try:
                cur.execute(f'UPDATE {csv_t["tablename"]} SET t = to_timestamp({csv_t["timestamp"]});')
            except psycopg2.DatabaseError as msg:
                logger.exception(msg)
                continue
        if "geom" in csv_t:
            try:
                cur.execute(f'UPDATE {csv_t["tablename"]} SET geom = ST_SetSRID(ST_MakePoint({csv_t["geom"]}),4326);')
            except psycopg2.DatabaseError as msg:
                logger.exception(msg)
                continue
        load_time = timer() - start
        total_time += load_time
        if "scalable" in csv_t and args.scale > 0:
            logger.info("Loaded %s_%s in %6.3f seconds" % (csv_t["tablename"],args.scale,load_time))
        else:
            logger.info("Loaded %s in %6.3f seconds" % (filename, load_time))
    return total_time

#PostGIS extension can only be in one schema at a time.
#We need it in the bench_geo schema to add geometry support
#But shapefiles can only be loaded if the extension is in the public (default) schema
#TODO: Check if this is really necessary
def move_postgis_extension (cur, schema, postgis_version):
    cur.execute("UPDATE pg_extension SET extrelocatable = TRUE WHERE extname = 'postgis';")
    cur.execute(f"ALTER EXTENSION postgis SET SCHEMA {schema};")
    cur.execute(f"ALTER EXTENSION postgis UPDATE TO \"{postgis_version}next\";")
    cur.execute(f"ALTER EXTENSION postgis UPDATE TO \"{postgis_version}\";")

#Load data through the shp2pgsql shell command
def load_shp_psql():
    total_time = 0
    for csv_t in load_tables["shape_tables"]:
        #TODO Some shapefiles can have different SRID
        query = f'shp2pgsql -I -s 4326 \'{data_dir}/{csv_t["filename"]}\' bench_geo.{csv_t["tablename"]} | psql  -d {args.database};'
        logger.debug(f"Executing command '{query}'")
        start = timer()
        try:
            subprocess.run(query, shell=True, stdout=subprocess.DEVNULL)
        except subprocess.CalledProcessError as msg:
            logger.exception(msg)
        load_time = timer() - start
        total_time += load_time
        logger.info("Loaded %s in %6.3f seconds" % (csv_t["tablename"],load_time))
    return total_time

def load_data_psql(cur):
    #Get PostGIS version
    try:
        cur.execute("select PostGIS_Lib_Version();")
    except psycopg2.DatabaseError as msg:
        logger.exception(msg)
        return

    result = cur.fetchone()
    if result is not None:
        postgis_version = result[0]
    else:
        #Default value, latest version
        postgis_version = "3.1.2"

    logger.debug("Loading data")

    total_time = load_csv_psql(cur)
    move_postgis_extension(cur, "public", postgis_version)
    total_time += load_shp_psql()
    move_postgis_extension(cur, "bench_geo", postgis_version)

    logger.info("All loads in %6.3f seconds" % total_time)

def run_queries_psql(cur):
    try:
        filename = os.getcwd() + "/sql/psql_queries.sql"
        f = open(filename, "r")
    except IOError as msg:
        logger.exception(msg)
        sys.exit()
    geo_queries = f.read().split(";")
    logger.debug("Running queries")

    total_time = 0
    q_id = 1
    for q in geo_queries:
        if not q:
            continue
        logger.debug(f"Executing query '{q}'")
        start = timer()
        try:
            cur.execute(q)
        except psycopg2.DatabaseError as msg:
            logger.exception(msg)
            continue
        query_time = timer() - start
        total_time += query_time
        logger.info("Executed query %s in %6.3f seconds" % (q_id, query_time))
        q_id +=1
    logger.info("Executed all queries in %6.3f seconds" % total_time)

def drop_schema_psql(cur):
    logger.debug("Dropping schema")
    cur.execute("SET SCHEMA 'public';")
    cur.execute("DROP SCHEMA bench_geo cascade;")

def benchmark_psql():
    conn = psycopg2.connect(f"dbname={args.database}")
    if not conn:
        logger.error(f'Could not access the database {args.database}')
        sys.exit()
    logger.debug(f'Postgres: Connected to database {args.database}')
    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()
    if args.load:
        create_schema_psql(cur)
        load_data_psql(cur)
    if args.query:
        run_queries_psql(cur)
    if args.drop:
        drop_schema_psql(cur)
    conn.close()

if __name__ == "__main__":
    args = parser.parse_args()
    data_dir = args.data

    #Configure logger
    logger = logging.getLogger(__name__)
    configure_logger()

    if args.system == 'monetdb' or args.system == 'monet' or args.system == 'mdb':
        benchmark_monet()
    elif args.system == 'postgres' or args.system == 'psql'or args.system == 'postgis':
        benchmark_psql()
    else:
        logger.error('Choose a system to benchmark: monetdb (alias mdb, monet) or postgis (alias psql, postgres)')
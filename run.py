#!/usr/bin/env python3
import pymonetdb
import argparse
import sys
import os
from timeit import default_timer as timer
import psycopg2
import subprocess
import logging

#TODO: Start with simpler queries, move on to more meaningful queries
#TODO: Unify the methods, change Exception to general exception
#TODO: Time from database server, not external
#TODO: Sanitize: Save query output results to file

#TODO: Read from CSV header; Create table from columns of CSV
#TODO: Format SQL queries; separating them with split on ;
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
parser.add_argument('--scale', type=float, help='Benchmark scale', default=0)

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

def configurelogger():
    #TODO Add option to log to file
    if args.debug:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s: %(levelname)s: %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

#Create a new CSV file with a subset of data from an input CSV, given a scale
#If the file already exists, use it. We currently don't delete the file after execution
def scalecsv(inputname, scale):
    outputfilename = f'{datadir}/{inputname}_{scale}.csv'
    if os.path.isfile(outputfilename):
        logger.debug(f"Found already existing csv {outputfilename}")
        return outputfilename
    inputfilename = f'{datadir}/{inputname}.csv'
    with open(inputfilename, 'r') as inputfile, open(outputfilename, 'w+') as outputfile:
        try:
            inputlines = inputfile.readlines()
            records_scaled = int(len(inputlines) * scale)
            outputfile.writelines(inputlines[0:records_scaled])
            logger.debug(f"Number of records in scaled dataset '{inputname}': {records_scaled}")
        except IOError as msg:
            logger.warning("cutcsv() operation failed, using original csv")
            logger.exception(msg)
            return inputfilename
    return outputfilename



def createschemamonet(cur):
    fname = os.getcwd() + "/sql/monet_ddl.sql"
    try:
        f = open(fname, "r")
        geo_ddl = f.read().splitlines()
    except IOError as msg:
        logger.exception(msg)
        sys.exit()
    logger.info("Creating schema")
    for q in geo_ddl:
        logger.debug(f"Executing query '{q}'")
        try:
            cur.execute(q)
        except pymonetdb.DatabaseError as msg:
            logger.exception(msg)
            continue

def loadshpmonet(cur):
    totaltime = 0
    for csv_t in load_tables["shape_tables"]:
        query = f'call shpload(\'{datadir}/{csv_t["filename"]}\',\'bench_geo\',\'{csv_t["tablename"]}\');'
        logger.debug(f"Executing query '{query}'")
        start = timer()
        try:
            cur.execute(query)
        except pymonetdb.DatabaseError as msg:
            logger.exception(msg)
            continue
        loadtime = timer() - start
        totaltime += loadtime
        logger.info("Loaded " + csv_t["tablename"] + " in %6.3f seconds" % loadtime)
    return totaltime

def loadcsvmonet(cur):
    totaltime = 0
    for csv_t in load_tables["csv_tables"]:
        if "scalable" in csv_t and args.scale != 0:
            filename = scalecsv(csv_t["filename"],args.scale)
        else:
            filename = f'{datadir}/{csv_t["filename"]}.csv'

        query = f'COPY OFFSET 2 INTO {csv_t["tablename"]} ({csv_t["columns"]}) FROM \'{filename}\' \
        ({csv_t["columns"]}) DELIMITERS \',\',\'\\n\',\'\"\' NULL AS \'\';'
        logger.debug(f"Executing query '{query}'")
        start = timer()
        try:
            cur.execute(query)
        except pymonetdb.DatabaseError as msg:
            logger.exception(msg)
            continue

        if "timestamp" in csv_t:
            try:
                cur.execute(f'UPDATE {csv_t["tablename"]} SET t = epoch(cast({csv_t["timestamp"]} as int));')
            except pymonetdb.DatabaseError as msg:
                logger.exception(msg)
                continue
        if "geom" in csv_t:
            try:
                cur.execute(f'UPDATE {csv_t["tablename"]} SET geom = ST_SetSRID(ST_MakePoint({csv_t["geom"]}),4326);')
            except pymonetdb.DatabaseError as msg:
                logger.exception(msg)
                continue
        loadtime = timer() - start
        totaltime += loadtime
        if "scalable" in csv_t and args.scale > 0:
            logger.info("Loaded " + csv_t["tablename"] + "_" + str(args.scale) + " in %6.3f seconds" % loadtime)
        else:
            logger.info("Loaded " + filename + " in %6.3f seconds" % loadtime)
    return totaltime

def loaddatamonet(cur):
    logger.info("Loading data")
    totaltime = loadcsvmonet(cur)
    totaltime += loadshpmonet(cur)
    logger.info("All loads in %6.3f seconds" % totaltime)

def runqueriesmonet(cur):
    try:
        fname = os.getcwd() + "/sql/monet_queries.sql"
        f = open(fname, "r")
    except IOError as msg:
        logger.exception(msg)
        sys.exit()
    geo_queries = f.read().splitlines()

    logger.info("Running queries")

    totaltime = 0
    q_id = 1
    for q in geo_queries:
        if q.startswith("--"):
            continue
        logger.debug(f"Executing query '{q}'")
        start = timer()
        try:
            cur.execute(q)
        except pymonetdb.DatabaseError as msg:
            logger.exception(msg)
            continue
        querytime = timer() - start
        totaltime += querytime
        logger.info("Executed query " + str(q_id) + " in %6.3f seconds" % querytime)
        q_id +=1
    logger.info("Executed all queries in %6.3f seconds" % totaltime)

def dropschemamonet(cur):
    logger.info("Dropping schema")
    cur.execute("SET SCHEMA = sys;")
    cur.execute("DROP SCHEMA bench_geo cascade;")

def enablequeryhistory(cur):
    logger.info("Enabling query history")
    cur.execute("call sys.querylog_enable();")

def benchmarkmonet():
    conn = pymonetdb.connect(args.database, autocommit=True)
    if not conn:
        logger.error(f'Could not access the database {args.database}')
        sys.exit()
    logger.info(f'MonetDB: Connected to database {args.database}')

    cur = conn.cursor()
    #enablequeryhistory(cur)
    if args.load:
        createschemamonet(cur)
        loaddatamonet(cur)
    if args.query:
        runqueriesmonet(cur)
    if args.drop:
        dropschemamonet(cur)
    conn.close()

def createschemapsql(cur):
    fname = os.getcwd() + "/sql/psql_ddl.sql"
    try:
        f = open(fname, "r")
        geo_ddl = f.read().splitlines()
    except IOError as msg:
        logger.exception(msg)
        sys.exit()
    logger.info("Creating schema")
    for q in geo_ddl:
        logger.debug(f"Executing query '{q}'")
        try:
            cur.execute(q)
        except psycopg2.DatabaseError as msg:
            logger.exception(msg)
            continue

def loadcsvpsql(cur):
    totaltime = 0
    for csv_t in load_tables["csv_tables"]:
        if "scalable" in csv_t and args.scale != 0:
            filename = scalecsv(csv_t["filename"],args.scale)
        else:
            filename = f'{datadir}/{csv_t["filename"]}.csv'
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
        loadtime = timer() - start
        totaltime += loadtime
        if "scalable" in csv_t and args.scale > 0:
            logger.info("Loaded " + csv_t["tablename"] + "_" + str(args.scale) + " in %6.3f seconds" % loadtime)
        else:
            logger.info("Loaded " + filename + " in %6.3f seconds" % loadtime)
    return totaltime

#PostGIS extension can only be in one schema at a time.
#We need it in the bench_geo schema to add geometry support
#But shapefiles can only be loaded if the extension is in the public (default) schema
def movepostgisextension (cur, schema, postgisversion):
    cur.execute("UPDATE pg_extension SET extrelocatable = TRUE WHERE extname = 'postgis';")
    cur.execute(f"ALTER EXTENSION postgis SET SCHEMA {schema};")
    cur.execute(f"ALTER EXTENSION postgis UPDATE TO \"{postgisversion}next\";")
    cur.execute(f"ALTER EXTENSION postgis UPDATE TO \"{postgisversion}\";")

#Load data through the shp2pgsql shell command
def loadshppsql():
    totaltime = 0

    for csv_t in load_tables["shape_tables"]:
        #TODO Some shapefiles can have different SRID
        query = f'shp2pgsql -I -s 4326 \'{datadir}/{csv_t["filename"]}\' bench_geo.{csv_t["tablename"]} | psql  -d {args.database};'
        logger.debug(f"Executing command '{query}'")
        start = timer()
        #TODO Change to subprocess.check_output()
        subprocess.run(query, shell=True, stdout=subprocess.DEVNULL)
        loadtime = timer() - start
        totaltime += loadtime
        logger.info("Loaded " + csv_t["tablename"] + " in %6.3f seconds" % loadtime)
    return totaltime

def loaddatapsql(cur):
    #Get PostGIS version
    try:
        cur.execute("select PostGIS_Lib_Version();")
    except psycopg2.DatabaseError as msg:
        logger.exception(msg)
        return

    result = cur.fetchone()
    if result is not None:
        postgisversion = result[0]
    else:
        #Default value, latest version
        postgisversion = "3.1.2"

    logger.info("Loading data")

    totaltime = loadcsvpsql(cur)
    movepostgisextension(cur,"public",postgisversion)
    totaltime += loadshppsql()
    movepostgisextension(cur, "bench_geo",postgisversion)

    logger.info("All loads in %6.3f seconds" % totaltime)

def runqueriespsql(cur):
    try:
        fname = os.getcwd() + "/sql/psql_queries.sql"
        f = open(fname, "r")
    except IOError as msg:
        logger.exception(msg)
        sys.exit()
    geo_queries = f.read().splitlines()

    logger.info("Running queries")

    totaltime = 0
    q_id = 1
    for q in geo_queries:
        if q.startswith("--"):
            continue
        logger.debug(f"Executing query '{q}'")
        start = timer()
        try:
            cur.execute(q)
        except psycopg2.DatabaseError as msg:
            logger.exception(msg)
            continue
        querytime = timer() - start
        totaltime += querytime
        logger.info("Executed query " + str(q_id) + " in %6.3f seconds" % querytime)
        q_id +=1
    logger.info("Executed all queries in %6.3f seconds" % totaltime)

def dropschemapsql(cur):
    logger.info("Dropping schema")
    cur.execute("SET SCHEMA 'public';")
    cur.execute("DROP SCHEMA bench_geo cascade;")

def benchmarkpsql():
    conn = psycopg2.connect(f"dbname={args.database}")
    if not conn:
        logger.error(f'Could not access the database {args.database}')
        sys.exit()
    logger.info(f'Postgres: Connected to database {args.database}')
    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()
    if args.load:
        createschemapsql(cur)
        loaddatapsql(cur)
    if args.query:
        runqueriespsql(cur)
    if args.drop:
        dropschemapsql(cur)
    conn.close()

if __name__ == "__main__":
    args = parser.parse_args()
    datadir = args.data

    #Configure logger
    logger = logging.getLogger(__name__)
    configurelogger()

    if args.system == 'monetdb' or args.system == 'monet' or args.system == 'mdb':
        benchmarkmonet()
    elif args.system == 'postgres' or args.system == 'psql'or args.system == 'postgis':
        benchmarkpsql()
    else:
        logger.error('Choose a system to benchmark: monetdb (alias mdb, monet) or postgis (alias psql, postgres)')
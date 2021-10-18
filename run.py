#!/usr/bin/env python3
import pymonetdb
import argparse
import sys
import os
from timeit import default_timer as timer
import psycopg2
import subprocess

#TODO: Should scale be a fraction of the dataset (0.1,...) or the number of records?
#Scale factor
#TODO: Should scale only apply to some tables?
#Maybe -> Leave it for later
#TODO: Delete intermediate scaled csv files after the run ends
#Maybe do the caching thing, but only if it takes too much time to do the cut -> Time it

#TODO: Start with simpler queries, move on to more meaningful queries
#TODO: Unify the methods, change Exception to general exception
#TODO: Time from database server, not external
#TODO: Sanitize: Save query output results to file

#TODO: Change prints to logging framework
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
parser.add_argument('--scale', type=int, help='Benchmark scale - number of records to be imported in size-varying tables', default=0)

#Switch (bool) arguments
parser.add_argument('--debug', help='Turn on traces', dest='debug', action='store_true')
parser.add_argument('--no-debug', help='Turn off traces (default is on)', dest='debug', action='store_false')
parser.set_defaults(debug=True)
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

#Create a new CSV file with a subset of data from an input CSV
def cutcsv(inputfilename,outputfilename,startindex,endindex):
    with open(inputfilename, 'r') as inputfile, open(outputfilename, 'w+') as outputfile:
        try:
            inputlines = inputfile.readlines()
            outputfile.writelines(inputlines[startindex:endindex])
        except IOError:
            print(f"Could not read/write to files")
            return False
        return True

def createschemamonet(cur):
    fname = os.getcwd() + "/sql/monet_ddl.sql"
    try:
        f = open(fname, "r")
        geo_ddl = f.read().splitlines()
    except IOError:
        print(f"Could not open/read {fname}")
        sys.exit()
    if debug:
        print("Creating schema")
    for d in geo_ddl:
        if debug:
            print(d)
        try:
            cur.execute(d)
        except pymonetdb.DatabaseError as msg:
            print('Exception', msg)
            continue

def loadshpmonet(cur):
    totaltime = 0
    for csv_t in load_tables["shape_tables"]:
        query = f'call shpload(\'{datadir}/{csv_t["filename"]}\',\'bench_geo\',\'{csv_t["tablename"]}\');'
        if debug:
            print(query)
        start = timer()
        try:
            cur.execute(query)
        except pymonetdb.DatabaseError as msg:
            print('Exception', msg)
            continue
        loadtime = timer() - start
        totaltime += loadtime
        print("Loaded " + csv_t["tablename"] + " in %6.3f seconds" % loadtime)
    return totaltime

def loadcsvmonet(cur):
    totaltime = 0
    for csv_t in load_tables["csv_tables"]:
        if "scalable" in csv_t and args.scale > 0:
            outputfilename = f'{datadir}/{csv_t["filename"]}_{args.scale}.csv'
            if cutcsv(f'{datadir}/{csv_t["filename"]}.csv',outputfilename,0,args.scale):
                filename = outputfilename
            else:
                print("cutcsv() operation failed, using original csv")
                filename = f'{datadir}/{csv_t["filename"]}.csv'
        else:
            filename = f'{datadir}/{csv_t["filename"]}.csv'

        query = f'COPY OFFSET 2 INTO {csv_t["tablename"]} ({csv_t["columns"]}) FROM \'{filename}\' \
        ({csv_t["columns"]}) DELIMITERS \',\',\'\\n\',\'\"\' NULL AS \'\';'
        if debug:
            print(query)
        start = timer()
        try:
            cur.execute(query)
        except pymonetdb.DatabaseError as msg:
            print('Exception', msg)
            continue

        if "timestamp" in csv_t:
            try:
                cur.execute(f'UPDATE {csv_t["tablename"]} SET t = epoch(cast({csv_t["timestamp"]} as int));')
            except pymonetdb.DatabaseError as msg:
                print('Exception', msg)
                continue
        if "geom" in csv_t:
            try:
                cur.execute(f'UPDATE {csv_t["tablename"]} SET geom = ST_SetSRID(ST_MakePoint({csv_t["geom"]}),4326);')
            except pymonetdb.DatabaseError as msg:
                print('Exception', msg)
                continue
        loadtime = timer() - start
        totaltime += loadtime
        print("Loaded " + filename + " in %6.3f seconds" % loadtime)
        #TODO Delete temporary csv file
    return totaltime

def loaddatamonet(cur):
    if debug:
        print("\nLoading data")
    totaltime = loadcsvmonet(cur)
    totaltime += loadshpmonet(cur)

    print("All loads in %6.3f seconds" % totaltime)

def runqueriesmonet(cur):
    try:
        fname = os.getcwd() + "/sql/monet_queries.sql"
        f = open(fname, "r")
    except IOError:
        print(f"Could not open/read {fname}")
        sys.exit()
    geo_queries = f.read().splitlines()

    if debug:
        print("\nRunning queries")

    totaltime = 0
    q_id = 1
    for q in geo_queries:
        if q.startswith("--"):
            continue
        if debug:
            print(q)
        start = timer()
        try:
            cur.execute(q)
        except pymonetdb.DatabaseError as msg:
            print('Exception', msg)
            continue
        querytime = timer() - start
        totaltime += querytime
        print("Executed query " + str(q_id) + " in %6.3f seconds" % querytime)
        q_id +=1
    print("Executed all queries in %6.3f seconds" % totaltime)

def dropschemamonet(cur):
    if debug:
        print("\nDropping schema")
    cur.execute("SET SCHEMA = sys;")
    cur.execute("DROP SCHEMA bench_geo cascade;")

def benchmarkmonet():
    conn = pymonetdb.connect(args.database, autocommit=True)
    if not conn:
        print(f'Could not access the database {args.database}')
        sys.exit()
    if debug:
        print(f'MonetDB\nConnected to database {args.database}')

    cur = conn.cursor()
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
    except IOError:
        print(f"Could not open/read {fname}")
        sys.exit()
    if debug:
        print("Creating schema")
    for d in geo_ddl:
        if debug:
            print(d)
        try:
            cur.execute(d)
        except psycopg2.DatabaseError as msg:
            print('Exception', msg)
            continue

def loadcsvpsql(cur):
    totaltime = 0
    for csv_t in load_tables["csv_tables"]:
        if "scalable" in csv_t and args.scale > 0:
            outputfilename = f'{datadir}/{csv_t["filename"]}_{args.scale}.csv'
            if cutcsv(f'{datadir}/{csv_t["filename"]}.csv',outputfilename,0,args.scale):
                filename = outputfilename
            else:
                print("cutcsv() operation failed, using original csv")
                filename = f'{datadir}/{csv_t["filename"]}.csv'
        else:
            filename = f'{datadir}/{csv_t["filename"]}.csv'
        #TODO Change to psycopg copy_from() method?
        query = f'COPY {csv_t["tablename"]} ({csv_t["columns"]}) FROM \'{filename}\' delimiter \',\' csv HEADER;'
        if debug:
            print(query)
        start = timer()
        try:
            cur.execute(query)
        except psycopg2.DatabaseError as msg:
            print('Exception', msg)
            continue

        if "timestamp" in csv_t:
            try:
                cur.execute(f'UPDATE {csv_t["tablename"]} SET t = to_timestamp({csv_t["timestamp"]});')
            except psycopg2.DatabaseError as msg:
                print('Exception', msg)
                continue
        if "geom" in csv_t:
            try:
                cur.execute(f'UPDATE {csv_t["tablename"]} SET geom = ST_SetSRID(ST_MakePoint({csv_t["geom"]}),4326);')
            except psycopg2.DatabaseError as msg:
                print('Exception', msg)
                continue
        loadtime = timer() - start
        totaltime += loadtime
        print("Loaded " + filename + " in %6.3f seconds" % loadtime)
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
        if debug:
            print(query)
        start = timer()
        #TODO Change to subprocess.check_output()
        subprocess.run(query, shell=True, stdout=subprocess.DEVNULL)
        loadtime = timer() - start
        totaltime += loadtime
        print("Loaded " + csv_t["tablename"] + " in %6.3f seconds" % loadtime)
    return totaltime

def loaddatapsql(cur):
    #Get PostGIS version
    try:
        cur.execute("select PostGIS_Lib_Version();")
    except psycopg2.DatabaseError as msg:
        print('Exception', msg)
        return

    result = cur.fetchone()
    if result is not None:
        postgisversion = result[0]
    else:
        #Default value, latest version
        postgisversion = "3.1.2"

    if debug:
        print("\nLoading data")

    totaltime = loadcsvpsql(cur)
    movepostgisextension(cur,"public",postgisversion)
    totaltime += loadshppsql()
    movepostgisextension(cur, "bench_geo",postgisversion)

    print("All loads in %6.3f seconds" % totaltime)

def runqueriespsql(cur):
    try:
        fname = os.getcwd() + "/sql/psql_queries.sql"
        f = open(fname, "r")
    except IOError:
        print(f"Could not open/read {fname}")
        sys.exit()
    geo_queries = f.read().splitlines()

    if debug:
        print("\nRunning queries")

    totaltime = 0
    q_id = 1
    for q in geo_queries:
        if q.startswith("--"):
            continue
        if debug:
            print(q)
        start = timer()
        try:
            cur.execute(q)
        except psycopg2.DatabaseError as msg:
            print('Exception', msg)
            continue
        querytime = timer() - start
        totaltime += querytime
        print("Executed query " + str(q_id) + " in %6.3f seconds" % querytime)
        q_id +=1
    print("Executed all queries in %6.3f seconds" % totaltime)

def dropschemapsql(cur):
    if debug:
        print("\nDropping schema")
    cur.execute("SET SCHEMA 'public';")
    cur.execute("DROP SCHEMA bench_geo cascade;")

def benchmarkpsql():
    conn = psycopg2.connect(f"dbname={args.database}")
    if not conn:
        print(f'Could not access the database {args.database}')
        sys.exit()
    if debug:
        print(f'Postgres\nConnected to database {args.database}')
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
    debug = args.debug
    datadir = args.data

    if args.system == 'monetdb' or args.system == 'monet' or args.system == 'mdb':
        benchmarkmonet()
    elif args.system == 'postgres' or args.system == 'psql'or args.system == 'postgis':
        benchmarkpsql()
    else:
        print('Choose a system to benchmark: monetdb (alias mdb, monet) or postgis (alias psql, postgres)')


#!/usr/bin/env python3
import pymonetdb
import argparse
import sys
import os
from timeit import default_timer as timer
import psycopg2
import subprocess
import logging
import datetime
import csv

#TODO: Version number in txt metadata file, add MDB commit hash

#TODO: Pass number of records to COPY INTO commands
#TODO: Use the MinTimeLogger from extras (psql)
#TODO: Abstract classes -> Unify the methods, change Exception to general exception

#TODO: Time from database server in Postgres -> Leave it for now

#TODO: Import download data script into here?
#TODO: Read from CSV header; Create table from columns of CSV
#TODO: Integrate the server startup (for Monet and Postgres)
#TODO: Change os.getcwd() to a "root directory" variable (maybe replacing data_dir var)

parser = argparse.ArgumentParser(
    description='Geom benchmark (MonetDB Geo vs PostGIS)',
    epilog='''
    This program loads and executes the Geographic data benchmark on MonetDB and PostGIS.
    ''')
#Program arguments
parser.add_argument('--data', type=str, help='Absolute path to the dataset directory', required = True, default=None)
parser.add_argument('--system', type=str, help='Database system to benchmark (default is both)', default=None)
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
parser.add_argument('--export', help='Turn on exporting query tables after execution', dest='export', action='store_true')
parser.add_argument('--no-export', help='Turn off exporting query tables after execution (default is off)', dest='export', action='store_false')
parser.set_defaults(export=False)

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

results = {
    "monet": [],
    "psql": []
}

results_header = ['SF','Operation','Monet_Server_Time','Monet_Client_Time','PSQL_Server_Time','PSQL_Client_Time']

psql_version = None
postgis_version = None
monet_version = None

class DatabaseHandler:
    args = parser.parse_args()
    data_dir = args.data

    def __init__(self, system):
        self.system = system

    # Open SQL file, read content and split strings on semicolon
    @staticmethod
    def open_and_split_sql_file(sql_filename):
        try:
            filename = os.getcwd() + "/sql/" + sql_filename
            f = open(filename, "r")
        except IOError as msg:
            logger.exception(msg)
            sys.exit()
        return f.read().split(";")

    # Create a new CSV file with a subset of data from an input CSV, given a scale factor (only < 1 SF allowed)
    # If the file already exists, use it. We currently don't delete the file after execution
    @staticmethod
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

    def get_server_query_time(self,cur):
        return 0

    def execute_query(self,cur,q):
        pass

    def connect_database(self):
        pass

    def prepare_connection(self, conn):
        pass

    def create_schema(self, cur):
        geo_ddl = open_and_split_sql_file(f"{self.system}_ddl.sql")
        logger.debug("Creating schema")
        for q in geo_ddl:
            if not q:
                continue
            logger.debug(f"Executing query '{q}'")
            if not self.execute_query(cur,q):
                continue

    def get_version(self,cur):
        pass

    def load_shp(self,cur):
        pass

    def add_timestamp(self, cur, tablename, attribute):
        pass

    def add_geom(self, cur, tablename, attribute):
        pass

    def copy_into_query(self, tablename, columns, filename):
        pass

    def load_csv(self,cur):
        total_time = 0
        for csv_t in load_tables["csv_tables"]:
            if "scalable" in csv_t and args.scale != 0:
                filename = self.scale_csv(csv_t["filename"], args.scale)
            else:
                filename = f'{data_dir}/{csv_t["filename"]}.csv'
            query = self.copy_into_query(csv_t["tablename"],csv_t["columns"],filename)
            logger.debug(f"Executing query '{query}'")
            start = timer()
            if not self.execute_query(cur,query):
                continue
            server_time = self.get_server_query_time(cur)

            if "timestamp" in csv_t:
                if not self.add_timestamp(cur,csv_t["tablename"],csv_t["timestamp"]):
                    continue
                server_time += self.get_server_query_time(cur)
            if "geom" in csv_t:
                if not self.add_geom(cur,csv_t["tablename"],csv_t["geom"]):
                    continue
                server_time += self.get_server_query_time(cur)

            client_time = timer() - start
            register_result(self.system, str(args.scale), f'CSV_{csv_t["tablename"]}', str(server_time), str(client_time))
            total_time += client_time
            if "scalable" in csv_t and args.scale > 0:
                logger.info("CLIENT: Loaded %s_%s in %6.3f seconds" % (csv_t["tablename"], args.scale, client_time))
            else:
                logger.info("CLIENT: Loaded %s in %6.3f seconds" % (filename, client_time))
        return total_time

    def load_data(self, cur):
        logger.debug("Loading data")

    def run_queries(self, cur):
        geo_queries = open_and_split_sql_file(f"{self.system}_queries.sql")
        logger.debug("Running queries")

        total_time = 0
        query_id = 1
        for q in geo_queries:
            if not q:
                continue
            logger.debug(f"Executing query '{q}'")
            start = timer()
            if not self.execute_query(cur,q):
                continue
            client_time = timer() - start
            server_time = self.get_server_query_time(cur)
            total_time += client_time

            register_result(self.system, str(args.scale), f'Q{query_id}', str(server_time), str(client_time))
            logger.info("CLIENT: Executed query %s in %6.3f seconds" % (query_id, client_time))
            if server_time:
                logger.info("SERVER: Executed query %s in %6.3f seconds" % (query_id, server_time))
            query_id += 1
        logger.info("CLIENT: Executed all queries in %6.3f seconds" % total_time)

    def export_query_tables(self,cur):
        geo_export = open_and_split_sql_file(f"{self.system}_export.sql")
        logger.debug("Exporting data")

        for q in geo_export:
            if not q:
                continue
            # Replace placeholder %OUT% string with output directory
            # TODO: Change os.getcwd()?
            q = q.replace("%OUT%", os.getcwd() + "/out")
            logger.debug(f"Executing export query '{q}'")
            if not self.execute_query(cur,q):
                continue

    def drop_schema(self,cur):
        logger.debug("Dropping schema")

    def benchmark(self):
        conn = self.connect_database()
        if not conn:
            logger.error(f'Could not access the database {args.database}')
            sys.exit()
        logger.debug(f'{self.system}: Connected to database {args.database}')
        self.prepare_connection(conn)
        cur = conn.cursor()
        if args.load:
            self.create_schema(cur)
            #TODO Move this, BUT -> it needs postgis to be installed to work (after create extension command)
            self.get_version(cur)
            self.load_data(cur)
        if args.query:
            self.run_queries(cur)
        if args.export:
            self.export_query_tables(cur)
        if args.drop:
            self.drop_schema(cur)
        conn.close()

class MonetHandler(DatabaseHandler):
    def __init__(self):
        super().__init__("monet")

    def get_monet_version(self,cur):
        global monet_version
        if not self.execute_query(cur,"select value from sys.env() where name = 'monet_version';"):
            return "0"

        result = cur.fetchone()
        if result is not None:
            monet_version = result[0]
        else:
            monet_version = "0"

    def get_server_query_time(self,cur):
        if not self.execute_query(cur,f"select extract(epoch from t) "
                        f"from (select max(stop)-max(start) as t from querylog_history) "
                        f"as a;"):
            return -1
        result = cur.fetchone()
        if result is not None:
            return result[0]
        else:
            return -1

    # TODO: Is this return value okay? Should it be an exception?
    def execute_query(self, cur, q):
        try:
            cur.execute(q)
        except pymonetdb.DatabaseError as msg:
            logger.exception(msg)
            return 0
        return 1

    def load_shp(self, cur):
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
            client_time = timer() - start
            server_time = self.get_server_query_time(cur)
            total_time += client_time
            register_result('monet', str(args.scale), f'SHP_{csv_t["tablename"]}', str(server_time), str(client_time))
            logger.info("CLIENT: Loaded %s in %6.3f seconds" % (csv_t["tablename"], client_time))
            logger.info("SERVER: Loaded %s in %6.3f seconds" % (csv_t["tablename"], server_time))
        return total_time

    def add_timestamp(self, cur, tablename, attribute):
        return self.execute_query(cur,f'UPDATE {tablename} SET t = epoch(cast({attribute} as int));')

    def add_geom(self, cur, tablename, attribute):
        return self.execute_query(cur, f'UPDATE {tablename} SET geom = ST_SetSRID(ST_MakePoint({attribute}),4326);')

    def copy_into_query(self, tablename, columns, filename):
        return f'COPY OFFSET 2 INTO {tablename} ({columns}) FROM \'{filename}\' \
        ({columns}) DELIMITERS \',\',\'\\n\',\'\"\' NULL AS \'\';'

    def load_csv(self, cur):
        return super().load_csv(cur)

    def load_data(self, cur):
        super().load_data(cur)
        #TODO: Move this?
        self.enable_query_history(cur)
        total_time = self.load_csv(cur)
        total_time += self.load_shp(cur)
        logger.info("All loads in %6.3f seconds" % total_time)

    def connect_database(self):
        conn = pymonetdb.connect(args.database, autocommit=True)
        return conn

    def enable_query_history(self,cur):
        logger.debug("Enabling query history")
        self.execute_query(cur,"call sys.querylog_enable();")

    def drop_schema(self,cur):
        cur.execute("SET SCHEMA = sys;")
        cur.execute("DROP SCHEMA bench_geo cascade;")

class PostgresHandler(DatabaseHandler):
    postgis_version = ""
    psql_version = ""

    def __init__(self):
        super().__init__("psql")

    def get_postgis_version(self,cur):
        global postgis_version
        logger.debug("Getting postgis version")
        if not self.execute_query(cur,"select PostGIS_Lib_Version();"):
            return "0"

        result = cur.fetchone()
        if result is not None:
            postgis_version = result[0]
        else:
            postgis_version = "0"

    def get_psql_version(self,cur):
        global psql_version
        logger.debug("Getting psql version")
        if not self.execute_query(cur,"SHOW server_version;"):
            return "0"

        result = cur.fetchone()
        if result is not None:
            psql_version = result[0]
        else:
            psql_version = "0"

    def get_version(self, cur):
        self.get_postgis_version(cur)
        self.get_psql_version(cur)

    #TODO: Is this return value okay?
    def execute_query(self,cur,q):
        try:
            cur.execute(q)
        except psycopg2.DatabaseError as msg:
            logger.exception(msg)
            return 0
        return 1

    def connect_database(self):
        conn = psycopg2.connect(f"dbname={args.database}")
        return conn

    def prepare_connection(self,conn):
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

    # PostGIS extension can only be in one schema at a time.
    # We need it in the bench_geo schema to add geometry support
    # But shapefiles can only be loaded if the extension is in the public (default) schema
    # TODO: Check if this is really necessary
    def move_postgis_extension(self, cur, schema):
        cur.execute("UPDATE pg_extension SET extrelocatable = TRUE WHERE extname = 'postgis';")
        cur.execute(f"ALTER EXTENSION postgis SET SCHEMA {schema};")
        cur.execute(f"ALTER EXTENSION postgis UPDATE TO \"{postgis_version}next\";")
        cur.execute(f"ALTER EXTENSION postgis UPDATE TO \"{postgis_version}\";")

    def load_shp(self, cur):
        total_time = 0
        for csv_t in load_tables["shape_tables"]:
            # TODO Some shapefiles can have different SRID
            query = f'shp2pgsql -I -s 4326 \'{data_dir}/{csv_t["filename"]}\' bench_geo.{csv_t["tablename"]} | psql  -d {args.database};'
            logger.debug(f"Executing command '{query}'")
            start = timer()
            try:
                subprocess.check_output(query, shell=True, stderr=subprocess.STDOUT)
            except subprocess.CalledProcessError as msg:
                logger.exception(msg)
            load_time = timer() - start
            total_time += load_time
            register_result('psql', str(args.scale), f'SHP_{csv_t["tablename"]}', '', str(load_time))
            logger.info("CLIENT: Loaded %s in %6.3f seconds" % (csv_t["tablename"], load_time))
        return total_time

    def add_timestamp(self, cur, tablename, attribute):
        return self.execute_query(cur,f'UPDATE {tablename} SET t = to_timestamp({attribute});')

    def add_geom(self, cur, tablename, attribute):
        return self.execute_query(cur, f'UPDATE {tablename} SET geom = ST_SetSRID(ST_MakePoint({attribute}),4326);')

    def copy_into_query(self, tablename, columns, filename):
        return f'COPY {tablename} ({columns}) FROM \'{filename}\' delimiter \',\' csv HEADER;'

    def load_csv(self, cur):
        return super().load_csv(cur)

    def load_data(self, cur):
        super().load_data(cur)
        total_time = self.load_csv(cur)
        self.move_postgis_extension(cur, "public")
        total_time += self.load_shp(cur)
        self.move_postgis_extension(cur, "bench_geo")
        logger.info("CLIENT: All loads in %6.3f seconds" % total_time)

    def drop_schema(self,cur):
        super().drop_schema(cur)
        cur.execute("SET SCHEMA 'public';")
        cur.execute("DROP SCHEMA bench_geo cascade;")


def get_postgis_version(cur):
    global postgis_version
    try:
        cur.execute("select PostGIS_Lib_Version();")
    except psycopg2.DatabaseError as msg:
        logger.exception(msg)
        return

    result = cur.fetchone()
    if result is not None:
        postgis_version = result[0]
    else:
        postgis_version = "0"

def get_psql_version(cur):
    global psql_version
    try:
        cur.execute("SHOW server_version;")
    except psycopg2.DatabaseError as msg:
        logger.exception(msg)
        return

    result = cur.fetchone()
    if result is not None:
        psql_version = result[0]
    else:
        psql_version = "0"

def get_monet_version(cur):
    global monet_version
    try:
        cur.execute("select value from sys.env() where name = 'monet_version';")
    except pymonetdb.DatabaseError as msg:
        logger.exception(msg)
        return

    result = cur.fetchone()
    if result is not None:
        monet_version = result[0]
    else:
        monet_version = "0"

#Open SQL file, read content and split strings on semicolon
def open_and_split_sql_file(sql_filename):
    try:
        filename = os.getcwd() + "/sql/" + sql_filename
        f = open(filename, "r")
    except IOError as msg:
        logger.exception(msg)
        sys.exit()
    return f.read().split(";")

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

def register_result(system,scale,operation,server_time,client_time):
    if server_time == "0":
        server_time = ""
    result = {
        "scale": scale,
        "operation": operation,
        "server_time": server_time,
        "client_time": client_time
    }
    results[system].append(result)

def write_results_csv():
    now = datetime.datetime.now()
    monet_array = results['monet']
    psql_array = results['psql']
    with open(f'{os.getcwd()}/results/result_'
              f'{now.strftime("%d")}-{now.strftime("%m")}_{now.strftime("%H")}:{now.strftime("%M")}_('
              f'{monet_version}_{psql_version}_{postgis_version}).csv', 'w', encoding='UTF8') as f:
        writer = csv.writer(f)
        writer.writerow(results_header)
        #TODO: Is this okay?
        line_count = len(monet_array)
        for i in range(line_count):
            monet_result = monet_array[i]
            psql_result = psql_array[i]
            writer.writerow([f'{monet_result["scale"]}',f'{monet_result["operation"]}',
                            f'{monet_result["server_time"]}',f'{monet_result["client_time"]}',
                            f'{psql_result["server_time"]}',f'{psql_result["client_time"]}'])

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
    geo_ddl = open_and_split_sql_file("monet_ddl.sql")
    logger.debug("Creating schema")
    for q in geo_ddl:
        if not q:
            continue
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
        register_result('monet',str(args.scale),f'SHP_{csv_t["tablename"]}',str(load_time),str(end))
        logger.info("CLIENT: Loaded %s in %6.3f seconds" % (csv_t["tablename"],end))
        logger.info("SERVER: Loaded %s in %6.3f seconds" % (csv_t["tablename"],load_time))
    return total_time

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
        register_result('monet', str(args.scale), f'CSV_{csv_t["tablename"]}', str(load_time), str(end))
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
    geo_queries = open_and_split_sql_file("monet_queries.sql")
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
        query_time = get_last_exec_time_monet(cur)
        register_result('monet', str(args.scale), f'Q{query_id}', str(query_time), str(end))
        logger.info("CLIENT: Executed query %s in %6.3f seconds" % (query_id,end))
        logger.info("SERVER: Executed query %s in %6.3f seconds" % (query_id, query_time))
        total_time += query_time
        query_id +=1
    logger.info("Executed all queries in %6.3f seconds" % total_time)

def export_query_tables_monet(cur):
    geo_export = open_and_split_sql_file("monet_export.sql")
    logger.debug("Exporting data")

    for q in geo_export:
        if not q:
            continue
        #Replace placeholder %OUT% string with output directory
        #TODO: Change os.getcwd()?
        q = q.replace("%OUT%",os.getcwd()+"/out")
        logger.debug(f"Executing export query '{q}'")
        try:
            cur.execute(q)
        except pymonetdb.DatabaseError as msg:
            logger.exception(msg)
            continue

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
    get_monet_version(cur)
    enable_query_history(cur)
    if args.load:
        create_schema_monet(cur)
        load_data_monet(cur)
    if args.query:
        run_queries_monet(cur)
    if args.export:
        export_query_tables_monet(cur)
    if args.drop:
        drop_schema_monet(cur)
    conn.close()

def create_schema_psql(cur):
    geo_ddl = open_and_split_sql_file("psql_ddl.sql")
    logger.debug("Creating schema")
    for q in geo_ddl:
        if not q:
            continue
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
        register_result('psql', str(args.scale), f'CSV_{csv_t["tablename"]}', '', str(load_time))
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
            subprocess.check_output(query, shell=True,stderr=subprocess.STDOUT)
        except subprocess.CalledProcessError as msg:
            logger.exception(msg)
        load_time = timer() - start
        total_time += load_time
        register_result('psql', str(args.scale), f'SHP_{csv_t["tablename"]}', '', str(load_time))
        logger.info("CLIENT: Loaded %s in %6.3f seconds" % (csv_t["tablename"],load_time))
    return total_time

def load_data_psql(cur):
    logger.debug("Loading data")

    total_time = load_csv_psql(cur)
    move_postgis_extension(cur, "public", postgis_version)
    total_time += load_shp_psql()
    move_postgis_extension(cur, "bench_geo", postgis_version)

    logger.info("All loads in %6.3f seconds" % total_time)

def run_queries_psql(cur):
    geo_queries = open_and_split_sql_file("psql_queries.sql")
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
        except psycopg2.DatabaseError as msg:
            logger.exception(msg)
            continue
        query_time = timer() - start
        register_result('psql', str(args.scale), f'Q{query_id}', '', str(query_time))
        total_time += query_time
        logger.info("CLIENT: Executed query %s in %6.3f seconds" % (query_id, query_time))
        query_id +=1
    logger.info("Executed all queries in %6.3f seconds" % total_time)

def export_query_tables_psql(cur):
    geo_export = open_and_split_sql_file("psql_export.sql")
    logger.debug("Exporting data")

    for q in geo_export:
        if not q:
            continue
        #Replace placeholder %OUT% string with output directory
        #TODO: Change os.getcwd()?
        q = q.replace("%OUT%",os.getcwd()+"/out")
        logger.debug(f"Executing export query '{q}'")
        try:
            cur.execute(q)
        except psycopg2.DatabaseError as msg:
            logger.exception(msg)
            continue

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
        #TODO Move this, it needs postgis to be installed to work
        get_postgis_version(cur)
        get_psql_version(cur)
        load_data_psql(cur)
    if args.query:
        run_queries_psql(cur)
    if args.export:
        export_query_tables_psql(cur)
    if args.drop:
        drop_schema_psql(cur)
    conn.close()


def configure_logger():
    # TODO Add option to log to file
    if args.debug:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s: %(levelname)s: %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

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
        '''benchmark_monet()
        benchmark_psql()
        write_results_csv()'''
        MonetHandler().benchmark()
        PostgresHandler().benchmark()
        write_results_csv()
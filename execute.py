#!/usr/bin/env python3
import pymonetdb
import argparse
import sys
import os
from timeit import default_timer as timer
import psycopg2
from subprocess import run, CalledProcessError, check_output, STDOUT
import logging
import datetime
import csv

MONET_ONLY = 'monet'
PGRES_ONLY = 'postgres'

parser = argparse.ArgumentParser(
    description='Geom benchmark (MonetDB Geo vs PostGIS)',
    epilog='This program executes the VesselAI Geographic data '
           'benchmark on MonetDB and PostGIS.')
# Program arguments
parser.add_argument('--database', type=str, default='marine',
                    help='Database to connect to (default is marine)')

# Switch (bool) arguments
parser.add_argument('--debug', dest='debug', action='store_true',
                    help='Turn on debugging log (default is off)')
parser.add_argument('--no-drop', dest='drop', action='store_false',
                    help='Turn off dropping the query tables after execution (default is on)')
parser.add_argument('--single-system', dest='system', choices=[MONET_ONLY, PGRES_ONLY],
                    help='Choose to run the benchmark on only one db '
                    'system (default both systems)') 

args = parser.parse_args()

queries = [
    {
        "q_num": 9,
        "q_name": "get_distinct_vessels_1250",
        "comparison": "bool"
    },
    {
        "q_num": 10,
        "q_name": "get_distinct_vessels_2500",
        "comparison": "bool"
    },
    {
        "q_num": 11,
        "q_name": "get_distinct_vessels_5000",
        "comparison": "bool"
    },
    {
        "q_num": 12,
        "q_name": "get_vessels_interval_45_days",
        "comparison": "bool"
    },
    {
        "q_num": 13,
        "q_name": "get_vessels_interval_91_days",
        "comparison": "bool"
    },
    {
        "q_num": 14,
        "q_name": "get_vessels_interval_183_days",
        "comparison": "bool"
    },
    {
        "q_num": 15,
        "q_name": "get_vessels_polygon1_proj3035",
        "comparison": "bool"
    },
    {
        "q_num": 16,
        "q_name": "get_vessels_polygon2_proj3035",
        "comparison": "bool"
    },
    {
        "q_num": 17,
        "q_name": "get_vessels_polygon3_proj3035",
        "comparison": "bool"
    },
    {
        "q_num": 18,
        "q_name": "get_vessels_polygon4_proj3035",
        "comparison": "bool"
    },
    {
        "q_num": 19,
        "q_name": "get_vessels_polygon5_proj3035",
        "comparison": "bool"
    },
    {
        "q_num": 20,
        "q_name": "get_vessels_polygon1_geodetic",
        "comparison": "bool"
    },
    {
        "q_num": 21,
        "q_name": "get_vessels_polygon2_geodetic",
        "comparison": "bool"
    },
    {
        "q_num": 22,
        "q_name": "get_vessels_polygon3_geodetic",
        "comparison": "bool"
    },
    {
        "q_num": 23,
        "q_name": "get_vessels_polygon4_geodetic",
        "comparison": "bool"
    },
    {
        "q_num": 24,
        "q_name": "get_vessels_polygon5_geodetic",
        "comparison": "bool"
    }
]

results = {
    "monet": [],
    "psql": []
}
# CSV Header for performance comparison
results_header = ['Operation', 
                  'Monet_Client_Time',
                  'PSQL_Client_Time']

FILE_TIME_FORMAT = "%d-%m_%H:%M"

class DatabaseHandler:
    def __init__(self, system, result_dir):
        self.system = system
        self.results_dir = f'{result_dir}/query_{system}'
        self.conn = self.connect_database()
        if not self.conn:
            logger.error(f'Could not access the database {args.database}')
            sys.exit()
        logger.info(f'{self.system.upper()}: Connected to database {args.database}')
        self.prepare_connection(self.conn)
        self.cur = self.conn.cursor()

    def close(self):
        self.conn.close()

    @staticmethod
    def register_result(system, operation, client_time):
        result = {
            "operation": operation,
            "client_time": client_time
        }
        results[system].append(result)

    # Open SQL file, read content and split strings on semicolon
    @staticmethod
    def open_and_split_sql_file(sql_filename):
        try:
            filename = os.getcwd() + "/sql/" + sql_filename
            f = open(filename, "r")
        except IOError as msg:
            logger.exception(msg)
            sys.exit()
        return f.read().split(";")[:-1]

    @staticmethod
    def execute_query(cur, q):
        try:
            logger.debug(f"Executing query '{q}'")
            cur.execute(q)
        except Exception as msg:
            logger.exception(msg)
            return 0
        return 1
    
    def benchmark(self):
        self.get_version(self.cur)
        self.run_queries(self.cur)
        if args.drop:
            self.drop_schema(self.cur)

    def connect_database(self):
        pass

    def prepare_connection(self, conn):
        pass

    def get_version(self, cur):
        pass

    def get_server_query_time(self, cur):
        return -1

    def run_queries(self, cur):  
        geo_queries = self.open_and_split_sql_file(f"{self.system}_queries_new.sql")      
        logger.debug("Running queries")

        total_time = 0
        query_num = 1
        for q in geo_queries:
            if not q:
                continue
            #TODO: Improve
            for i in range(2):
                if not self.execute_query(cur, q):
                    logger.error(f'Query {queries[query_num-1]["q_name"]} failed')
                    continue
            start = timer()
            if not self.execute_query(cur, q):
                self.register_result(
                    self.system,
                    f'Q{queries[query_num-1]["q_num"]}_{queries[query_num-1]["q_name"]}',
                    -1)
                query_num += 1
                continue
            client_time = timer() - start
            total_time += client_time

            self.register_result(self.system, 
                                 f'Q{queries[query_num-1]["q_num"]}_{queries[query_num-1]["q_name"]}',
                                 client_time)
            logger.debug("CLIENT: Executed query %s in %6.3f seconds"
                         % (query_num, client_time))
            query_num += 1
        logger.info("Executed all queries in %6.3f seconds" % total_time)
        self.register_result(self.system, "All queries", total_time)

    def drop_schema(self, cur):
        logger.debug("Dropping schema")
        if self.system == "monet":
            if not self.execute_query(cur, "SET SCHEMA = sys;"):
                return
        else:
            if not self.execute_query(cur, "SET SCHEMA 'public';"):
                return
        if not self.execute_query(cur, "DROP SCHEMA bench_geo cascade;"):
            return


class MonetHandler(DatabaseHandler):
    def __init__(self, results_dir):
        super().__init__("monet", results_dir)
        self.monet_version = None
        self.monet_revision = None

    def connect_database(self):
        conn = pymonetdb.connect(args.database, autocommit=True)
        return conn

    def get_version(self, cur):
        query = """
            SELECT name, value
            FROM sys.env()
            WHERE name IN ('monet_version', 'revision');
            """
        if not self.execute_query(cur, query):
            return "0"
        result = cur.fetchall()
        for r in result:
            if r[0] == 'monet_version':
                self.monet_version = r[1]
            elif r[0] == 'revision':
                self.monet_revision = r[1]

    def get_server_query_time(self, cur):
        '''query = """
            SELECT extract(epoch FROM t)
            FROM (
                SELECT max(stop) - max(start) AS t
                FROM querylog_history
            ) AS a;
            """
        if not self.execute_query(cur, query):
            return -1
        result = cur.fetchone()
        if result is not None:
            return result[0]
        else:
            return -1'''
        return -1


class PostgresHandler(DatabaseHandler):
    def __init__(self, results_dir):
        super().__init__("psql", results_dir)
        self.postgis_version = None
        self.psql_version = None

    def connect_database(self):
        conn = psycopg2.connect(f"dbname={args.database}")
        return conn

    def prepare_connection(self, conn):
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        conn.cursor().execute("CREATE EXTENSION IF NOT EXISTS postgis;")

    def get_version(self, cur):
        self.get_postgis_version(cur)
        self.get_psql_version(cur)

    def get_postgis_version(self, cur):
        logger.debug("Getting postgis version")
        if not self.execute_query(cur, "select PostGIS_Lib_Version();"):
            return "0"

        result = cur.fetchone()
        if result is not None:
            self.postgis_version = result[0]
        else:
            self.postgis_version = "0"

    def get_psql_version(self, cur):
        logger.debug("Getting psql version")
        if not self.execute_query(cur, "SHOW server_version;"):
            return "0"

        result = cur.fetchone()
        if result is not None:
            self.psql_version = result[0]
        else:
            self.psql_version = "0"

    # We have not implemented server-side timing retrieval for Postgres yet, return placeholder value
    def get_server_query_time(self, cur):
        return -1

def write_performance_results(result_dir, timestamp, dbsys):
    monet_array = results['monet'] if dbsys != PGRES_ONLY else []
    psql_array = results['psql'] if dbsys != MONET_ONLY else []
    
    results_file = f'{result_dir}/result_{timestamp.strftime(FILE_TIME_FORMAT)}.csv'
    write_header = not os.path.isfile(results_file)
    with open(results_file, 'a', encoding='UTF8') as f:
        writer = csv.writer(f)
        if write_header:
            writer.writerow(results_header)

        line_count = 0
        if dbsys == PGRES_ONLY:
            line_count = len(psql_array)
        elif dbsys == MONET_ONLY:
            line_count = len(monet_array)
        else:
            line_count = min(len(monet_array), len(psql_array))
        
        for i in range(line_count):
            try:
                # each result row will have info + monet_times + psql_times 
                info = monet_times = psql_times = ['', ''] 
                 
                # we always want to write operation
                if dbsys != PGRES_ONLY: 
                    info = [f'{monet_array[i].get("operation")}']
                elif dbsys != MONET_ONLY: 
                    info = [f'{psql_array[i].get("operation")}']

                # lambda for exluding negative server time from results
                stime_filter = lambda t : t if t > 0 else '' 
                
                if dbsys != PGRES_ONLY:
                    monet_times = [stime_filter(monet_array[i].get("client_time"))]
                
                if dbsys != MONET_ONLY:
                    psql_times = [stime_filter(psql_array[i].get("client_time"))] 

                writer.writerow(info + monet_times + psql_times)                
            except Exception as e:
                logger.exception(e)
                break
    results["monet"] = []
    results["psql"] = []


def write_performance_results_metadata(result_dir, timestamp, monet_handler, psql_handler):
    with open(
        f'{result_dir}/result_{timestamp.strftime(FILE_TIME_FORMAT)}_meta.txt',
        'w', encoding='UTF8'
    ) as f:
        if monet_handler: 
            f.write(f"MonetDB server version{monet_handler.monet_version}\n")
            f.write(f"(hg id {monet_handler.monet_revision})\n")
            f.write(f"pymonetdb client version {pymonetdb.__version__}\n")
       
        if psql_handler:
            f.write(f"Postgres server version {psql_handler.psql_version}\n")
            f.write(f"with PostGIS extension version {psql_handler.postgis_version}\n")
            f.write(f"psycopg2 client version {psycopg2.__version__}\n\n")
        
        f.write(f"Distance is calculated with the sphere model\n\n")


def configure_logger():
    if args.debug:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] [%(filename)s:%(lineno)d] %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

def create_query_results_dirs(time):
    output_directory = f'{os.getcwd()}/results/{time.strftime(FILE_TIME_FORMAT)}'

    try:
        os.makedirs(output_directory)
    except FileExistsError as msg:
        logger.exception(msg)
        return None
    else:
        return output_directory

if __name__ == "__main__":
    # Configure logger
    logger = logging.getLogger(__name__)
    configure_logger()
    
    m_handler = None
    p_handler = None

    try:
        now = datetime.datetime.now()
        results_dir = create_query_results_dirs(now)
        # initialize handlers 
        m_handler = MonetHandler(results_dir) if args.system != PGRES_ONLY else None 
        p_handler = PostgresHandler(results_dir) if args.system != MONET_ONLY else None
        if args.system != PGRES_ONLY:
                m_handler.benchmark()
        if args.system != MONET_ONLY:
            p_handler.benchmark()
        write_performance_results(results_dir, now, args.system)
        write_performance_results_metadata(results_dir, now, m_handler, p_handler)
    except Exception as msg:
        logger.exception(msg)

    # Disconnecting clients
    if m_handler:
        m_handler.close()
    if p_handler:
        p_handler.close()
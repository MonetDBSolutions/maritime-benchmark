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
from time import sleep

# TODO: Improve the return values of functions like execute_query, get_version and get_server_query_time
# TODO: Use the MinTimeLogger from extras (psql)
# TODO: Read from CSV header; Create table from columns of CSV
# TODO: Change os.getcwd() to a "root directory" variable (maybe replacing data_dir var)
# TODO: Delete results directory if the run had an exception

MONET_ONLY = 'mdb'
PGRES_ONLY = 'pgdb'

parser = argparse.ArgumentParser(
    description='Geom benchmark (MonetDB Geo vs PostGIS)',
    epilog='This program loads and executes the Geographic data '
           'benchmark on MonetDB and PostGIS.')
# Program arguments
parser.add_argument('--data', type=str, required=True, default=None,
                    help='Absolute path to the dataset directory')
parser.add_argument('--scale', type=float, nargs='+', default=0,
                    help='Benchmark scale factor(s) (only values < 1 allowed)')

parser.add_argument('--dbfarm_monet', type=str, default=None,
                    help='MonetDB'' database farm to be used in '
                    'starting the server from the script')
parser.add_argument('--dbfarm_psql', type=str, default=None,
                    help='PostgreSQL database directory to be used in'
                    'starting the server from the script')

parser.add_argument('--database', type=str, default='marine',
                    help='Database to connect to (default is marine)')

# Switch (bool) arguments
parser.add_argument('--debug', dest='debug', action='store_true',
                    help='Turn on debugging log (default is off)')
parser.add_argument('--export', dest='export', action='store_true',
                    help='Turn on exporting query tables after execution (default is off)')
parser.add_argument('--no-drop', dest='drop', action='store_false',
                    help='Turn off dropping the data after execution (default is on)')
parser.add_argument('--single-system', dest='system', choices=[MONET_ONLY, PGRES_ONLY],
                    help='Choose to run the benchmark on only one db '
                    'system (default both systems)') 

args = parser.parse_args()

load_tables = {
    "csv_tables":
        [
            {
                "tablename": "ais_dynamic",
                "filename": "nari_dynamic",
                "columns": "mmsi,status,turn,speed,course,heading,lon,lat,ts",
                "timestamp": "ts",
                "geom": "lon,lat",
                "scalable": "true"
            }
        ],
    "shape_tables":
        [
            {
                "tablename": "brittany_ports",
                "filename": "port.shp",
                "srid": "4326"
            },
            # {
                # "tablename": "europe_maritime_boundaries",
                # "filename": "MBEULSIV1.shp",
                # "srid": "4258"
            # },
            # {
                # "tablename": "europe_coastline",
                # "filename": "Europe Coastline.shp",
                # "srid": "3035"
            # },
            {
                "tablename": "fao_areas",
                "filename": "FAO_AREAS.shp",
                "srid": "4326"
            },
            {
                "tablename": "wpi_ports",
                "filename": "WPI.shp",
                "srid": "4326"
            },
            {
                "tablename": "fishing_areas",
                "filename": "v_recode_fish_area_clean.shp",
                "srid": "4326"
            },
            # {
                # "tablename": "fishing_interdiction",
                # "filename": "fishing_interdiction.shp",
                # "srid": "4326"
            # },
            # {
                # "tablename":"world_eez",
                # "filename":"eez.shp",
                # "srid":"4326"
            # }
        ]
}

queries = [
    {
        "q_num": 1,
        "q_name": "trajectory_segments",
        "comparison": None
    },
    {
        "q_num": 2,
        "q_name": "trajectory",
        "comparison": None
    },
    {
        "q_num": 3,
        "q_name": "ship_port_distance",
        "comparison": "float"
    },
    {
        "q_num": 4,
        "q_name": "trajectory_port_distance",
        "comparison": "float"
    },
    {
        "q_num": 5,
        "q_name": "trajectory_close_france",
        "comparison": "float"
    },
    {
        "q_num": 6,
        "q_name": "close_trajectories",
        "comparison": "float"
    },
    {
        "q_num": 7,
        "q_name": "fao_trajectory_intersect",
        "comparison": "bool"
    },
    {
        "q_num": 8,
        "q_name": "fao_fishing_intersect",
        "comparison": "bool"
    }
]

results = {
    "monet": [],
    "psql": []
}
# CSV Header for performance comparison
results_header = ['SF', 'Operation', 
                  'Monet_Server_Time', 'Monet_Client_Time',
                  'PSQL_Server_Time', 'PSQL_Client_Time']
# CSV Header for result comparison (floats)
comparison_header_float = ['Monet_Result', 'PSQL_Result',
                           'Dif_Result', 'Relative_Difference']
# CSV Header for result comparison (bool)
comparison_header_bool = ['Monet_Result', 'PSQL_Result', 'Same_Result']

FILE_TIME_FORMAT = "%d-%m_%H:%M"

class DatabaseHandler:
    
    def __init__(self, system, result_dir):
        self.system = system
        self.cur_scale = None
        # Used to store record number for Scale Factors (used in performance results metadata)
        self.record_number = []
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
    def register_result(system, scale, operation, server_time, client_time):
        result = {
            "scale": scale,
            "operation": operation,
            "server_time": server_time,
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

    # Create a new CSV file with a subset of data from an input CSV,
    # given a scale factor (only < 1 SF allowed) If the file already
    # exists, use it. We currently don't delete the file after execution
    def scale_csv(self, input_name, scale):
        if scale > 1:
            logger.warning("Scale factor must be less than 1, using original csv (SF 1)")
            return f'{args.data}/{input_name}.csv'
        output_file_name = f'{args.data}/{input_name}_SF_{scale}.csv'
        if os.path.isfile(output_file_name):
            logger.debug(f"Found already existing csv {output_file_name}")
            # Register the number of records in Scale Factor
            with open(output_file_name) as f:
                self.record_number.append(sum(1 for l in f))
            return output_file_name
        input_file_name = f'{args.data}/{input_name}.csv'
        with open(input_file_name, 'r') as input_file, \
             open(output_file_name, 'w+') as output_file:
            try:
                input_lines = input_file.readlines()
                records_scaled = int(len(input_lines) * scale)
                self.record_number.append(records_scaled)
                output_file.writelines(input_lines[0:records_scaled])
                logger.debug(f"Scaled umber of records '{input_name}': {records_scaled}")
            except IOError as msg:
                logger.warning("cutcsv() operation failed, using original csv (SF 1)")
                logger.exception(msg)
                return input_file_name
        return output_file_name

    # TODO Is this return value okay? Should we do exceptions?
    @staticmethod
    def execute_query(cur, q):
        try:
            logger.debug(f"Executing query '{q}'")
            cur.execute(q)
        except Exception as msg:
            logger.exception(msg)
            return 0
        return 1

    def benchmark(self, cur_scale):
        self.cur_scale = cur_scale
        logger.info(f'Benchmarking scale {self.cur_scale}')
        self.create_schema(self.cur)
        self.get_version(self.cur)
        self.load_data(self.cur)
        self.run_queries(self.cur)
        if args.export:
            self.export_query_tables(self.cur)
        if args.drop:
            self.drop_schema(self.cur)

    def connect_database(self):
        pass

    def prepare_connection(self, conn):
        pass

    def create_schema(self, cur):
        geo_ddl = self.open_and_split_sql_file(f"{self.system}_ddl.sql")
        logger.debug("Creating schema")
        for q in geo_ddl:
            if not q:
                continue
            if not self.execute_query(cur, q):
                continue

    def get_version(self, cur):
        pass

    def load_data(self, cur):
        logger.debug("Loading data")
        total_time = self.load_csv(cur)
        total_time += self.load_shp(cur)
        logger.info("All loads in %6.3f seconds" % total_time)
        self.register_result(self.system, self.cur_scale, "All loads", -1, total_time)

    def load_csv(self, cur):
        total_time = 0
        for csv_t in load_tables["csv_tables"]:
            if "scalable" in csv_t and self.cur_scale != 0:
                filename = self.scale_csv(csv_t["filename"], self.cur_scale)
            else:
                filename = f'{args.data}/{csv_t["filename"]}.csv'
            
            start = timer()
            if not self.copy_into(cur, csv_t["tablename"], csv_t["columns"], filename):
                continue
            server_time = self.get_server_query_time(cur)

            # handle explicitly 'timestamp' column
            if "timestamp" in csv_t:
                if not self.add_timestamp(cur, csv_t["tablename"], csv_t["timestamp"]):
                    continue
                server_time += self.get_server_query_time(cur)
            # handle excplicitly 'geom' column 
            if "geom" in csv_t:
                if not self.add_geom(cur, csv_t["tablename"], csv_t["geom"]):
                    continue
                server_time += self.get_server_query_time(cur)

            client_time = timer() - start
            self.register_result(
                self.system, self.cur_scale, f'CSV_{csv_t["tablename"]}',
                server_time, client_time)
            total_time += client_time
            if "scalable" in csv_t and self.cur_scale > 0:
                logger.debug(
                    "CLIENT: Loaded %s_%s in %6.3f seconds"
                    % (csv_t["tablename"], self.cur_scale, client_time))
            else:
                logger.debug("CLIENT: Loaded %s in %6.3f seconds"
                             % (filename, client_time))
        return total_time

    def get_server_query_time(self, cur):
        return -1

    def copy_into(self, cur, tablename, columns, filename):
        pass

    def add_geom(self, cur, tablename, attribute):
        pass

    def add_timestamp(self, cur, tablename, attribute):
        pass

    def load_shp(self, cur):
        pass

    def run_queries(self, cur):
        geo_queries = self.open_and_split_sql_file(f"{self.system}_queries.sql")
        logger.debug("Running queries")

        total_time = 0
        query_id = 1
        for q in geo_queries:
            if not q:
                continue
            start = timer()
            if not self.execute_query(cur, q):
                self.register_result(
                    self.system, self.cur_scale,
                    f'Q{query_id}_{queries[query_id-1]["q_name"]}',
                    -1, -1)
                query_id += 1
                continue
            client_time = timer() - start
            server_time = self.get_server_query_time(cur)
            total_time += client_time

            self.register_result(self.system, self.cur_scale, 
                                 f'Q{query_id}_{queries[query_id-1]["q_name"]}',
                                 server_time, client_time)
            logger.debug("CLIENT: Executed query %s in %6.3f seconds"
                         % (query_id, client_time))
            if server_time > 0:
                logger.debug("SERVER: Executed query %s in %6.3f seconds"
                             % (query_id, server_time))
            query_id += 1
        logger.info("Executed all queries in %6.3f seconds" % total_time)
        self.register_result(self.system, self.cur_scale, "All queries", -1, total_time)

    def export_query_tables(self, cur):
        geo_export = self.open_and_split_sql_file(f"{self.system}_export.sql")
        logger.debug("Exporting data")

        for q in geo_export:
            # Replace placeholder %OUT% string with output directory
            q = q.replace("%OUT%", self.results_dir)
            # If there is a Scale Factor, replace placehold %SF% with current scale factor
            if self.cur_scale > 0:
                q = q.replace("%SF%", f"_{self.cur_scale}")
            else:
                q = q.replace("%SF%", "")
            if not self.execute_query(cur, q):
                continue

    # TODO What should we do if the queries fail?
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
        query = """
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
            return -1

    def copy_into(self, cur, tablename, columns, filename):
        # TODO: Is it worth it to count the number of lines for the COPY INTO to be faster? -> Time it
        with open(filename) as f:
            record_number = sum(1 for l in f)
        query = f"""
            COPY {record_number} OFFSET 2 RECORDS 
            INTO {tablename} 
                ({columns}) 
            FROM \'{filename}\'
                ({columns}) 
            DELIMITERS \',\',\'\\n\',\'\"\' 
            NULL AS \'\';
            """
        return self.execute_query(cur, query)

    def add_geom(self, cur, tablename, attribute):
        query = f"""
            UPDATE {tablename}
            SET geom = ST_SetSRID(ST_MakePoint({attribute}),4326);
            """ 
        return self.execute_query(cur, query)

    def add_timestamp(self, cur, tablename, attribute):
        query = f"""
            UPDATE {tablename}
            SET t = epoch(cast({attribute} AS int));
            """
        return self.execute_query(cur, query)

    def load_shp(self, cur):
        total_time = 0
        for csv_t in load_tables["shape_tables"]:
            query = f"""call shpload(
                \'{args.data}/{csv_t["filename"]}\',
                \'bench_geo\', \'{csv_t["tablename"]}\');"""
            start = timer()
            try:
                cur.execute(query)
            except pymonetdb.DatabaseError as msg:
                logger.exception(msg)
                self.register_result('monet', self.cur_scale, 
                                     f'SHP_{csv_t["tablename"]}',
                                     -1, -1)
            else: 
                client_time = timer() - start
                server_time = self.get_server_query_time(cur)
                total_time += client_time
                self.register_result('monet', self.cur_scale,
                                     f'SHP_{csv_t["tablename"]}',
                                     server_time, client_time)
                logger.debug("CLIENT: Loaded %s in %6.3f seconds"
                             % (csv_t["tablename"], client_time))
                logger.debug("SERVER: Loaded %s in %6.3f seconds"
                             % (csv_t["tablename"], server_time))
        return total_time


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

    def copy_into(self, cur, tablename, columns, filename):
        query = f"""
            COPY {tablename}
                ({columns})
            FROM \'{filename}\'
            DELIMITER \',\' csv HEADER;
            """
        return self.execute_query(cur, query)

    def add_geom(self, cur, tablename, attribute):
        query = f"""
                UPDATE {tablename}
                SET geom = ST_SetSRID(ST_MakePoint({attribute}),4326);
                """
        return self.execute_query(cur, query)

    def add_timestamp(self, cur, tablename, attribute):
        query = f"""
                UPDATE {tablename} SET t = to_timestamp({attribute});
                """
        return self.execute_query(cur, query)

    def load_shp(self, cur):
        total_time = 0
        for csv_t in load_tables["shape_tables"]:
            cmd = f"""
                shp2pgsql -h && \
                shp2pgsql -I -s {csv_t["srid"]} \
                    \'{args.data}/{csv_t["filename"]}\' \
                    bench_geo.{csv_t["tablename"]} \
                | psql  -d {args.database};"""
            start = timer()
            try:
                check_output(cmd, shell=True, stderr=STDOUT)
            except CalledProcessError as msg:
                logger.exception(msg)
                self.register_result('psql', self.cur_scale,
                                     f'SHP_{csv_t["tablename"]}',
                                     -1, -1)
            else:
                client_time = timer() - start
                total_time += client_time
                self.register_result('psql', self.cur_scale,
                                     f'SHP_{csv_t["tablename"]}',
                                     -1, client_time)
                logger.debug("CLIENT: Loaded %s in %6.3f seconds" 
                             % (csv_t["tablename"], client_time))
        return total_time


class MonetServer:
    def __init__(self, dbfarm, dbname):
        self.dbfarm = dbfarm
        self.dbname = dbname
        self.start_server()

    def start_server(self):
        if not self.farm_is_running(self.dbfarm):
            if not self.farm_up(self.dbfarm):
                logger.error("MonetDB: Database farm could not be started.")
                self.destroy_farm(self.dbfarm)
                return
            if not self.server_up(self.dbname):
                logger.error("MonetDB: Server could not be started.")
                self.stop_server(args.drop)
            else:
                logger.info(f'MonetDB: Started database {self.dbname} on {self.dbfarm}')
        else:
            logger.error("MonetDB: Database farm already running, doing nothing.")

    def stop_server(self, destroy):
        self.server_down(self.dbname)
        if destroy:
            self.destroy_farm(self.dbfarm)
        else:
            self.farm_down(self.dbfarm)

    def farm_up(self, dbfarm='mydbfarm', prefix='') -> bool:
        if not os.path.exists(dbfarm):
            cmd = [os.path.join(prefix, 'monetdbd'), 'create', str(dbfarm)]
            try:
                run(cmd, check=True)
                logger.info('created dbfarm %s' % dbfarm)
            except CalledProcessError as e:
                logger.error(e)
                return False
            except:
                logger.error(sys.exc_info())
                return False

        cmd = [os.path.join(prefix, 'monetdbd'), 'start', str(dbfarm)]
        try:
            run(cmd, check=True)
            logger.info('started dbfarm %s' % dbfarm)
        except CalledProcessError as e:
            logger.error(e)
            return False
        except:
            logger.error(sys.exc_info())
            return False

        return True

    def farm_down(self, dbfarm, prefix='') -> bool:
        cmd = [os.path.join(prefix, 'monetdbd'), 'stop', str(dbfarm)]
        try:
            run(cmd, check=True)
            logger.info('stopped dbfarm %s' % dbfarm)
        except CalledProcessError as e:
            logger.error(e)
            return False
        except:
            logger.error(sys.exc_info())
            return False
        return True

    def farm_is_running(self, dbfarm) -> bool:
        if os.path.exists(dbfarm):
            cmd = "ps aux | grep monetdbd"
            try:
                res = check_output(cmd, shell=True).decode()
                return 'monetdbd' in res and dbfarm in res
            except CalledProcessError as e:
                logger.info('monetdbd is not running')
                return False
            except:
                logger.error(sys.exc_info())
                return False
        else:
            return False

    def destroy_farm(self, dbfarm):
        if os.path.exists(dbfarm):
            is_down = True
            if self.farm_is_running(dbfarm):
                is_down = self.farm_down(dbfarm)
            if is_down:
                cmd = ['rm', '-rf', str(dbfarm)]
                try:
                    run(cmd, check=True)
                    logger.info('succesfully removed dbfarm %s' % dbfarm)
                except CalledProcessError as e:
                    logger.error(e)
                except:
                    logger.error(sys.exc_info())
            else:
                logger.info('could not stop dbfram %s' % dbfarm)
        else:
            logger.error('dbfarm %s does NOT exists' % dbfarm)

    def server_up(self, dbname='marine', prefix='') -> bool:
        cmd = [os.path.join(prefix, 'monetdb'), '-q', 'create', str(dbname)]
        try:
            run(cmd, check=True)
            logger.info('created database %s' % dbname)
        except CalledProcessError as e:
            logger.error(e)
            return False
        except:
            logger.error(sys.exc_info())
            return False

        sleep(2)

        cmd = [os.path.join(prefix, 'monetdb'), '-q', 'release', str(dbname)]
        try:
            run(cmd, check=True)
            logger.info('released database %s' % dbname)
        except CalledProcessError as e:
            logger.error(e)
            return False
        except:
            logger.error(sys.exc_info())
            return False
        return True

    def server_down(self, dbname='marine', prefix='') -> bool:
        cmd = [os.path.join(prefix, 'monetdb'), '-q', 'stop', str(dbname)]
        try:
            run(cmd, check=True)
            logger.info('stopped database %s' % dbname)
        except CalledProcessError as e:
            logger.error(e)
            return False
        except:
            logger.error(sys.exc_info())
            return False
        return True


class PostgresServer:
    def __init__(self, dbfarm, dbname):
        self.dbfarm = dbfarm
        self.dbname = dbname
        self.start_server()

    def start_server(self):
        if not self.farm_is_running(self.dbfarm):
            if not self.farm_up(self.dbfarm):
                logger.error("Postgres: Database farm could not be started.")
                self.destroy_farm(self.dbfarm)
                return
            if not self.db_create(self.dbname):
                logger.error("Postgres: Server could not be started.")
                self.stop_server(args.drop)
            else:
                logger.info(f'Postgres: Started database {self.dbname} on {self.dbfarm}')
        else:
            logger.error("Postgres: Database already running, doing nothing.")

    def stop_server(self, destroy):
        if destroy:
            self.db_destroy(self.dbname)
            self.destroy_farm(self.dbfarm)
        else:
            self.farm_down(self.dbfarm)

    def farm_up(self, dbfarm='mydbfarm', prefix='') -> bool:
        if not os.path.exists(dbfarm):
            cmd = [os.path.join(prefix, 'initdb'), '-D', str(dbfarm)]
            try:
                check_output(cmd, stderr=STDOUT)
                logger.info('created dbfarm %s' % dbfarm)
            except CalledProcessError as e:
                logger.error(e)
                return False
            except:
                logger.error(sys.exc_info())
                return False

        cmd = [os.path.join(prefix, 'pg_ctl'),
               '-D', str(dbfarm),
               '-l', f'{dbfarm}/logfile',
               'start']
        try:
            check_output(cmd)
            logger.info('started dbfarm %s' % dbfarm)
        except CalledProcessError as e:
            logger.error(e)
            return False
        except:
            logger.error(sys.exc_info())
            return False

        return True

    def farm_down(self, dbfarm, prefix='') -> bool:
        cmd = [os.path.join(prefix, 'pg_ctl'), 
               '-D', str(dbfarm),
               '-l', f'{dbfarm}/logfile',
               'stop']
        try:
            check_output(cmd)
            logger.info('stopped dbfarm %s' % dbfarm)
        except CalledProcessError as e:
            logger.error(e)
            return False
        except:
            logger.error(sys.exc_info())
            return False
        return True

    def farm_is_running(self, dbfarm) -> bool:
        if os.path.exists(dbfarm):
            cmd = "ps aux | grep postgres"
            try:
                res = check_output(cmd, shell=True).decode()
                return 'postgres' in res and dbfarm in res
            except CalledProcessError as e:
                logger.info('postgres is not running')
                return False
            except:
                logger.error(sys.exc_info())
                return False
        else:
            return False

    def destroy_farm(self, dbfarm):
        if os.path.exists(dbfarm):
            is_down = True
            if self.farm_is_running(dbfarm):
                is_down = self.farm_down(dbfarm)
            if is_down:
                cmd = ['rm', '-rf', str(dbfarm)]
                try:
                    run(cmd, check=True)
                    logger.info('succesfully removed dbfarm %s' % dbfarm)
                except CalledProcessError as e:
                    logger.error(e)
                except:
                    logger.error(sys.exc_info())
            else:
                logger.info('could not stop dbfram %s' % dbfarm)
        else:
            logger.error('dbfarm %s does NOT exists' % dbfarm)

    def db_create(self, dbname='marine', prefix='') -> bool:
        cmd = [os.path.join(prefix, 'createdb'), '-h', 'localhost', str(dbname)]
        try:
            run(cmd, check=True)
            logger.info('created database %s' % dbname)
        except CalledProcessError as e:
            logger.error(e)
            return False
        except:
            logger.error(sys.exc_info())
            return False
        return True

    def db_destroy(self, dbname='marine', prefix='') -> bool:
        cmd = [os.path.join(prefix, 'dropdb'), '-h', 'localhost', str(dbname)]
        try:
            run(cmd, check=True)
            logger.info('destroyed database %s' % dbname)
        except CalledProcessError as e:
            logger.error(e)
            return False
        except:
            logger.error(sys.exc_info())
            return False
        return True


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
                 
                # we always want to write info (scale + operation)
                if dbsys != PGRES_ONLY: 
                    info = [f'{monet_array[i].get("scale")}',
                            f'{monet_array[i].get("operation")}']
                elif dbsys != MONET_ONLY: 
                    info = [f'{psql_array[i].get("scale")}',
                            f'{psql_array[i].get("operation")}']

                # lambda for exluding negative server time from results
                stime_filter = lambda t : t if t > 0 else '' 
                
                if dbsys != PGRES_ONLY:
                    monet_times = [stime_filter(monet_array[i].get("server_time")), 
                                   f'{round(monet_array[i].get("client_time"),3)}']
                
                if dbsys != MONET_ONLY:
                    psql_times = [stime_filter(psql_array[i].get("server_time")), 
                                  f'{round(psql_array[i].get("client_time"),3)}'] 

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
        f.write("Number of records for Scale Factors:\n")
       
        for i in range(0, len(args.scale)):
            r = monet_handler.record_number[i] if not psql_handler else psql_handler.record_number[i]
            f.write(f"SF {args.scale[i]}: {r} ais_dynamic records\n")


def configure_logger():
    if args.debug:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] [%(filename)s:%(lineno)d] %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)


def create_query_results_dirs(time, export):
    output_directory = f'{os.getcwd()}/results/{time.strftime(FILE_TIME_FORMAT)}'

    try:
        os.makedirs(output_directory)
        if export:
            os.mkdir(f"{output_directory}/query_monet/")
            os.mkdir(f"{output_directory}/query_psql/")
            os.mkdir(f"{output_directory}/query_comparison/")
    except FileExistsError as msg:
        logger.exception(msg)
        return None
    else:
        return output_directory


def compare_results_bool(monet_reader, psql_reader, compare_writer):
    compare_writer.writerow(comparison_header_bool)
    for m_row, p_row in zip(monet_reader, psql_reader):
        if m_row[len(m_row) - 1] == "true":
            m_value = True
        elif m_row[len(m_row) - 1] == "false":
            m_value = False
        if p_row[len(p_row) - 1] == "t":
            p_value = True
        elif p_row[len(p_row) - 1] == "f":
            p_value = False
        compare_writer.writerow([m_value, p_value, m_value == p_value])


def compare_results_float(monet_reader, psql_reader, compare_writer, compare_summary):
    csv.field_size_limit(sys.maxsize)
    compare_writer.writerow(comparison_header_float)
    row_compare = []
    absolute_result_array = []
    relative_result_array = []
    for m_row, p_row in zip(monet_reader, psql_reader):
        # As a convention, let's always use the last column as the processing results column
        m_value = m_row[len(m_row) - 1]
        p_value = p_row[len(p_row) - 1]
        m_float = float(m_value)
        p_float = float(p_value)
        dif = abs(m_float - p_float)
        absolute_result_array.append(dif)
        row_compare.append("{:.5f}".format(m_float))
        row_compare.append("{:.5f}".format(p_float))
        row_compare.append("{:.5f}".format(dif))
        if p_float != 0:
            relative_dif = (dif / abs(p_float)) * 100
            row_compare.append("{:.4f}%".format(relative_dif))
            relative_result_array.append(relative_dif)
        elif m_float == 0:
            relative_result_array.append(0)
            row_compare.append("{:.4f}%".format(0))
        else:
            # If the Postgres value is 0 and Monet value is not 0, we can't calculate the relative difference
            row_compare.append("NaN")
        compare_writer.writerow(row_compare)
        row_compare = []
    # Get avg, max and min from comparisons
    compare_summary.write(f"Absolute Avg: {'{:.5f}'.format(sum(absolute_result_array)/len(absolute_result_array))}\n")
    compare_summary.write(f"Absolute Min: {'{:.5f}'.format(min(absolute_result_array))}\n")
    compare_summary.write(f"Absolute Max: {'{:.5f}'.format(max(absolute_result_array))}\n")
    compare_summary.write(f"Relative Avg: {'{:.4f}%'.format(sum(relative_result_array)/len(relative_result_array))}\n")
    compare_summary.write(f"Relative Min: {'{:.4f}%'.format(min(relative_result_array))}\n")
    compare_summary.write(f"Relative Max: {'{:.4f}%'.format(max(relative_result_array))}\n")


def compare_query_results(output_dir, cur_scale):
    logger.info("Comparing query results")
    directory_monet = f'{output_dir}/query_monet/'
    directory_psql = f'{output_dir}/query_psql/'
    i = 0
    for file in sorted(os.listdir(directory_monet)):
        if file.endswith(f"{cur_scale}.csv"):
            if os.path.isfile(directory_psql + file) and queries[i]["comparison"] is not None:
                logger.debug(f"Comparing query result file {file}")
                with open(f'{directory_monet}/{file}', 'r') as monet_file,\
                     open(f'{directory_psql}/{file}','r') as psql_file,\
                     open(f'{output_dir}/query_comparison/{file}', 'w') as compare_file:
                    monet_reader = csv.reader(monet_file, delimiter=',')
                    psql_reader = csv.reader(psql_file, delimiter=',')
                    compare_writer = csv.writer(compare_file, delimiter=',')
                    if queries[i]["comparison"] == "float":
                        with open(f'{output_dir}/query_comparison/{file[:-4]}_summary.txt', 'w') as compare_summary:
                            compare_results_float(monet_reader, psql_reader, compare_writer, compare_summary)
                    elif queries[i]["comparison"] == "bool":
                        compare_results_bool(monet_reader, psql_reader, compare_writer)
            i += 1


if __name__ == "__main__":
    # Configure logger
    logger = logging.getLogger(__name__)
    configure_logger()

    # If the user specified a dbfarm for MDB, start the server from the script
    if args.dbfarm_monet and args.system != PGRES_ONLY:
        m_server = MonetServer(args.dbfarm_monet, args.database)
    # If the user specified a dbfarm for PSQL, start the server from the script
    if args.dbfarm_psql and args.system != MONET_ONLY:
        p_server = PostgresServer(args.dbfarm_psql, args.database)

    m_handler = None
    p_handler = None

    try:
        now = datetime.datetime.now()
        results_dir = create_query_results_dirs(now, args.export)
        # initialize handlers 
        m_handler = MonetHandler(results_dir) if args.system != PGRES_ONLY else None 
        p_handler = PostgresHandler(results_dir) if args.system != MONET_ONLY else None
        # for every scale factor run the benchmark (schema creation, data loading, queries)
        for scale in args.scale:
            if args.system != PGRES_ONLY:
                m_handler.benchmark(scale)
            if args.system != MONET_ONLY:
                p_handler.benchmark(scale)
            write_performance_results(results_dir, now, args.system)
            if args.export:
                compare_query_results(results_dir, scale)
        write_performance_results_metadata(results_dir, now, m_handler, p_handler)
    except Exception as msg:
        logger.exception(msg)

    # Disconnecting clients
    if m_handler:
        m_handler.close()
    if p_handler:
        p_handler.close()

    # Stopping servers, if they were started from the script
    if args.dbfarm_monet and args.system != PGRES_ONLY:
        m_server.stop_server(args.drop)
    if args.dbfarm_psql and args.system != MONET_ONLY:
        p_server.stop_server(args.drop)


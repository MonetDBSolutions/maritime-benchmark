#!/usr/bin/env python3
import pymonetdb
import argparse
import sys
import os
from timeit import default_timer as timer
import psycopg2
from subprocess import run, CalledProcessError, check_output, STDOUT, DEVNULL
import logging
import datetime
import csv
from time import sleep
import re

#TODO: Fix timezone difference in results between psql and monet

#TODO: Improve the return values of functions like execute_query, get_version and get_server_query_time
#TODO: Use the MinTimeLogger from extras (psql)
#TODO: Import download data script into here?
#TODO: Read from CSV header; Create table from columns of CSV
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
parser.add_argument('--scale', type=float, nargs='+', help='Benchmark scale factor(s) (only values < 1 allowed)', default=0)
parser.add_argument('--dbfarm_monet', type=str, help='MonetDB database farm to be used in starting the server from the script', default=None)
parser.add_argument('--dbfarm_psql', type=str, help='PostgreSQL database directory to be used in starting the server from the script', default=None)

#Switch (bool) arguments
parser.add_argument('--debug', help='Turn on debugging log', dest='debug', action='store_true')
parser.add_argument('--no-debug', help='Turn off debugging log (default is off)', dest='debug', action='store_false')
parser.set_defaults(debug=False)
parser.add_argument('--query', help='Turn on querying the data', dest='query', action='store_true')
parser.add_argument('--no-query', help='Turn off querying the data (default is on)', dest='query', action='store_false')
parser.set_defaults(query=True)
parser.add_argument('--export', help='Turn on exporting query tables after execution', dest='export', action='store_true')
parser.add_argument('--no-export', help='Turn off exporting query tables after execution (default is off)', dest='export', action='store_false')
parser.set_defaults(export=False)
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

results = {
    "monet": [],
    "psql": []
}

results_header = ['SF','Operation','Monet_Server_Time','Monet_Client_Time','PSQL_Server_Time','PSQL_Client_Time']

class DatabaseHandler:
    args = parser.parse_args()
    data_dir = args.data

    def __init__(self, system):
        self.system = system
        self.cur_scale = None
        #Used to store record number for Scale Factors (used in performance results metadata)
        self.record_number = []

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
        return f.read().split(";")

    # Create a new CSV file with a subset of data from an input CSV, given a scale factor (only < 1 SF allowed)
    # If the file already exists, use it. We currently don't delete the file after execution
    def scale_csv(self, input_name, scale):
        if scale > 1:
            logger.warning("Scale factor must be less than 1, using original csv (SF 1)")
            return f'{data_dir}/{input_name}.csv'
        output_file_name = f'{data_dir}/{input_name}_SF_{scale}.csv'
        if os.path.isfile(output_file_name):
            logger.debug(f"Found already existing csv {output_file_name}")
            #Register the number of records in Scale Factor
            with open(output_file_name) as f:
                self.record_number.append(sum(1 for l in f))
            return output_file_name
        input_file_name = f'{data_dir}/{input_name}.csv'
        with open(input_file_name, 'r') as input_file, open(output_file_name, 'w+') as output_file:
            try:
                input_lines = input_file.readlines()
                records_scaled = int(len(input_lines) * scale)
                self.record_number.append(records_scaled)
                output_file.writelines(input_lines[0:records_scaled])
                logger.debug(f"Number of records in scaled dataset '{input_name}': {records_scaled}")
            except IOError as msg:
                logger.warning("cutcsv() operation failed, using original csv (SF 1)")
                logger.exception(msg)
                return input_file_name
        return output_file_name

    # TODO Is this return value okay? Should we do exceptions?
    @staticmethod
    def execute_query(cur, q):
        try:
            cur.execute(q)
        except Exception as msg:
            logger.exception(msg)
            return 0
        return 1

    def benchmark(self):
        conn = self.connect_database()
        if not conn:
            logger.error(f'Could not access the database {args.database}')
            sys.exit()
        logger.info(f'{self.system.upper()}: Connected to database {args.database}')
        self.prepare_connection(conn)
        cur = conn.cursor()
        for scale in scales:
            self.cur_scale = scale
            logger.info(f'Benchmarking scale {self.cur_scale}')
            self.create_schema(cur)
            self.get_version(cur)
            self.load_data(cur)
            if args.query:
                self.run_queries(cur)
            if args.export:
                self.export_query_tables(cur)
            if args.drop:
                self.drop_schema(cur)
            elif len(scales) > 1:
                logger.info("Dropping schemas is turned off, so multiple scale factors are not allowed\nWrapping up.")
                break
        conn.close()

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
            logger.debug(f"Executing query '{q}'")
            if not self.execute_query(cur,q):
                continue

    def get_version(self,cur):
        pass

    def load_data(self, cur):
        logger.debug("Loading data")
        total_time = self.load_csv(cur)
        total_time += self.load_shp(cur)
        logger.info("All loads in %6.3f seconds" % total_time)

    def load_csv(self,cur):
        total_time = 0
        for csv_t in load_tables["csv_tables"]:
            if "scalable" in csv_t and self.cur_scale != 0:
                filename = self.scale_csv(csv_t["filename"], self.cur_scale)
            else:
                filename = f'{data_dir}/{csv_t["filename"]}.csv'
            start = timer()
            if not self.copy_into(cur,csv_t["tablename"],csv_t["columns"],filename):
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
            self.register_result(self.system, self.cur_scale, f'CSV_{csv_t["tablename"]}', server_time, client_time)
            total_time += client_time
            if "scalable" in csv_t and self.cur_scale > 0:
                logger.debug("CLIENT: Loaded %s_%s in %6.3f seconds" % (csv_t["tablename"], self.cur_scale, client_time))
            else:
                logger.debug("CLIENT: Loaded %s in %6.3f seconds" % (filename, client_time))
        return total_time

    def get_server_query_time(self,cur):
        return -1

    def copy_into(self, cur, tablename, columns, filename):
        pass

    def add_geom(self, cur, tablename, attribute):
        pass

    def add_timestamp(self, cur, tablename, attribute):
        pass

    def load_shp(self,cur):
        pass

    def run_queries(self, cur):
        geo_queries = self.open_and_split_sql_file(f"{self.system}_queries.sql")
        logger.debug("Running queries")

        total_time = 0
        query_id = 1
        for q in geo_queries:
            if not q:
                continue
            logger.debug(f"Executing query '{q}'")
            start = timer()
            if not self.execute_query(cur,q):
                self.register_result(self.system, self.cur_scale, f'Q{query_id}', -1, -1)
                continue
            client_time = timer() - start
            server_time = self.get_server_query_time(cur)
            total_time += client_time

            self.register_result(self.system, self.cur_scale, f'Q{query_id}', server_time, client_time)
            logger.debug("CLIENT: Executed query %s in %6.3f seconds" % (query_id, client_time))
            if server_time > 0:
                logger.debug("SERVER: Executed query %s in %6.3f seconds" % (query_id, server_time))
            query_id += 1
        logger.info("Executed all queries in %6.3f seconds" % total_time)
        self.register_result(self.system, self.cur_scale, "All queries", -1, total_time)

    def export_query_tables(self,cur):
        geo_export = self.open_and_split_sql_file(f"{self.system}_export.sql")
        logger.debug("Exporting data")

        for q in geo_export:
            if not q:
                continue
            # Replace placeholder %OUT% string with output directory
            q = q.replace("%OUT%", f"{os.getcwd()}/out/{self.system}")
            # If there is a Scale Factor, replace placehold %SF% with current scale factor
            if self.cur_scale > 0:
                q = q.replace("%SF%", f"_{self.cur_scale}")
            else:
                q = q.replace("%SF%", "")
            logger.debug(f"Executing export query '{q}'")
            if not self.execute_query(cur,q):
                continue

    # TODO What should we do if the queries fail?
    def drop_schema(self,cur):
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
    def __init__(self):
        super().__init__("monet")
        self.monet_version = None
        self.monet_revision = None

    def connect_database(self):
        conn = pymonetdb.connect(args.database, autocommit=True)
        return conn

    def get_version(self, cur):
        if not self.execute_query(cur,"select name, value from sys.env() where name in ('monet_version','revision');"):
            return "0"
        result = cur.fetchall()
        for r in result:
            if r[0] == 'monet_version':
                self.monet_version = r[1]
            elif r[0] == 'revision':
                self.monet_revision = r[1]

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

    def copy_into(self, cur, tablename, columns, filename):
        #TODO: Is it worth it to count the number of lines for the COPY INTO to be faster? -> Time it
        with open(filename) as f:
            record_number = sum(1 for l in f)
        query = f'COPY {record_number} OFFSET 2 RECORDS INTO {tablename} ({columns}) FROM \'{filename}\' \
        ({columns}) DELIMITERS \',\',\'\\n\',\'\"\' NULL AS \'\';'
        logger.debug(f"Executing query '{query}'")
        return self.execute_query(cur,query)

    def add_geom(self, cur, tablename, attribute):
        return self.execute_query(cur, f'UPDATE {tablename} SET geom = ST_SetSRID(ST_MakePoint({attribute}),4326);')

    def add_timestamp(self, cur, tablename, attribute):
        return self.execute_query(cur,f'UPDATE {tablename} SET t = epoch(cast({attribute} as int));')

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
            self.register_result('monet', self.cur_scale, f'SHP_{csv_t["tablename"]}', server_time, client_time)
            logger.debug("CLIENT: Loaded %s in %6.3f seconds" % (csv_t["tablename"], client_time))
            logger.debug("SERVER: Loaded %s in %6.3f seconds" % (csv_t["tablename"], server_time))
        return total_time

class PostgresHandler(DatabaseHandler):
    def __init__(self):
        super().__init__("psql")
        self.postgis_version = None
        self.psql_version = None

    def connect_database(self):
        conn = psycopg2.connect(f"dbname={args.database}")
        return conn

    def prepare_connection(self,conn):
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

    def get_version(self, cur):
        self.get_postgis_version(cur)
        self.get_psql_version(cur)

    def get_postgis_version(self,cur):
        logger.debug("Getting postgis version")
        if not self.execute_query(cur,"select PostGIS_Lib_Version();"):
            return "0"

        result = cur.fetchone()
        if result is not None:
            self.postgis_version = result[0]
        else:
            self.postgis_version = "0"

    def get_psql_version(self,cur):
        logger.debug("Getting psql version")
        if not self.execute_query(cur,"SHOW server_version;"):
            return "0"

        result = cur.fetchone()
        if result is not None:
            self.psql_version = result[0]
        else:
            self.psql_version = "0"

    # We have not implemented server-side timing retrieval for Postgres yet, return placeholder value
    def get_server_query_time(self,cur):
        return -1

    def copy_into(self, cur, tablename, columns, filename):
        query = f'COPY {tablename} ({columns}) FROM \'{filename}\' delimiter \',\' csv HEADER;'
        logger.debug(f"Executing query '{query}'")
        return self.execute_query(cur, query)

    def add_geom(self, cur, tablename, attribute):
        return self.execute_query(cur, f'UPDATE {tablename} SET geom = ST_SetSRID(ST_MakePoint({attribute}),4326);')

    def add_timestamp(self, cur, tablename, attribute):
        return self.execute_query(cur,f'UPDATE {tablename} SET t = to_timestamp({attribute});')

    def load_shp(self, cur):
        total_time = 0
        for csv_t in load_tables["shape_tables"]:
            # TODO Some shapefiles can have different SRID
            query = f'shp2pgsql -I -s 4326 \'{data_dir}/{csv_t["filename"]}\' bench_geo.{csv_t["tablename"]} | psql  -d {args.database};'
            logger.debug(f"Executing command '{query}'")
            start = timer()
            try:
                check_output(query, shell=True, stderr=STDOUT)
            except CalledProcessError as msg:
                logger.exception(msg)
            client_time = timer() - start
            total_time += client_time
            self.register_result('psql', self.cur_scale, f'SHP_{csv_t["tablename"]}', -1, client_time)
            logger.debug("CLIENT: Loaded %s in %6.3f seconds" % (csv_t["tablename"], client_time))
        return total_time

class MonetServer:
    def __init__(self,dbfarm,dbname):
        self.dbfarm = dbfarm
        self.dbname = dbname
        self.start_server()

    def start_server(self):
        if not self.farm_is_running(self.dbfarm):
            if not self.farm_up(self.dbfarm):
                self.destroy_farm(self.dbfarm)
                return
        if not self.server_up(self.dbname):
            self.stop_server(args.drop)
        else:
            logger.info(f'MonetDB: Started database {self.dbname} on {self.dbfarm}')

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
                res = check_output(cmd,shell=True).decode()
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
    def __init__(self,dbfarm,dbname):
        self.dbfarm = dbfarm
        self.dbname = dbname
        self.start_server()

    def start_server(self):
        if not self.farm_is_running(self.dbfarm):
            if not self.farm_up(self.dbfarm):
                self.destroy_farm(self.dbfarm)
                return
        if not self.db_create(self.dbname):
            self.stop_server(args.drop)
        else:
            logger.info(f'POSTGRES: Started database {self.dbname} on {self.dbfarm}')

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
                check_output(cmd,stderr=STDOUT)
                logger.info('created dbfarm %s' % dbfarm)
            except CalledProcessError as e:
                logger.error(e)
                return False
            except:
                logger.error(sys.exc_info())
                return False

        cmd = [os.path.join(prefix, 'pg_ctl'), '-D', str(dbfarm), '-l', f'{dbfarm}/logfile', 'start']
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
        cmd = [os.path.join(prefix, 'pg_ctl'), '-D', str(dbfarm), '-l', f'{dbfarm}/logfile', 'stop']
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
                res = check_output(cmd,shell=True).decode()
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
        cmd = [os.path.join(prefix, 'createdb'), str(dbname)]
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
        cmd = [os.path.join(prefix, 'dropdb'), str(dbname)]
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

def write_performance_results_csv(timestamp):
    monet_array = results['monet']
    psql_array = results['psql']
    with open(f'{os.getcwd()}/results/result_{timestamp.strftime("%d")}-{timestamp.strftime("%m")}_'
              f'{timestamp.strftime("%H")}:{timestamp.strftime("%M")}.csv', 'w', encoding='UTF8') as f:
        writer = csv.writer(f)
        writer.writerow(results_header)
        line_count = len(monet_array)
        for i in range(line_count):
            monet_result = monet_array[i]
            psql_result = psql_array[i]
            #When server_time is not available, leave the column as an empty string
            if monet_result["server_time"] < 0:
                monet_result["server_time"] = ""
            if psql_result["server_time"] < 0:
                psql_result["server_time"] = ""
            writer.writerow([f'{monet_result["scale"]}',f'{monet_result["operation"]}',
                            f'{monet_result["server_time"]}',f'{round(monet_result["client_time"],3)}',
                            f'{psql_result["server_time"]}',f'{round(psql_result["client_time"],3)}'])

def write_performance_results_metadata(timestamp, monet_handler, psql_handler):
    with open(f'{os.getcwd()}/results/result_{timestamp.strftime("%d")}-{timestamp.strftime("%m")}_'
              f'{timestamp.strftime("%H")}:{timestamp.strftime("%M")}_meta.txt', 'w', encoding='UTF8') as f:
        f.write(f"MonetDB server version {monet_handler.monet_version} (hg id {monet_handler.monet_revision})\n")
        f.write(f"pymonetdb client version {pymonetdb.__version__}\n")
        f.write(f"Postgres server version {psql_handler.psql_version} with PostGIS extension version {psql_handler.postgis_version}\n")
        f.write(f"psycopg2 client version {psycopg2.__version__}\n\n")
        f.write("Number of records for Scale Factors:\n")
        for i in range(0,len(scales)):
            f.write(f"SF {scales[i]}: {monet_handler.record_number[i]} ais_dynamic records\n")

def write_performance_results(monet_handler, psql_handler):
    now = datetime.datetime.now()
    write_performance_results_csv(now)
    write_performance_results_metadata(now, monet_handler, psql_handler)

def configure_logger():
    if args.debug:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s: %(levelname)s: %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

#As a convention, let's always use the last column as the processing results column
def compare_results_file(monet_directory,psql_directory, filename):
    row_number = 1
    logger.info(f"Comparing query result file {filename}, outputting to {os.getcwd()}/out/comparison/{filename}")
    with open(monet_directory+filename,'r') as monet_file, open(psql_directory+filename,'r') as psql_file, open(f'{os.getcwd()}/out/comparison/{filename}','w') as compare_file:
        monet_reader = csv.reader(monet_file,delimiter=',')
        psql_reader = csv.reader(psql_file, delimiter=',')
        compare_writer = csv.writer(compare_file,delimiter=',')
        row_compare = []
        for m_row, p_row in zip(monet_reader,psql_reader):
            for i in range(0,len(m_row)):
                #Date matching
                if re.match('[0-9]{4}-[0-9]{2}-[0-9]{2}',m_row[i]):
                    m_date = datetime.datetime.strptime(m_row[i],'%Y-%m-%d %H:%M:%S.%f')
                    p_date = datetime.datetime.strptime(p_row[i], '%Y-%m-%d %H:%M:%S')
                    #TODO Change timezone in database, don't compensate here
                    m_date = m_date + datetime.timedelta(hours=2, minutes=0)
                    if not m_date == p_date:
                        logger.error(f"Dates are not the same ({filename} -> Row {row_number} Column {i+1}")
                #Float matching
                elif re.match('[0-9]+\.[0-9]+',m_row[i]):
                    m_float = float(m_row[i])
                    p_float = float(p_row[i])
                    if i+1 == len(m_row):
                        row_compare.append("{:.2f}".format(m_float - p_float))
                    elif m_float != p_float:
                        logger.error(f"Floats are not the same ({filename} -> Row {row_number} Column {i+1}")
                #Geometric object matching
                elif re.match('[A-Z]+ \(-*[0-9]',m_row[i]):
                    if i+1 == len(m_row):
                        if m_row[i] == p_row[i]:
                            row_compare.append(0)
                        else:
                            row_compare.append(-1)
                    elif m_row[i] != p_row[i]:
                        logger.error(f"Geoms are not the same ({filename} -> Row {row_number} Column {i+1}")
                #Other types matching
                else:
                    if i + 1 == len(m_row):
                        if m_row[i] == p_row[i]:
                            row_compare.append(0)
                        else:
                            row_compare.append(-1)
                    else:
                        logger.error(f"Data is not the same ({filename} -> Row {row_number} Column {i+1}")
            row_number += 1
            compare_writer.writerow(row_compare)
            row_compare = []


def compare_query_results():
    logger.info("Comparing query results")
    directory_monet = f"{os.getcwd()}/out/monet/"
    directory_psql = f"{os.getcwd()}/out/psql/"

    for file in os.listdir(directory_monet):
        if os.path.isfile(directory_psql+file):
            compare_results_file(directory_monet,directory_psql,file)

if __name__ == "__main__":
    args = parser.parse_args()
    data_dir = args.data
    scales = args.scale

    #Configure logger
    logger = logging.getLogger(__name__)
    configure_logger()

    #If the user specified a dbfarm for MDB, start the server from the script
    if args.dbfarm_monet:
        m_server = MonetServer(args.dbfarm_monet,args.database)
    # If the user specified a dbfarm for PSQL, start the server from the script
    if args.dbfarm_psql:
        p_server = PostgresServer(args.dbfarm_psql, args.database)

    if args.system == 'monetdb' or args.system == 'monet' or args.system == 'mdb':
        MonetHandler().benchmark()
    elif args.system == 'postgres' or args.system == 'psql'or args.system == 'postgis':
        PostgresHandler().benchmark()
    else:
        m_handler = MonetHandler()
        p_handler = PostgresHandler()
        m_handler.benchmark()
        p_handler.benchmark()
        write_performance_results(m_handler, p_handler)
        if args.export:
            compare_query_results()

    #Stopping servers, if they were started from the script
    if args.dbfarm_monet:
        m_server.stop_server(args.drop)
    if args.dbfarm_psql:
        p_server.stop_server(args.drop)

#!/usr/bin/env python3

import os
import fileinput
import subprocess
import argparse
import pymonetdb
import psycopg2
import re
import sys

MONET_ONLY = 'monet'
PGRES_ONLY = 'postgres'

parser = argparse.ArgumentParser(
    description='Maritime Geometric Data Loading (MonetDB Geo and '
                'Postgres PostGIS)',
    epilog="This program loads the Maritime Geographic datasets benchmark "
    "from the 'Guide to Maritime Informatics' book to MonetDB and PostGIS.")

parser.add_argument('--data', type=str, required=False, default='data',
                    help='Path to the data (default \'data\')')
parser.add_argument('--system', type=str, required=False, default=None,
                    help='System to load the data (default is both)',
                    choices=[MONET_ONLY, PGRES_ONLY])
parser.add_argument('--database', type=str, required=False, default="maritime",
                    help='Name of the database to load the data (default is maritime)')
parser.add_argument('--benchmark-set-only', action='store_true', dest='bench_only',
                    help='Load only the datasets needed for geo-benchmark. '
                         'By default all Maritime data are loaded')
parser.add_argument('--scale', type=float, default=None, required=False,
                    help='Load the data after scaling them in scale')
args = parser.parse_args()

GENERIC_PATH='/path/to/data'

def main():
    data_dir = os.path.abspath(args.data)
    scripts_dir = os.path.abspath('load_scripts')

    print(f"using {data_dir} instead of {GENERIC_PATH} in SQL scripts")

    if args.system is None or args.system == MONET_ONLY:
        # get a tuple with all monet files
        monet_scripts_root = scripts_dir + '/monetdb'
        monet_scripts = tuple(fsTree(monet_scripts_root, '.sql')) 

        if args.scale:
            append_scale(monet_scripts, args.scale)
        set_pwd(monet_scripts, data_dir) 

        try:
            load_monetdb(scripts_dir)
        except Exception as e:
            print(e)
        finally:
            set_generic_pwd(monet_scripts)
            remove_scale(monet_scripts) 

    if args.system is None or args.system == PGRES_ONLY:
        # get a tuple with all postgres files
        postgres_scripts_root = scripts_dir + '/postgres'      
        postgres_scripts = tuple(fsTree(postgres_scripts_root, '.sql'))
        # append the load script for the shapefiles 
        postgres_scripts += tuple([f"{scripts_dir}/load_psql.sh"])

        if args.bench_only:
            print("ERROR: --benchmark-set-only is not supported for postgres")
            print("No data are loaded in postgres")
            return
        set_pwd(postgres_scripts, data_dir)

        try:
            load_postgres(scripts_dir)
            pass
        except Exception as e:
            print(e)
        finally:
            pass
            set_generic_pwd(postgres_scripts)

# Change all instances of /path/to/data
# with the relevant path.

def fsTree(path, ext):
    entries = os.scandir(path)
    for e in entries:
        if e.is_dir():
            yield from fsTree(e.path, ext)
        elif os.path.splitext(e.name)[1] == ext:
            yield e.path

def append_scale(filename, sf):
    scale_tag = f'_SF_{str(sf)}'
    rgx = re.compile(r'\.csv')
    with fileinput.input(filename, inplace=True) as f:
        for line in f:
            m = rgx.search(line) 
            if m:
                line = line[:m.start()] + scale_tag + line[m.start():]
            print(line, end='')

def remove_scale(filename):
    rgx = re.compile(r'_SF_[0-9\.]*csv')
    with fileinput.input(filename, inplace=True) as f:
        for line in f:
            m = rgx.search(line) 
            if m:
                line = line[:m.start()] + ".csv" + line[m.end():]
            print(line, end='')

def set_pwd(filename, data_dir):
    with fileinput.input(filename, inplace=True) as f:
        for line in f:
            if GENERIC_PATH in line:
                line = line.replace(GENERIC_PATH, data_dir)
            print(line, end='')

def set_generic_pwd(filename):
    # matches any '/User/blah/blah/blah/[' string 
    rgx = '\/.*\['
    with fileinput.input(filename, inplace=True) as f:
        for line in f:
            m = re.search(rgx, line)
            if m:
                line = line.replace(m.group(0)[:-2], GENERIC_PATH)
            print(line, end='')

def open_and_split_sql_file(filename):
    try:
        f = open(filename, "r")
    except IOError as msg:
        print(msg)
        return None
    return f.read().split(";\n")[:-1]

def execute_query(cur, q):
    try:
        #print(q)
        cur.execute(q)
    except Exception as msg:
        print(msg)
        return 0
    return 1

def load_monetdb(scripts_dir):
    conn = pymonetdb.connect(args.database, autocommit=True)
    if conn:
        print(f"Connected to MonetDB database {args.database}")
        cur = conn.cursor()

        schemas_desc = [
            {'schema': 'ais_data', 'scripts': [
                {'file': "navigation/dynamic_sar.sql", 'bench': False},
                {'file': "navigation/dynamic_aton.sql", 'bench': False},
                {'file': "navigation/static_ships.sql", 'bench': False},
                {'file': "navigation/dynamic_ships.sql", 'bench': True}
            ]},
            {'schema': 'geographic_data', 'scripts': [
                {'file': "geographic/bench_data.sql", 'bench': True},
                {'file': "geographic/general_data.sql", 'bench': False}
            ]},
            {'schema': 'environment_data', 'scripts': [
                {'file': "environmental/ocean_conditions.sql", 'bench': False},
                {'file': "environmental/weather_data.sql", 'bench': False}
            ]},
            {'schema': 'vessel_data', 'scripts': [
                {'file': "anfr_vessel_list.sql", 'bench': False},
                {'file': "aton.sql", 'bench': False},
                {'file': "eu_fishing_vessels.sql", 'bench': False},
                {'file': "mmsi_country_codes.sql", 'bench': False},
                {'file': "navigational_status.sql", 'bench': False},
                {'file': "ship_types.sql", 'bench': False}
            ]}
        ]

        # for every schema
        for sd in schemas_desc:
            queries = []
            
            # first clear schema 
            queries.append(f"DROP SCHEMA IF EXISTS {sd['schema']} CASCADE;")
            queries.append(f"CREATE SCHEMA IF NOT EXISTS {sd['schema']};")
            
            # load all the data through the sql scripts 
            for s in sd['scripts']:
                
                # excluding {'bench': 'false'} ones when --benchmark-set-only
                if args.bench_only and not s['bench']:
                    print(f"File {s['file']} is not required for the benchmark. Skiping!")
                    continue
                
                # read and split the file to individual queries
                queries += open_and_split_sql_file(f"{scripts_dir}/monetdb/{s['file']}")
                print(f"Loading file {s['file']}")
                if queries is None:
                    print(f"Could not read queries in {scripts_dir}/monetdb/")
                    return 0
                
                for q in queries:
                    execute_query(cur,q)
    else:
        print(f"Could not connect to MonetDB database {args.database}")
        return 0
    return 1

def load_postgres(scripts_dir):
    conn = psycopg2.connect(f"dbname={args.database}")
    if conn:
        print(f"Connected to Postgres database {args.database}")
        #CSV loading
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()
        #Turn on postgis
        execute_query(cur,"CREATE EXTENSION postgis;")
        load_files = ["navigation_data.sql",
                      "vessel_data.sql",
                      "environmental_data.sql"]
        for file in load_files:
            queries = open_and_split_sql_file(f"{scripts_dir}/postgres/{file}")
            print(f"Loading file {file}")
            if queries is None:
                print(f"Could not read queries in {scripts_dir}/postgres/")
                return 0
            for q in queries:
                if not execute_query(cur,q):
                    return 0
        #Shapefile loading
        print(f"Loading Shapefiles")
        try:
            subprocess.run(
                ["sh", f"{scripts_dir}/load_psql.sh", f"{args.database}"],
                check=True)
        except subprocess.CalledProcessError as msg:
            print(msg)
    else:
        print(f"Could not connect to Postgres database {args.database}")
        return 0
    return 1


if __name__ == '__main__':
    main()

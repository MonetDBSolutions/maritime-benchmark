#!/usr/bin/env python3

import os
import fileinput
import subprocess
import argparse
import pymonetdb
import psycopg2

parser = argparse.ArgumentParser(
    description='Maritime Geometric Data Loading (MonetDB Geo and Postgres PostGIS)',
    epilog='''
    This program loads the Maritime Geographic datasets benchmark from the 'Guide to Maritime Informatics' book to MonetDB and PostGIS.
    ''')
parser.add_argument('--system', type=str, help='System to load the data (default is both)', required=False, default=None)
parser.add_argument('--database', type=str, help='Name of the database to load the data (default is maritime)', required=False, default="maritime")
args = parser.parse_args()

def main():
    pwd = os.getcwd()

    data_dir = os.path.join(pwd + "/data")
    scripts_dir = os.path.join(pwd + "/load_scripts")

    print(f"replacing /path/to/data in sql scripts with actual pwd: {pwd}/data/")
    change_pwd_in_files(f"{scripts_dir}/monetdb", data_dir)
    change_pwd_in_files(f"{scripts_dir}/postgres", data_dir)
    change_pwd_in_file(f"{scripts_dir}/load_psql.sh", data_dir)

    if args.system is None or args.system =="monet":
        if load_monetdb(scripts_dir):
            print("monetdb data is loaded")
        else:
            print("monetdb data failed")
    if args.system is None or args.system =="postgres":
        if load_postgres(scripts_dir):
            print("postgres data is loaded")
        else:
            print("postgres data failed")



# Change all instances of /path/to/data
# with the relevant path.
def change_pwd_in_files(path, data_dir):
    files = os.listdir(path)
    for name in files:
        change_pwd_in_file(f"{path}/{name}", data_dir)


def change_pwd_in_file(filename, data_dir):
    TARGET = '/path/to/data'

    with fileinput.input(filename, inplace=True) as f:
        for line in f:
            if TARGET in line:
                line = line.replace(TARGET, data_dir)
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
        load_files = ["navigation_data.sql","vessel_data.sql","geographic_data.sql","environmental_data.sql"]
        for file in load_files:
            queries = open_and_split_sql_file(f"{scripts_dir}/monetdb/{file}")
            print(f"Loading file {file}")
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
        load_files = ["navigation_data.sql","vessel_data.sql","environmental_data.sql"]
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
            subprocess.run(["sh", f"{scripts_dir}/load_psql.sh", f"{args.database}"], check=True)
        except subprocess.CalledProcessError as msg:
            print(msg)
    else:
        print(f"Could not connect to Postgres database {args.database}")
        return 0
    return 1


if __name__ == '__main__':
    main()

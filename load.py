#!/usr/bin/env python3

import os
import fileinput
import subprocess
import sys


def main():
    system = None
    if len(sys.argv) > 1:
        if sys.argv[1] == "monet":
            system = "monet"
        elif sys.argv[1] == "postgres" or sys.argv[1] == "psql":
            system = "postgres"
        else:
            print(f"Unkown argument {sys.argv[1]}")
            exit(0)
    pwd = os.getcwd()

    data_dir = os.path.join(pwd + "/data")
    scripts_dir = os.path.join(pwd + "/load_scripts")

    print(f"replacing /path/to/data in sql scripts with actual pwd: {pwd}/data/")
    change_pwd_in_files(f"{scripts_dir}/monetdb", data_dir)
    change_pwd_in_files(f"{scripts_dir}/postgres", data_dir)
    change_pwd_in_file(f"{scripts_dir}/load_psql.sh", data_dir)

    if system is None or system =="monet":
        try:
            subprocess.run(["sh", f"{pwd}/load_scripts/load_mdb.sh"], check=True)
        except subprocess.CalledProcessError as msg:
            print(msg)
        else:
            print("monetdb data is loaded")
    if system is None or system =="postgres":
        try:
            subprocess.run(["sh", f"{pwd}/load_scripts/load_psql.sh"], check=True)

        except subprocess.CalledProcessError as msg:
            print(msg)
        else:
            print("postgres is loaded")



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


if __name__ == '__main__':
    main()

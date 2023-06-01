# Maritime Benchmark

## Quick start (MonetDB only)

At first you need to install [pymonetdb](github.com/MonetDB/pymonetdb)
the monetdb driver for python. Optionally you can install it in a python
[virtual environment](docs.python.org/3/library/vevn).

Download only the benchmark dataset
```sh
download-data.py --benchmark-set-only
```

If you would like to operate on scaled (down) version of the dataset you
can simply do
```sh
scale.py --scale 0.01 --target scaled-data-0.01/
```
Note that the scaling script is **not** scaling shapefiles.

To load the data on a running instance of monetdb (for instruction check
the official
[docs](monetdb.org/documentation/user-guide/tutorials/voc-tutorial/))
execute the _load.py_ script
```sh
load.py --system monet \
    --database <DB-NAME> \
    --data <DATA-DIR> \
    --scale<SCALE> \
    --benchmark-set-only 
```
Additionally, if the database is not running locally or it is using a
different port append the _hostname_ and _port_ `--hostname <HOST>
--port <PORT>`. 

To execute the benchmark
```sh
execute.py --database <DB-NAME> --no-drop --single-system monet
```
The _hostname_ and _port_ can be specified as with the _load.py_ script.

## Loading datasets into MonetDB/Postgres

This repo contains scripts to download and load datasets from the 'Guide
to Maritime Informatics' book into MonetDB and PostgreSQL. 

**NOTES:**
* In order to load geo data into MonetDB, the **geo-update** branch must
  be used. For Postgres, you must have the **PostGIS extension**
  installed.
* The default name of the database where the data is loaded into is
  **maritime**, but you can change it through *load.py* arguments.
* The **database servers must be running** before running the loading
  script: you must **start the server** and **create the database**
  yourself).

### How to download and load data

1. Use the *download_data.py* script to download all the datasets to the
   *data/* directory
```
./download_data
```

2. After getting the datasets, use the *load.py* script to load the data
   into either MonetDB or Postgres (or both)

Loading data into both MonetDB and Postgres (with default database name
"maritime"):
```
./load.py
```

Loading data only into MonetDB:
```
./load.py --system monet
```

Loading data into Postgres:
```
./load.py --system postgres
```

Loading data into a MonetDB database with the database name "test":
```
./load.py --system monet --database test
```

3. You have the data loaded in the database!
```
$ mclient -d maritime

sql> \dn
SCHEMA  ais_data
SCHEMA  environment_data
SCHEMA  geographic_data
SCHEMA  vessel_data
```

## Comparison between MonetDB and Postgres (run.py script)

**TODO**

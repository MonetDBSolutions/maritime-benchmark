# Maritime Import
## Loading datasets into MonetDB/Postgres
This repo contains scripts to download and load datasets from the 'Guide to Maritime Informatics' book into MonetDB and PostgreSQL. 

**NOTES:**
* In order to load geo data into MonetDB, the **geo-update** branch must be used. For Postgres, you must have the **PostGIS extension** installed.
* The name of the database where the data is loaded into is **maritime**.
* The **database servers must be running** before running the loading script (you must start the server and create a database name maritime yourself).

### How to download and load data
1. Use the *download_data.py* script to download all the datasets to the *data/* directory
```
./download_data
```
2. After getting the datasets, use the *load.py* script to load the data into either MonetDB or Postgres (or both)

Loading data into both MonetDB and Postgres:
```
./load.py
```
Loading data into MonetDB:
```
./load.py monet
```
Loading data into Postgres:
```
./load.py psql
./load.py postgres
```

3. You have the data loaded in the database!
```
$ mclient -d marine

sql> \dn
SCHEMA  ais_data
SCHEMA  environment_data
SCHEMA  geographic_data
SCHEMA  vessel_data
```

## Comparison between MonetDB and Postgres (run.py script)
**TODO**

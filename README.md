# Maritime Import and Queries
This repo contains both scripts to load datasets from the 'Guide to Maritime Informatics' book and maritime-related queries for MonetDB and PostgreSQL. The *load/* directory contains the SQL scripts to load the data, while the *queries/* directory contains the maritime queries.

In order to load and query data with MonetDB, the **geo-update** branch must be used.

# Loading data
## Getting the dataset
The first step is getting the dataset. You can use the **download_data.py** script to automatically download all datasets, or you can download it manually [here](https://zenodo.org/record/1167595).

**Note**: In order to use the load scripts (*load_mdb.sql*/*load_psql.sql*) you must replace the placeholder string **'/path/to/data'** with the directory where the datasets are stored in your system in the following files:
- load_psql.sh
- load/monetdb/environmental_data.sql
- load/monetdb/geographic_data.sql
- load/monetdb/navigation_data.sql
- load/monetdb/vessel_data.sql
- load/postgres/environmental_data.sql
- load/postgres/navigation_data.sql
- load/postgres/vessel_data.sql

## Loading the data into MonetDB/PostgreSQL
There are two scripts for loading data: *load_mdb.sh* for MonetDB and *load_psql.sh* for PostgreSQL.

Before executing them, you must start the corresponding database server (mserver5/pg_ctl) and create an empty database (the default name for the database in the load scripts is *maritime*, but you can choose any name).

The scripts execute the SQL scripts in the *load/* directory. You can pass the database name as an argument, if you chose a different database name from the default (*maritime*).

```
./load_mdb.sh
./load_mdb.sh marine_database
```

Alternatively, you can also load the data manually:
```
mclient -d maritime load/monetdb/navigation_data.sql
psql -d maritime -f load/postgres/navigation_data.sql
```

# Querying data
**TODO**

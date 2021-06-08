# Using the sql scripts
To use the .sql scripts in this repo, you need to replace the string '/path/to/data' in the sql scripts (environmental_data.sql, geographic_data.sql, navigation_data.sql, vessel_data.sql) to the Maritime Informatics dataset location in your system. 

You can get the [dataset here](https://zenodo.org/record/1167595).

# Using the load_db.sh script
The load_db.sh script creates a new database farm, a new database, and imports all the datasets into the running instance. You need to pass it the database farm path as the first argument. 

After loading, the database is ready to be connected, until a CTRL-C signal comes, which will stop the database farm. The default name for the database is 'maritime'.

If you run the script again, it will delete the old data and import all the datasets again. So for reconnecting to a previously loaded database, you need to start it again:

monetdbd start /path/to/dbfarm
monetdb start maritime
mclient -d maritime

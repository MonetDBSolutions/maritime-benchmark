#!/bin/bash

db_name="maritime"

if [ $# -eq 1 ]; then
	db_name=$1
fi

echo "Connecting to '$db_name' database"

sql_load=("navigation_data.sql" "vessel_data.sql" "geographic_data.sql" "environmental_data.sql")

for s in ${sql_load[@]}; do
	echo "Loading $s"
	mclient -d $db_name $PWD/load/monetdb/$s
done
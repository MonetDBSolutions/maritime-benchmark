#!/bin/bash

function user_stop {
	printf "\nStopping database\n"
	monetdbd stop $db_farm

	echo "Database farm stopped"
	exit
}

#mdb_install="/Users/bernardo/Monet/MonetDB-Oct2020"
#export PATH=$mdb_install/bin:$PATH

if [ $# -eq 0 ]; then
    echo "Please enter the db_farm to create as the first argument"
    exit 1
fi

trap 'user_stop' SIGINT
trap 'user_stop' SIGTERM

db_farm=$1
db_name="maritime"

printf "Using MonetDB version:\n"
mserver5 --version | head -n1
which mserver5

printf "\nCreating db-farm and database\n"
monetdbd stop $db_farm
monetdbd create $db_farm
monetdbd start $db_farm

monetdb create $db_name
monetdb start $db_name

printf "Database %s created and started\n\n" $db_name

printf "Creating tables and loading data\n"

printf "\nLoading navigation_data.sql\n"
mclient -d $db_name $PWD/navigation_data.sql

printf "\nLoading vessel_data.sql\n"
mclient -d $db_name $PWD/vessel_data.sql

printf "\nLoading geographic_data.sql\n"
mclient -d $db_name $PWD/geographic_data.sql

printf "\nLoading environmental_data.sql\n"
mclient -d $db_name $PWD/environmental_data.sql

printf "\nWaiting for a CTRL-C, database is open to connections:\nmclient -d %s\n" $db_name
( trap exit SIGINT ; read -r -d '' _ </dev/tty )

user_stop
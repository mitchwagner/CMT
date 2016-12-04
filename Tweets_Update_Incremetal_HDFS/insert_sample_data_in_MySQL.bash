#!/bin/bash

MYSQLFILE=$1

if [ $# -ne 1 ]; then
	echo "Enter the SQL filename."
	exit
fi

# Transfer the AQL data into the MySQL database
if mysql -u root -p'infostorage' infostorage < $MYSQLFILE; then
    echo Import of data into MySQL successful!
else
    echo Import of data into MySQL unsuccessful.
fi


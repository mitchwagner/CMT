#!/bin/bash

HOST=$1
DATABASE=$2
TABLENAME=$3

if [ $# -ne 3 ]; then
	echo "Provide host name, database name, and table name only."
	echo "For example, localhost infostorage z_703"
	echo "We assume you have a table called "Archives" in your database."
	exit
fi

# Run pt-archiver to transfer tweets to the Archives table
# Also output contents into a text file
# Note that pt-archiver MOVES tweets to the Archives table.
# That means that tweets will be deleted from the original table
cmd="sudo pt-archiver --source h=$HOST,D=$DATABASE,t=$TABLENAME \
    -u root -p'infostorage' \
    --dest h=$HOST,D=$DATABASE,t=Archives \
    --file '%Y-%m-%d-%D.%t' \
    --where "1=1" --statistics --ignore"
eval $cmd

# Check if the process failed
retcode=$?
if [ $retcode -ne 0 ]; then
    echo pt-archiver process failed.
else
    echo Successfully transferred tweets to the ArchiveDB, and also to a text file.
fi

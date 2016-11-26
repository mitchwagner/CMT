#!/bin/bash

if [ $# -ne 2 ]; then
	echo "Provide exactly two arguments - path to the file, and path to the schema."
	exit 1
fi

FILE_PATH=$1
SCHEMA_PATH=$2

# Find the name of the file without the path
CSV_FILE=$(basename $FILE_PATH)
SCHEMA=($basename $SCHEMA_PATH)i

# Also find the name of the file without csv suffix
FILE="${CSV_FILE::-4}"

# Find the directory in which the avro file would be save
AVRO_DIR_PATH=$(dirname "${FILE_PATH}")

if csv2avro \
	--schema $SCHEMA_PATH \
	--delimiter "\t" \
	--line-ending "\n" \
	--bad-rows bad.rows \
	$FILE_PATH; then
	echo "Conversion to avro successful!"
else
	echo "Conversion to avro not successful."
fi

# Regex to find the name of the table
# We will use this to put our file to the correct path on HDFS
regex='z_+[0-9]+'

if [[ $CSV_FILE =~ $regex ]]
then
	echo "Table name is ${BASH_REMATCH[0]}."
	echo "We will copy $FILE.avro to /collections/tweets-705/${BASH_REMATCH[0]}/"
else
	echo "Table name not found."
	exit $?
fi

# Copy this avro file to HDFS
if cat $AVRO_DIR_PATH/$FILE.avro | ssh cs5604f16_cmt@hadoop.dlib.vt.edu "hadoop dfs -put - /collections/tweets-705/${BASH_REMATCH[0]}/"; then
	echo "Successfully copied the avro file to HDFS on /collections/tweets-705/${BASH_REMATCH[0]}/"
else
	echo "Unsuccessful at copying the avro file to HDFS."
fi


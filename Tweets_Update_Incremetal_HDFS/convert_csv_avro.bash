#!/bin/bash

if [ $# -ne 2 ]; then
	echo "Provide exactly two arguments - path to the file, and path to the schema."
	exit 1
fi

FILE_PATH=$1
SCHEMA_PATH=$2

FILE=$(basename $FILE_PATH)
SCHEMA=($basename $SCHEMA_PATH)

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

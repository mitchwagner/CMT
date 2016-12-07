#!/bin/bash

if [ $# -ne 3 ]; then
	echo "Please enter the two Avro files on the HDFS system to be merged."
	echo "Also enter the path on the HDFS system where the files need to be merged."
	echo "For example - faiz_testing/2016-11-22-test_faiz.z_312.avro faiz_testing/2016-11-22-test_faiz.z_312.avro-copy faiz_testing"
	exit
fi 

AVRO_FILE1=$1
AVRO_FILE2=$2
HDFS_PATH=$3

# Make sure you have avro-tools-1.8.1.jar in the path
if  hadoop jar avro-tools-1.8.1.jar concat $AVRO_FILE1 $AVRO_FILE2 $HDFS_PATH/combined.avro; then
	# Remove the two Avro files that were combined
	hadoop fs -rm $AVRO_FILE1 $AVRO_FILE2
	echo "Merge of $AVRO_FILE1 and $AVRO_FILE2 successful!"
else
	echo "Merge unsuccessful."
fi


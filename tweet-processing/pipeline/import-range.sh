#! /bin/bash
# This script takes in a Pig script, an HBase table, and a range of 
# collections (inclusive) in the table over which to run the Pig script.
#
# Outer loop: multiple collections
# example: import-range load-avro-into-hbase-arg-ckey.pig ideal-tweet-10 1 10 
# example: import-range load-avro-into-hbase-cf-readable.pig ideal-tweet-10 1 10 
#
# Author: Sunshin Lee
pig_script=$1
table=$2
i=$3
j=$4
while [ $i -lt $[j+1] ]
do
  echo "=====" $i "====="
  out=$(pig -f $pig_script -p table=$table -p num=$i)
  echo "$out"
i=$[$i + 1]
done # End of outer loop


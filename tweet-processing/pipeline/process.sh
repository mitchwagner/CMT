#! /bin/bash
# This script enacts the entire pipeline on an Avro file, when
# provided with the HBase table to work with, as well as a range
# of collections to work over. At the moment, each of components
# of the pipeline work with the files under the HDFS location
# /collections/tweets-705, but each could be easily modified
# to take in an additional parameter for the location of
# the file.
#
# Because the pipeline uses Avro files, it can be used
# to either run over entire collections (as it being done
# at present) or over the result of Faiz's incremental
# update script, without issue (this would just require
# modifying the default file locations somewhat).
# 
# Outer loop: multiple collections
# Usage example: process ideal-tweet-10 1 10 
#
#@author Mitch Wagner

table=$1
i=$2
j=$3
while [ $i -lt $[j+1] ]
do
  echo "=====" $i "====="

  # Do the initial loading of the tweets
  echo "Initial Loading..."
  out=$(pig -f load-avro-initial.pig -p table=$table -p num=$i)
  echo "$out"

  # Run another Pig script to add some extra information to the tweets
  echo "Making Readable..."
  out=$(pig -f load-readable.pig -p table=$table -p num=$i)
  echo "$out"

  # Use the Stanford NLP package to lemmatize the tweets
  # and run the named entity recognition extraction on them
  echo "Stanford NLP..."
  out=$(java -jar nlp.jar $table $i)
  echo "$out"

  # Initiate the final round of cleaning, including extraction
  # of hashtags and mentions, the removal of bad words and stop
  # words, and more.
  echo "Final Cleaning..."
  out=$(pig -f clean-tweets.pig -p table=$table -p num=$i)
  echo "$out"

i=$[$i + 1]
done # End of outer loop

















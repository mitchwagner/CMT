#!/bin/bash

if [ $# -ne 1 ]; then
    echo "Provide exactly one argument that is the name of the file."
    exit 1
fi

filename=$1

# Remove all newlines
tr -d '\n' < $filename > newlines_removed.txt
mv newlines_removed.txt $filename

# Insert newlines after the keyword "twitter-serach"
CMD1="sed -r 's/twitter\-search/\\ntwitter\-search/g' $filename > tempfile1.txt"
eval $CMD1

# Insert newlines after the keyword "twitter-stream"
CMD2="sed -r 's/twitter\-stream/\\ntwitter\-stream/g' tempfile1.txt > $filename"
eval $CMD2

# Remove the interim file
rm tempfile1.txt

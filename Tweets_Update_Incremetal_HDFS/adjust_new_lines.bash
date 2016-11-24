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

# Remove the top blank line
sed -i 1d $filename

# Add headers to the file
#sed -i.bak '1i\
#archivesource\ttext\tto_user_id\tfrom_user\tid\tfrom_user_id\tiso_language_code\tsource\t/
#profile_image_url\tgeo_type\tgeo_coordinates_0\tgeo_coordinates_1\tcreated_at\ttime' $filename

# Replace missing data with 0
CMD3="awk 'BEGIN { FS = OFS = "\t" } { for(i=1; i<=NF; i++) /
if($i ~ /^ *$/) $i = 0 }; 1' $filename > tempfile2.txt"
eval CMD3

# Move interim file to the original filename
mv tempfile2.txt > $filename



#!/bin/bash

if [ $# -ne 1 ]; then
    echo "Provide exactly one argument that is the name of the file."
    exit 1
fi

filename=$1

# Remove all newlines used in UNIX
tr -d '\n' < $filename > newlines_removed.txt

# Remove all newlines used in Windows
tr -d '\r\n' < newlines_removed.txt > $filename

# Remove all newlines used in Mac
tr -d '\r' < $filename > newlines_removed.txt

# Delete interim file, and move to the original file
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
sed -i '1i\
archivesource\ttext\tto_user_id\tfrom_user\tid\tfrom_user_id\tiso_language_code\tsource\tprofile_image_url\tgeo_type\tgeo_coordinates_0\tgeo_coordinates_1\tcreated_at\ttime' $filename

# Remove non-ascii characters
sed -i 's/[\d128-\d255]//g' $filename

# Replace comma with null
sed -i 's/,//g' $filename

# Replace double quotes with null
sed -i 's/"//g' $filename

# Replace missing data with 0
awk 'BEGIN { FS = OFS = "\t" } { for(i=1; i<=NF; i++) if($i ~ /^ *$/) $i = 0 }; 1' $filename > tempfile2.txt

# Move interim file to the original filename
mv tempfile2.txt $filename

# Add csv suffix
mv $filename $filename.csv

-- This script was originally written by Sunshin Lee, modified
-- by Mitch Wagner. It takes an avro file, loads it, and generates:
-- 1) A readable language column from the abbreviated column Twitter provides
-- 2) A collection name from the collection the tweets are a part of

/* Load AVRO */
raw = LOAD '/collections/tweets-705/z_$num/' USING AvroStorage();
language_names = LOAD 'lookup_codes/language_names.csv' using PigStorage(',') AS (iso_language_code:chararray, lang_name:chararray);
collection_names = LOAD 'lookup_codes/collection_names.csv' using PigStorage(',') AS (colnum:int, col_name:chararray);

/* Lookup language code */
raw_lang = JOIN raw by LOWER(iso_language_code) LEFT, language_names by LOWER(iso_language_code) USING 'replicated';

/* Add columns: composite key, collection number, language name */
data_lang = FOREACH raw_lang GENERATE CONCAT((chararray)$num, CONCAT('-',(chararray)id)) AS ckey, $num AS colnum, lang_name;

/* Lookup collection name */
data_lang_col = JOIN data_lang by colnum LEFT, collection_names by colnum USING 'replicated';

/* Select columns to save */
data_out = FOREACH data_lang_col GENERATE ckey, lang_name, CONCAT((chararray)$num, CONCAT('. ',(chararray)col_name)) AS col_name;

/* Store data into HBase */
STORE data_out into 'hbase://$table' using org.apache.pig.backend.hadoop.hbase.HBaseStorage('clean-tweet:readable-lang, clean-tweet:readable-collection');

-- This script demonstrates how Pig can be removed to eliminate a number of
-- bad words from a body of text. At present, it is not a part of the final
-- pipeline.
-- @author: Mitch Wagner


-- Load the data from HBase
curses = LOAD 'hbase://curses' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage(
         'badwords:line', '-loadKey true') AS 
         (id:chararray, line:chararray);

-- Remove the bad words
profanities_clean = FOREACH curses GENERATE id,
    REPLACE(
    REPLACE(
    REPLACE(
    REPLACE(
    REPLACE(
    REPLACE(
    REPLACE(
    REPLACE(
    REPLACE(line, 'fuck',  '<CENSORED>'),
    'damn',  '<CENSORED>'),
    'bitch', '<CENSORED>'),
    'crap',  '<CENSORED>'),
    'piss',  '<CENSORED>'),
    'dick',  '<CENSORED>'),
    'darn',  '<CENSORED>'),
    'cunt',  '<CENSORED>'),
    'slut',  '<CENSORED>') AS line_clean;

DUMP profanities_clean;

-- NOTE: In this case, the first unique column will be considered as the
--       row key
--STORE curses INTO 'hbase://curses' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('badwords:line');

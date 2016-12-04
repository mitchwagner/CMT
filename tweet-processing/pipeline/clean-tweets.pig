-- This script executes a number of functions to clean the tweets and
-- load them into HBase. It almost certainly could have an improved
-- pipeline.
--
-- Author: Mitch Wagner

REGISTER 'process-pig.py' using jython as myfuncs;

-- Load the tweets from HBase
raw = LOAD 'hbase://$table' using org.apache.pig.backend.hadoop.hbase.HBaseStorage(
    'tweet:text, tweet:time, clean-tweet:lemmatized, clean-tweet:clean-text-solr', 
    '-loadKey=true -gte=$num- -lte=$num-9999999999999999999999999') 
    AS (ckey:chararray, text:chararray, time:chararray, lemmatized:chararray, 
        text_solr:chararray);

-- The new tweets to process are those where the clean text for the Solr team
-- has not yet been populated
unprocessed = FILTER raw by text_solr == '';

------------------------------------------------------------------------------
-- Clean and otherwise process tweets
------------------------------------------------------------------------------

-- This has several issues for extraction, but a rough approximation is
-- likely good enough. For starters, users might actually have multiple
-- URLs in a tweet, and those might follow a format not specified here
urls = FOREACH unprocessed GENERATE ckey, 
                                    REGEX_EXTRACT(text, '.*(http://\\S+).*', 1) AS url;

-- Remove all of the bad words from the original text. This constitutes
-- the extent of tweet doctoring for Solr.
solr_cleaned = FOREACH unprocessed GENERATE ckey, 
                                            (myfuncs.removeBadWords(text)) AS clean_text,
                                            time;

-- We wound up not storing redacted urls, but the following does that processesing:
-- (myfuncs.removeBadWords(REPLACE(url, 'â€¦','...'))) AS url_cleaned;

-- Get the hashtags, mentions, retweet status, and created month/year for 
-- all of the cleaned tweets
clean_info = FOREACH solr_cleaned GENERATE ckey, 
                                           myfuncs.getRetweetStatus(clean_text) AS rt, 
                                           myfuncs.getCreatedYear(time) AS created_year,
                                           myfuncs.getCreatedMonth(time) AS created_month,
                                           myfuncs.getHashtags(clean_text) AS hashtags,
                                           myfuncs.getMentions(clean_text) AS mentions;

-- CTA did not want bad words, URLs, stop words, or @ signs. They wanted 
-- text to be lemmatized
cta_cleaned1 = FOREACH unprocessed GENERATE ckey,
                                            myfuncs.removeAtSymbols(
                                            myfuncs.removeStopWords(
                                            myfuncs.removeBadWords(lemmatized))) AS t;

-- Remove URLs
-- This was INCREDIBLY annoying to write -_- the Pig Manual says that this uses
-- Java syntax, but you have to escape the slashes for Pig.
cta_cleaned2 = FOREACH cta_cleaned1 GENERATE ckey,
                                             REPLACE(t, '\\S*(http://\\S+)\\S*', '') as text;

-- CLA was the same as CTA, but wanted # signs removed as well
cla_cleaned = FOREACH cta_cleaned2 GENERATE ckey,
                                           myfuncs.removeOctothorpes(text) AS text; 

------------------------------------------------------------------------------
-- Store everything back into HBase
------------------------------------------------------------------------------

-- NOTE: first field is used as the row key
STORE urls into 'hbase://$table' using org.apache.pig.backend.hadoop.hbase.HBaseStorage('tweet:url');

STORE solr_cleaned into 'hbase://$table' using org.apache.pig.backend.hadoop.hbase.HBaseStorage('clean-tweet:clean-text-solr, tweet:time');


STORE clean_info into 'hbase://$table' using org.apache.pig.backend.hadoop.hbase.HBaseStorage('clean-tweet:rt, 
                                                                                               clean-tweet:created-year, 
                                                                                               clean-tweet:created-month, 
                                                                                               clean-tweet:hashtags, 
                                                                                               clean-tweet:mentions');

STORE cta_cleaned2 into 'hbase://$table' using org.apache.pig.backend.hadoop.hbase.HBaseStorage('clean-tweet:clean-text-cta'); 

STORE cla_cleaned into 'hbase://$table' using org.apache.pig.backend.hadoop.hbase.HBaseStorage('clean-tweet:clean-text-cla'); 

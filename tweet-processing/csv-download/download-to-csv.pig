-- We use the functions in this jar for the csv capabilities.
-- The version specified is the one necessary for compatibility
-- with the cluster.

REGISTER piggybank-0.12.0.jar

-- Load the table for our semester
-- The parameters to -gte and -lte specify the range of rows to
-- retrieve. In this instance, we are retrieving all the rows
-- in collection 3. The extra 9's clarify the range according to
-- the schema that we are using. If they were omitted, then we 
-- would retrieve rows with similar prefixes, like '312-', as well.

x = LOAD 'hbase://ideal-cs5604f16' USING 
    org.apache.pig.backend.hadoop.hbase.HBaseStorage('tweet:tweet_id, 
                                                      tweet:from_user,
                                                      tweet:from_user_id, 
                                                      clean-tweet:mentions, 
                                                      clean-tweet:rt, 
                                                      tweet:url, 
                                                      tweet:text', 
                                                     '-loadKey true -gte=3- -lte=3-99999999999999999999999') AS 
                                                     (ID:chararray, 
                                                      tweet_id:chararray,
                                                      screenname:chararray,
                                                      user_id:chararray,
                                                      mentions:chararray,
                                                      rt:chararray,
                                                      url:chararray,
                                                      text:chararray);

-- Write the csv to our HDFS home directory. The result will be a folder with the
-- specified name that 

STORE x INTO 'out.csv'
USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'YES_MULTILINE', 'UNIX'); 
/* USING CSVExcelStorage(['<delimiter>' [,{'YES_MULTILINE' | 'NO_MULTILINE'} [,{'UNIX' | 'WINDOWS' | 'UNCHANGED'}]]]); */

-- Reference:
-- https://pig.apache.org/docs/r0.11.0/api/org/apache/pig/backend/hadoop/hbase/HBaseStorage.html#gt_

-- Load the Avro file containing the collection's data. The 
-- $num parameter specifies which collection to do this for
raw = LOAD '/collections/tweets/z_$num/' USING AvroStorage();

-- Add the columns, including our composite key that consists
-- of the collection number and tweet id, the collection number
-- itself, and several empty strings to initialize a number of 
-- columns.
data = FOREACH raw GENERATE CONCAT((chararray)$num, 
                                   CONCAT('-',(chararray)id)) AS ckey, 
                                   $num as colnum, 
                                   archivesource, 
                                   text, 
                                   to_user_id, 
                                   from_user, 
                                   id, 
                                   from_user_id, 
                                   iso_language_code, 
                                   source, 
                                   profile_image_url, 
                                   geo_type, 
                                   geo_coordinates_0, 
                                   geo_coordinates_1,  
                                   created_at, 
                                   time,
                                   '' AS url,
                                   '' AS clean_text_solr, 
                                   '' AS clean_text_cla,
                                   '' AS clean_text_cta,
                                   '' AS lemmatized,
                                   '' AS readable_lang,
                                   '' AS readable_collection,
                                   '' AS rt,
                                   '' AS geo_location,
                                   '' AS created_year,
                                   '' AS created_month,
                                   '' AS hashtags,
                                   '' AS mentions,
                                   '' AS sner_people,
                                   '' AS sner_organizations,
                                   '' AS sner_locations,
                                   '' AS tweet_importance;

-- Store the augmented collection information into HBase in the appropriate columns,
-- as defined by our schema.
STORE data into 'hbase://$table' using org.apache.pig.backend.hadoop.hbase.HBaseStorage(
    'tweet:colnum, 
     tweet:archivesource, 
     tweet:text, 
     tweet:to_user_id, 
     tweet:from_user, 
     tweet:tweet_id, 
     tweet:from_user_id,  
     tweet:iso_language_code, 
     tweet:source,  
     tweet:profile_img_url,
     tweet:geo_type, 
     tweet:geo_coordinates_0,   
     tweet:geo_coordinates_1, 
     tweet:created_at, 
     tweet:time,
     tweet:url,
     clean-tweet:clean-text-solr,
     clean-tweet:clean-text-cla,
     clean-tweet:clean-text-cta,
     clean-tweet:lemmatized,
     clean-tweet:readable-lang
     clean-tweet:readable-collection,
     clean-tweet:rt,
     clean-tweet:geo-location,
     clean-tweet:created-year,
     clean-tweet:created-month,
     clean-tweet:hashtags,
     clean-tweet:mentions,
     clean-tweet:sner-people,
     clean-tweet:sner-organizations,
     clean-tweet:sner-locations,
     clean-tweet:tweet-importance');



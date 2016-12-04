package cmt.nlp;

import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

import edu.stanford.nlp.ling.CoreAnnotations.NamedEntityTagAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.LemmaAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.util.CoreMap;

import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;

/**
 * This class will be used for processing the tweets in HBase using Stanford's
 * NLPCore.
 *
 */
public class NLPTweets {

    /**
     * The main method takes in a table name and a collection number,
     * lemmatizing the tweets in that collection in the table, and
     * storing them back into that table lemmatized.
     *
     * @param args The arguments here are intended to be a String indicating
     *       the name of the HBase table to read in, and a String
     *       indicating the number of the collection to analyze.
     */
    public static void main(String[] args) throws IOException {

        String tableName;
        int collectionNumber;

        if (args.length != 2) {
            throw new IllegalArgumentException(
                "Usage: <table name> <collection number>");
        }
        else {
            tableName = args[0];
            collectionNumber = Integer.parseInt(args[1]);
            System.out.println(tableName);
            System.out.println(collectionNumber);
        }

        Configuration config = HBaseConfiguration.create();
        
        HTable table = new HTable(config, tableName);
        
        // Tells the scanner where to start and stop scanning.
        // This is actually tricky because 10 is stored before 9
        // in HBase (byte by byte comparison). The extra 9's
        // on the end are meant to cover the 64 bit byte id (and
        // then some).
        Scan scan = new Scan(Bytes.toBytes(collectionNumber + "-"), 
                             Bytes.toBytes(collectionNumber +
                                 "-99999999999999999999999")); 
 
        FilterList list = new FilterList();
        SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes("clean-tweet"),
                                                                     Bytes.toBytes("lemmatized"),
                                                                     CompareOp.EQUAL,
                                                                     Bytes.toBytes(""));
        list.addFilter(filter);
        scan.setFilter(list);

        // Create StanfordCoreNLP object properties, with POS
        // tagging (required for lemmatization), and lemmatization
        Properties props = new Properties();
        props.put("annotators", "tokenize, ssplit, pos, lemma, ner");
       
        /* 
         * The idea behind StanfordCoreNLP is that you first build up
         * the pipeline by adding Annotators, and then you take the objects
         * you wish to annotoate and pass them in to get a fully annotated
         * object. Loading the models is an expensive operation that you
         * should only do once, so batch processing is the way to go.
         */
        StanfordCoreNLP pipeline = new StanfordCoreNLP(props);
        ResultScanner scanner = table.getScanner(scan);

        // Counter for the number of records that have been read in
        int counter = 0; 
        for (Result result = scanner.next(); result != null; result = scanner.next()) {
            // Get the row key 
            byte[] rowKey = result.getRow(); 

            // Get the text from the row
            Cell c = result.getColumnLatestCell(Bytes.toBytes("tweet"),
                                                Bytes.toBytes("text"));
            byte[] data = getValue(c);

            // Annotate the document and get the sentences
            Annotation document = new Annotation(Bytes.toString(data));
            pipeline.annotate(document);
            List<CoreMap> sentences = document.get(SentencesAnnotation.class);

            List<String> lemmas = lemmatize(sentences);
            String lemmatizedText = listToString(lemmas);
            
            String orgs = getEntity(sentences, "ORGANIZATION");
            String locs = getEntity(sentences, "LOCATION");
            String people = getEntity(sentences, "PERSON");

            // Instantiate a new Put object with the row key
            Put p = new Put(rowKey);
 
            // Add method takes column family, column, value
            p.add(Bytes.toBytes("clean-tweet"), 
                  Bytes.toBytes("lemmatized"), 
                  Bytes.toBytes(lemmatizedText));

            p.add(Bytes.toBytes("clean-tweet"),
                  Bytes.toBytes("sner-people"),
                  Bytes.toBytes(people));                  

            p.add(Bytes.toBytes("clean-tweet"),
                  Bytes.toBytes("sner-organizations"),
                  Bytes.toBytes(orgs));                  

            p.add(Bytes.toBytes("clean-tweet"),
                  Bytes.toBytes("sner-locations"),
                  Bytes.toBytes(locs));                  
  
            table.put(p);

            System.out.println(Integer.toString(counter));
            counter++;
        }
        scanner.close();
        table.close();
    }

    /**
     * Take in a string and a StanfordCoreNLP pipeline, apply
     * the pipeline's processing to the string, and extract
     * the annotations from that into a list of lemmatized words
     *
     * stackoverflow.com/questions/1578062/lemmatization-java
     */
    public static List<String> lemmatize(List<CoreMap> sentences) {
        List<String> lemmas = new LinkedList<String>();

        // Iterate over all sentences found
        for (CoreMap sentence: sentences) {
            // Iterate overall the tokens in a sentence
            for(CoreLabel token: sentence.get(TokensAnnotation.class)) {
                lemmas.add(token.get(LemmaAnnotation.class));
            }
        } 
        return lemmas;
    }

    /**
     * Takes a list of sentences and an entity string to search for,
     * and return a list of all such entities in the sentence list.
     */
    public static String getEntity(List<CoreMap> sentences, 
                                         String entity) {
        StringBuilder builder = new StringBuilder();

        for (CoreMap sentence : sentences) {
           String prevLabel = null;

           for (CoreLabel word : sentence.get(TokensAnnotation.class)) {
               if (word.get(NamedEntityTagAnnotation.class).equals(entity)) {
                   
                   if (prevLabel != null) {
                        builder.append(" ");
                   }
                   builder.append(word.originalText());
                   
                   prevLabel = word.get(NamedEntityTagAnnotation.class);
               }
               else {
                   if (prevLabel != null) {
                       builder.append(";");
                       prevLabel = null;
                   }
               }
           }
       }
       return builder.toString();
    }

    /**
     * Take a List of strings and use a StringBuilder to concatentate
     * all of them into a single String, space-delimited.
     */
    public static String listToString(List<String> list) {
        StringBuilder builder = new StringBuilder();

        for (String s: list) {
            builder.append(s);
            builder.append(" ");
        } 
        return builder.toString();
    }

    /**
     * Utility method for getting the actual 
     * bytes of a Cell value from a Cell. As it turns out,
     * the data is stored at an offset in the cell's region,
     * and has a certain length (both of these need to
     * be accounted for).
     */
    public static byte[] getValue(Cell c) {
        byte[] oldBytes = c.getValueArray();
        int length = c.getValueLength();
        int offset = c.getValueOffset();

        byte[] newBytes = new byte[length]; 
        for (int i = 0; i < length; i++) {
            newBytes[i] = oldBytes[offset + i];
        }
        return newBytes;
    }
}

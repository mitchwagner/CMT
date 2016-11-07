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

// TODO: Copying from an example. Need to figure out what
// each of these is and if I really need all of them...
import edu.stanford.nlp.ling.CoreAnnotations.LemmaAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.util.CoreMap;



/**
 * This class will be used for processing
 * the tweets in HBase using Stanford's
 * NLPCore. It will be used to populate
 * various fields in the clean-tweet 
 * column family.
 *
 * TODO: The clean-tweet text stuff is
 * a multiple-step process. It might
 * make sense to just store the
 * lemmatized version and then
 * make the other 3 more intense
 * versions using pig scripts, storing
 * each of these under a separate
 * column. These things are basically
 * going to have to operate by
 * looking at what hasn't been
 * added yet
 */
public class NLPTweets {

    // TODO: In terms of configuration, this should probably take
    // a table name. The column family names, on the other hand,
    // are fine being unchanged, because our schema defines those
    public static void main(String[] args) throws IOException {
        Configuration config = HBaseConfiguration.create();
        
        HTable table = new HTable(config, "curses");
        Scan scan = new Scan();

        // Create StanfordCoreNLP object properties, with POS
        // tagging (required for lemmatization), and lemmatization
        
        Properties props = new Properties();
        props.put("annotators", "tokenize, ssplit, pos, lemma");
    
        /*
         * This class is designed to apply multiple Annotators to
         * an Annotation. The idea is that you first build up the pipeline
         * by adding Annotators, and then you take the objects you wish
         * to annotate and pass them in to get a fully annotated object.
         * 
         * StanfordCoreNLP loads a lot of models, so you probably only
         * want to do this once per execution.
         */
        StanfordCoreNLP pipeline = new StanfordCoreNLP(props);
        
        ResultScanner scanner = table.getScanner(scan);
        for (Result result = scanner.next(); result != null; result = scanner.next()) {
            // Get the row key 
            byte[] rowKey = result.getRow(); 

            // TODO: Get the right row from the result
            // Run stanford NLP lemmatizer on the row
            // save the result back
            Cell c = result.getColumnLatestCell(Bytes.toBytes("badwords"),
                                                Bytes.toBytes("line"));
            byte[] data = getValue(c);
            List<String> lemmas = lemmatize(Bytes.toString(data), pipeline);
            printList(lemmas);
	    System.out.print("\n");
            System.out.println(Bytes.toString(data)); 
	    System.out.print("\n");
            
            // Instantiate a new Put object with the row key
            Put p = new Put(rowKey);
 
            // Add method takes column family, column, value
            p.add(Bytes.toBytes("badwords"), 
                  Bytes.toBytes("insertednice"), 
                  Bytes.toBytes("a"));
  
            table.put(p);

            //System.out.println("Found row: " + result);
        }
        scanner.close();
        table.close();
    
    }

    // stackoverflow.com/questions/1578062/lemmatization-java

    public static List<String> lemmatize(String text, StanfordCoreNLP pipeline) {
        List<String> lemmas = new LinkedList<String>();

        Annotation document = new Annotation(text);
        pipeline.annotate(document);

        List<CoreMap> sentences = document.get(SentencesAnnotation.class);
         
        // Iterate over all sentences found
        for (CoreMap sentence: sentences) {
            // Iterate overall the tokens in a sentence
            for(CoreLabel token: sentence.get(TokensAnnotation.class)) {
                lemmas.add(token.get(LemmaAnnotation.class));
            }
        } 
        return lemmas;
    }

    public static void printList(List<String> list) {
        for (String s: list) {
            System.out.print(s + " "); 
        } 
    }

    /**
     * Utility method for getting the actual 
     * bytes of a Cell value from a Cell
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

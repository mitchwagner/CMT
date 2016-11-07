package cmt;

// I will need to import:
// HBase
// Google API

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hbase.util.Bytes;

/**
 * This class is meant to provide a wrapper
 * for static functions that allow you to 
 * query Google's API.
 */
public class FetchGeo {

    public static void main(String[] args) {
        HTable table = null;
        try {


        }
        finally {
            if (htable != null) {
                htable.close();
            } 
        }
    }
}


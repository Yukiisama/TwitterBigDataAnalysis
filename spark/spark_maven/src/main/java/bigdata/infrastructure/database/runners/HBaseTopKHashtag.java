package bigdata.infrastructure.database.runners;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.spark.SparkContext;
import org.apache.hadoop.hbase.util.Bytes;

import bigdata.data.User;
import bigdata.infrastructure.database.SparkToDatabase;
import scala.Tuple2;

import static bigdata.TPSpark.logger;
// https://github.com/IvanFernandez/hbase-spark-playground/blob/master/src/main/java/spark/examples/SparkToHBase.java
public class HBaseTopKHashtag extends SparkToDatabase {
	private static int pos = 0;
    private static final String tableName = "topKHashtag";
    private static final String[] familyName = new String[]{
        "global"
    };


    private static HBaseTopKHashtag __instance__;

    public HBaseTopKHashtag() {
        super(tableName, familyName, new String[]{"hashtag", "count"});
    }
    public HBaseTopKHashtag(String tablename) {
        super(tablename, familyName, new String[]{"hashtag", "count"});
    }

    public static HBaseTopKHashtag INSTANCE(String tableName) {
        if (__instance__ == null) {
            __instance__ = new HBaseTopKHashtag(tableName);
        }

        return __instance__;
    }

    @Override
    public void readTable(Configuration conf, String mode) {
        SparkContext sc = new SparkContext(mode, "get HBase Data For TopKHashtag");

        try {
            // TODO

        } catch (Exception e) {

            logger.fatal("Unable to read Table HBaseUser using mode: [" + mode + "] and with configuration: "
                    + conf.toString());

        } finally {
            sc.stop();
        }

    }
    
    public void resetPos() {
        pos = 0;
    }

    public void writeTable(Tuple2<String, Integer> data) {
        Put value = new Put(Bytes.toBytes(Integer.toString(pos))); 
        value.add(
                Bytes.toBytes("global"), // Family Name
                Bytes.toBytes("hashtag"),  // column qualifier
                Bytes.toBytes(data._1.toString())  // Value
            );
        value.add(
                Bytes.toBytes("global"), // Family Name
                Bytes.toBytes("count"),  // column qualifier
                Bytes.toBytes(data._2.toString())  // Value
            );
        
        try {
            if(value == null || data == null || table == null) {
                logger.error("Unable to put the following value: " + value + "inside the database table.");

                return;
            }
            table.put(value);
            pos++;
        } catch (IllegalArgumentException | IOException e) {
            e.printStackTrace();

            logger.error("Unable to put the following value: " + value + "inside the database table.");
        }


    }


    @Override
    public void writeTable(Put value) {

        try {
            table.put(value);
        } catch (IllegalArgumentException | IOException e) {
            e.printStackTrace();
        }


    }
    



}

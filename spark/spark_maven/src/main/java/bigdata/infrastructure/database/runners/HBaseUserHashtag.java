package bigdata.infrastructure.database.runners;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkContext;

import bigdata.data.User;
import bigdata.infrastructure.database.SparkToDatabase;
import scala.Tuple2;
// https://github.com/IvanFernandez/hbase-spark-playground/blob/master/src/main/java/spark/examples/SparkToHBase.java
public class HBaseUserHashtag extends SparkToDatabase {
    private static int pos = 0;
    private static final String tableName = "usersHashtagsList";
    private static final String[] familyName = new String[]{
        "global"
    };


    private static HBaseUserHashtag __instance__;
    public HBaseUserHashtag() {
        super(tableName, familyName, new String[]{"username", "hashtags"} );
    }

    public static HBaseUserHashtag INSTANCE() {
        if (__instance__ == null) {
            __instance__ = new HBaseUserHashtag();
        }

        return __instance__;
    }

    @Override
    public void readTable(Configuration conf, String mode) {
        SparkContext sc = new SparkContext(mode, "get HBase Data For Users Hashtags");

        try {
            // TODO
            
        } catch (Exception e) {

            // logger.fatal("Unable to read Table HBaseUserHashtag using mode: [" + mode + "] and with configuration: "
            //         + conf.toString());

        } finally {
            sc.stop();
        }

    }


    public void writeTable(Tuple2<String, User> tuple) {
    
        Put value = new Put(Bytes.toBytes(Integer.toString(pos)));

        value.add(
                Bytes.toBytes("global"), // Family Name
                Bytes.toBytes("username"),  // column qualifier
                Bytes.toBytes(tuple._1.toString())  // Value
            );

        value.add(
            Bytes.toBytes("global"), // Family Name
            Bytes.toBytes("hashtags"),  // column qualifier
            Bytes.toBytes(tuple._2()._hashtags().keySet().toString())  // Value
        );

        try {
            if(value == null || tuple == null || table == null) {
                return;
            }
            table.put(value);
            pos++;
        } catch (IllegalArgumentException | IOException e) {
            e.printStackTrace();

            // logger.error("Unable to put the following value: " + value + "inside the database table. Skipping.");
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

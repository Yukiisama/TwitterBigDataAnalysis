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
public class HBaseInfluenceurs extends SparkToDatabase {
    private static int pos = 0;
    private static final String tableName = "influenceurs";
    private static final String[] familyName = new String[]{
        "global"
    };


    private static HBaseInfluenceurs __instance__;
    public HBaseInfluenceurs() {
        super(tableName, familyName, new String[]{"influenceurs", "nb_messages"} );
    }

    public static HBaseInfluenceurs INSTANCE() {
        if (__instance__ == null) {
            __instance__ = new HBaseInfluenceurs();
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


    public void writeTable(Tuple2<String, Integer> tuple) {
    
        Put value = new Put(Bytes.toBytes(Integer.toString(pos)));

        value.add(
                Bytes.toBytes("global"), // Family Name
                Bytes.toBytes("influenceurs"),  // column qualifier
                Bytes.toBytes(tuple._1.toString())  // Value
            );

        value.add(
            Bytes.toBytes("global"), // Family Name
            Bytes.toBytes("nb_messages"),  // column qualifier
            Bytes.toBytes(Integer.toString(tuple._2()))  // Value
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

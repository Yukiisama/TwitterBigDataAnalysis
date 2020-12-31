package bigdata.infrastructure.database.runners;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.spark.SparkContext;

import bigdata.data.User;
import bigdata.infrastructure.database.SparkToDatabase;

// https://github.com/IvanFernandez/hbase-spark-playground/blob/master/src/main/java/spark/examples/SparkToHBase.java
public class HBaseUser extends SparkToDatabase {

    private static final String tableName = "users";
    private static final String[] familyName = new String[]{
        "global",
        "influence",
        "misc"
    };


    private static HBaseUser __instance__;

    public HBaseUser() {
        super(tableName, familyName, User.getColumnsName());
    }

    public static HBaseUser INSTANCE() {
        if (__instance__ == null) {
            __instance__ = new HBaseUser();


        }

        return __instance__;
    }

    @Override
    public void readTable(Configuration conf, String mode) {
        SparkContext sc = new SparkContext(mode, "get HBase Data For Users");

        try {

            // JavaRDD<Tuple2<ImmutableBytesWritable, Result>> hBaseRDD = sc.
            // .newAPIHadoopRDD(
            // conf,
            // TableInputFormat.class,
            // org.apache.hadoop.hbase.io.ImmutableBytesWritable.class,
            // org.apache.hadoop.hbase.client.Result.class);

            // long count = hBaseRDD.count();
            // System.out.println("Number of register in hbase table: " + count);

        } catch (Exception e) {

            System.err.println("Unable to read Table HBaseUser using mode: [" + mode + "] and with configuration: "
                    + conf.toString());

        } finally {
            sc.stop();
        }

    }

    @Override
    public void readTableJava(Configuration conf, String mode) {

    }

    @Override
    public void writeTable(User user) {
        Put value = user.getContent();

        try {
            table.put(value);
        } catch (IllegalArgumentException | IOException e) {
            e.printStackTrace();
        }


    }


    public void writeTable(Put value) {

        try {
            table.put(value);
        } catch (IllegalArgumentException | IOException e) {
            e.printStackTrace();
        }


    }

    @Override
    public void writeTableJava(User user) {

    }


}

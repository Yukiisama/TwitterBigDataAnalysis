package bigdata.infrastructure.database.runners;

import org.apache.hadoop.conf.Configuration;

import bigdata.infrastructure.database.SparkToDatabase;
public class HBaseUser extends SparkToDatabase  {

    private static final String tableName = "users";
    private static HBaseUser __instance__;

    
    public HBaseUser () {
        super(tableName);
    }

    public static HBaseUser INSTANCE() {
        if (__instance__ == null) {
            __instance__ = new HBaseUser();
        }

        return __instance__;
    }

    @Override
    public void readTable(Configuration conf, String mode) {

    }


    @Override
    public void readTableJava(Configuration conf, String mode) {

    }

    
    @Override
    public void writeTable(Configuration conf, String mode) {

    }

    
    @Override
    public void writeTableJava(Configuration conf, String mode) {

    }

}

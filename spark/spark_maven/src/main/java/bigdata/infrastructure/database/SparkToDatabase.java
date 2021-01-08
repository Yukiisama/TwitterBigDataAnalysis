package bigdata.infrastructure.database;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;

import bigdata.TPSpark;
import bigdata.data.User;

public abstract class SparkToDatabase extends Configured implements Tool {


    private final String[] familyNames;
    private static final byte[] TABLE_PREFIX = Bytes.toBytes("ape-jma_");

    public static Configuration config = HBaseConfiguration.create();
    private Connection connection;
    private byte[] TABLE_NAME;
    private String[] COLUMN_NAME;


    protected Table table;

    public Job APIJobConfiguration;

    static {
        config.addResource(new Path("/espace/Auber_PLE-203/hbase/conf/hbase-site.xml"));
        config.addResource(new Path("/espace/Auber_PLE-203/hbase/conf/hbase-env.xml"));
        config.addResource(new Path("/espace/Auber_PLE-203/hadoop/etc/hadoop/core-site.xml"));
        config.set("mapreduce.outputformat.class", "org.apache.hadoop.hbase.mapreduce.TableOutputFormat");

        config.set(TableInputFormat.INPUT_TABLE, "dataset");
    }

    public SparkToDatabase (String tableName, String[] family, String[] columnsName) {
        this.familyNames = family;
        try{
            
            
            COLUMN_NAME = columnsName;
            TABLE_NAME = com.google.common.primitives.Bytes.concat(TABLE_PREFIX, Bytes.toBytes(tableName));

            this.config.set(TableOutputFormat.OUTPUT_TABLE, Bytes.toString(TABLE_NAME));

            APIJobConfiguration = Job.getInstance(config);
            APIJobConfiguration.setOutputFormatClass(TableOutputFormat.class);

            // config.set(TableInputFormat.INPUT_TABLE, Bytes.toString(TABLE_NAME));
            

  /**
   * Get an RDD for a given Hadoop file with an arbitrary new API InputFormat
   * and extra configuration options to pass to the input format.
   *
   * '''Note:''' Because Hadoop's RecordReader class re-uses the same Writable object for each
   * record, directly caching the returned RDD will create many references to the same object.
   * If you plan to directly cache Hadoop writable objects, you should first copy them using
   * a `map` function.
   */

            ToolRunner.run(HBaseConfiguration.create(), this, new String[0]);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }





    public int run(String[] args) throws Exception {
        this.connection = ConnectionFactory.createConnection(getConf());
        createTable(this.connection);
        table = this.connection.getTable(TableName.valueOf(TABLE_NAME));

        // Init rows
        for(String key : COLUMN_NAME){
            Put put = new Put(Bytes.toBytes(key));
            table.put(put);
        }

        return 0;
    }



    public void createOrOverwrite(Admin admin, HTableDescriptor table) throws Exception {
        if (admin.tableExists(table.getTableName())) {
            if(admin.isTableEnabled(table.getTableName()))
                admin.disableTable(table.getTableName());
            admin.deleteTable(table.getTableName());
        }
        
        admin.createTable(table);
        if(admin.isTableDisabled(table.getTableName()))
        admin.enableTable(table.getTableName());
    }

    public void createTable(Connection connect) throws Exception {
        try {
            final Admin admin = connect.getAdmin(); 
            HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(TABLE_NAME));


            for(String familyName : this.familyNames){
                HColumnDescriptor famLoc = new HColumnDescriptor(familyName);
                
                tableDescriptor.addFamily(famLoc);
            }

            //createOrOverwrite(admin, tableDescriptor);


            admin.close();
        } catch (Exception e) {
            e.printStackTrace();
            
            throw new IOException("Unable to create Table: " + TABLE_NAME.toString());
        }
    }



    public abstract void readTable(Configuration conf, String mode);

    public abstract void readTableJava(Configuration conf, String mode);
    
    public abstract void writeTable(User user);
    
    public abstract void writeTableJava(User user);
}

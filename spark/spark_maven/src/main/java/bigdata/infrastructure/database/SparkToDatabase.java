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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public abstract class SparkToDatabase extends Configured implements Tool {

    protected Configuration config = null;
    private String tableName;


    private static final byte[] FAMILY = Bytes.toBytes("AAA");
    private static final byte[] ROW    = Bytes.toBytes("BBB");
    private static final byte[] TABLE_NAME = Bytes.toBytes("CCC");
    public SparkToDatabase (String tableName, String[] args) {
        this.tableName = tableName;
        try{

            // /espace/Auber_PLE-203/hbase/conf/hbase-site.xml
            Configuration config = HBaseConfiguration.create();
            config.addResource(new Path("/espace/Auber_PLE-203/hbase/conf/hbase-site.xml"));
            config.addResource(new Path("/espace/Auber_PLE-203/hbase/conf/core-site.xml"));
            // config.clear();
            // config.set("hbase.zookeeper.quorum", "10.0.203.4");
            // config.set("hbase.zookeeper.property.clientPort", "2181");
            // config.set("hbase.master.info.port", "60000");
             


            ToolRunner.run(HBaseConfiguration.create(), this, args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public SparkToDatabase (String tableName) {
        this(tableName, new String[0]);
    }



    public int run(String[] args) throws Exception {
        Connection connection = ConnectionFactory.createConnection(getConf());
        createTable(connection);
        Table table = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes("KEY"));
        table.put(put);
        return 0;
    }



    public void createOrOverwrite(Admin admin, HTableDescriptor table) throws Exception {
        if (admin.tableExists(table.getTableName())) {
            admin.disableTable(table.getTableName());
            admin.deleteTable(table.getTableName());
        }
        admin.createTable(table);
    }

    public void createTable(Connection connect) throws Exception {
        try {
            final Admin admin = connect.getAdmin(); 
            HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
            HColumnDescriptor famLoc = new HColumnDescriptor(FAMILY); 
            //famLoc.set...
            tableDescriptor.addFamily(famLoc);
            createOrOverwrite(admin, tableDescriptor);
            admin.close();
        } catch (Exception e) {
            e.printStackTrace();
            throw new IOException("Unable to create Table: " + this.tableName);
        }
    }



    public abstract void readTable(Configuration conf, String mode);

    public abstract void readTableJava(Configuration conf, String mode);
    
    public abstract void writeTable(Configuration conf, String mode);
    
    public abstract void writeTableJava(Configuration conf, String mode);
}

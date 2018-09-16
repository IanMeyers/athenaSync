package il.amazon.bigdata;

import java.sql.*;
import org.apache.hadoop.hive.metastore.MetaStoreEventListener;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.events.AddPartitionEvent;
import org.apache.hadoop.hive.metastore.events.CreateTableEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;

import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import com.amazonaws.athena.jdbc.AthenaDriver;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;

public class athenaSync extends MetaStoreEventListener  {

    private static final Logger LOG = LoggerFactory.getLogger(athenaSync.class);
    private static  Configuration config = null;

    public athenaSync(final Configuration conf) {
        super(conf);
        config = conf;
        Iterator<Map.Entry<String, String>> iterator = conf.iterator();

    }

    // Trying to reconstruct the create table statement from the Table object requires too much work
    // so I'm checking if the table is external and it's location is on S3, if so I connect via JDBC to Hive
    // and issue a "show create table" statement on table which is later sent to Athena.
    public void onCreateTable(CreateTableEvent tableEvent)
    {
        Table tbl = tableEvent.getTable();
        if( tbl.getTableType().equals("EXTERNAL_TABLE") && tbl.getSd().getLocation().startsWith("s3"))
        {
            String ddl = "";
            try {
            // Assuming HiveServer2 is running on the same machine as the metastore
            Connection conn = DriverManager.getConnection("jdbc:hive2://localhost:10000/default");
            Statement stmt = conn.createStatement();
            ResultSet res = stmt.executeQuery("show create table " + tbl.getDbName() + "." + tbl.getTableName());

            while(res.next())
            {
               ddl += " " + res.getString(1) ;
            }

            stmt.close();
            conn.close();
            }
            catch(Exception e)
            {
                LOG.error("Unable to replicate to Athena:"   + e.getMessage());
            }

            LOG.debug("DDL Statement:" + ddl);

            sendToAthena(ddl);
        }
        else
        {
            LOG.debug("#### Table should not be replicated to Athena. Type: " + tbl.getTableType() + ", Location:" + tbl.getSd().getLocation() );
        }
   }


    public void onAddPartition(AddPartitionEvent partitionEvent)
            throws MetaException {
        if (partitionEvent.getStatus()) {
            Table table = partitionEvent.getTable();
            String fqtn = table.getDbName() +"." + table.getTableName();

            if (fqtn != null && !fqtn.equals("")) {
                Iterator<Partition> iterator = partitionEvent.getPartitionIterator();
                while(iterator.hasNext()) {
                    Partition partition = iterator.next();
                    String partitionSpec = "";
                    for (int i = 0; i < table.getPartitionKeysSize(); ++i) {
                        if(table.getPartitionKeys().get(i).getType().equals("string"))
                            partitionSpec += table.getPartitionKeys().get(i).getName() + "='" + partition.getValues().get(i) + "',";
                        else
                            partitionSpec += table.getPartitionKeys().get(i).getName() + "=" + partition.getValues().get(i) + ",";
                    }
                    partitionSpec = partitionSpec.substring(0,partitionSpec.length() - 1);
                    sendToAthena("alter table " + fqtn + " add partition(" + partitionSpec + ") location '" + partition.getSd().getLocation() + "'");
                }

            }
        }
    }

    private void sendToAthena(String query)
    {
        try {
            Properties info = new Properties();
            info.put("s3_staging_dir", config.get("athena.s3.staging.dir"));
            info.put("log_path","/tmp/jdbc.log");
            info.put("log_level","ERROR");

//                        info.put("user", config.get("athena.user.name"));
//                        info.put("password", config.get("athena.user.password"));
            info.put("aws_credentials_provider_class", com.amazonaws.auth.InstanceProfileCredentialsProvider.class.getName());
            Connection conn = DriverManager.getConnection( config.get("athena.jdbc.url"), info);
            Statement stmt = conn.createStatement();
            stmt.execute(query);
            stmt.close();
            conn.close();
        }
        catch (Exception e)
        {
            LOG.error("*** ERROR: " + e.getMessage());
        }
    }
}

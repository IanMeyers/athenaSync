package il.amazon.bigdata;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.MetaStoreEventListener;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.events.AddPartitionEvent;
import org.apache.hadoop.hive.metastore.events.CreateTableEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveGlueCatalogSyncAgent extends MetaStoreEventListener {
	private static final Logger LOG = LoggerFactory.getLogger(HiveGlueCatalogSyncAgent.class);
	private static Configuration config = null;
	private Statement stmt;

	private final class ConnectionCloser implements Runnable {
		private Connection c;
		private Logger LOG;

		protected ConnectionCloser(Connection c, Logger LOG) {
			this.c = c;
			this.LOG = LOG;
		}

		public void run() {
			try {
				c.close();
			} catch (SQLException e) {
				LOG.error(e.getMessage());
			}
		}
	};

	public HiveGlueCatalogSyncAgent(final Configuration conf) throws Exception {
		super(conf);
		config = conf;

		Properties info = new Properties();
		info.put("s3_staging_dir", config.get("athena.s3.staging.dir"));
		info.put("log_path", "/tmp/jdbc.log");
		info.put("log_level", "ERROR");

		// info.put("user", config.get("athena.user.name"));
		// info.put("password", config.get("athena.user.password"));
		info.put("aws_credentials_provider_class",
				com.amazonaws.auth.InstanceProfileCredentialsProvider.class.getName());
		Connection conn = DriverManager.getConnection(config.get("athena.jdbc.url"), info);
		stmt = conn.createStatement();

		// add a shutdown hook to close the connection
		Runtime.getRuntime().addShutdownHook(new Thread(new ConnectionCloser(conn, LOG), "Shutdown-thread"));
	}

	// Trying to reconstruct the create table statement from the Table object
	// requires too much work
	// so I'm checking if the table is external and it's location is on S3, if so I
	// connect via JDBC to Hive
	// and issue a "show create table" statement on table which is later sent to
	// Athena.
	public void onCreateTable(CreateTableEvent tableEvent) {
		Table tbl = tableEvent.getTable();
		if (tbl.getTableType().equals("EXTERNAL_TABLE") && tbl.getSd().getLocation().startsWith("s3")) {
			String ddl = "";
			try {
				// Get the hive metastore URL from properties
				String hiveMetastoreURL = System.getProperty("hive-metastore-url");

				if (hiveMetastoreURL == null) {
					hiveMetastoreURL = "jdbc:hive2://localhost:10000/default";
				}
				Connection conn = DriverManager.getConnection(hiveMetastoreURL);
				Statement stmt = conn.createStatement();
				ResultSet res = stmt.executeQuery("show create table " + tbl.getDbName() + "." + tbl.getTableName());

				while (res.next()) {
					ddl += " " + res.getString(1);
				}

				stmt.close();
				conn.close();
			} catch (Exception e) {
				LOG.error("Unable to replicate to AWS Glue Catalog:" + e.getMessage());
			}

			LOG.debug("DDL Statement:" + ddl);
			sendToAthena(ddl);
		} else {
			LOG.debug(String.format("Ignoring Table %s as it should not be replicated to Athena. Type: %s",
					tbl.getTableType(), tbl.getSd().getLocation()));
		}
	}

	public void onAddPartition(AddPartitionEvent partitionEvent) throws MetaException {
		if (partitionEvent.getStatus()) {
			Table table = partitionEvent.getTable();
			String fqtn = table.getDbName() + "." + table.getTableName();

			if (fqtn != null && !fqtn.equals("")) {
				Iterator<Partition> iterator = partitionEvent.getPartitionIterator();
				while (iterator.hasNext()) {
					Partition partition = iterator.next();
					String partitionSpec = "";
					for (int i = 0; i < table.getPartitionKeysSize(); ++i) {
						if (table.getPartitionKeys().get(i).getType().equals("string"))
							partitionSpec += table.getPartitionKeys().get(i).getName() + "='"
									+ partition.getValues().get(i) + "',";
						else
							partitionSpec += table.getPartitionKeys().get(i).getName() + "="
									+ partition.getValues().get(i) + ",";
					}
					partitionSpec = partitionSpec.substring(0, partitionSpec.length() - 1);
					sendToAthena("alter table " + fqtn + " add partition(" + partitionSpec + ") location '"
							+ partition.getSd().getLocation() + "'");
				}

			}
		}
	}

	private void sendToAthena(String query) {
		try {
			stmt.execute(query);
			stmt.close();
		} catch (Exception e) {
			LOG.error("*** ERROR: " + e.getMessage());
		}
	}
}

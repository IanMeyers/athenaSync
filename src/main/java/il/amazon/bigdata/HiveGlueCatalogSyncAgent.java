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
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.events.AddPartitionEvent;
import org.apache.hadoop.hive.metastore.events.CreateTableEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.athena.jdbc.shaded.org.apache.commons.lang3.StringUtils;

public class HiveGlueCatalogSyncAgent extends MetaStoreEventListener {
	private static final Logger LOG = LoggerFactory.getLogger(HiveGlueCatalogSyncAgent.class);
	private static Configuration config = null;
	private Statement athenaStmt;
	private Connection hiveMetastoreConnection;
	private final String EXTERNAL_TABLE_TYPE = "EXTERNAL_TABLE";

	private final class ConnectionCloser implements Runnable {
		private Connection[] connections;
		private Logger LOG;

		protected ConnectionCloser(Connection[] connections, Logger LOG) {
			this.connections = connections;
			this.LOG = LOG;
		}

		public void run() {
			for (Connection c : this.connections) {
				try {
					c.close();
				} catch (SQLException e) {
					LOG.error(e.getMessage());
				}
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
		String athenaURL = config.get("athena.jdbc.url");
		Connection athenaConnection = DriverManager.getConnection(athenaURL, info);
		athenaStmt = athenaConnection.createStatement();

		// Get the hive metastore URL from properties
		String hiveMetastoreURL = config.get("hive-metastore-url");

		if (hiveMetastoreURL == null) {
			hiveMetastoreURL = "jdbc:hive2://localhost:10000/default";
		}
		hiveMetastoreConnection = DriverManager.getConnection(hiveMetastoreURL);

		// add a shutdown hook to close the connections
		Connection[] connections = new Connection[2];
		connections[0] = athenaConnection;
		connections[1] = hiveMetastoreConnection;
		Runtime.getRuntime().addShutdownHook(new Thread(new ConnectionCloser(connections, LOG), "Shutdown-thread"));

		LOG.info(String.format("%s online, connected to %s and %s", this.getClass().getCanonicalName(),
				hiveMetastoreURL, athenaURL));
	}

	// Trying to reconstruct the create table statement from the Table object
	// requires too much work so I'm checking if the table is external and it's
	// location is on S3, if so I connect via JDBC to Hive and issue a "show create
	// table" statement on table which is later sent to Catalog via Athena
	// connection.
	public void onCreateTable(CreateTableEvent tableEvent) {
		Table table = tableEvent.getTable();
		if (table.getTableType().equals(EXTERNAL_TABLE_TYPE) && table.getSd().getLocation().startsWith("s3")) {
			String ddl = "";
			try {
				Statement stmt = hiveMetastoreConnection.createStatement();
				ResultSet res = stmt
						.executeQuery("show create table " + table.getDbName() + "." + table.getTableName());

				while (res.next()) {
					ddl += " " + res.getString(1);
				}

				stmt.close();
			} catch (Exception e) {
				LOG.error("Unable to replicate to AWS Glue Catalog:" + e.getMessage());
			}

			LOG.debug(ddl);
			sendToAthena(ddl);
		} else {
			LOG.debug(String.format("Ignoring Table %s as it should not be replicated to AWS Glue Catalog. Type: %s",
					table.getTableType(), table.getSd().getLocation()));
		}
	}

	public void onAddPartition(AddPartitionEvent partitionEvent) throws MetaException {
		if (partitionEvent.getStatus()) {
			Table table = partitionEvent.getTable();

			if (table.getTableType().equals(EXTERNAL_TABLE_TYPE) && table.getSd().getLocation().startsWith("s3")) {
				String fqtn = table.getDbName() + "." + table.getTableName();

				if (fqtn != null && !fqtn.equals("")) {
					Iterator<Partition> iterator = partitionEvent.getPartitionIterator();

					while (iterator.hasNext()) {
						Partition partition = iterator.next();
						String partitionSpec = "";

						for (int i = 0; i < table.getPartitionKeysSize(); ++i) {
							FieldSchema p = table.getPartitionKeys().get(i);

							String specAppend;
							if (p.getType().equals("string")) {
								// add quotes to appended value
								specAppend = "'" + partition.getValues().get(i) + "'";
							} else {
								// don't quote the appended value
								specAppend = partition.getValues().get(i);
							}

							partitionSpec += p.getName() + "=" + specAppend + ",";
						}
						partitionSpec = StringUtils.stripEnd(partitionSpec, ",");

						String addPartitionDDL = "alter table " + fqtn + " add if not exists partition(" + partitionSpec
								+ ") location '" + partition.getSd().getLocation() + "'";
						LOG.debug(addPartitionDDL);
						sendToAthena(addPartitionDDL);
					}
				}
			} else {
				LOG.debug(String.format("Ignoring Add Partition Event for Table %s as it is not stored on S3",
						table.getTableName()));
			}
		}
	}

	private void sendToAthena(String query) {
		try {
			athenaStmt.execute(query);
			athenaStmt.close();
		} catch (Exception e) {
			LOG.error("*** ERROR: " + e.getMessage());
		}
	}
}

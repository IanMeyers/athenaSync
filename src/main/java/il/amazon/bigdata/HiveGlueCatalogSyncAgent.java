package il.amazon.bigdata;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.sql.SQLTimeoutException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.LinkedList;
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
	private Configuration config = null;
	private Properties info;
	private String athenaURL;
	private Connection athenaConnection;
	private Connection hiveMetastoreConnection;
	private final String EXTERNAL_TABLE_TYPE = "EXTERNAL_TABLE";
	private final String ATHENA_JDBC_URL = "athena.jdbc.url";
	private LinkedList<String> ddlQueue = new LinkedList<>();
	private int noEventSleepDuration;
	private int reconnectSleepDuration;

	/**
	 * Private class to cleanup the sync agent - to be used in a Runtime shutdown
	 * hook
	 * 
	 * @author meyersi
	 *
	 */
	private final class SyncAgentShutdownRoutine implements Runnable {
		private Connection[] connections;
		private AthenaQueueProcessor p;

		protected SyncAgentShutdownRoutine(Connection[] connections, AthenaQueueProcessor queueProcessor) {
			this.connections = connections;
			this.p = queueProcessor;
		}

		public void run() {
			// stop the queue processing thread
			p.stop();

			// close all the connections
			for (Connection c : this.connections) {
				try {
					c.close();
				} catch (SQLException e) {
					LOG.error(e.getMessage());
				}
			}
		}
	};

	/**
	 * Private class which processes the ddl queue and pushes the ddl through
	 * Athena. If the Athena connection is broken, try and reconnect, and if not
	 * then back off for a period of time and hope that the conneciton is fixed
	 * 
	 * @author meyersi
	 *
	 */
	private final class AthenaQueueProcessor implements Runnable {
		private boolean run = true;

		/**
		 * Method to send a shutdown message to the queue processor
		 */
		public void stop() {
			LOG.info(String.format("Stopping %s", this.getClass().getCanonicalName()));
			this.run = false;
		}

		public void run() {
			// run forever or until stop is called
			while (this.run) {
				if (!ddlQueue.isEmpty()) {
					String query = ddlQueue.removeFirst();

					// log the query now so that the logfile will reflect when this is actually
					// being run
					LOG.info(query);

					// implement an infinite retry - the queue will be building up, but we don't
					// want to drop any ddl from being processed. At some point we'll run out of
					// memory, but hopefully the connection will be reset before this is a problem
					boolean completed = false;
					while (!completed) {
						try {
							Statement athenaStmt = athenaConnection.createStatement();
							athenaStmt.execute(query);
							athenaStmt.close();
							completed = true;
						} catch (SQLException e) {
							if (e instanceof SQLRecoverableException || e instanceof SQLTimeoutException) {
								// attempt to reconnect to Athena
								try {
									configureAthenaConnection();
								} catch (SQLException e1) {
									// this will probably be because we can't open the connection
									LOG.error(e1.getMessage());

									try {
										Thread.sleep(reconnectSleepDuration);
									} catch (InterruptedException e2) {
										e2.printStackTrace();
									}
								}
							} else {
								LOG.error(e.getMessage());
							}
						}
					}
				} else {
					// put the thread to sleep for a configured duration
					try {
						LOG.debug(String.format("DDL Queue is empty. Sleeping for %s", noEventSleepDuration));
						Thread.sleep(noEventSleepDuration);
					} catch (InterruptedException e) {
						LOG.error(e.getMessage());
					}
				}
			}
		}
	}

	public HiveGlueCatalogSyncAgent(final Configuration conf) throws Exception {
		super(conf);
		this.config = conf;
		this.athenaURL = conf.get(ATHENA_JDBC_URL);

		String noopSleepDuration = this.config.get("no-event-sleep-duration");
		if (noopSleepDuration == null) {
			this.noEventSleepDuration = 1000;
		} else {
			this.noEventSleepDuration = new Integer(noopSleepDuration).intValue();
		}

		String reconnectSleepDuration = conf.get("reconnect-failed-sleep-duration");
		if (reconnectSleepDuration == null) {
			this.reconnectSleepDuration = 1000;
		} else {
			this.reconnectSleepDuration = new Integer(noopSleepDuration).intValue();
		}

		this.info = new Properties();
		this.info.put("s3_staging_dir", config.get("athena.s3.staging.dir"));
		this.info.put("log_path", "/tmp/jdbc.log");
		this.info.put("log_level", "ERROR");

		// info.put("user", config.get("athena.user.name"));
		// info.put("password", config.get("athena.user.password"));
		this.info.put("aws_credentials_provider_class",
				com.amazonaws.auth.InstanceProfileCredentialsProvider.class.getName());

		configureAthenaConnection();

		// Get the hive metastore URL from properties
		String hiveMetastoreURL = config.get("hive-metastore-url");

		if (hiveMetastoreURL == null) {
			hiveMetastoreURL = "jdbc:hive2://localhost:10000/default";
		}
		hiveMetastoreConnection = DriverManager.getConnection(hiveMetastoreURL);

		// start the queue processor thread
		AthenaQueueProcessor athenaQueueProcessor = new AthenaQueueProcessor();
		Thread queueProcessor = new Thread(athenaQueueProcessor);
		queueProcessor.start();

		// add a shutdown hook to close the connections
		Connection[] connections = new Connection[2];
		connections[0] = athenaConnection;
		connections[1] = hiveMetastoreConnection;
		Runtime.getRuntime().addShutdownHook(
				new Thread(new SyncAgentShutdownRoutine(connections, athenaQueueProcessor), "Shutdown-thread"));

		LOG.info(String.format("%s online, connected to %s and %s", this.getClass().getCanonicalName(),
				hiveMetastoreURL, this.athenaURL));
	}

	private final void configureAthenaConnection() throws SQLException, SQLTimeoutException {
		LOG.info(String.format("Connecting to Amazon Athena using endpoint %s", this.athenaURL));
		athenaConnection = DriverManager.getConnection(this.athenaURL, this.info);
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
				LOG.error("Unable to get current Create Table statement for replication:" + e.getMessage());
			}

			if (!addToAthenaQueue(ddl)) {
				LOG.error("Failed to add the CreateTable event to the processing queue");
			}
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
						if (!addToAthenaQueue(addPartitionDDL)) {
							LOG.error("Failed to add the AddPartition event to the processing queue");
						}
					}
				}
			} else {
				LOG.debug(String.format("Ignoring Add Partition Event for Table %s as it is not stored on S3",
						table.getTableName()));
			}
		}
	}

	private boolean addToAthenaQueue(String query) {
		return ddlQueue.offerLast(query);
	}
}

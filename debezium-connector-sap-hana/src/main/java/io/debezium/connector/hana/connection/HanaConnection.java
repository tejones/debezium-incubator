/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.hana.connection;

import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.ZoneId;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.connect.errors.ConnectException;
import org.postgresql.core.BaseConnection;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.util.PSQLState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.annotation.VisibleForTesting;
import io.debezium.config.Configuration;
import io.debezium.connector.hana.HanaConnectorConfig;
import io.debezium.connector.hana.HanaType;
import io.debezium.connector.hana.SourceTimestampMode;
import io.debezium.connector.hana.TypeRegistry;
import io.debezium.connector.hana.spi.SlotState;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.util.Clock;
import io.debezium.util.Metronome;

/**
 * {@link JdbcConnection} connection extension used for connecting to SAP HANA instances.
 *
 * @author Joao Tavares
 */
public class HanaConnection extends JdbcConnection {

    private static Logger LOGGER = LoggerFactory.getLogger(HanaConnection.class);

    private static final String URL_PATTERN = "jdbc:sap://${" + JdbcConfiguration.HOSTNAME + "}:${"
            + JdbcConfiguration.PORT + "}/?databaseName=${" + JdbcConfiguration.DATABASE + "}";
    
    protected static final ConnectionFactory FACTORY = JdbcConnection.patternBasedFactory(URL_PATTERN,
    		com.sap.db.jdbc.Driver.class.getName(),
            HanaConnection.class.getClassLoader());

    /**
     * Obtaining a replication slot may fail if there's a pending transaction. We're retrying to get a slot for 30 min.
     */
    //private static final int MAX_ATTEMPTS_FOR_OBTAINING_REPLICATION_SLOT = 900;

    //private static final Duration PAUSE_BETWEEN_REPLICATION_SLOT_RETRIEVAL_ATTEMPTS = Duration.ofSeconds(2);

    private final TypeRegistry typeRegistry;

    private final Charset databaseCharset;
    

    /**
     * Creates a SAP Hana connection using the supplied configuration.
     *
     * @param config {@link Configuration} instance, may not be null.
     */
    public HanaConnection(Configuration config) {
        super(config, FACTORY);
        this.typeRegistry = new TypeRegistry(this);
        //databaseCharset = determineDatabaseCharset();
        databaseCharset = Charset.defaultCharset();
    }

    /**
     * Returns a JDBC connection string for the current configuration.
     *
     * @return a {@code String} where the variables in {@code urlPattern} are replaced with values from the configuration
     */
    public String connectionString() {
        return connectionString(URL_PATTERN);
    }

    /**
     * Prints out information about the REPLICA IDENTITY status of a table.
     * This in turn determines how much information is available for UPDATE and DELETE operations for logical replication.
     *
     * @param tableId the identifier of the table
     * @return the replica identity information; never null
     * @throws SQLException if there is a problem obtaining the replica identity information for the given table
     */
    /* IS THIS NECESSARY?? HOW CAN WE GET THIS IN SAP HANA?
    public ServerInfo.ReplicaIdentity readReplicaIdentityInfo(TableId tableId) throws SQLException {
        String statement = "SELECT relreplident FROM pg_catalog.pg_class c " +
                "LEFT JOIN pg_catalog.pg_namespace n ON c.relnamespace=n.oid " +
                "WHERE n.nspname=? and c.relname=?";
        String schema = tableId.schema() != null && tableId.schema().length() > 0 ? tableId.schema() : "public";
        StringBuilder replIdentity = new StringBuilder();
        prepareQuery(statement, stmt -> {
            stmt.setString(1, schema);
            stmt.setString(2, tableId.table());
        }, rs -> {
            if (rs.next()) {
                replIdentity.append(rs.getString(1));
            }
            else {
                LOGGER.warn("Cannot determine REPLICA IDENTITY information for table '{}'", tableId);
            }
        });
        return ServerInfo.ReplicaIdentity.parseFromDB(replIdentity.toString());
    }
    */

    /**
     * Returns the current state of the replication slot
     * @param slotName the name of the slot
     * @param pluginName the name of the plugin used for the desired slot
     * @return the {@link SlotState} or null, if no slot state is found
     * @throws SQLException
     */
    /* IS THIS NECESSARY?? HOW CAN WE GET THIS IN SAP HANA?
    public SlotState getReplicationSlotState(String slotName, String pluginName) throws SQLException {
        ServerInfo.ReplicationSlot slot;
        try {
            slot = readReplicationSlotInfo(slotName, pluginName);
            if (slot.equals(ServerInfo.ReplicationSlot.INVALID)) {
                return null;
            }
            else {
                return slot.asSlotState();
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ConnectException("Interrupted while waiting for valid replication slot info", e);
        }
    }
    */


    /**
     * Fetches the state of a replication stage given a slot name and plugin name
     * @param slotName the name of the slot
     * @param pluginName the name of the plugin used for the desired slot
     * @return the {@link ServerInfo.ReplicationSlot} object or a {@link ServerInfo.ReplicationSlot#INVALID} if
     *         the slot is not valid
     * @throws SQLException is thrown by the underlying JDBC
     */
    /* IS THIS NECESSARY?? HOW CAN WE GET THIS IN SAP HANA?
    private ServerInfo.ReplicationSlot fetchReplicationSlotInfo(String slotName, String pluginName) throws SQLException {
        final String database = database();
        final ServerInfo.ReplicationSlot slot = queryForSlot(slotName, database, pluginName,
                rs -> {
                    if (rs.next()) {
                        boolean active = rs.getBoolean("active");
                        Long confirmedFlushedLsn = parseConfirmedFlushLsn(slotName, pluginName, database, rs);
                        if (confirmedFlushedLsn == null) {
                            return null;
                        }
                        Long restartLsn = parseRestartLsn(slotName, pluginName, database, rs);
                        if (restartLsn == null) {
                            return null;
                        }
                        Long xmin = rs.getLong("catalog_xmin");
                        return new ServerInfo.ReplicationSlot(active, confirmedFlushedLsn, restartLsn, xmin);
                    }
                    else {
                        LOGGER.debug("No replication slot '{}' is present for plugin '{}' and database '{}'", slotName,
                                pluginName, database);
                        return ServerInfo.ReplicationSlot.INVALID;
                    }
                });
        return slot;
    }
    */

    /**
     * Fetches a replication slot, repeating the query until either the slot is created or until
     * the max number of attempts has been reached
     *
     * To fetch the slot without the retries, use the {@link HanaConnection#fetchReplicationSlotInfo} call
     * @param slotName the slot name
     * @param pluginName the name of the plugin
     * @return the {@link ServerInfo.ReplicationSlot} object or a {@link ServerInfo.ReplicationSlot#INVALID} if
     *         the slot is not valid
     * @throws SQLException is thrown by the underyling jdbc driver
     * @throws InterruptedException is thrown if we don't return an answer within the set number of retries
     */
    /* IS THIS NECESSARY?? HOW CAN WE GET THIS IN SAP HANA?
    @VisibleForTesting
    ServerInfo.ReplicationSlot readReplicationSlotInfo(String slotName, String pluginName) throws SQLException, InterruptedException {
        final String database = database();
        final Metronome metronome = Metronome.parker(PAUSE_BETWEEN_REPLICATION_SLOT_RETRIEVAL_ATTEMPTS, Clock.SYSTEM);

        for (int attempt = 1; attempt <= MAX_ATTEMPTS_FOR_OBTAINING_REPLICATION_SLOT; attempt++) {
            final ServerInfo.ReplicationSlot slot = fetchReplicationSlotInfo(slotName, pluginName);
            if (slot != null) {
                LOGGER.info("Obtained valid replication slot {}", slot);
                return slot;
            }
            LOGGER.warn(
                    "Cannot obtain valid replication slot '{}' for plugin '{}' and database '{}' [during attempt {} out of {}, concurrent tx probably blocks taking snapshot.",
                    slotName, pluginName, database, attempt, MAX_ATTEMPTS_FOR_OBTAINING_REPLICATION_SLOT);
            metronome.pause();
        }

        throw new ConnectException("Unable to obtain valid replication slot. "
                + "Make sure there are no long-running transactions running in parallel as they may hinder the allocation of the replication slot when starting this connector");
    }

    protected ServerInfo.ReplicationSlot queryForSlot(String slotName, String database, String pluginName,
                                                      ResultSetMapper<ServerInfo.ReplicationSlot> map)
            throws SQLException {
        return prepareQueryAndMap("select * from pg_replication_slots where slot_name = ? and database = ? and plugin = ?", statement -> {
            statement.setString(1, slotName);
            statement.setString(2, database);
            statement.setString(3, pluginName);
        }, map);
    }
    */

    /**
     * Obtains the LSN to resume streaming from. On PG 9.5 there is no confirmed_flushed_lsn yet, so restart_lsn will be
     * read instead. This may result in more records to be re-read after a restart.
     */
    /* IS THIS NECESSARY?? HOW CAN WE GET THIS IN SAP HANA?
    private Long parseConfirmedFlushLsn(String slotName, String pluginName, String database, ResultSet rs) {
        Long confirmedFlushedLsn = null;

        try {
            confirmedFlushedLsn = tryParseLsn(slotName, pluginName, database, rs, "confirmed_flush_lsn");
        }
        catch (SQLException e) {
            LOGGER.info("unable to find confirmed_flushed_lsn, falling back to restart_lsn");
            try {
                confirmedFlushedLsn = tryParseLsn(slotName, pluginName, database, rs, "restart_lsn");
            }
            catch (SQLException e2) {
                throw new ConnectException("Neither confirmed_flush_lsn nor restart_lsn could be found");
            }
        }

        return confirmedFlushedLsn;
    }

    private Long parseRestartLsn(String slotName, String pluginName, String database, ResultSet rs) {
        Long restartLsn = null;
        try {
            restartLsn = tryParseLsn(slotName, pluginName, database, rs, "restart_lsn");
        }
        catch (SQLException e) {
            throw new ConnectException("restart_lsn could be found");
        }

        return restartLsn;
    }

    private Long tryParseLsn(String slotName, String pluginName, String database, ResultSet rs, String column) throws ConnectException, SQLException {
        Long lsn = null;

        String lsnStr = rs.getString(column);
        if (lsnStr == null) {
            return null;
        }
        try {
            lsn = LogSequenceNumber.valueOf(lsnStr).asLong();
        }
        catch (Exception e) {
            throw new ConnectException("Value " + column + " in the pg_replication_slots table for slot = '"
                    + slotName + "', plugin = '"
                    + pluginName + "', database = '"
                    + database + "' is not valid. This is an abnormal situation and the database status should be checked.");
        }
        if (lsn == LogSequenceNumber.INVALID_LSN.asLong()) {
            throw new ConnectException("Invalid LSN returned from database");
        }
        return lsn;
    }
    */

    /**
     * Drops a replication slot that was created on the DB
     *
     * @param slotName the name of the replication slot, may not be null
     * @return {@code true} if the slot was dropped, {@code false} otherwise
     */
    /* IS THIS NECESSARY?? HOW CAN WE GET THIS IN SAP HANA?
    public boolean dropReplicationSlot(String slotName) {
        try {
            execute("select pg_drop_replication_slot('" + slotName + "')");
            return true;
        }
        catch (SQLException e) {
            // slot is active
            if (PSQLState.OBJECT_IN_USE.getState().equals(e.getSQLState())) {
                LOGGER.warn("Cannot drop replication slot '{}' because it's still in use", slotName);
                return false;
            }
            else if (PSQLState.UNDEFINED_OBJECT.getState().equals(e.getSQLState())) {
                LOGGER.debug("Replication slot {} has already been dropped", slotName);
                return false;
            }
            else {
                LOGGER.error("Unexpected error while attempting to drop replication slot", e);
            }
            return false;
        }
    }
    */

    /**
     * Drops the debezium publication that was created.
     *
     * @param publicationName the publication name, may not be null
     * @return {@code true} if the publication was dropped, {@code false} otherwise
     */
    public boolean dropPublication(String publicationName) {
        try {
            LOGGER.debug("Dropping publication '{}'", publicationName);
            execute("DROP PUBLICATION " + publicationName);
            return true;
        }
        catch (SQLException e) {
            if (PSQLState.UNDEFINED_OBJECT.getState().equals(e.getSQLState())) {
                LOGGER.debug("Publication {} has already been dropped", publicationName);
            }
            else {
                LOGGER.error("Unexpected error while attempting to drop publication", e);
            }
            return false;
        }
    }

    @Override
    public synchronized void close() {
        try {
            super.close();
        }
        catch (SQLException e) {
            LOGGER.error("Unexpected error while closing SAP HANA connection", e);
        }
    }

    /**
     * Returns the id of the current active transaction
     *
     * @return a SAP HANA transaction identifier, or null if no tx is active
     * @throws SQLException if anything fails.
     */
    public Long currentTransactionId() throws SQLException {
        AtomicLong txId = new AtomicLong(0);
        query("SELECT TRANSACTION_ID FROM SYS.M_TRANSACTIONS mt WHERE TRANSACTION_STATUS='ACTIVE'", rs -> {
            if (rs.next()) {
                txId.compareAndSet(0, rs.getLong(1));
            }
        });
        long value = txId.get();
        return value > 0 ? value : null;
    }


    /**
     * Returns the current position in the server tx log.
     *
     * @return a long value, never negative
     * @throws SQLException if anything unexpected fails.
     */
    /* IS THIS NECESSARY?? HOW CAN WE GET THIS IN SAP HANA?
    public long currentXLogLocation() throws SQLException {
        AtomicLong result = new AtomicLong(0);
        int majorVersion = connection().getMetaData().getDatabaseMajorVersion();
        query(majorVersion >= 10 ? "select * from pg_current_wal_lsn()" : "select * from pg_current_xlog_location()", rs -> {
            if (!rs.next()) {
                throw new IllegalStateException("there should always be a valid xlog position");
            }
            result.compareAndSet(0, LogSequenceNumber.valueOf(rs.getString(1)).asLong());
        });
        return result.get();
    }
    */

    /**
     * Returns information about the PG server to which this instance is connected.
     *
     * @return a {@link ServerInfo} instance, never {@code null}
     * @throws SQLException if anything fails
     */
    /* IS THIS NECESSARY?? HOW CAN WE GET THIS IN SAP HANA?
    public ServerInfo serverInfo() throws SQLException {
        ServerInfo serverInfo = new ServerInfo();
        query("SELECT version(), current_user, current_database()", rs -> {
            if (rs.next()) {
                serverInfo.withServer(rs.getString(1)).withUsername(rs.getString(2)).withDatabase(rs.getString(3));
            }
        });
        String username = serverInfo.username();
        if (username != null) {
            query("SELECT oid, rolname, rolsuper, rolinherit, rolcreaterole, rolcreatedb, rolcanlogin, rolreplication FROM pg_roles " +
                    "WHERE pg_has_role('" + username + "', oid, 'member')",
                    rs -> {
                        while (rs.next()) {
                            String roleInfo = "superuser: " + rs.getBoolean(3) + ", replication: " + rs.getBoolean(8) +
                                    ", inherit: " + rs.getBoolean(4) + ", create role: " + rs.getBoolean(5) +
                                    ", create db: " + rs.getBoolean(6) + ", can log in: " + rs.getBoolean(7);
                            String roleName = rs.getString(2);
                            serverInfo.addRole(roleName, roleInfo);
                        }
                    });
        }
        return serverInfo;
    }
    */

    public Charset getDatabaseCharset() {
        return databaseCharset;
    }

    private Charset determineDatabaseCharset() {
        try {
            return Charset.forName(((BaseConnection) connection()).getEncoding().name());
        }
        catch (SQLException e) {
            throw new RuntimeException("Couldn't obtain encoding for database " + database(), e);
        }
    }

/*
    @Override
    protected int resolveNativeType(String typeName) {
        return getTypeRegistry().get(typeName).getRootType().getOid();
    }


    @Override
    protected int resolveJdbcType(int metadataJdbcType, int nativeType) {
        // Special care needs to be taken for columns that use user-defined domain type data types
        // where resolution of the column's JDBC type needs to be that of the root type instead of
        // the actual column to properly influence schema building and value conversion.
        return getTypeRegistry().get(nativeType).getRootType().getJdbcId();
    }
*/
    
    @Override
    protected Optional<ColumnEditor> readTableColumn(ResultSet columnMetadata, TableId tableId, Tables.ColumnNameFilter columnFilter) throws SQLException {
        final String columnName = columnMetadata.getString(4);
        if (columnFilter == null || columnFilter.matches(tableId.catalog(), tableId.schema(), tableId.table(), columnName)) {
            final ColumnEditor column = Column.editor().name(columnName);
            column.type(columnMetadata.getString(6));

            // first source the length/scale from the column metadata provided by the driver
            // this may be overridden below if the column type is a user-defined domain type
            column.length(columnMetadata.getInt(7));
            if (columnMetadata.getObject(9) != null) {
                column.scale(columnMetadata.getInt(9));
            }

            column.optional(isNullable(columnMetadata.getInt(11)));
            column.position(columnMetadata.getInt(17));
            column.autoIncremented("YES".equalsIgnoreCase(columnMetadata.getString(23)));

            String autogenerated = null;
            try {
                autogenerated = columnMetadata.getString(24);
            }
            catch (SQLException e) {
                // ignore, some drivers don't have this index - e.g. Postgres
            }
            column.generated("YES".equalsIgnoreCase(autogenerated));

            /*
            // Lookup the column type from the TypeRegistry
            // For all types, we need to set the Native and Jdbc types by using the root-type
            final HanaType nativeType = getTypeRegistry().get(column.typeName());
            column.nativeType(nativeType.getRootType().getOid());
            column.jdbcType(nativeType.getRootType().getJdbcId());

            // For domain types, the postgres driver is unable to traverse a nested unbounded
            // hierarchy of types and report the right length/scale of a given type. We use
            // the TypeRegistry to accomplish this since it is capable of traversing the type
            // hierarchy upward to resolve length/scale regardless of hierarchy depth.
            if (TypeRegistry.DOMAIN_TYPE == nativeType.getJdbcId()) {
                column.length(nativeType.getDefaultLength());
                column.scale(nativeType.getDefaultScale());
            }
            */

            return Optional.of(column);
        }

        return Optional.empty();
    }

    public TypeRegistry getTypeRegistry() {
        return typeRegistry;
    }
    
    //Testing 
	public static void main(String [] args)
	{
		
		Configuration config = JdbcConfiguration.copy(Configuration.fromSystemProperties("database."))
        .withDefault(JdbcConfiguration.DATABASE, "")
        .withDefault(JdbcConfiguration.HOSTNAME, "192.168.1.3")
        .withDefault(JdbcConfiguration.PORT, 39015)
        .withDefault(JdbcConfiguration.USER, "SYSTEM")
        .withDefault(JdbcConfiguration.PASSWORD, "RHdatabase123")
        .build();
		
		
		HanaConnection connection = new HanaConnection(config);
		String statement = "SELECT * FROM SYS.DATA_TYPES dt;";
		//ResultSetConsumer result = null;
		
		//com.sap.db.jdbc.ConnectionProperty
        try {
            connection.setAutoCommit(false);

            connection.query(statement, rs -> {
                while (rs.next()) {
                    System.out.println(rs.getString(1) );
                }
            });
            
            
            Connection jdbcConn = connection.connection();
            if (!statement.endsWith("ROLLBACK;")) {
                jdbcConn.commit();
            }
            else {
                jdbcConn.rollback();
            }
            connection.close();
        }
        catch (Exception e) {
        	System.out.println(e.getMessage());
            throw new RuntimeException(e);
        }
	}
}
/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.hana.spi;

import java.time.Duration;
import java.util.Optional;
import java.util.Set;

import io.debezium.common.annotation.Incubating;
import io.debezium.connector.hana.HanaConnectorConfig;
import io.debezium.relational.TableId;

/**
 * This interface is used to determine details about the snapshot process:
 *
 * Namely:
 * - Should a snapshot occur at all
 * - Should streaming occur
 * - What queries should be used to snapshot
 *
 * While many default snapshot modes are provided with debezium (see documentation for details)
 * a custom implementation of this interface can be provided by the implementor which
 * can provide more advanced functionality, such as partial snapshots
 *
 * Implementor's must return true for either {@link #shouldSnapshot()} or {@link #shouldStream()}
 * or true for both.
 */
@Incubating
public interface Snapshotter {

    void init(HanaConnectorConfig config, OffsetState sourceInfo,
              SlotState slotState);

    /**
     * @return true if the snapshotter should take a snapshot
     */
    boolean shouldSnapshot();

    /**
     * @return true if the snapshotter should stream after taking a snapshot
     */
    boolean shouldStream();

    /**
     * @return true if when creating a slot, a snapshot should be exported, which
     * can be used as an alternative to taking a lock
     */
    default boolean exportSnapshot() {
        return false;
    }

    /**
     * Generate a valid postgres query string for the specified table, or an empty {@link Optional}
     * to skip snapshotting this table (but that table will still be streamed from)
     *
     * @param tableId the table to generate a query for
     * @return a valid query string, or none to skip snapshotting this table
     */
    Optional<String> buildSnapshotQuery(TableId tableId);

    /**
     * Return a new string that set up the transaction for snapshotting
     *
     */
    default String snapshotTransactionIsolationLevelStatement() {
        // Removed , DEFERRABLE after READ ONLY because SAP HANA does not seem to have this option
    	//https://help.sap.com/viewer/4fe29514fd584807ac9f2a04f6754767/2.0.03/en-US/20fdf9cb75191014b85aaa9dec841291.html
        return "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE, READ ONLY;";
    }

    /**
     * Returns a SQL statement for locking the given tables during snapshotting, if required by the specific snapshotter
     * implementation.
     */
    default Optional<String> snapshotTableLockingStatement(Duration lockTimeout, Set<TableId> tableIds) {
        String lineSeparator = System.lineSeparator();
        StringBuilder statements = new StringBuilder();
        statements.append("SET lock_timeout = ").append(lockTimeout.toMillis()).append(";").append(lineSeparator);
        // EXCLUSIVE MODE used to block DDL commands so that the table structure does not change
        // however, the table will still allow DML commands (SELECT, INSERT,...)
        // http://sap.optimieren.de/hana/hana/html/sql_lock_table.html
        tableIds.forEach(tableId -> statements.append("LOCK TABLE ")
                .append(tableId.toDoubleQuotedString())
                .append(" IN EXCLUSIVE MODE;")
                .append(lineSeparator));
        return Optional.of(statements.toString());
    }
}

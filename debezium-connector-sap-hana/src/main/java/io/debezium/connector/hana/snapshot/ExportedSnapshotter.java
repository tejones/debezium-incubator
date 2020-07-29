/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.hana.snapshot;

import java.time.Duration;
import java.util.Optional;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.hana.HanaConnectorConfig;
import io.debezium.connector.hana.spi.OffsetState;
import io.debezium.connector.hana.spi.SlotCreationResult;
import io.debezium.connector.hana.spi.SlotState;
import io.debezium.connector.hana.spi.Snapshotter;
import io.debezium.relational.TableId;

/**
 * @author Joao Tavares
 */
public class ExportedSnapshotter implements Snapshotter {

    private final static Logger LOGGER = LoggerFactory.getLogger(ExportedSnapshotter.class);
    private OffsetState sourceInfo;

    @Override
    public void init(HanaConnectorConfig config, OffsetState sourceInfo, SlotState slotState) {
        this.sourceInfo = sourceInfo;
    }

    @Override
    public boolean shouldSnapshot() {
        if (sourceInfo == null) {
            LOGGER.info("Taking exported snapshot for new datasource");
            return true;
        }
        else if (sourceInfo.snapshotInEffect()) {
            LOGGER.info("Found previous incomplete snapshot");
            return true;
        }
        else {
            LOGGER.info("Previous exported snapshot completed, streaming logical changes from last known position");
            return false;
        }
    }

    @Override
    public boolean shouldStream() {
        return true;
    }

    @Override
    public boolean exportSnapshot() {
        return true;
    }

    @Override
    public Optional<String> buildSnapshotQuery(TableId tableId) {
        return Optional.of("select * from " + tableId.toDoubleQuotedString());
    }

    @Override
    public Optional<String> snapshotTableLockingStatement(Duration lockTimeout, Set<TableId> tableIds) {
        return Optional.empty();
    }

    @Override
    public String snapshotTransactionIsolationLevelStatement() {
        //if (newSlotInfo != null) {
            //String snapSet = String.format("SET TRANSACTION SNAPSHOT '%s';", newSlotInfo.snapshotName());
            
        //}
        //return Snapshotter.super.snapshotTransactionIsolationLevelStatement(newSlotInfo);
        
        return "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE, READ ONLY; \n";
    }
}

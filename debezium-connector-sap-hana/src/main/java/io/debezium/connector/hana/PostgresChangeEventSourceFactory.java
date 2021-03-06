/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.hana;

import io.debezium.connector.hana.connection.HanaConnection;
import io.debezium.connector.hana.connection.ReplicationConnection;
import io.debezium.connector.hana.spi.SlotCreationResult;
import io.debezium.connector.hana.spi.Snapshotter;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.ChangeEventSourceFactory;
import io.debezium.pipeline.source.spi.SnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;

public class PostgresChangeEventSourceFactory implements ChangeEventSourceFactory {

    private final HanaConnectorConfig configuration;
    private final HanaConnection jdbcConnection;
    private final ErrorHandler errorHandler;
    private final EventDispatcher<TableId> dispatcher;
    private final Clock clock;
    private final HanaSchema schema;
    private final HanaTaskContext taskContext;
    private final Snapshotter snapshotter;
    private final ReplicationConnection replicationConnection;
    private final SlotCreationResult slotCreatedInfo;

    public PostgresChangeEventSourceFactory(HanaConnectorConfig configuration, Snapshotter snapshotter, HanaConnection jdbcConnection,
                                            ErrorHandler errorHandler, EventDispatcher<TableId> dispatcher, Clock clock, HanaSchema schema,
                                            HanaTaskContext taskContext,
                                            ReplicationConnection replicationConnection, SlotCreationResult slotCreatedInfo) {
        this.configuration = configuration;
        this.jdbcConnection = jdbcConnection;
        this.errorHandler = errorHandler;
        this.dispatcher = dispatcher;
        this.clock = clock;
        this.schema = schema;
        this.taskContext = taskContext;
        this.snapshotter = snapshotter;
        this.replicationConnection = replicationConnection;
        this.slotCreatedInfo = slotCreatedInfo;
    }

    @Override
    public SnapshotChangeEventSource getSnapshotChangeEventSource(OffsetContext offsetContext, SnapshotProgressListener snapshotProgressListener) {
        return new HanaSnapshotChangeEventSource(
                configuration,
                snapshotter,
                (PostgresOffsetContext) offsetContext,
                jdbcConnection,
                schema,
                dispatcher,
                clock,
                snapshotProgressListener,
                slotCreatedInfo);
    }

    @Override
    public StreamingChangeEventSource getStreamingChangeEventSource(OffsetContext offsetContext) {
        return new PostgresStreamingChangeEventSource(
                configuration,
                snapshotter,
                (PostgresOffsetContext) offsetContext,
                jdbcConnection,
                dispatcher,
                errorHandler,
                clock,
                schema,
                taskContext,
                replicationConnection);
    }
}

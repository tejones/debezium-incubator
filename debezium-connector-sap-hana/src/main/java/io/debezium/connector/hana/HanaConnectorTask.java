/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.hana;

import java.nio.charset.Charset;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.common.BaseSourceTask;
import io.debezium.connector.hana.connection.HanaConnection;
import io.debezium.connector.hana.connection.ReplicationConnection;
import io.debezium.connector.hana.spi.SlotCreationResult;
import io.debezium.connector.hana.spi.SlotState;
import io.debezium.connector.hana.spi.Snapshotter;
import io.debezium.heartbeat.Heartbeat;
import io.debezium.pipeline.ChangeEventSourceCoordinator;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.metrics.DefaultChangeEventSourceMetricsFactory;
import io.debezium.relational.TableId;
import io.debezium.schema.TopicSelector;
import io.debezium.util.Clock;
import io.debezium.util.LoggingContext;
import io.debezium.util.Metronome;

/**
 * Kafka connect source task which uses Hana logical decoding over a streaming replication connection to process DB changes.
 *
 * @author Joao Tavares
 */
public class HanaConnectorTask extends BaseSourceTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(HanaConnectorTask.class);
    private static final String CONTEXT_NAME = "hana-connector-task";

    private volatile PostgresTaskContext taskContext;
    private volatile ChangeEventQueue<DataChangeEvent> queue;
    private volatile HanaConnection jdbcConnection;
    private volatile HanaConnection heartbeatConnection;
    private volatile ErrorHandler errorHandler;
    private volatile HanaSchema schema;

    @Override
    public ChangeEventSourceCoordinator start(Configuration config) {
        final HanaConnectorConfig connectorConfig = new HanaConnectorConfig(config);
        final TopicSelector<TableId> topicSelector = HanaTopicSelector.create(connectorConfig);
        final Snapshotter snapshotter = connectorConfig.getSnapshotter();

        if (snapshotter == null) {
            throw new ConnectException("Unable to load snapshotter, if using custom snapshot mode, double check your settings");
        }

        jdbcConnection = new HanaConnection(connectorConfig.jdbcConfig());
        heartbeatConnection = new HanaConnection(connectorConfig.jdbcConfig());
        final TypeRegistry typeRegistry = jdbcConnection.getTypeRegistry();
        final Charset databaseCharset = jdbcConnection.getDatabaseCharset();

        schema = new HanaSchema(connectorConfig, typeRegistry, databaseCharset, topicSelector);
        this.taskContext = new PostgresTaskContext(connectorConfig, schema, topicSelector);
        final PostgresOffsetContext previousOffset = (PostgresOffsetContext) getPreviousOffset(new PostgresOffsetContext.Loader(connectorConfig));
        final Clock clock = Clock.system();

        final SourceInfo sourceInfo = new SourceInfo(connectorConfig);
        LoggingContext.PreviousContext previousContext = taskContext.configureLoggingContext(CONTEXT_NAME);
        try {
            // Print out the server information
            SlotState slotInfo = null;
            try (HanaConnection connection = taskContext.createConnection()) {
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info(connection.serverInfo().toString());
                }
            }
            catch (SQLException e) {
                LOGGER.warn("unable to load info of replication slot, Debezium will try to create the slot");
            }

            if (previousOffset == null) {
                LOGGER.info("No previous offset found");
                // if we have no initial offset, indicate that to Snapshotter by passing null
                snapshotter.init(connectorConfig, null, slotInfo);
            }
            else {
                LOGGER.info("Found previous offset {}", sourceInfo);
                snapshotter.init(connectorConfig, previousOffset.asOffsetState(), slotInfo);
            }

            ReplicationConnection replicationConnection = null;
            SlotCreationResult slotCreatedInfo = null;
            if (snapshotter.shouldStream()) {
                boolean shouldExport = snapshotter.exportSnapshot();
                replicationConnection = createReplicationConnection(this.taskContext, shouldExport,
                        connectorConfig.maxRetries(), connectorConfig.retryDelay());

                // we need to create the slot before we start streaming if it doesn't exist
                // otherwise we can't stream back changes happening while the snapshot is taking place
                if (slotInfo == null) {
                    try {
                        slotCreatedInfo = replicationConnection.createReplicationSlot().orElse(null);
                    }
                    catch (SQLException ex) {
                        throw new ConnectException(ex);
                    }
                }
                else {
                    slotCreatedInfo = null;
                }
            }

            queue = new ChangeEventQueue.Builder<DataChangeEvent>()
                    .pollInterval(connectorConfig.getPollInterval())
                    .maxBatchSize(connectorConfig.getMaxBatchSize())
                    .maxQueueSize(connectorConfig.getMaxQueueSize())
                    .loggingContextSupplier(() -> taskContext.configureLoggingContext(CONTEXT_NAME))
                    .build();

            errorHandler = new PostgresErrorHandler(connectorConfig.getLogicalName(), queue);

            final HanaEventMetadataProvider metadataProvider = new HanaEventMetadataProvider();

            Heartbeat heartbeat = Heartbeat.create(connectorConfig.getConfig(), topicSelector.getHeartbeatTopic(),
                    connectorConfig.getLogicalName(), heartbeatConnection);

            final EventDispatcher<TableId> dispatcher = new EventDispatcher<>(
                    connectorConfig,
                    topicSelector,
                    schema,
                    queue,
                    connectorConfig.getTableFilters().dataCollectionFilter(),
                    DataChangeEvent::new,
                    PostgresChangeRecordEmitter::updateSchema,
                    metadataProvider,
                    heartbeat);

            ChangeEventSourceCoordinator coordinator = new ChangeEventSourceCoordinator(
                    previousOffset,
                    errorHandler,
                    PostgresConnector.class,
                    connectorConfig,
                    new PostgresChangeEventSourceFactory(
                            connectorConfig,
                            snapshotter,
                            jdbcConnection,
                            errorHandler,
                            dispatcher,
                            clock,
                            schema,
                            taskContext,
                            replicationConnection,
                            slotCreatedInfo),
                    new DefaultChangeEventSourceMetricsFactory(),
                    dispatcher,
                    schema);

            coordinator.start(taskContext, this.queue, metadataProvider);

            return coordinator;
        }
        finally {
            previousContext.restore();
        }
    }

    public ReplicationConnection createReplicationConnection(HanaTaskContext taskContext, boolean shouldExport,
                                                             int maxRetries, Duration retryDelay)
            throws ConnectException {
        final Metronome metronome = Metronome.parker(retryDelay, Clock.SYSTEM);
        short retryCount = 0;
        ReplicationConnection replicationConnection = null;
        while (retryCount <= maxRetries) {
            try {
                return taskContext.createReplicationConnection(shouldExport);
            }
            catch (SQLException ex) {
                retryCount++;
                if (retryCount > maxRetries) {
                    LOGGER.error("Too many errors connecting to server. All {} retries failed.", maxRetries);
                    throw new ConnectException(ex);
                }

                LOGGER.warn("Error connecting to server; will attempt retry {} of {} after {} " +
                        "seconds. Exception message: {}", retryCount, maxRetries, retryDelay.getSeconds(), ex.getMessage());
                try {
                    metronome.pause();
                }
                catch (InterruptedException e) {
                    LOGGER.warn("Connection retry sleep interrupted by exception: " + e);
                    Thread.currentThread().interrupt();
                }
            }
        }
        return replicationConnection;
    }

    @Override
    public List<SourceRecord> doPoll() throws InterruptedException {
        final List<DataChangeEvent> records = queue.poll();

        final List<SourceRecord> sourceRecords = records.stream()
                .map(DataChangeEvent::getRecord)
                .collect(Collectors.toList());

        return sourceRecords;
    }

    @Override
    protected void doStop() {
        if (jdbcConnection != null) {
            jdbcConnection.close();
        }

        if (heartbeatConnection != null) {
            heartbeatConnection.close();
        }

        if (schema != null) {
            schema.close();
        }
    }

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    protected Iterable<Field> getAllConfigurationFields() {
        return HanaConnectorConfig.ALL_FIELDS;
    }

    public HanaTaskContext getTaskContext() {
        return taskContext;
    }
}

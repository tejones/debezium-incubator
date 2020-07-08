/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.hana;

import java.nio.charset.Charset;
import java.sql.SQLException;
import java.time.Duration;

import org.apache.kafka.connect.errors.ConnectException;
import org.junit.Assert;
import org.junit.Test;

import io.debezium.connector.hana.connection.ReplicationConnection;
import io.debezium.doc.FixFor;

/**
 * Integration test for {@link HanaConnectorTask} class.
 */
public class PostgresConnectorTaskIT {

    @Test
    @FixFor("DBZ-519")
    public void shouldNotThrowNullPointerExceptionDuringCommit() throws Exception {
        HanaConnectorTask postgresConnectorTask = new HanaConnectorTask();
        postgresConnectorTask.commit();
    }

    class FakeContext extends HanaTaskContext {
        public FakeContext(HanaConnectorConfig postgresConnectorConfig, HanaSchema postgresSchema) {
            super(postgresConnectorConfig, postgresSchema, null);
        }

        @Override
        protected ReplicationConnection createReplicationConnection(boolean exportSnapshot) throws SQLException {
            throw new SQLException("Could not connect");
        }
    }

    @Test(expected = ConnectException.class)
    @FixFor("DBZ-1426")
    public void retryOnFailureToCreateConnection() throws Exception {
        HanaConnectorTask postgresConnectorTask = new HanaConnectorTask();
        HanaConnectorConfig config = new HanaConnectorConfig(TestHelper.defaultConfig().build());
        long startTime = System.currentTimeMillis();
        postgresConnectorTask.createReplicationConnection(new FakeContext(config, new HanaSchema(
                config,
                null,
                Charset.forName("UTF-8"),
                HanaTopicSelector.create(config))), true, 3, Duration.ofSeconds(2));

        // Verify retry happened for 10 seconds
        long endTime = System.currentTimeMillis();
        long timeElapsed = endTime - startTime;
        Assert.assertTrue(timeElapsed > 5);
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.hana;

import java.sql.SQLException;

import org.postgresql.util.PSQLException;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.pipeline.ErrorHandler;

/**
 * Error handler for SAP HANA.
 *
 * @author Joao Tavares
 */
public class HanaErrorHandler extends ErrorHandler {

    public HanaErrorHandler(String logicalName, ChangeEventQueue<?> queue) {
        super(HanaConnector.class, logicalName, queue);
    }

    @Override
    protected boolean isRetriable(Throwable throwable) {
        if (throwable instanceof SQLException
                && (throwable.getMessage().contains("Database connection failed when writing to copy")
                        || throwable.getMessage().contains("Database connection failed when reading from copy"))) {
            return true;
        }

        return false;
    }
}

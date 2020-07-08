package io.debezium.connector.hana;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.AbstractSourceInfo;
import io.debezium.connector.LegacyV1AbstractSourceInfoStructMaker;

public class LegacyV1HanaSourceInfoStructMaker extends LegacyV1AbstractSourceInfoStructMaker<SourceInfo> {

    private final Schema schema;

    public LegacyV1HanaSourceInfoStructMaker(String connector, String version, CommonConnectorConfig connectorConfig) {
        super(connector, version, connectorConfig);
        schema = commonSchemaBuilder()
                .name("io.debezium.connector.hana.Source")
                .field(AbstractSourceInfo.SERVER_NAME_KEY, Schema.STRING_SCHEMA)
                .field(SourceInfo.SERVER_NAME_KEY, Schema.INT64_SCHEMA)
                .field(SourceInfo.TIMESTAMP_KEY, Schema.INT64_SCHEMA)
                .field(SourceInfo.SNAPSHOT_KEY, SchemaBuilder.bool().optional().defaultValue(false).build())
                .field(SourceInfo.DATABASE_NAME_KEY, Schema.OPTIONAL_STRING_SCHEMA)
                .field(SourceInfo.TABLE_NAME_KEY, Schema.OPTIONAL_STRING_SCHEMA)
                .build();
    }

    @Override
    public Schema schema() {
        return schema;
    }

    @Override
    public Struct struct(SourceInfo sourceInfo) {
        Struct result = commonStruct();
        result.put(SourceInfo.SERVER_NAME_KEY, serverName);
        result.put(SourceInfo.TIMESTAMP_KEY, sourceInfo.timestamp());

        if (sourceInfo.database() != null) {
            result.put(SourceInfo.DATABASE_NAME_KEY, sourceInfo.database());
        }
        if (sourceInfo.tableName() != null) {
            result.put(SourceInfo.TABLE_NAME_KEY, sourceInfo.tableName());
        }

        return result;
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.hana;

import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.nio.charset.IllegalCharsetNameException;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.errors.ConnectException;
import org.postgresql.geometric.PGpoint;
import org.postgresql.jdbc.PgArray;
import org.postgresql.util.HStoreConverter;
import org.postgresql.util.PGInterval;
import org.postgresql.util.PGobject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;

import io.debezium.config.CommonConnectorConfig.BinaryHandlingMode;
import io.debezium.connector.hana.data.Ltree;
import io.debezium.connector.hana.proto.PgProto;
import io.debezium.data.Bits;
import io.debezium.data.Json;
import io.debezium.data.SpecialValueDecimal;
import io.debezium.data.Uuid;
import io.debezium.data.VariableScaleDecimal;
import io.debezium.data.geometry.Geography;
import io.debezium.data.geometry.Geometry;
import io.debezium.data.geometry.Point;
import io.debezium.jdbc.JdbcValueConverters;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.Column;
import io.debezium.relational.ValueConverter;
import io.debezium.time.Interval;
import io.debezium.time.MicroDuration;
import io.debezium.time.ZonedTime;
import io.debezium.time.ZonedTimestamp;
import io.debezium.util.NumberConversions;
import io.debezium.util.Strings;

/**
 * A provider of {@link ValueConverter}s and {@link SchemaBuilder}s for various SAP HANA specific column types.
 *
 *
 * @author Joao Tavares
 */
public class HanaValueConverter extends JdbcValueConverters {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(HanaValueConverter.class);
	
    /**
     * Used to parse values of TIME columns. Format: 00:00:00
     */
    //private static final Pattern TIME_FIELD_PATTERN = Pattern.compile("([0-1][0-9]|2[0-3])(:([0-5][0-9])){2}");
    
    /**
     * Used to parse values of TIMESTAMP columns. Format: 00-00-00 00:00:00.000.
     */
    //private static final Pattern TIMESTAMP_FIELD_PATTERN = Pattern.compile("([0-9][0-9][0-9][0-9])-(0[0-9]|1[0-2]?)-([0-2][0-9]|3[0-1]?)\\s([0-1][0-9]|2[0-3])(:([0-5][0-9])){2}\\.(0000000|9999999)");
	
    /**
     * A formatter used to parse TIME columns when provided as strings.
     */
    private static final DateTimeFormatter TIME = new DateTimeFormatterBuilder()
            .appendPattern("HH:mm:ss")
            .toFormatter();

    /**
     * Variable scale decimal/numeric is defined by metadata
     * scale - 0
     * length - 131089
     */
    private static final int VARIABLE_SCALE_DECIMAL_LENGTH = 131089;

    /**
     * A string denoting not-a- number for FP and Numeric types
     */
    public static final String N_A_N = "NaN";

    /**
     * A string denoting positive infinity for FP and Numeric types
     */
    public static final String POSITIVE_INFINITY = "Infinity";

    /**
     * A string denoting negative infinity for FP and Numeric types
     */
    public static final String NEGATIVE_INFINITY = "-Infinity";

    private static final BigDecimal MICROSECONDS_PER_SECOND = new BigDecimal(1_000_000);

    /**
     * A formatter used to parse TIMETZ columns when provided as strings.
     */
    private static final DateTimeFormatter TIME_WITH_TIMEZONE_FORMATTER = new DateTimeFormatterBuilder()
            .appendPattern("HH:mm:ss")
            .appendFraction(ChronoField.MICRO_OF_SECOND, 0, 6, true)
            .appendPattern("[XXX][XX][X]")
            .toFormatter();

    private static final Duration ONE_DAY = Duration.ofDays(1);
    private static final long NANO_SECONDS_PER_DAY = TimeUnit.DAYS.toNanos(1);

    /**
     * {@code true} if fields of data type not know should be handle as opaque binary;
     * {@code false} if they should be omitted
     */
    private final boolean includeUnknownDatatypes;

    private final TypeRegistry typeRegistry;

    /**
     * The current database's character encoding.
     */
    private final Charset databaseCharset;

    private final JsonFactory jsonFactory;

    private final String toastPlaceholderString;
    private final byte[] toastPlaceholderBinary;

    protected HanaValueConverter(Charset databaseCharset, DecimalMode decimalMode,
                                     TemporalPrecisionMode temporalPrecisionMode, ZoneOffset defaultOffset,
                                     BigIntUnsignedMode bigIntUnsignedMode, boolean includeUnknownDatatypes, TypeRegistry typeRegistry,
                                     byte[] toastPlaceholder, BinaryHandlingMode binaryMode) {
        super(decimalMode, temporalPrecisionMode, defaultOffset, null, bigIntUnsignedMode, binaryMode);
        this.databaseCharset = databaseCharset;
        this.jsonFactory = new JsonFactory();
        this.includeUnknownDatatypes = includeUnknownDatatypes;
        this.typeRegistry = typeRegistry;
        this.toastPlaceholderBinary = toastPlaceholder;
        this.toastPlaceholderString = new String(toastPlaceholder);
    }
  
    /**
	 * Handle SAP HANA types not defined in the java JDBC Types
     */
    @Override
    public SchemaBuilder schemaBuilder(Column column) {
        
        String typeName = column.typeName().toUpperCase();

        if (matches(typeName, "ST_POINT")) {
            return io.debezium.data.geometry.Point.builder();
        }
        if (matches(typeName, "ST_GEOMETRY")
        		|| matches(typeName, "ST_CIRCULARSTRING")
                || matches(typeName, "ST_LINESTRING")
                || matches(typeName, "ST_MULTILINESTRING")
                || matches(typeName, "ST_MULTIPOINT")
                || matches(typeName, "ST_MULTIPOLYGON")
                || matches(typeName, "ST_POINT")
                || matches(typeName, "ST_POLYGON")
                || isGeometryCollection(typeName)) {
            return io.debezium.data.geometry.Geometry.builder();
        }
        
        /*
         * CLOB has ID -1 in SAP HANA and ID 2005 in jdbc Types class.
         * BLOB as well and a few others. Will this influence the super class schema Builder?
         *
         *
          	if (matches(typeName, "CLOB")) {
            // In order to capture signed SMALLINT 16-bit data source, INT32 will be required to safely capture all valid values
            // Source: https://kafka.apache.org/0102/javadoc/org/apache/kafka/connect/data/Schema.Type.html
            	return SchemaBuilder.int32();
        	}
         */

        // Otherwise, let the base class handle it ...
        return super.schemaBuilder(column);
    }

    
    /**
     * Determine if the uppercase form of a column's type exactly matches or begins with the specified prefix.
     * Note that this logic works when the column's {@link Column#typeName() type} contains the type name followed by parentheses.
     *
     * @param upperCaseTypeName the upper case form of the column's {@link Column#typeName() type name}
     * @param upperCaseMatch the upper case form of the expected type or prefix of the type; may not be null
     * @return {@code true} if the type matches the specified type, or {@code false} otherwise
     */
    protected boolean matches(String upperCaseTypeName, String upperCaseMatch) {
        if (upperCaseTypeName == null) {
            return false;
        }
        return upperCaseMatch.equals(upperCaseTypeName) || upperCaseTypeName.startsWith(upperCaseMatch + "(");
    }
    
    /**
     * Determine if the uppercase form of a column's type is geometry collection independent of JDBC driver or server version.
     *
     * @param upperCaseTypeName the upper case form of the column's {@link Column#typeName() type name}
     * @return {@code true} if the type is geometry collection
     */
    protected boolean isGeometryCollection(String upperCaseTypeName) {
        if (upperCaseTypeName == null) {
            return false;
        }

        return upperCaseTypeName.equals("ST_GEOMETRYCOLLECTION") || upperCaseTypeName.equals("GEOMCOLLECTION")
                || upperCaseTypeName.endsWith(".GEOMCOLLECTION");
    }

    @Override
    public ValueConverter converter(Column column, Field fieldDefn) {
    	// Handle a few SAP HANA-specific types
        String typeName = column.typeName().toUpperCase();

        if (matches(typeName, "GEOMETRY")
                || matches(typeName, "LINESTRING")
                || matches(typeName, "POLYGON")
                || matches(typeName, "MULTIPOINT")
                || matches(typeName, "MULTILINESTRING")
                || matches(typeName, "MULTIPOLYGON")
                || isGeometryCollection(typeName)) {
            return (data -> convertGeometry(column, fieldDefn, data));
        }
        if (matches(typeName, "POINT")) {
            // backwards compatibility
            return (data -> convertPoint(column, fieldDefn, data));
        }
        
        /* Have to find a class similar to com.mysql.cj.CharsetMapping
        // We have to convert bytes encoded in the column's character set ...
        switch (column.jdbcType()) {
            case Types.CHAR: // variable-length
            case Types.VARCHAR: // variable-length
            case Types.CLOB: // variable-length
            case Types.NCHAR: // fixed-length
            case Types.NVARCHAR: // fixed-length
            case Types.NCLOB: // fixed-length
                Charset charset = charsetFor(column);
                if (charset != null) {
                    logger.debug("Using {} charset by default for column: {}", charset, column);
                    return (data) -> convertString(column, fieldDefn, charset, data);
                }
                logger.warn("Using UTF-8 charset by default for column without charset: {}", column);
                return (data) -> convertString(column, fieldDefn, StandardCharsets.UTF_8, data);
            case Types.TIME:
                if (adaptiveTimeMicrosecondsPrecisionMode) {
                    return data -> convertDurationToMicroseconds(column, fieldDefn, data);
                }
            case Types.TIMESTAMP:
                return ((ValueConverter) (data -> convertTimestampToLocalDateTime(column, fieldDefn, data))).and(super.converter(column, fieldDefn));
            default:
                break;
        }
        */

        // Otherwise, let the base class handle it ...
        return super.converter(column, fieldDefn);

    }
    
    /**
     * Return the {@link Charset} instance with the SAP HANA-specific character set name used by the given column.
     *
     * @param column the column in which the character set is used; never null
     * @return the Java {@link Charset}, or null if there is no mapping
     */
    /* Have to find a class similar to com.mysql.cj.CharsetMapping
    protected Charset charsetFor(Column column) {
        String hanaCharsetName = column.charsetName();
        if (hanaCharsetName == null) {
            logger.warn("Column is missing a character set: {}", column);
            return null;
        }
        String encoding = CharsetMapping.getJavaEncodingForMysqlCharset(hanaCharsetName);
        if (encoding == null) {
            logger.warn("Column uses MySQL character set '{}', which has no mapping to a Java character set", hanaCharsetName);
        }
        else {
            try {
                return Charset.forName(encoding);
            }
            catch (IllegalCharsetNameException e) {
                logger.error("Unable to load Java charset '{}' for column with MySQL character set '{}'", encoding, hanaCharsetName);
            }
        }
        return null;
    }
    */

    
    /**
     * Converts a value representing a SAP HANA POINT for a column, to a Kafka Connect value.
     *
     * @param column the JDBC column; never null
     * @param fieldDefn the Connect field definition for this column; never null
     * @param data a data for the point column, either coming from the JDBC driver or logical decoding plugin
     * @return a value which will be used by Connect to represent the actual point value
     */
    protected Object convertGeometry(Column column, Field fieldDefn, Object data) {
        final HanaGeometry empty = HanaGeometry.createEmpty();
        return convertValue(column, fieldDefn, data, io.debezium.data.geometry.Geometry.createValue(fieldDefn.schema(), empty.getWkb(), empty.getSrid()), (r) -> {
            try {
                final Schema schema = fieldDefn.schema();
                if (data instanceof byte[]) {
                    HanaGeometry geom = HanaGeometry.fromBytes((byte[]) data);
                    r.deliver(io.debezium.data.geometry.Geometry.createValue(schema, geom.getWkb(), geom.getSrid()));
                }
                else if (data instanceof String) {
                    HanaGeometry geom = HanaGeometry.fromHexEwkb((String) data);
                    r.deliver(io.debezium.data.geometry.Geometry.createValue(schema, geom.getWkb(), geom.getSrid()));
                }
            }
            catch (IllegalArgumentException e) {
                logger.warn("Error converting to a Geometry type", column);
            }
        });
    }



    /**
     * Converts a value representing a SAP HANA point for a column, to a Kafka Connect value.
     *
     * @param column the JDBC column; never null
     * @param fieldDefn the Connect field definition for this column; never null
     * @param data a data for the point column, either coming from the JDBC driver or logical decoding plugin
     * @return a value which will be used by Connect to represent the actual point value
     */
    protected Object convertPoint(Column column, Field fieldDefn, Object data) {
    	
    	final HanaGeometry empty = HanaGeometry.createEmpty();
        return convertValue(column, fieldDefn, data, Point.createValue(fieldDefn.schema(), empty.getWkb(), empty.getSrid()), (r) -> {
            final Schema schema = fieldDefn.schema();
            
            if (data instanceof byte[]) {
                // byte array for any Geometry type, we will use our own binaryParse to parse the byte to WKB, hence
                // to the suitable class
                HanaGeometry hanaGeometry = HanaGeometry.fromBytes((byte[]) data);
                if (hanaGeometry.isPoint()) {
                    r.deliver(io.debezium.data.geometry.Point.createValue(schema, hanaGeometry.getWkb(), hanaGeometry.getSrid()));
                }
                else {
                    throw new ConnectException("Failed to parse and read a value of type POINT on " + column);
                }
            }
            else if (data instanceof String) {
                HanaGeometry hanaGeometry = HanaGeometry.fromHexEwkb((String) data);
                if (hanaGeometry.isPoint()) {
                    r.deliver(io.debezium.data.geometry.Point.createValue(schema, hanaGeometry.getWkb(), hanaGeometry.getSrid()));
                }
                else {
                    throw new ConnectException("Failed to parse and read a value of type POINT on " + column);
                }
            }

        });
    }



    protected Object convertTimestampToLocalDateTime(Column column, Field fieldDefn, Object data) {
        if (data == null) {
            return null;
        }
        if (!(data instanceof Timestamp)) {
            return data;
        }
        final Timestamp timestamp = (Timestamp) data;

        return timestamp.toLocalDateTime();
    }

    @Override
    protected int getTimePrecision(Column column) {
        return column.scale().orElse(-1);
    }

}

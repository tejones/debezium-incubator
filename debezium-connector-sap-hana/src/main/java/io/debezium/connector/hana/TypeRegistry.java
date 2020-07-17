/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.hana;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.connect.errors.ConnectException;
import org.postgresql.core.BaseConnection;
import org.postgresql.core.TypeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.hana.connection.HanaConnection;
import io.debezium.util.Collect;

/**
 * A registry of types supported by a SAP HANA instance. Allows lookup of the types according to
 * type name or ID.
 * 
 * This was copied from the PostgreSQL connector, currently not completed for the HANA usecase. 
 * Also it was using Postgre OIDs, which are being phased out
 * https://stackoverflow.com/questions/5625585/sql-postgres-oids-what-are-they-and-why-are-they-useful
 *
 * @author Joao Tavares
 *
 */
public class TypeRegistry {

    private static final Logger LOGGER = LoggerFactory.getLogger(TypeRegistry.class);

    //public static final String TYPE_NAME_GEOGRAPHY = "geography";
    public static final String TYPE_NAME_GEOMETRY = "geometry";
    //public static final String TYPE_NAME_CITEXT = "citext";
    //public static final String TYPE_NAME_HSTORE = "hstore";
    //public static final String TYPE_NAME_LTREE = "ltree";

    //public static final String TYPE_NAME_HSTORE_ARRAY = "_hstore";
    //public static final String TYPE_NAME_GEOGRAPHY_ARRAY = "_geography";
    public static final String TYPE_NAME_GEOMETRY_ARRAY = "_geometry";
    //public static final String TYPE_NAME_CITEXT_ARRAY = "_citext";
    //public static final String TYPE_NAME_LTREE_ARRAY = "_ltree";

    public static final int NO_TYPE_MODIFIER = -1;
    public static final int UNKNOWN_LENGTH = -1;

    // PostgreSQL driver reports user-defined Domain types as Types.DISTINCT
    public static final int DOMAIN_TYPE = Types.DISTINCT;

    private static final String CATEGORY_ENUM = "E";
    
    
    /* The view SYS.DATA_TYPES in SAP HANA contains all the accepted data types in the databases,
     * it does not contain information similar to this. I could be wrong...

    private static final String SQL_NON_ARRAY_TYPES = "SELECT t.oid AS oid, t.typname AS name, t.typbasetype AS parentoid, t.typtypmod as modifiers, t.typcategory as category "
            + "FROM pg_catalog.pg_type t JOIN pg_catalog.pg_namespace n ON (t.typnamespace = n.oid) "
            + "WHERE n.nspname != 'pg_toast' AND t.typcategory <> 'A'";

    private static final String SQL_ARRAY_TYPES = "SELECT t.oid AS oid, t.typname AS name, t.typelem AS element, t.typbasetype AS parentoid, t.typtypmod as modifiers, t.typcategory as category "
            + "FROM pg_catalog.pg_type t JOIN pg_catalog.pg_namespace n ON (t.typnamespace = n.oid) "
            + "WHERE n.nspname != 'pg_toast' AND t.typcategory = 'A'";

    private static final String SQL_NON_ARRAY_TYPE_NAME_LOOKUP = "SELECT t.oid as oid, t.typname AS name, t.typbasetype AS parentoid, t.typtypmod AS modifiers, t.typcategory as category "
            + "FROM pg_catalog.pg_type t JOIN pg_catalog.pg_namespace n ON (t.typnamespace = n.oid) "
            + "WHERE n.nspname != 'pg_toast' AND t.typcategory <> 'A' AND t.typname = ?";

    private static final String SQL_NON_ARRAY_TYPE_OID_LOOKUP = "SELECT t.oid as oid, t.typname AS name, t.typbasetype AS parentoid, t.typtypmod AS modifiers, t.typcategory as category "
            + "FROM pg_catalog.pg_type t JOIN pg_catalog.pg_namespace n ON (t.typnamespace = n.oid) "
            + "WHERE n.nspname != 'pg_toast' AND t.typcategory <> 'A' AND t.oid = ?";

    private static final String SQL_ENUM_VALUES_LOOKUP = "select t.enumlabel as enum_value "
            + "FROM pg_catalog.pg_enum t "
            + "WHERE t.enumtypid=? ORDER BY t.enumsortorder";
            
         */
    
    private static final String SQL_TYPES = "SELECT dt.type_id AS id, dt.type_name AS name"
            + "FROM SYS.DATA_TYPES dt";

    private static final Map<String, String> LONG_TYPE_NAMES = Collections.unmodifiableMap(getLongTypeNames());

    private static Map<String, String> getLongTypeNames() {
        Map<String, String> longTypeNames = new HashMap<>();
        
        //Character string types
        longTypeNames.put("character varying non-unicode", "VARCHAR");
        longTypeNames.put("character varying unicode", "NVARCHAR");
        longTypeNames.put("variable-length alphanumeric character string", "ALPHANUM");  
        longTypeNames.put("variable-length character string that supports text search features", "SHORTTEXT");  
        
        //Datetime types
        longTypeNames.put("date YYYY-MM-DD", "DATE");
        longTypeNames.put("time without time zone HH24:MI:SS", "TIME");
        longTypeNames.put("date with time YYYY-MM-DD HH24:MI:SS", "SECONDDATE");
        longTypeNames.put("timestamp YYYY-MM-DD HH24:MI:SS.FF7. FF", "TIMESTAMP");
        
        //Numeric types
        longTypeNames.put("tinyint 8-bit unsigned integer", "TINYINT");
        longTypeNames.put("smallint 16-bit signed integer", "SMALLINT");
        longTypeNames.put("integer 32-bit signed integer", "INTEGER");        
        longTypeNames.put("bigint 64-bit signed integer", "BIGINT");
        longTypeNames.put("decimal fixed-point decimals", "DECIMAL");
        longTypeNames.put("smalldecimal fixed-point decimals", "SMALLDECIMAL");
        longTypeNames.put("real single-precision, 32-bit floating-point number", "REAL");
        longTypeNames.put("double-precision, 64-bit floating-point number", "DOUBLE");
        longTypeNames.put("float 32-bit or 64-bit real number", "FLOAT");
        
        //Boolean Type
        longTypeNames.put("boolean values", "BOOLEAN");
        
        //Binary types
        longTypeNames.put("binary data", "VARBINARY");

        //Large Object types
        longTypeNames.put("large amounts of binary data", "BLOB");
        longTypeNames.put("large amounts of 7-bit ASCII character data", "CLOB");
        longTypeNames.put("large Unicode character object", "NCLOB");
        longTypeNames.put("enables text search features", "TEXT");
        longTypeNames.put("enables text search features, possible to insert binary data", "BINTEXT");
        
        //Multi-valued types
        longTypeNames.put("collections of values sharing the same data type", "ARRAY");
        
        //Spatial types
        longTypeNames.put("point spacial", "ST_Point");
        longTypeNames.put("geometry spacial", "ST_Geometry");

        return longTypeNames;
    }

    private final Map<String, HanaType> nameToType = new HashMap<>();
    private final Map<Integer, HanaType> idToType = new HashMap<>();

    private final HanaConnection connection;

    private int geometryOid = Integer.MIN_VALUE;
    //private int geographyOid = Integer.MIN_VALUE;
    //private int citextOid = Integer.MIN_VALUE;
    //private int hstoreOid = Integer.MIN_VALUE;
    //private int ltreeOid = Integer.MIN_VALUE;

    //private int hstoreArrayOid = Integer.MIN_VALUE;
    private int geometryArrayOid = Integer.MIN_VALUE;
    //private int geographyArrayOid = Integer.MIN_VALUE;
    //private int citextArrayOid = Integer.MIN_VALUE;
    //private int ltreeArrayOid = Integer.MIN_VALUE;


    public TypeRegistry(HanaConnection connection) {
        this.connection = connection;
        //prime();
    }


    private void addType(HanaType type) {
        idToType.put(type.getOid(), type);
        nameToType.put(type.getName(), type);

        if (TYPE_NAME_GEOMETRY.equals(type.getName())) {
            geometryOid = type.getOid();
        }
        else if (TYPE_NAME_GEOMETRY_ARRAY.equals(type.getName())) {
            geometryArrayOid = type.getOid();
        }
    }

    /**
     *
     * @param oid - SAP HANA OID
     * @return type associated with the given OID
     */
    /*
    public HanaType get(int id) {
        HanaType r = idToType.get(id);
        if (r == null) {
            r = resolveUnknownType(id);
            if (r == null) {
                LOGGER.warn("Unknown OID {} requested", id);
                r = HanaType.UNKNOWN;
            }
        }
        return r;
    }
    */

    /**
     *
     * @param name - PostgreSQL type name
     * @return type associated with the given type name
     */
    /*
    public HanaType get(String name) {
        switch (name) {
            case "serial":
                name = "int4";
                break;
            case "smallserial":
                name = "int2";
                break;
            case "bigserial":
                name = "int8";
                break;
        }
        String[] parts = name.split("\\.");
        if (parts.length > 1) {
            name = parts[1];
        }
        if (name.charAt(0) == '"') {
            name = name.substring(1, name.length() - 1);
        }
        HanaType r = nameToType.get(name);
        if (r == null) {
            r = resolveUnknownType(name);
            if (r == null) {
                LOGGER.warn("Unknown type named {} requested", name);
                r = HanaType.UNKNOWN;
            }
        }
        return r;
    }
    */

    /**
     *
     * @return OID for {@code GEOMETRY} type of this PostgreSQL instance
     */
    public int geometryOid() {
        return geometryOid;
    }
  

    /**
     *
     * @return OID for array of {@code GEOMETRY} type of this PostgreSQL instance
     */
    public int geometryArrayOid() {
        return geometryArrayOid;
    }


    /**
     * Converts a type name in long (readable) format like <code>boolean</code> to s standard
     * data type name like <code>bool</code>.
     *
     * @param typeName - a type name in long format
     * @return - the type name in standardized format
     */
    public static String normalizeTypeName(String typeName) {
        return LONG_TYPE_NAMES.getOrDefault(typeName, typeName);
    }

    /**
     * Prime the {@link TypeRegistry} with all existing database types
     */
    // Still have to change this
    private void prime() {
        Connection hanaConnection = null;
        try {
        	hanaConnection = connection.connection();

            final TypeInfo typeInfo = ((BaseConnection) hanaConnection).getTypeInfo();
            final SqlTypeMapper sqlTypeMapper = new SqlTypeMapper(hanaConnection, typeInfo);

            try (final Statement statement = hanaConnection.createStatement()) {
                // Read types
                try (final ResultSet rs = statement.executeQuery(SQL_TYPES)) {
                    final List<HanaType.Builder> delayResolvedBuilders = new ArrayList<>();
                    while (rs.next()) {
                        // Coerce long to int so large unsigned values are represented as signed
                        // Same technique is used in TypeInfoCache
                        final int id = (int) rs.getLong("id");
                        String typeName = rs.getString("name");

                        HanaType.Builder builder = new HanaType.Builder(
                                this,
                                typeName,
                                id,
                                sqlTypeMapper.getSqlType(typeName),
                                0,
                                typeInfo);
                        
                        /*
                        if (CATEGORY_ENUM.equals(category)) {
                            builder = builder.enumValues(resolveEnumValues(pgConnection, oid));
                        }
                        
                        // If the type does have have a base type, we can build/add immediately.
                        if (parentTypeOid == 0) {
                            addType(builder.build());
                            continue;
                        }

                        // For types with base type mappings, they need to be delayed.
                        builder = builder.parentType(parentTypeOid);
                        delayResolvedBuilders.add(builder);
                        */
                    }

                    // Resolve delayed builders
                    for (HanaType.Builder builder : delayResolvedBuilders) {
                        addType(builder.build());
                    }
                }
                	
                // Read array types
	
            }

        }
        catch (SQLException e) {
            if (hanaConnection == null) {
                throw new ConnectException("Could not create Hana connection", e);
            }
            else {
                throw new ConnectException("Could not initialize type registry", e);
            }
        }
    }

    /*
    private HanaType resolveUnknownType(String name) {
        try {
            LOGGER.trace("Type '{}' not cached, attempting to lookup from database.", name);
            final Connection connection = this.connection.connection();
            final TypeInfo typeInfo = ((BaseConnection) connection).getTypeInfo();
            final SqlTypeMapper sqlTypeMapper = new SqlTypeMapper(connection, typeInfo);

            try (final PreparedStatement statement = connection.prepareStatement(SQL_NON_ARRAY_TYPE_NAME_LOOKUP)) {
                statement.setString(1, name);
                try (final ResultSet rs = statement.executeQuery()) {
                    while (rs.next()) {
                        final int oid = (int) rs.getLong("oid");
                        final int parentTypeOid = (int) rs.getLong("parentoid");
                        final int modifiers = (int) rs.getLong("modifiers");
                        String typeName = rs.getString("name");
                        String category = rs.getString("category");

                        HanaType.Builder builder = new HanaType.Builder(
                                this,
                                typeName,
                                oid,
                                sqlTypeMapper.getSqlType(typeName),
                                modifiers,
                                typeInfo);

                        if (CATEGORY_ENUM.equals(category)) {
                            builder = builder.enumValues(resolveEnumValues(connection, oid));
                        }

                        HanaType result = builder.parentType(parentTypeOid).build();
                        addType(result);

                        return result;
                    }
                }
            }
        }
        catch (SQLException e) {
            throw new ConnectException("Database connection failed during resolving unknown type", e);
        }

        return null;
    }

    private HanaType resolveUnknownType(int lookupOid) {
        try {
            LOGGER.trace("Type OID '{}' not cached, attempting to lookup from database.", lookupOid);
            final Connection connection = this.connection.connection();
            final TypeInfo typeInfo = ((BaseConnection) connection).getTypeInfo();
            final SqlTypeMapper sqlTypeMapper = new SqlTypeMapper(connection, typeInfo);

            try (final PreparedStatement statement = connection.prepareStatement(SQL_NON_ARRAY_TYPE_OID_LOOKUP)) {
                statement.setInt(1, lookupOid);
                try (final ResultSet rs = statement.executeQuery()) {
                    while (rs.next()) {
                        final int oid = (int) rs.getLong("oid");
                        final int parentTypeOid = (int) rs.getLong("parentoid");
                        final int modifiers = (int) rs.getLong("modifiers");
                        String typeName = rs.getString("name");
                        String category = rs.getString("category");

                        HanaType.Builder builder = new HanaType.Builder(
                                this,
                                typeName,
                                oid,
                                sqlTypeMapper.getSqlType(typeName),
                                modifiers,
                                typeInfo);

                        if (CATEGORY_ENUM.equals(category)) {
                            builder = builder.enumValues(resolveEnumValues(connection, oid));
                        }

                        HanaType result = builder.parentType(parentTypeOid).build();
                        addType(result);

                        return result;
                    }
                }
            }
        }
        catch (SQLException e) {
            throw new ConnectException("Database connection failed during resolving unknown type", e);
        }

        return null;
    }

    private List<String> resolveEnumValues(Connection pgConnection, int enumOid) throws SQLException {
        List<String> enumValues = new ArrayList<>();
        try (final PreparedStatement enumStatement = pgConnection.prepareStatement(SQL_ENUM_VALUES_LOOKUP)) {
            enumStatement.setInt(1, enumOid);
            try (final ResultSet enumRs = enumStatement.executeQuery()) {
                while (enumRs.next()) {
                    enumValues.add(enumRs.getString("enum_value"));
                }
            }
        }
        return enumValues.isEmpty() ? null : enumValues;
    }
     */

    /**
     * Allows to obtain the SQL type corresponding to HANA types.
     *
     */
    private static class SqlTypeMapper {

        /**
         * View that shows the available Data types
         */
        private static final String SQL_TYPE_DETAILS = " SELECT * FROM SYS.DATA_TYPES dt ;";

        private final TypeInfo typeInfo;
        private final Set<String> preloadedSqlTypes;
        private final Map<String, Integer> sqlTypesByPgTypeNames;

        private SqlTypeMapper(Connection db, TypeInfo typeInfo) throws SQLException {
            this.typeInfo = typeInfo;
            this.preloadedSqlTypes = Collect.unmodifiableSet(typeInfo.getPGTypeNamesWithSQLTypes());
            this.sqlTypesByPgTypeNames = getSqlTypes(db, typeInfo);
        }

        public int getSqlType(String typeName) throws SQLException {
            boolean isCoreType = preloadedSqlTypes.contains(typeName);

            // obtain core types such as bool, int2 etc. from the driver, as it correctly maps these types to the JDBC
            // type codes. Also those values are cached in TypeInfoCache.
            if (isCoreType) {
                return typeInfo.getSQLType(typeName);
            }
            if (typeName.endsWith("[]")) {
                return Types.ARRAY;
            }
            // get custom type mappings from the map which was built up with a single query
            else {
                try {
                    return sqlTypesByPgTypeNames.get(typeName);
                }
                catch (Exception e) {
                    LOGGER.warn("Failed to obtain SQL type information for type {} via custom statement, falling back to TypeInfo#getSQLType()", typeName, e);
                    return typeInfo.getSQLType(typeName);
                }
            }
        }

        /**
         * Builds up a map of SQL (JDBC) types by PG type name; contains only values for non-core types.
         */
        private static Map<String, Integer> getSqlTypes(Connection db, TypeInfo typeInfo) throws SQLException {
            Map<String, Integer> sqlTypesByPgTypeNames = new HashMap<>();

            try (final Statement statement = db.createStatement()) {
                try (final ResultSet rs = statement.executeQuery(SQL_TYPE_DETAILS)) {
                    while (rs.next()) {
                        int type;
                        boolean isArray = rs.getBoolean(2);
                        String typtype = rs.getString(3);
                        if (isArray) {
                            type = Types.ARRAY;
                        }
                        else if ("c".equals(typtype)) {
                            type = Types.STRUCT;
                        }
                        else if ("d".equals(typtype)) {
                            type = Types.DISTINCT;
                        }
                        else if ("e".equals(typtype)) {
                            type = Types.VARCHAR;
                        }
                        else {
                            type = Types.OTHER;
                        }

                        sqlTypesByPgTypeNames.put(rs.getString(1), type);
                    }
                }
            }

            return sqlTypesByPgTypeNames;
        }
    }
}

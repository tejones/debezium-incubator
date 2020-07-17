/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.hana;

import java.util.List;
import java.util.Objects;

import org.postgresql.core.Oid;
import org.postgresql.core.TypeInfo;

/**
 * A class that binds together a SAP HANA OID, JDBC type id and the string name of the type.
 * The array types contain link to their element type.
 *
 * @author Joao Tavares
 *
 */
public class HanaType {

    public static final HanaType UNKNOWN = new HanaType("unknown", -1, Integer.MIN_VALUE, null, null, null, null);

    private final String name;
    private final int oid;
    private final int jdbcId;
    private final HanaType parentType;
    private final HanaType elementType;
    private final TypeInfo typeInfo;
    private final int modifiers;
    private final List<String> enumValues;

    private HanaType(String name, int oid, int jdbcId, TypeInfo typeInfo, List<String> enumValues, HanaType parentType, HanaType elementType) {
        this(name, oid, jdbcId, TypeRegistry.NO_TYPE_MODIFIER, typeInfo, enumValues, parentType, elementType);
    }

    private HanaType(String name, int oid, int jdbcId, int modifiers, TypeInfo typeInfo, List<String> enumValues, HanaType parentType, HanaType elementType) {
        Objects.requireNonNull(name);
        this.name = name;
        this.oid = oid;
        this.jdbcId = jdbcId;
        this.typeInfo = typeInfo;
        this.parentType = parentType;
        this.elementType = elementType;
        this.modifiers = modifiers;
        this.enumValues = enumValues;
    }

    /**
     * @return true if this type is an array
     */
    public boolean isArrayType() {
        return elementType != null;
    }

    /**
     * The type system allows for the creation of user defined types (UDTs) which can be based
     * on any existing type.  When a type does not extend another type, it is considered to be
     * a base or root type in the type hierarchy.
     *
     * @return true if this type is a base/root type
     */
    public boolean isRootType() {
        return parentType == null;
    }

    /**
     * @return true if this type is an enum type
     */
    public boolean isEnumType() {
        return enumValues != null;
    }

    /**
     *
     * @return symbolic name of the type
     */
    public String getName() {
        return name;
    }

    /**
     *
     * @return SAP HANA OID of this type
     */
    public int getOid() {
        return oid;
    }

    /**
     *
     * @return JDBC id of the type as reported by JDBC metadata
     */
    public int getJdbcId() {
        return jdbcId;
    }

    /**
     *
     * @return the type of element in arrays or null for primitive types
     */
    public HanaType getElementType() {
        return elementType;
    }

    /**
     *
     * @return the parent sap hana type this type is based upon
     */
    public HanaType getParentType() {
        return parentType;
    }

    /**
     *
     * @return the sap hana type at the top/root level for this type's hierarchy
     */
    public HanaType getRootType() {
    	HanaType rootType = this;
        while (!rootType.isRootType()) {
            rootType = rootType.getParentType();
        }
        return rootType;
    }

    public List<String> getEnumValues() {
        return enumValues;
    }

    /**
     *
     * @return the default length of the type
     */
    public int getDefaultLength() {
        if (typeInfo == null) {
            return TypeRegistry.UNKNOWN_LENGTH;
        }
        if (parentType != null) {
            if (modifiers == TypeRegistry.NO_TYPE_MODIFIER) {
                return parentType.getDefaultLength();
            }
            else {
                int size = typeInfo.getPrecision(parentType.getOid(), modifiers);
                if (size == 0) {
                    size = typeInfo.getDisplaySize(parentType.getOid(), modifiers);
                }
                if (size != 0 && size != Integer.MAX_VALUE) {
                    return size;
                }
            }
        }
        int size = typeInfo.getPrecision(oid, modifiers);
        if (size == 0) {
            size = typeInfo.getDisplaySize(oid, modifiers);
        }
        return size;
    }

    /**
     *
     * @return the default scale of the type
     */
    public int getDefaultScale() {
        if (typeInfo == null) {
            return TypeRegistry.UNKNOWN_LENGTH;
        }
        if (parentType != null) {
            if (modifiers == TypeRegistry.NO_TYPE_MODIFIER) {
                return parentType.getDefaultScale();
            }
            else {
                return typeInfo.getScale(parentType.getOid(), modifiers);
            }
        }
        return typeInfo.getScale(oid, modifiers);
    }

    /**
     * @param modifier - type modifier coming from decoder
     * @return length of the type based on the modifier
     */
    public int length(int modifier) {
        if (typeInfo == null) {
            return TypeRegistry.UNKNOWN_LENGTH;
        }
        switch (oid) {
            case Oid.TIMESTAMP:
            case Oid.TIMESTAMPTZ:
            case Oid.TIME:
            case Oid.TIMETZ:
            case Oid.INTERVAL:
                return typeInfo.getPrecision(oid, modifier);
        }
        return modifier;
    }

    /**
     * Get the underlying sap hana type information object
     * @return the type information object; may be null
     */
    public TypeInfo getTypeInfo() {
        return typeInfo;
    }

    /**
     * @param modifier - type modifier coming from decoder
     * @return scale of the type based on the modifier
     */
    public int scale(int modifier) {
        if (typeInfo == null) {
            return TypeRegistry.UNKNOWN_LENGTH;
        }
        switch (oid) {
            case Oid.TIMESTAMP:
            case Oid.TIMESTAMPTZ:
            case Oid.TIME:
            case Oid.TIMETZ:
            case Oid.INTERVAL:
                return typeInfo.getScale(oid, modifier);
        }
        return getDefaultScale();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + oid;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        HanaType other = (HanaType) obj;
        if (oid != other.oid) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "HanaType [name=" + name + ", oid=" + oid + ", jdbcId=" + jdbcId + ", modifiers=" + modifiers + ", defaultLength=" + getDefaultLength()
                + ", defaultScale=" + getDefaultScale() + ", parentType=" + parentType + ", elementType=" + elementType + "]";
    }

    public static class Builder {
        private final TypeRegistry typeRegistry;
        private final String name;
        private final int oid;
        private final int jdbcId;
        private final int modifiers;
        private final TypeInfo typeInfo;
        private int parentTypeOid;
        private int elementTypeOid;
        private List<String> enumValues;

        public Builder(TypeRegistry typeRegistry, String name, int oid, int jdbcId, int modifiers, TypeInfo typeInfo) {
            this.typeRegistry = typeRegistry;
            this.name = name;
            this.oid = oid;
            this.jdbcId = jdbcId;
            this.typeInfo = typeInfo;
            this.modifiers = modifiers;
        }

        public Builder parentType(int parentTypeOid) {
            this.parentTypeOid = parentTypeOid;
            return this;
        }

        public Builder elementType(int elementTypeOid) {
            this.elementTypeOid = elementTypeOid;
            return this;
        }

        public Builder enumValues(List<String> enumValues) {
            this.enumValues = enumValues;
            return this;
        }

        public HanaType build() {
        	
        	HanaType parentType = null;
        	
        	/*
            if (parentTypeOid != 0) {
                parentType = typeRegistry.get(parentTypeOid);
            }
          	*/

            HanaType elementType = null;
            
            /*
            if (elementTypeOid != 0) {
                elementType = typeRegistry.get(elementTypeOid);
            }
            */

            return new HanaType(name, oid, jdbcId, modifiers, typeInfo, enumValues, parentType, elementType);
        }
    }
}

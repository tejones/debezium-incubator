/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.hana;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import io.debezium.util.HexConverter;

/**
 * A parser API for SAP HANA Geometry types
 *
 * @author Joao Tavares
 */
public class HanaGeometry {
	
    // WKB constants from http://www.opengeospatial.org/standards/sfa
    private static final int WKB_POINT_SIZE = (1 + 4 + 8 + 8); // fixed size

    /**
     * Static Hex EKWB for a GEOMETRYCOLLECTION EMPTY.
     */
    private static final String HEXEWKB_EMPTY_GEOMETRYCOLLECTION = "010700000000000000";
    
    // 0x010700000000000000
    private static final byte[] WKB_EMPTY_GEOMETRYCOLLECTION = { 0x01, 0x07, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 }; 


    /**
     * PostGIS Extended-Well-Known-Binary (EWKB) geometry representation. An extension of the
     * Open Geospatial Consortium Well-Known-Binary format. Since EWKB is a superset of
     * WKB, we use EWKB here.
     * http://www.opengeospatial.org/standards/sfa
     */
    private final byte[] wkb;

    /**
     * Coordinate reference system identifier. While it's technically user-defined,
     * the standard/common values in use are the EPSG code list http://www.epsg.org/
     * null if unset/unspecified
     */
    private final Integer srid;
    
    /**
     * Create a HanaGeometry using the supplied EWKB and SRID.
     *
     * @param ewkb the Extended Well-Known binary representation of the coordinate in the standard format
     * @param srid the coordinate system identifier (SRID); null if unset/unknown
     */
    private HanaGeometry(byte[] ewkb, Integer srid) {
        this.wkb = ewkb;
        this.srid = srid;
    }

    /**
     * Create a HanaGeometry the supplied wkb String
     * SRID is extracted from the EWKB
     * @param wkb the Well-Known binary representation of the coordinate in the standard format
     */
    public static HanaGeometry fromHexEwkb(String hexEwkb) {
        byte[] ewkb = HexConverter.convertFromHex(hexEwkb);
        return fromEwkb(ewkb);
    }

    /**
     * Create a HanaGeometry using the supplied EWKB Byte Array
     * SRID is extracted from the EWKB
     */
    public static HanaGeometry fromEwkb(byte[] ewkb) {
        return new HanaGeometry(ewkb, parseSrid(ewkb));
    }

    /**
     * Create a GEOMETRYCOLLECTION EMPTY PostgisGeometry
     *
     * @return a {@link HanaGeometry} which represents a PostgisGeometry API
     */
    public static HanaGeometry createEmpty() {
        return new HanaGeometry(WKB_EMPTY_GEOMETRYCOLLECTION, null);
    }

    /**
     * Create a HanaGeometry from the original byte array
     *
     * @param mysqlBytes he original byte array from MySQL binlog event
     *
     * @return a {@link MySqlGeometry} which represents a MySqlGeometry API
     */
    public static HanaGeometry fromBytes(final byte[] hanaBytes) {
        ByteBuffer buf = ByteBuffer.wrap(hanaBytes);
        buf.order(ByteOrder.LITTLE_ENDIAN);

        // first 4 bytes are SRID
        Integer srid = buf.getInt();
        if (srid == 0) {
            // Debezium uses null for an unset/unknown SRID
            srid = null;
        }

        // remainder is WKB
        byte[] wkb = new byte[buf.remaining()];
        buf.get(wkb);
        return new HanaGeometry(wkb, srid);
    }

    /**
     * Returns the standard well-known binary representation
     *
     * @return {@link byte[]} which represents the standard well-known binary
     */
    public byte[] getWkb() {
        return wkb;
    }

    /**
     * Returns the coordinate reference system identifier (SRID)
     * @return srid
     */
    public Integer getSrid() {
        return srid;
    }
    
    
    /**
     * Returns whether this geometry is a 2D POINT type.
     * @return true if the geometry is a 2D Point.
     */
    public boolean isPoint() {
        return wkb.length == WKB_POINT_SIZE;
    }

    /**
     * Parses an EWKB Geometry and extracts the SRID (if any)
     *
     * @return Geometry SRID or null if there is no SRID.
     */
    private static Integer parseSrid(byte[] ewkb) {
        if (ewkb.length < 9) {
            throw new IllegalArgumentException("Invalid EWKB length");
        }

        final ByteBuffer reader = ByteBuffer.wrap(ewkb);

        // Read the BOM
        reader.order((reader.get() != 0) ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN);

        int geomType = reader.getInt();
        final int EWKB_SRID = 0x20000000;
        // SRID flag in the type integer
        if ((geomType & EWKB_SRID) != 0) {
            // SRID is set
            // value is encoded as a 4 byte integer right after the type integer.
            return reader.getInt();
        }
        return null;
    }
}

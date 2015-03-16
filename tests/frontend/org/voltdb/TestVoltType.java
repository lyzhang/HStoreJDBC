/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package org.voltdb;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.Date;

import junit.framework.TestCase;

import org.voltdb.types.TimestampType;

public class TestVoltType extends TestCase {

    public void testGet() {
        boolean caught = false;
        try {
            VoltType.get(Byte.MAX_VALUE);
        } catch (AssertionError ex) {
            caught = true;
        }
        assertTrue(caught);

        VoltType vt = VoltType.get((byte)3);
        assertTrue(vt.getValue() == VoltType.TINYINT.getValue());
    }

    public void testTypeFromString() {
        assertEquals(VoltType.TINYINT, VoltType.typeFromString("TINYINT"));
        assertEquals(VoltType.SMALLINT, VoltType.typeFromString("SMALLINT"));
        assertEquals(VoltType.INTEGER, VoltType.typeFromString("INTEGER"));
        assertEquals(VoltType.BIGINT, VoltType.typeFromString("BIGINT"));
        assertEquals(VoltType.FLOAT, VoltType.typeFromString("FLOAT"));
        assertEquals(VoltType.FLOAT, VoltType.typeFromString("DOUBLE"));      // also floats
        assertEquals(VoltType.TIMESTAMP, VoltType.typeFromString("TIMESTAMP"));
        assertEquals(VoltType.STRING, VoltType.typeFromString("STRING"));
        assertEquals(VoltType.VOLTTABLE, VoltType.typeFromString("VOLTTABLE"));
        assertEquals(VoltType.STRING, VoltType.typeFromString("CHAR"));
        assertEquals(VoltType.STRING, VoltType.typeFromString("VARCHAR"));
        assertEquals(VoltType.TIMESTAMP, VoltType.typeFromString("TIMESTAMP"));
        assertEquals(VoltType.DECIMAL, VoltType.typeFromString("DECIMAL"));
        boolean caught = false;
        try {
            VoltType.typeFromString("Muhahaha");
        } catch (RuntimeException ex) {
           caught = true;
        }
        assertTrue(caught);
    }

    public void testGetLengthInBytesForFixedTypes() {
        assertEquals(1, VoltType.TINYINT.getLengthInBytesForFixedTypes());
        assertEquals(2, VoltType.SMALLINT.getLengthInBytesForFixedTypes());
        assertEquals(4, VoltType.INTEGER.getLengthInBytesForFixedTypes());
        assertEquals(8, VoltType.BIGINT.getLengthInBytesForFixedTypes());
        assertEquals(8, VoltType.FLOAT.getLengthInBytesForFixedTypes());
        assertEquals(8, VoltType.TIMESTAMP.getLengthInBytesForFixedTypes());
        assertEquals(16, VoltType.DECIMAL.getLengthInBytesForFixedTypes());
        boolean caught = false;
        try {
            VoltType.STRING.getLengthInBytesForFixedTypes();
        } catch (RuntimeException ex) {
            caught = true;
        }
        assertTrue(caught);
    }

    public void testToSQLString() {
        assertEquals("tinyint", VoltType.TINYINT.toSQLString());
        assertEquals("smallint", VoltType.SMALLINT.toSQLString());
        assertEquals("integer", VoltType.INTEGER.toSQLString());
        assertEquals("bigint", VoltType.BIGINT.toSQLString());
        assertEquals("float", VoltType.FLOAT.toSQLString());
        assertEquals("timestamp", VoltType.TIMESTAMP.toSQLString());
        assertEquals("decimal", VoltType.DECIMAL.toSQLString());
        assertEquals("varchar", VoltType.STRING.toSQLString());
        assertNull(VoltType.VOLTTABLE.toSQLString());
    }

    public void testTypeFromObject() {
        VoltType vt;
        vt = VoltType.typeFromObject(new Byte((byte) 0));
        assertTrue(vt.getValue() == VoltType.TINYINT.getValue());

        vt = VoltType.typeFromObject(new Short((short) 0));
        assertTrue(vt.getValue() == VoltType.SMALLINT.getValue());

        boolean caught = false;
        try {
            VoltType.typeFromClass(Class.class);
        } catch (RuntimeException ex) {
            caught = true;
        }
        assertTrue(caught);
    }

    public void testEquivalences() {

        assertEquals(VoltType.typeFromString("TINYINT"), VoltType.typeFromClass(Byte.class));
        assertEquals(VoltType.typeFromString("SMALLINT"), VoltType.typeFromClass(Short.class));
        assertEquals(VoltType.typeFromString("INTEGER"), VoltType.typeFromClass(Integer.class));
        assertEquals(VoltType.typeFromString("BIGINT"), VoltType.typeFromClass(Long.class));
        assertEquals(VoltType.typeFromString("FLOAT"), VoltType.typeFromClass(Float.class));
        assertEquals(VoltType.typeFromString("DOUBLE"), VoltType.typeFromClass(Double.class));
        assertEquals(VoltType.typeFromString("TIMESTAMP"), VoltType.typeFromClass(TimestampType.class));
        assertEquals(VoltType.typeFromString("STRING"), VoltType.typeFromClass(String.class));
        assertEquals(VoltType.typeFromString("VOLTTABLE"), VoltType.typeFromClass(VoltTable.class));
        assertEquals(VoltType.typeFromString("DECIMAL"), VoltType.typeFromClass(BigDecimal.class));
    }

    /* round trip the constructors */
    public void testTimestampCreation() {
        long usec = 999999999;
        TimestampType ts1 = new TimestampType(usec);
        assertEquals(usec, ts1.getTime());

        usec = 999999000;
        ts1 = new TimestampType(usec);
        assertEquals(usec, ts1.getTime());
    }

    /* Compare some values that differ by microseconds and by full millis */
    public void testTimestampEquality() {
        TimestampType ts1 = new TimestampType(150000);
        TimestampType ts2 = new TimestampType(150000);
        TimestampType ts3 = new TimestampType(150001);
        TimestampType ts4 = new TimestampType(160000);

        assertTrue(ts1.equals(ts2));
        assertTrue(ts2.equals(ts1));
        assertFalse(ts1.equals(ts3));
        assertFalse(ts3.equals(ts1));
        assertFalse(ts1.equals(ts4));
        assertFalse(ts4.equals(ts1));

        assertTrue(ts1.compareTo(ts2) == 0);
        assertTrue(ts2.compareTo(ts1) == 0);
        assertTrue(ts1.compareTo(ts3) < 0);
        assertTrue(ts3.compareTo(ts1) > 0);
        assertTrue(ts1.compareTo(ts4) < 0);
        assertTrue(ts4.compareTo(ts1) > 0);
    }

    public void testTimestampToString() {
        // I suppose these could fall across minute boundaries and fail the
        // test.. but that would seem exceedingly unlikely? Do this a few times
        // to try to avoid the false negative.
        for (int ii=0; ii < 5; ++ii) {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
            TimestampType now = new TimestampType();
            Date date = new Date();
            if (now.toString().startsWith(sdf.format(date))) {
                assertTrue(now.toString().startsWith(sdf.format(date)));
                return;
            }
        }
        fail();
    }

}

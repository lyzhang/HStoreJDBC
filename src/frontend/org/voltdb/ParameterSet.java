/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.voltdb.messaging.FastDeserializer;
import org.voltdb.messaging.FastSerializable;
import org.voltdb.messaging.FastSerializer;
import org.voltdb.types.TimestampType;
import org.voltdb.types.VoltDecimalHelper;

import edu.brown.pools.Poolable;
import edu.brown.utils.StringUtil;

/**
 * The ordered set of parameters of the proper types that is passed into
 * a stored procedure OR a plan fragment.
 */
public class ParameterSet implements FastSerializable, Poolable {

    static final byte ARRAY = -99;
    public static final ParameterSet EMPTY = new ParameterSet();
    
    private final boolean m_serializingToEE;
    private Object m_params[] = new Object[0];
    
    public ParameterSet() {
        this(false);
    }

    public ParameterSet(Object...params) {
        this(false);
        m_params = params;
    }
    
    public ParameterSet(boolean serializingToEE) {
        m_serializingToEE = serializingToEE;
    }
    
    @Override
    public boolean isInitialized() {
        return false;
    }
    
    @Override
    public void finish() {
        this.m_params = null;
    }

    /**
     * Sets the internal array to params. Note: this does *not* copy the argument.
     */
    public ParameterSet setParameters(Object... params) {
        this.m_params = params;
        return (this);
    }
    
    /**
     * Set the internal array of this ParameterSet to the same as the one given
     * @param other
     * @return
     */
    public ParameterSet setParameters(ParameterSet other) {
        this.m_params = other.m_params;
        return (this);
    }
    
    @Override
    public int hashCode() {
        return Arrays.hashCode(m_params);
    }

    public void clear() {
        this.m_params = null;
    }
    
    public Object[] toArray() {
        return m_params;
    }
    
    public int size() {
        return m_params.length;
    }

    static Object getParameterAtIndex(int partitionIndex, ByteBuffer unserializedParams) throws IOException {
        FastDeserializer in = new FastDeserializer(unserializedParams);
        int paramLen = in.readShort();
        if (partitionIndex >= paramLen) {
            // error if caller desires out of bounds parameter
            throw new RuntimeException("Invalid partition parameter requested.");
        }
        for (int i = 0; i < partitionIndex; ++i) {
            readOneParameter(in);
        }
        Object retval = readOneParameter(in);
        unserializedParams.rewind();
        return retval;
    }

    @Override
    public void readExternal(FastDeserializer in) throws IOException {
        int paramLen = in.readShort();
        m_params = new Object[paramLen];

        for (int i = 0; i < paramLen; i++) {
            m_params[i] = readOneParameter(in);
        }
    }

    @Override
    public void writeExternal(FastSerializer out) throws IOException {
        out.writeShort(m_params.length);

        for (Object obj : m_params) {
            if (obj == null) {
                VoltType type = VoltType.NULL;
                out.writeByte(type.getValue());
                continue;
            }

            Class<?> cls = obj.getClass();
            if (cls.isArray()) {

                // EE doesn't support array parameters. Arrays of bytes are
                // only useful to strings. Special case them here.
                if (m_serializingToEE && obj instanceof byte[]) {
                    final byte[] b = (byte[]) obj;
                    if (b.length > VoltType.MAX_VALUE_LENGTH) {
                        throw new VoltOverflowException(
                                "Value of string byte[] larger than allowed max " + VoltType.MAX_VALUE_LENGTH_STR);
                    }
                    out.writeByte(VoltType.STRING.getValue());
                    out.writeInt(b.length);
                    out.write(b);
                    continue;
                }

                out.writeByte(ARRAY);
                VoltType type = VoltType.typeFromClass(cls.getComponentType());
                out.writeByte(type.getValue());
                switch (type) {
                    case TINYINT:
                        out.writeArray((byte[])obj);
                        break;
                    case SMALLINT:
                        out.writeArray((short[]) obj);
                        break;
                    case INTEGER:
                        out.writeArray((int[]) obj);
                        break;
                    case BIGINT:
                        out.writeArray((long[]) obj);
                        break;
                    case FLOAT:
                        out.writeArray((double[]) obj);
                        break;
                    case STRING:
                        out.writeArray((String[]) obj);
                        break;
                    case TIMESTAMP:
                        out.writeArray((TimestampType[]) obj);
                        break;
                    case DECIMAL:
                        // converted long128 in serializer api
                        out.writeArray((BigDecimal[]) obj);
                        break;
                    case BOOLEAN:
                        out.writeArray((boolean[]) obj);
                        break;
                    case VOLTTABLE:
                        out.writeArray((VoltTable[]) obj);
                        break;
                    default:
                        throw new RuntimeException("FIXME: Unsupported type " + type);
                }
                continue;
            }

            // Handle NULL mappings not encoded by type.min_value convention
            if (obj == VoltType.NULL_TIMESTAMP) {
                out.writeByte(VoltType.TIMESTAMP.getValue());
                out.writeLong(VoltType.NULL_BIGINT);  // corresponds to EE value.h isNull()
                continue;
            }
            else if (obj == VoltType.NULL_STRING) {
                out.writeByte(VoltType.STRING.getValue());
                out.writeInt(VoltType.NULL_STRING_LENGTH);
                continue;
            }
            else if (obj == VoltType.NULL_DECIMAL) {
                out.writeByte(VoltType.DECIMAL.getValue());
                VoltDecimalHelper.serializeNull(out);
                continue;
            }

            VoltType type = VoltType.typeFromClass(cls);
            out.writeByte(type.getValue());
            switch (type) {
                case TINYINT:
                    out.writeByte((Byte)obj);
                    break;
                case SMALLINT:
                    out.writeShort((Short)obj);
                    break;
                case INTEGER:
                    out.writeInt((Integer) obj);
                    break;
                case BIGINT:
                    out.writeLong((Long) obj);
                    break;
                case FLOAT:
                    out.writeDouble((Double) obj);
                    break;
                case STRING:
                    out.writeString((String) obj);
                    break;
                case TIMESTAMP:
                    out.writeTimestamp((TimestampType) obj);
                    break;
                case DECIMAL:
                    VoltDecimalHelper.serializeBigDecimal((BigDecimal)obj, out);
                    break;
                case BOOLEAN:
                    out.writeBoolean((Boolean)obj);
                    break;
                case VOLTTABLE:
                    out.writeObject((VoltTable) obj);
                    break;
                default:
                    throw new RuntimeException("FIXME: Unsupported type " + type);
            }
        }
    }

    @Override
    public String toString() {
        return String.format("%s{%s}",
                this.getClass().getSimpleName(),
                StringUtil.toString(m_params, true, true));
    }
    
    static private Object readOneParameter(FastDeserializer in) throws IOException {
        byte nextTypeByte = in.readByte();
        if (nextTypeByte == ARRAY) {
            VoltType nextType = VoltType.get(in.readByte());
            if (nextType == null) return null;
            return in.readArray(nextType.classFromType());
        }
        else {
            VoltType nextType = VoltType.get(nextTypeByte);
            switch (nextType) {
                case NULL:
                    return null;
                case TINYINT:
                    return in.readByte();
                case SMALLINT:
                    return in.readShort();
                case INTEGER:
                    return in.readInt();
                case BIGINT:
                    return in.readLong();
                case FLOAT:
                    return in.readDouble();
                case STRING:
                    String string_val = in.readString();
                    if (string_val == null)
                    {
                        return VoltType.NULL_STRING;
                    }
                    return string_val;
                case TIMESTAMP:
                    return in.readTimestamp();
                case BOOLEAN:
                    return in.readBoolean();
                case VOLTTABLE:
                    return in.readObject(VoltTable.class);
                case DECIMAL: {
                    BigDecimal decimal_val = in.readBigDecimal();
                    if (decimal_val == null)
                    {
                        return VoltType.NULL_DECIMAL;
                    }
                    return decimal_val;
                }
                case DECIMAL_STRING: {
                    BigDecimal decimal_val = in.readBigDecimalFromString();
                    if (decimal_val == null)
                    {
                        return VoltType.NULL_DECIMAL;
                    }
                    return decimal_val;
                }
                default:
                    throw new RuntimeException("ParameterSet doesn't support type" + nextType);
            }
        }
    }
    
    static Object limitType(Object o) {
        Class<?> ctype = o.getClass();
        if (ctype == Integer.class) {
            return ((Integer) o).longValue();
        }

        return o;
    }
}

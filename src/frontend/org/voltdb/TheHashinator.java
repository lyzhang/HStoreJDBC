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

import java.io.UnsupportedEncodingException;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.Cluster;
import org.voltdb.utils.LogKeys;
import org.voltdb.utils.VoltLoggerFactory;

import edu.brown.hstore.HStore;

/**
 * Class that maps object values to partitions. It's rather simple
 * really. It'll get more complicated if you give it time.
 */
public abstract class TheHashinator {
    static int partitionCount;
    private static final Logger hostLogger = Logger.getLogger("HOST", VoltLoggerFactory.instance());

    /**
     * Initialize TheHashinator
     * @param catalog A pointer to the catalog data structure.
     */
    public static void initialize(Catalog catalog) {
        Cluster cluster = catalog.getClusters().get("cluster");
        partitionCount = cluster.getNum_partitions();
    }

    /**
     * Given a long value, pick a partition to store the data.
     *
     * @param value The value to hash.
     * @param partitionCount The number of partitions to choose from.
     * @return A value between 0 and partitionCount-1, hopefully pretty evenly
     * distributed.
     */
    static int hashinate(long value, int partitionCount) {
        int index = (int)(value^(value>>>32));
        return java.lang.Math.abs(index % partitionCount);
    }

    /**
     * Given an Object value, pick a partition to store the data. Currently only String objects can be hashed.
     *
     * @param value The value to hash.
     * @param partitionCount The number of partitions to choose from.
     * @return A value between 0 and partitionCount-1, hopefully pretty evenly
     * distributed.
     */
    static int hashinate(Object value, int partitionCount) {
        if (value instanceof String) {
            String string = (String) value;
            try {
                byte bytes[] = string.getBytes("UTF-8");
                int hashCode = 0;
                int offset = 0;
                for (int ii = 0; ii < bytes.length; ii++) {
                   hashCode = 31 * hashCode + bytes[offset++];
                }
                return java.lang.Math.abs(hashCode % partitionCount);
            } catch (UnsupportedEncodingException e) {
                hostLogger.l7dlog( Level.FATAL, LogKeys.host_TheHashinator_ExceptionHashingString.name(), new Object[] { string }, e);
                HStore.crashDB();
            }
        }
        hostLogger.l7dlog(Level.FATAL, LogKeys.host_TheHashinator_AttemptedToHashinateNonLongOrString.name(), new Object[] { value
                .getClass().getName() }, null);
        HStore.crashDB();
        return -1;
    }

    /**
     * Given an object, map it to a partition.
     * @param obj The object to be mapped to a partition.
     * @return The id of the partition desired.
     */
    public static int hashToPartition(Object obj) {
        return (hashToPartition(obj, TheHashinator.partitionCount));
    }

    /**
     * Given an object and a number of partitions, map the object to a partition.
     * @param obj The object to be mapped to a partition.
     * @param partitionCount The number of partitions TheHashinator will use
     * @return The id of the partition desired.
     */
    public static int hashToPartition(Object obj, int partitionCount) {
        int index = 0;
        if (obj instanceof Long) {
            long value = ((Long) obj).longValue();
            index = hashinate(value, partitionCount);
        } else if (obj instanceof String) {
            index = hashinate(obj, partitionCount);
        } else if (obj instanceof Integer) {
            long value = (long)((Integer)obj).intValue();
            index = hashinate(value, partitionCount);
        } else if (obj instanceof Short) {
            long value = (long)((Short)obj).shortValue();
            index = hashinate(value, partitionCount);
        } else if (obj instanceof Byte) {
            long value = (long)((Byte)obj).byteValue();
            index = hashinate(value, partitionCount);
        }
        return index;
    }
}

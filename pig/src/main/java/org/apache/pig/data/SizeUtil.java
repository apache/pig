/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.pig.data;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.pig.classification.InterfaceAudience;
import org.apache.pig.classification.InterfaceStability;

/**
 * Utility functions for estimating size of objects of pig types
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class SizeUtil {

    private static final int MAP_MEM_PER_ENTRY = 32 + 120;

    public static long getPigObjMemSize(Object o) {
        // 12 is added to each to account for the object overhead and the
        // pointer in the tuple.
        switch (DataType.findType(o)) {
        case DataType.BYTEARRAY: {
            byte[] bytes = ((DataByteArray) o).get();
            // bytearray size including rounding to 8 bytes
            long byte_array_sz = roundToEight(bytes.length + 12);

            return byte_array_sz + 16 /* 16 is additional size of DataByteArray */;
        }

        case DataType.CHARARRAY: {
            String s = (String) o;
            // See PIG-1443 for a reference for this formula
            return roundToEight((s.length() * 2) + 38);
        }

        case DataType.TUPLE: {
            Tuple t = (Tuple) o;
            return t.getMemorySize();
        }

        case DataType.BAG: {
            DataBag b = (DataBag) o;
            return b.getMemorySize();
        }

        case DataType.INTEGER:
            return 4 + 8 + 4/* +4 to round to 8 bytes */;

        case DataType.LONG:
            return 8 + 8;

        case DataType.DATETIME:
            return 8 + 2 + 8 + 6 /* one long (8) + one short (2) + 6 to round to 8 bytes */;

        case DataType.MAP: {
            @SuppressWarnings("unchecked")
            Map<String, Object> m = (Map<String, Object>) o;
            Iterator<Map.Entry<String, Object>> i = m.entrySet().iterator();
            long sum = 0;
            while (i.hasNext()) {
                Entry<String, Object> entry = i.next();
                sum += getMapEntrySize(entry.getKey(), entry.getValue());
            }
            return sum;
        }

        case DataType.FLOAT:
            return 4 + 8 + 4/* +4 to round to 8 bytes */;

        case DataType.DOUBLE:
            return 8 + 8;

        case DataType.BOOLEAN:
            // boolean takes 1 byte , +7 to round it to 8
            return 1 + 8 + 7;

        //used http://javamoods.blogspot.fr/2009/03/how-big-is-bigdecimal.html as reference
        case DataType.BIGINTEGER:
            return 56;

        case DataType.BIGDECIMAL:
            return 32;

        case DataType.NULL:
            return 0;

        default:
            // ??
            return 12;
        }
    }

    public static long getMapEntrySize(Object key, Object value) {
        // based on experiments on 32 bit Java HotSpot VM
        // size of map with 0 entries is 120 bytes
        // each additional entry have around 24 bytes overhead at
        // small number of entries. At larger number of entries, the
        // overhead is around 32 bytes, probably because of the expanded
        // data structures in anticapation of more entries being added
        return getPigObjMemSize(key) + getPigObjMemSize(value)
                + MAP_MEM_PER_ENTRY;
    }

    /**
     * Memory size of objects are rounded to multiple of 8 bytes
     *
     * @param i
     * @return i rounded to a equal of higher multiple of 8
     */
    public static long roundToEight(long i) {
        return 8 * ((i + 7) / 8); // integer division rounds the result down
    }

}

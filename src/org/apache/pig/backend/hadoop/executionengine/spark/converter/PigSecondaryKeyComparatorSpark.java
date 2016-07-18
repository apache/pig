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
package org.apache.pig.backend.hadoop.executionengine.spark.converter;

import java.io.Serializable;
import java.util.Comparator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;

/**
 * Utility class that handles secondary key for sorting.
 */
class PigSecondaryKeyComparatorSpark implements Comparator, Serializable {
    private static final Log LOG = LogFactory.getLog(PigSecondaryKeyComparatorSpark.class);
    private static final long serialVersionUID = 1L;

    private static boolean[] secondarySortOrder;

    public PigSecondaryKeyComparatorSpark(boolean[] pSecondarySortOrder) {
        secondarySortOrder = pSecondarySortOrder;
    }

    @Override
    public int compare(Object o1, Object o2) {
        Tuple t1 = (Tuple) o1;
        Tuple t2 = (Tuple) o2;
        try {
            if ((t1.size() < 3) || (t2.size() < 3)) {
                throw new RuntimeException("tuple size must bigger than 3, tuple[0] stands for index, tuple[1]" +
                        "stands for the compound key, tuple[3] stands for the value");
            }
            Tuple compoundKey1 = (Tuple) t1.get(1);
            Tuple compoundKey2 = (Tuple) t2.get(1);
            if ((compoundKey1.size() < 2) || (compoundKey2.size() < 2)) {
                throw new RuntimeException("compoundKey size must bigger than, compoundKey[0] stands for firstKey," +
                        "compoundKey[1] stands for secondaryKey");
            }
            Object secondaryKey1 = compoundKey1.get(1);
            Object secondaryKey2 = compoundKey2.get(1);
            int res = compareSecondaryKeys(secondaryKey1, secondaryKey2, secondarySortOrder);
            if (LOG.isDebugEnabled()) {
                LOG.debug("t1:" + t1 + "t2:" + t2 + " res:" + res);
            }
            return res;
        } catch (ExecException e) {
            throw new RuntimeException("Fail to get the compoundKey", e);
        }
    }

    private int compareSecondaryKeys(Object o1, Object o2, boolean[] asc){
        return compareKeys(o1, o2, asc);
    }

    public static int compareKeys(Object o1, Object o2, boolean[] asc) {
        int rc = 0;
        if (o1 != null && o2 != null && o1 instanceof Tuple && o2 instanceof Tuple) {
            // objects are Tuples, we may need to apply sort order inside them
            Tuple t1 = (Tuple) o1;
            Tuple t2 = (Tuple) o2;
            int sz1 = t1.size();
            int sz2 = t2.size();
            if (sz2 < sz1) {
                return 1;
            } else if (sz2 > sz1) {
                return -1;
            } else {
                for (int i = 0; i < sz1; i++) {
                    try {
                        rc = DataType.compare(t1.get(i), t2.get(i));
                        if (rc != 0 && asc != null && asc.length > 1 && !asc[i])
                            rc *= -1;
                        if ((t1.get(i) == null) || (t2.get(i) == null)) {
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("t1.get(i) is:" + t1.get(i) + " t2.get(i) is:" + t2.get(i));
                            }
                        }
                        if (rc != 0) break;
                    } catch (ExecException e) {
                        throw new RuntimeException("Unable to compare tuples", e);
                    }
                }
            }
        } else {
            // objects are NOT Tuples, delegate to DataType.compare()
            rc = DataType.compare(o1, o2);
        }
        // apply sort order for keys that are not tuples or for whole tuples
        if (asc != null && asc.length == 1 && !asc[0])
            rc *= -1;
        return rc;
    }
}

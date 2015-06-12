/**
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

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;

/**
 * IndexedKey records the index and key info.
 * This is used as key for JOINs. It addresses the case where key is
 * either empty (or is a tuple with one or more empty fields). In this case,
 * we must respect the SQL standard as documented in the equals() method.
 */
public class IndexedKey implements Serializable {
    private byte index;
    private Object key;

    public IndexedKey(byte index, Object key) {
        this.index = index;
        this.key = key;
    }

    public Object getKey() {
        return key;
    }

    @Override
    public String toString() {
        return "IndexedKey{" +
                "index=" + index +
                ", key=" + key +
                '}';
    }

    /**
     * If key is empty, we'd like compute equality based on key and index.
     * If key is not empty, we'd like to compute equality based on just the key (like we normally do).
     * There are two possible cases when two tuples are compared:
     * 1) Compare tuples of same table (same index)
     * 2) Compare tuples of different tables (different index values)
     * In 1)
     * key1    key2    equal?
     * null    null      Y
     * foo     null      N
     * null    foo       N
     * foo     foo       Y
     * (1,1)   (1,1)     Y
     * (1,)    (1,)      Y
     * (1,2)   (1,2)     Y
     * <p/>
     * <p/>
     * In 2)
     * key1    key2    equal?
     * null    null     N
     * foo     null     N
     * null    foo      N
     * foo     foo      Y
     * (1,1)   (1,1)    Y
     * (1,)    (1,)     N
     * (1,2)   (1,2)    Y
     *
     * @param o
     * @return
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IndexedKey that = (IndexedKey) o;
        if (index == that.index) {
            if (key == null && that.key == null) {
                return true;
            } else if (key == null || that.key == null) {
                return false;
            } else {
                return key.equals(that.key);
            }
        } else {
            if (key == null || that.key == null) {
                return false;
            } else if (key.equals(that.key) && !containNullfields(key)) {
                return true;
            } else {
                return false;
            }
        }
    }

    private boolean containNullfields(Object key) {
        if (key instanceof Tuple) {
            for (int i = 0; i < ((Tuple) key).size(); i++) {
                try {
                    if (((Tuple) key).get(i) == null) {
                        return true;
                    }
                } catch (ExecException e) {
                    throw new RuntimeException("exception found in " +
                            "containNullfields", e);

                }
            }
        }
        return false;

    }

    /**
     * Calculate hashCode by index and key
     * if key is empty, return index value
     * if key is not empty, return the key.hashCode()
     */
    @Override
    public int hashCode() {
        int result = 0;
        if (key == null) {
            result = (int) index;
        } else {
            result = key.hashCode();
        }
        return result;
    }
}
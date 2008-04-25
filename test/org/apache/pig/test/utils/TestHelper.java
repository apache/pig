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
package org.apache.pig.test.utils;

import java.util.Iterator;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DefaultBagFactory;
import org.apache.pig.data.DefaultTuple;
import org.apache.pig.data.Tuple;

/**
 * Will contain static methods that will be useful
 * for unit tests
 *
 */
public class TestHelper {
    public static boolean bagContains(DataBag db, Tuple t) {
        Iterator<Tuple> iter = db.iterator();
        for (Tuple tuple : db) {
            if (tuple.compareTo(t) == 0)
                return true;
        }
        return false;
    }
    
    public static boolean compareBags(DataBag db1, DataBag db2) {
        if (db1.size() != db2.size())
            return false;

        boolean equal = true;
        for (Tuple tuple : db2) {
            boolean contains = false;
            for (Tuple tuple2 : db1) {
                if (tuple.compareTo(tuple2) == 0) {
                    contains = true;
                    break;
                }
            }
            if (!contains) {
                equal = false;
                break;
            }
        }
        return equal;
    }
    
    public static DataBag projectBag(DataBag db2, int i) throws ExecException {
        DataBag ret = DefaultBagFactory.getInstance().newDefaultBag();
        for (Tuple tuple : db2) {
            Object o = tuple.get(i);
            Tuple t1 = new DefaultTuple();
            t1.append(o);
            ret.add(t1);
        }
        return ret;
    }
}

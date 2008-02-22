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
package org.apache.pig.test;

import java.io.IOException;

import org.apache.pig.data.*;

public class Util {
    // Helper Functions
    // =================
    static public Tuple loadFlatTuple(Tuple t, int[] input) throws IOException {
        for (int i = 0; i < input.length; i++) {
            t.set(i, new Integer(input[i]));
        }
        return t;
    }

    static public Tuple loadTuple(Tuple t, String[] input) throws IOException {
        for (int i = 0; i < input.length; i++) {
            t.set(i, input[i]);
        }
        return t;
    }

    static public Tuple loadTuple(Tuple t, DataByteArray[] input) throws IOException {
        for (int i = 0; i < input.length; i++) {
            t.set(i, input[i]);
        }
        return t;
    }

    static public Tuple loadNestTuple(Tuple t, int[] input) throws IOException {
        DataBag bag = BagFactory.getInstance().newDefaultBag();
        for(int i = 0; i < input.length; i++) {
            Tuple f = TupleFactory.getInstance().newTuple(1);
            f.set(0, input[i]);
            bag.add(f);
        }
        t.set(0, bag);
        return t;
    }

    static public Tuple loadNestTuple(Tuple t, long[] input) throws IOException {
        DataBag bag = BagFactory.getInstance().newDefaultBag();
        for(int i = 0; i < input.length; i++) {
            Tuple f = TupleFactory.getInstance().newTuple(1);
            f.set(0, new Long(input[i]));
            bag.add(f);
        }
        t.set(0, bag);
        return t;
    }


    static public Tuple loadNestTuple(Tuple t, int[][] input) throws IOException {
        for (int i = 0; i < input.length; i++) {
            DataBag bag = BagFactory.getInstance().newDefaultBag();
            Tuple f = loadFlatTuple(TupleFactory.getInstance().newTuple(input[i].length), input[i]);
            bag.add(f);
            t.set(i, bag);
        }
        return t;
    }

    static public Tuple loadTuple(Tuple t, String[][] input) throws IOException {
        for (int i = 0; i < input.length; i++) {
            DataBag bag = BagFactory.getInstance().newDefaultBag();
            Tuple f = loadTuple(TupleFactory.getInstance().newTuple(input[i].length), input[i]);
            bag.add(f);
            t.set(i, bag);
        }
        return t;
    }

}

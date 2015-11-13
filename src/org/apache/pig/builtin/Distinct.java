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

package org.apache.pig.builtin;

import java.io.IOException;

import org.apache.pig.Algebraic;
import org.apache.pig.EvalFunc;
import org.apache.pig.PigConfiguration;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigMapReduce;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.InternalDistinctBag;
import org.apache.pig.data.SingleTupleBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

/**
 * Find the distinct set of tuples in a bag.
 * This is a blocking operator. All the input is put in the hashset implemented
 * in DistinctDataBag which also provides the other DataBag interfaces.
 */
public class Distinct  extends EvalFunc<DataBag> implements Algebraic {

    private static TupleFactory tupleFactory = TupleFactory.getInstance();
    private static boolean initialized = false;
    private static boolean useDefaultBag = false;

    @Override
    public DataBag exec(Tuple input) throws IOException {
        return getDistinct(input);
    }

    @Override
    public String getFinal() {
        return Final.class.getName();
    }

    @Override
    public String getInitial() {
        return Initial.class.getName();
    }

    @Override
    public String getIntermed() {
        return Intermediate.class.getName();
    }

    static public class Initial extends EvalFunc<Tuple> {

        @Override
        public Tuple exec(Tuple input) throws IOException {
            // the input has  a single field which is a tuple
            // representing the data we want to distinct.
            // unwrap, put in a bag and send down
            try {
                Tuple single = (Tuple)input.get(0);
                DataBag bag = single == null ? createDataBag() : new SingleTupleBag(single);
                return tupleFactory.newTuple(bag);
            } catch (ExecException e) {
                throw e;
            }
        }
    }

    static public class Intermediate extends EvalFunc<Tuple> {

        @Override
        public Tuple exec(Tuple input) throws IOException {
            return tupleFactory.newTuple(getDistinctFromNestedBags(input, this));
        }
    }

    static public class Final extends EvalFunc<DataBag> {

        @Override
        public DataBag exec(Tuple input) throws IOException {
            return getDistinctFromNestedBags(input, this);
        }
    }

    private static DataBag createDataBag() {
        if (!initialized) {
            initialized = true;
            if (PigMapReduce.sJobConfInternal.get() != null) {
                String bagType = PigMapReduce.sJobConfInternal.get().get(PigConfiguration.PIG_CACHEDBAG_DISTINCT_TYPE);
                if (bagType != null && bagType.equalsIgnoreCase("default")) {
                    useDefaultBag = true;
                }
            }
        }
        // by default, we create InternalDistinctBag, unless user configures
        // explicitly to use old bag
        return useDefaultBag ? BagFactory.getInstance().newDistinctBag() : new InternalDistinctBag(3);
    }

    static private DataBag getDistinctFromNestedBags(Tuple input, EvalFunc evalFunc) throws IOException {
        DataBag result = createDataBag();
        long progressCounter = 0;
        try {
            DataBag bg = (DataBag)input.get(0);
            if (bg == null) {
                return result;
            }
            for (Tuple tuple : bg) {
                // Each tuple has a single column
                // which is a bag. Get tuples out of it
                // and distinct over all tuples
                for (Tuple t : (DataBag)tuple.get(0)) {
                    result.add(t);
                    ++progressCounter;
                    if((progressCounter % 1000) == 0){
                        evalFunc.progress();
                    }
                }
            }
        } catch (ExecException e) {
           throw e;
        }
        return result;
    }

    protected DataBag getDistinct(Tuple input) throws IOException {
        try {
            DataBag inputBg = (DataBag)input.get(0);
            DataBag result = createDataBag();
            if (inputBg == null) {
                return result;
            }
            long progressCounter = 0;
            for (Tuple tuple : inputBg) {
                result.add(tuple);
                ++progressCounter;
                if ((progressCounter % 1000) == 0) {
                    progress();
                }
            }
            return result;
        } catch (ExecException e) {
             throw e;
        }
    }

}

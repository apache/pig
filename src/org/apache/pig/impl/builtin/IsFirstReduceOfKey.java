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
package org.apache.pig.impl.builtin;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.pig.EvalFunc;
import org.apache.pig.PigConstants;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigMapReduce;
import org.apache.pig.backend.hadoop.executionengine.util.MapRedUtil;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.util.Pair;
import org.apache.pig.impl.util.UDFContext;

public class IsFirstReduceOfKey extends EvalFunc<Boolean> {
    protected static final TupleFactory tf = TupleFactory.getInstance();
    protected Map<Object, Pair<Integer, Integer> > reducerMap = null;
    /* Loads the key distribution file obtained from the sampler */
    protected void init() {
        Configuration conf = PigMapReduce.sJobConfInternal.get();
        String keyDistFile = conf.get("pig.keyDistFile", "");
        if (keyDistFile.length() == 0) {
            throw new RuntimeException(this.getClass().getSimpleName() +
                    " used but no key distribution found");
        }

        try {
            Integer [] redCnt = new Integer[1]; 
            reducerMap = MapRedUtil.loadPartitionFileFromLocalCache(
                    keyDistFile, redCnt, DataType.TUPLE, conf);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Boolean exec(Tuple input) throws IOException {
        if (reducerMap == null) {
            init();
        }
        Object key = input.get(0);
        Tuple keyTuple = tf.newTuple(1);
        keyTuple.set(0, key);
        if (!reducerMap.containsKey(keyTuple)) {
            return false;
        }
        int firstReducerOfKey = reducerMap.get(keyTuple).first;
        int reduceIndex = UDFContext.getUDFContext().getJobConf().getInt(PigConstants.TASK_INDEX, 0);
        if (firstReducerOfKey == reduceIndex) {
            return true;
        }
        return false;
    }

}

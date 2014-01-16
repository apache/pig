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
package org.apache.pig.backend.hadoop.executionengine.tez;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.HDataType;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.ExpressionOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.builtin.PartitionSkewedKeys;
import org.apache.pig.impl.io.NullablePartitionWritable;
import org.apache.pig.impl.io.NullableTuple;
import org.apache.pig.impl.io.PigNullableWritable;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.util.Pair;
import org.python.google.common.collect.Lists;

import com.google.common.collect.Maps;

/**
 * The partition rearrange operator is a part of the skewed join implementation.
 * It has an embedded physical plan that generates tuples of the form
 * (inpKey,reducerIndex,(indxed inp Tuple)).
 */
public class POPartitionRearrangeTez extends POLocalRearrangeTez {
    private static final long serialVersionUID = 1L;
    private static final TupleFactory tf = TupleFactory.getInstance();
    private static final BagFactory mBagFactory = BagFactory.getInstance();

    // ReducerMap will store the tuple, max reducer index & min reducer index
    private Map<Object, Pair<Integer, Integer>> reducerMap = Maps.newHashMap();
    private Integer totalReducers = -1;
    private boolean inited;

    public POPartitionRearrangeTez(OperatorKey k, int rp) {
        super(k, rp);
        index = -1;
        leafOps = Lists.newArrayList();
    }

    @Override
    public String name() {
        return getAliasString() + "Partition Rearrange" + "["
                + DataType.findTypeName(resultType) + "]" + "{"
                + DataType.findTypeName(keyType) + "}" + "(" + mIsDistinct
                + ") - " + mKey.toString() + "\t->\t " + outputKey;
    }

    /**
     * Calls getNext on the generate operator inside the nested physical plan.
     * Converts the generated tuple into the proper format, i.e,
     * (key,indexedTuple(value))
     */
    @Override
    public Result getNextTuple() throws ExecException {
        if (!inited) {
            init();
        }

        while (true) {
            inp = processInput();
            if (inp.returnStatus == POStatus.STATUS_EOP || inp.returnStatus == POStatus.STATUS_ERR) {
                break;
            }
            if (inp.returnStatus == POStatus.STATUS_NULL) {
                continue;
            }

            for (PhysicalPlan ep : plans) {
                ep.attachInput((Tuple)inp.result);
            }

            Result res = null;
            List<Result> resLst = new ArrayList<Result>();
            for (ExpressionOperator op : leafOps){
                res = op.getNext(op.getResultType());
                if (res.returnStatus != POStatus.STATUS_OK) {
                    return new Result();
                }
                resLst.add(res);
            }
            res.result = constructPROutput(resLst,(Tuple)inp.result);
            if (writer == null) { // In the case of combiner
                return res;
            }

            Iterator<Tuple> its = ((DataBag)res.result).iterator();
            while(its.hasNext()) {
                Tuple result = its.next();
                Byte tupleKeyIdx = 2;
                Byte tupleValIdx = 3;

                Integer partitionIndex = -1;
                partitionIndex = (Integer)result.get(1);

                PigNullableWritable key =
                        HDataType.getWritableComparableTypes(result.get(tupleKeyIdx), keyType);
                NullableTuple val = new NullableTuple((Tuple)result.get(tupleValIdx));

                NullablePartitionWritable wrappedKey = new NullablePartitionWritable(key);
                wrappedKey.setIndex(index);
                wrappedKey.setPartition(partitionIndex);
                val.setIndex(index);

                try {
                    writer.write(wrappedKey, val);
                } catch (IOException ioe) {
                    int errCode = 2135;
                    String msg = "Received error from POPartitionRearrage function." + ioe.getMessage();
                    throw new ExecException(msg, errCode, ioe);
                }
            }

            res = empty;
        }
        return inp;
    }

    // Returns bag of tuples
    protected DataBag constructPROutput(List<Result> resLst, Tuple value) throws ExecException{
        Tuple t = super.constructLROutput(resLst, null, value);

        //Construct key
        Object key = t.get(1);

        // Construct an output bag and feed in the tuples
        DataBag opBag = mBagFactory.newDefaultBag();

        // Put the index, key, and value in a tuple and return
        // first -> min, second -> max
        Pair <Integer, Integer> indexes = reducerMap.get(key);

        // For non skewed keys, we set the partition index to be -1
        if (indexes == null) {
            indexes = new Pair <Integer, Integer>(-1,0);
        }

        for (Integer reducerIdx=indexes.first, cnt=0; cnt <= indexes.second; reducerIdx++, cnt++) {
            if (reducerIdx >= totalReducers) {
                reducerIdx = 0;
            }
            Tuple opTuple = mTupleFactory.newTuple(4);
            opTuple.set(0, t.get(0));
            // set the partition index
            opTuple.set(1, reducerIdx.intValue());
            opTuple.set(2, key);
            opTuple.set(3, t.get(2));

            opBag.add(opTuple);
        }

        return opBag;
    }

    private void init() throws RuntimeException {
        Map<String, Object> distMap = null;
        if (PigProcessor.sampleMap != null) {
            // We've already collected sampleMap in PigProcessor
            distMap = PigProcessor.sampleMap;
        } else {
            throw new RuntimeException(this.getClass().getSimpleName() +
                    " used but no key distribution found");
        }

        try {
            if (distMap != null) {
                Integer[] totalReducers = new Integer[1];

                // The distMap is structured as (key, min, max) where min, max
                // being the index of the reducers
                DataBag partitionList = (DataBag) distMap.get(PartitionSkewedKeys.PARTITION_LIST);
                totalReducers[0] = Integer.valueOf("" + distMap.get(PartitionSkewedKeys.TOTAL_REDUCERS));
                Iterator<Tuple> it = partitionList.iterator();
                while (it.hasNext()) {
                    Tuple idxTuple = it.next();
                    Integer maxIndex = (Integer) idxTuple.get(idxTuple.size() - 1);
                    Integer minIndex = (Integer) idxTuple.get(idxTuple.size() - 2);
                    // Used to replace the maxIndex with the number of reducers
                    if (maxIndex < minIndex) {
                        maxIndex = totalReducers[0] + maxIndex;
                    }

                    Tuple keyT;
                    // if the join is on more than 1 key
                    if (idxTuple.size() > 3) {
                        // remove the last 2 fields of the tuple, i.e: minIndex
                        // and maxIndex and store it in the reducer map
                        Tuple keyTuple = tf.newTuple();
                        for (int i=0; i < idxTuple.size() - 2; i++) {
                            keyTuple.append(idxTuple.get(i));
                        }
                        keyT = keyTuple;
                    } else {
                        keyT = tf.newTuple(1);
                        keyT.set(0,idxTuple.get(0));
                    }
                    // number of reducers
                    Integer cnt = maxIndex - minIndex;
                    // 1 is added to account for the 0 index
                    reducerMap.put(keyT, new Pair<Integer, Integer>(minIndex, cnt));
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        inited = true;
    }
}

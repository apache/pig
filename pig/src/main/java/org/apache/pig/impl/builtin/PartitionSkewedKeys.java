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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.EvalFunc;
import org.apache.pig.PigWarning;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.util.Pair;

/**
 * Partition reducers for skewed keys. This is used in skewed join during
 * sampling process. It figures out how many reducers required to process a
 * skewed key without causing spill and allocate this number of reducers to this
 * key. This UDF outputs a map which contains 2 keys:
 *
 * <li>&quot;totalreducers&quot;: the value is an integer wich indicates the
 *         number of total reducers for this join job </li>
 * <li>&quot;partition.list&quot;: the value is a bag which contains a
 * list of tuples with each tuple representing partitions for a skewed key.
 * The tuple has format of &lt;join key&gt;,&lt;min index of reducer&gt;,
 * &lt;max index of reducer&gt; </li>
 *
 * For example, a join job configures 10 reducers, and the sampling process
 * finds out 2 skewed keys, &quot;swpv&quot; needs 4 reducers and &quot;swps&quot;
 * needs 2 reducers. The output file would be like following:
 *
 * {totalreducers=10, partition.list={(swpv,0,3), (swps,4,5)}}
 *
 * The name of this file is set into next MR job which does the actual join.
 * That job uses this information to partition skewed keys properly
 *
 */

public class PartitionSkewedKeys extends EvalFunc<Map<String, Object>> {

    public static final String PARTITION_LIST = "partition.list";

    public static final String TOTAL_REDUCERS = "totalreducers";

    public static final float DEFAULT_PERCENT_MEMUSAGE = 0.3f;

    private Log log = LogFactory.getLog(getClass());

    BagFactory mBagFactory = BagFactory.getInstance();

    TupleFactory mTupleFactory = TupleFactory.getInstance();

    private int currentIndex_;

    private long totalMemory_;

    private long totalSampleCount_;

    private double heapPercentage_;

    protected int totalReducers_;

    // specify how many tuple a reducer can hold for a key
    // this is for testing purpose. If not specified, then
    // it is calculated based on memory size and size of tuple
    private int tupleMCount_;

    public PartitionSkewedKeys() {
        this(null);
    }

    public PartitionSkewedKeys(String[] args) {
        totalReducers_ = -1;
        currentIndex_ = 0;

        if (args != null && args.length > 0) {
            heapPercentage_ = Double.parseDouble(args[0]);
            tupleMCount_ = Integer.parseInt(args[1]);
        } else {
            heapPercentage_ = DEFAULT_PERCENT_MEMUSAGE;
        }

        if (log.isDebugEnabled()) {
            log.debug("pig.skewedjoin.reduce.memusage=" + heapPercentage_);
        }
    }

    /**
     * first field in the input tuple is the number of reducers
     *
     * second field is the *sorted* bag of samples
     * this should be called only once
     */
    public Map<String, Object> exec(Tuple in) throws IOException {
        if (in == null || in.size() == 0) {
            return null;
        }
        Map<String, Object> output = new HashMap<String, Object>();

        totalMemory_ = (long) (Runtime.getRuntime().maxMemory() * heapPercentage_);
        log.info("Maximum of available memory is " + totalMemory_);

        ArrayList<Tuple> reducerList = new ArrayList<Tuple>();

        Tuple currentTuple = null;
        long count = 0;

        // total size in memory for tuples in sample
        long totalSampleMSize = 0;

        //total input rows for the join
        long totalInputRows = 0;

        try {
            if (totalReducers_ == -1) {
                totalReducers_ = (Integer) in.get(0);
            }
            DataBag samples = (DataBag) in.get(1);

            totalSampleCount_ = samples.size();

            log.info("totalSample: " + totalSampleCount_);
            log.info("totalReducers: " + totalReducers_);

            int maxReducers = 0;

            // first iterate the samples to find total number of rows
            Iterator<Tuple> iter1 = samples.iterator();
            while (iter1.hasNext()) {
                Tuple t = iter1.next();
                totalInputRows += (Long)t.get(t.size() - 1);
            }

            // now iterate samples to do the reducer calculation
            Iterator<Tuple> iter2 = samples.iterator();
            while (iter2.hasNext()) {
                Tuple t = iter2.next();
                if (hasSameKey(currentTuple, t) || currentTuple == null) {
                    count++;
                    totalSampleMSize += getMemorySize(t);
                } else {
                    Pair<Tuple, Integer> p = calculateReducers(currentTuple,
                            count, totalSampleMSize, totalInputRows);
                    Tuple rt = p.first;
                    if (rt != null) {
                        reducerList.add(rt);
                    }
                    if (maxReducers < p.second) {
                        maxReducers = p.second;
                    }
                    count = 1;
                    totalSampleMSize = getMemorySize(t);
                }

                currentTuple = t;
            }

            // add last key
            if (count > 0) {
                Pair<Tuple, Integer> p = calculateReducers(currentTuple, count,
                        totalSampleMSize, totalInputRows);
                Tuple rt = p.first;
                if (rt != null) {
                    reducerList.add(rt);
                }
                if (maxReducers < p.second) {
                    maxReducers = p.second;
                }
            }

            if (maxReducers > totalReducers_) {
                if(pigLogger != null) {
                    pigLogger.warn(this,"You need at least " + maxReducers
                            + " reducers to avoid spillage and run this job efficiently.", PigWarning.REDUCER_COUNT_LOW);
                } else {
                    log.warn("You need at least " + maxReducers
                            + " reducers to avoid spillage and run this job efficiently.");
                }
            }

            output.put(PARTITION_LIST, mBagFactory.newDefaultBag(reducerList));
            output.put(TOTAL_REDUCERS, Integer.valueOf(totalReducers_));

            log.info(output.toString());
            if (log.isDebugEnabled()) {
                log.debug(output.toString());
            }

            return output;
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    private Pair<Tuple, Integer> calculateReducers(Tuple currentTuple,
            long count, long totalMSize, long totalTuples) {
        // get average memory size per tuple
        double avgM = totalMSize / (double) count;

        // get the number of tuples that can fit into memory
        long tupleMCount = (tupleMCount_ <= 0)?(long) (totalMemory_ / avgM): tupleMCount_;

        // estimate the number of total tuples for this key
        long keyTupleCount = (long)  ( ((double) count/ totalSampleCount_) * totalTuples);

        int redCount = (int) Math.round(Math.ceil((double) keyTupleCount / tupleMCount));

        if (log.isDebugEnabled())
        {
            log.debug("avgM: " + avgM);
            log.debug("tuple count: " + keyTupleCount);
            log.debug("count: " + count);
            log.debug("A reducer can take " + tupleMCount + " tuples and "
                    + keyTupleCount + " tuples are find for " + currentTuple);
            log.debug("key " + currentTuple + " need " + redCount + " reducers");
        }

        // this is not a skewed key
        if (redCount <= 1) {
            return new Pair<Tuple, Integer>(null, 1);
        }

        Tuple t = this.mTupleFactory.newTuple(currentTuple.size());
        int i = 0;
        try {
            // set keys
            for (; i < currentTuple.size() - 2; i++) {
                t.set(i, currentTuple.get(i));
            }

            int effectiveRedCount = redCount > totalReducers_? totalReducers_:redCount;
            // set the min index of reducer for this key
            t.set(i++, currentIndex_);
            currentIndex_ = (currentIndex_ + effectiveRedCount) % totalReducers_ - 1;
            if (currentIndex_ < 0) {
                currentIndex_ += totalReducers_;
            }
            // set the max index of reducer for this key
            t.set(i++, currentIndex_);
        } catch (ExecException e) {
            throw new RuntimeException("Failed to set value to tuple." + e);
        }

        currentIndex_ = (currentIndex_ + 1) % totalReducers_;

        Pair<Tuple, Integer> p = new Pair<Tuple, Integer>(t, redCount);

        return p;
    }

    // the last field of the tuple is a tuple for memory size and disk size
    protected long getMemorySize(Tuple t) {
        int s = t.size();
        try {
            return (Long) t.get(s - 2);
        } catch (ExecException e) {
            throw new RuntimeException(
                    "Unable to retrive the size field from tuple.", e);
        }
    }


    private boolean hasSameKey(Tuple t1, Tuple t2) {
        // Have to break the tuple down and compare it field to field.
        int sz1 = t1 == null ? 0 : t1.size();
        int sz2 = t2 == null ? 0 : t2.size();
        if (sz2 != sz1) {
            return false;
        }

        for (int i = 0; i < sz1 - 2; i++) {
            try {
                int c = DataType.compare(t1.get(i), t2.get(i));
                if (c != 0) {
                    return false;
                }
            } catch (ExecException e) {
                throw new RuntimeException("Unable to compare tuples", e);
            }
        }

        return true;
    }

}

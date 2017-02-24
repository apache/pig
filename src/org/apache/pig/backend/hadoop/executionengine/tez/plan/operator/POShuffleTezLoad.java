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

package org.apache.pig.backend.hadoop.executionengine.tez.plan.operator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.WritableComparator;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.JobControlCompiler;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.AccumulativeTupleBuffer;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POPackage;
import org.apache.pig.backend.hadoop.executionengine.tez.runtime.TezInput;
import org.apache.pig.backend.hadoop.executionengine.util.AccumulatorOptimizerUtil;
import org.apache.pig.data.AccumulativeBag;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.InternalCachedBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.io.NullableTuple;
import org.apache.pig.impl.io.PigNullableWritable;
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.library.api.KeyValuesReader;
import org.apache.tez.runtime.library.common.ConfigUtils;

public class POShuffleTezLoad extends POPackage implements TezInput {

    private static final long serialVersionUID = 1L;

    protected List<String> inputKeys = new ArrayList<String>();
    private boolean isSkewedJoin = false;

    private transient List<LogicalInput> inputs;
    private transient List<KeyValuesReader> readers;
    private transient int numTezInputs;
    private transient boolean[] finished;
    private transient boolean[] readOnce;
    private transient WritableComparator comparator = null;
    private transient WritableComparator groupingComparator = null;
    private transient Configuration conf;
    private transient int accumulativeBatchSize;

    public POShuffleTezLoad(POPackage pack) {
        super(pack);
    }

    @Override
    public String[] getTezInputs() {
        return inputKeys.toArray(new String[inputKeys.size()]);
    }

    @Override
    public void replaceInput(String oldInputKey, String newInputKey) {
        while (inputKeys.remove(oldInputKey)) {
            inputKeys.add(newInputKey);
        }
    }

    @Override
    public void addInputsToSkip(Set<String> inputsToSkip) {
    }

    @Override
    public void attachInputs(Map<String, LogicalInput> inputs, Configuration conf)
            throws ExecException {
        this.conf = conf;
        this.inputs = new ArrayList<LogicalInput>();
        this.readers = new ArrayList<KeyValuesReader>();
        this.comparator = (WritableComparator) ConfigUtils.getIntermediateInputKeyComparator(conf);
        this.groupingComparator = (WritableComparator) ConfigUtils.getInputKeySecondaryGroupingComparator(conf);
        this.accumulativeBatchSize = AccumulatorOptimizerUtil.getAccumulativeBatchSize();

        try {
            for (String inputKey : inputKeys) {
                LogicalInput input = inputs.get(inputKey);
                // 1) Case of self join/cogroup/cross with Split - numTezInputs < numInputs/inputKeys
                //     - Same TezInput will contain multiple indexes in case of join
                // 2) data unioned within Split - inputKeys > numInputs/numTezInputs
                //     - Input key will be repeated, but index would be same within a TezInput
                if (!this.inputs.contains(input)) {
                    this.inputs.add(input);
                    this.readers.add((KeyValuesReader)input.getReader());
                }
            }

            this.numInputs = this.pkgr.getKeyInfo().size();
            this.numTezInputs = this.inputs.size();

            readOnce = new boolean[numInputs];
            for (int i = 0; i < numInputs; i++) {
                readOnce[i] = false;
            }

            finished = new boolean[numTezInputs];
            for (int i = 0; i < numTezInputs; i++) {
                finished[i] = !readers.get(i).next();
            }
        } catch (Exception e) {
            throw new ExecException(e);
        }
    }

    @Override
    public Result getNextTuple() throws ExecException {
        Result res = pkgr.getNext();
        TezAccumulativeTupleBuffer buffer = null;

        if (isAccumulative()) {
            buffer = new TezAccumulativeTupleBuffer(accumulativeBatchSize);
        }

        while (res.returnStatus == POStatus.STATUS_EOP) {
            boolean hasData = false;
            Object cur = null;
            PigNullableWritable min = null;

            try {
                if (numTezInputs == 1) {
                    if (!finished[0]) {
                        hasData = true;
                        cur = readers.get(0).getCurrentKey();
                        // Just move to the next key without comparison
                        min = ((PigNullableWritable)cur).clone();
                    }
                } else {
                    for (int i = 0; i < numTezInputs; i++) {
                        if (!finished[i]) {
                            hasData = true;
                            cur = readers.get(i).getCurrentKey();
                            // TODO: PIG-4652 should compare key bytes instead
                            // of deserialized objects when using BytesComparator
                            // for faster comparison
                            if (min == null || comparator.compare(min, cur) > 0) {
                                //Not a deep clone. Writable is referenced.
                                min = ((PigNullableWritable)cur).clone();
                            }
                        }
                    }
                }
            } catch (Exception e) {
                throw new ExecException(e);
            }

            if (!hasData) {
                // For certain operators (such as STREAM), we could still have some work
                // to do even after seeing the last input. These operators set a flag that
                // says all input has been sent and to run the pipeline one more time.
                if (Boolean.valueOf(conf.get(JobControlCompiler.END_OF_INP_IN_MAP, "false"))) {
                    this.parentPlan.endOfAllInput = true;
                }
                return RESULT_EOP;
            }

            key = pkgr.getKey(min);
            keyWritable = min;

            try {
                DataBag[] bags = new DataBag[numInputs];
                if (isAccumulative()) {

                    buffer.setCurrentKey(min);
                    for (int i = 0; i < numInputs; i++) {
                        bags[i] = new AccumulativeBag(buffer, i);
                    }

                } else {

                    for (int i = 0; i < numInputs; i++) {
                        bags[i] = new InternalCachedBag(numInputs);
                    }

                    if (numTezInputs == 1) {
                        do {
                            Iterable<Object> vals = readers.get(0).getCurrentValues();
                            for (Object val : vals) {
                                NullableTuple nTup = (NullableTuple) val;
                                int index = nTup.getIndex();
                                Tuple tup = pkgr.getValueTuple(keyWritable, nTup, index);
                                bags[index].add(tup);
                            }
                            finished[0] = !readers.get(0).next();
                            if (finished[0]) {
                                break;
                            }
                            cur = readers.get(0).getCurrentKey();
                        } while (groupingComparator.compare(min, cur) == 0); // We need to loop in case of Grouping Comparators
                    } else {
                        for (int i = 0; i < numTezInputs; i++) {
                            if (!finished[i]) {
                                cur = readers.get(i).getCurrentKey();
                                // We need to loop in case of Grouping Comparators
                                while (groupingComparator.compare(min, cur) == 0) {
                                    Iterable<Object> vals = readers.get(i).getCurrentValues();
                                    for (Object val : vals) {
                                        NullableTuple nTup = (NullableTuple) val;
                                        int index = nTup.getIndex();
                                        Tuple tup = pkgr.getValueTuple(keyWritable, nTup, index);
                                        bags[index].add(tup);
                                    }
                                    finished[i] = !readers.get(i).next();
                                    if (finished[i]) {
                                        break;
                                    }
                                    cur = readers.get(i).getCurrentKey();
                                }
                            }
                        }
                    }
                }

                pkgr.attachInput(key, bags, readOnce);
                res = pkgr.getNext();

            } catch (IOException e) {
                throw new ExecException(e);
            }
        }

        return res;
    }

    public void setInputKeys(List<String> inputKeys) {
        this.inputKeys = inputKeys;
    }

    public void addInputKey(String inputKey) {
        inputKeys.add(inputKey);
    }

    public void setSkewedJoins(boolean isSkewedJoin) {
        this.isSkewedJoin = isSkewedJoin;
    }

    public boolean isSkewedJoin() {
        return isSkewedJoin;
    }

    @Override
    public boolean supportsMultipleInputs() {
        return true;
    }

    private class TezAccumulativeTupleBuffer implements AccumulativeTupleBuffer {

        private int batchSize;
        private List<Tuple>[] bags;
        private PigNullableWritable min;
        private boolean clearedCurrent = true;

        @SuppressWarnings("unchecked")
        public TezAccumulativeTupleBuffer(int batchSize) {
            this.batchSize = batchSize;
            this.bags = new List[numInputs];
            for (int i = 0; i < numInputs; i++) {
                this.bags[i] = new ArrayList<Tuple>(batchSize);
            }
        }

        public void setCurrentKey(PigNullableWritable curKey) {
            if (!clearedCurrent) {
                // If buffer.clear() is not called from POForEach ensure it is called here.
                clear();
            }
            this.min = curKey;
            clearedCurrent = false;
        }

        @Override
        public boolean hasNextBatch() {
            Object cur = null;
            try {
                for (int i = 0; i < numTezInputs; i++) {
                    if (!finished[i]) {
                        cur = readers.get(i).getCurrentKey();
                        if (groupingComparator.compare(min, cur) == 0) {
                            return true;
                        }
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException(
                        "Error while checking for next Accumulator batch", e);
            }
            return false;
        }

        @Override
        public void nextBatch() throws IOException {
            Object cur = null;
            for (int i = 0; i < bags.length; i++) {
                bags[i].clear();
            }
            try {
                for (int i = 0; i < numTezInputs; i++) {
                    if (!finished[i]) {
                        cur = readers.get(i).getCurrentKey();
                        int batchCount = 0;
                        while (groupingComparator.compare(min, cur) == 0) {
                            Iterator<Object> iter = readers.get(i).getCurrentValues().iterator();
                            while (iter.hasNext() && batchCount < batchSize) {
                                NullableTuple nTup = (NullableTuple) iter.next();
                                int index = nTup.getIndex();
                                bags[index].add(pkgr.getValueTuple(keyWritable, nTup, index));
                                batchCount++;
                            }
                            if (batchCount == batchSize) {
                                if (!iter.hasNext()) {
                                    // Move to next key and update finished
                                    finished[i] = !readers.get(i).next();
                                }
                                break;
                            }
                            finished[i] = !readers.get(i).next();
                            if (finished[i]) {
                                break;
                            }
                            cur = readers.get(i).getCurrentKey();
                        }
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException(
                        "Error while reading next Accumulator batch", e);
            }
        }

        @Override
        public void clear() {
            for (int i = 0; i < bags.length; i++) {
                bags[i].clear();
            }
            // Skip through current keys and its values not processed because of
            // early termination of accumulator
            Object cur = null;
            try {
                for (int i = 0; i < numTezInputs; i++) {
                    if (!finished[i]) {
                        cur = readers.get(i).getCurrentKey();
                        while (groupingComparator.compare(min, cur) == 0) {
                            finished[i] = !readers.get(i).next();
                            if (finished[i]) {
                                break;
                            }
                            cur = readers.get(i).getCurrentKey();
                        }
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException(
                        "Error while cleaning up for next Accumulator batch", e);
            }
            clearedCurrent = true;
        }

        @Override
        public Iterator<Tuple> getTuples(int index) {
            return bags[index].iterator();
        }

        //TODO: illustratorMarkup

    }

}

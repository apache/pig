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
    protected List<LogicalInput> inputs = new ArrayList<LogicalInput>();
    protected List<KeyValuesReader> readers = new ArrayList<KeyValuesReader>();

    private boolean[] finished;
    private boolean[] readOnce;

    private WritableComparator comparator = null;
    private boolean isSkewedJoin = false;

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
        if (inputKeys.remove(oldInputKey)) {
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
        comparator = (WritableComparator) ConfigUtils.getInputKeySecondaryGroupingComparator(conf);
        try {
            for (String key : inputKeys) {
                LogicalInput input = inputs.get(key);
                this.inputs.add(input);
                this.readers.add((KeyValuesReader)input.getReader());
            }

            // We need to adjust numInputs because it's possible for both
            // OrderedGroupedKVInput and non-OrderedGroupedKVInput to be attached
            // to the same vertex. If so, we're only interested in
            // OrderedGroupedKVInputs. So we ignore the others.
            this.numInputs = this.inputs.size();

            readOnce = new boolean[numInputs];
            for (int i = 0; i < numInputs; i++) {
                readOnce[i] = false;
            }

            finished = new boolean[numInputs];
            for (int i = 0; i < numInputs; i++) {
                finished[i] = !readers.get(i).next();
            }
        } catch (Exception e) {
            throw new ExecException(e);
        }
        accumulativeBatchSize = AccumulatorOptimizerUtil.getAccumulativeBatchSize();
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
            int minIndex = -1;

            try {
                for (int i = 0; i < numInputs; i++) {
                    if (!finished[i]) {
                        hasData = true;
                        cur = readers.get(i).getCurrentKey();
                        if (min == null || comparator.compare(min, cur) > 0) {
                            //Not a deep clone. Writable is referenced.
                            min = ((PigNullableWritable)cur).clone();
                            minIndex = i;
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
                return new Result(POStatus.STATUS_EOP, null);
            }

            key = pkgr.getKey(min);
            keyWritable = min;

            try {
                DataBag[] bags = new DataBag[numInputs];
                if (isAccumulative()) {

                    buffer.setCurrentKey(min);
                    buffer.setCurrentKeyIndex(minIndex);
                    for (int i = 0; i < numInputs; i++) {
                        bags[i] = new AccumulativeBag(buffer, i);
                    }

                } else {

                    for (int i = 0; i < numInputs; i++) {

                        DataBag bag = null;

                        if (!finished[i]) {
                            cur = readers.get(i).getCurrentKey();
                            // We need to loop in case of Grouping Comparators
                            while (comparator.compare(min, cur) == 0
                                    && (!min.isNull() || (min.isNull() && i == minIndex))) {
                                Iterable<Object> vals = readers.get(i).getCurrentValues();
                                bag = bags[i] == null ? new InternalCachedBag(numInputs) : bags[i];
                                for (Object val : vals) {
                                    NullableTuple nTup = (NullableTuple) val;
                                    int index = nTup.getIndex();
                                    Tuple tup = pkgr.getValueTuple(keyWritable, nTup, index);
                                    bag.add(tup);
                                }
                                bags[i] = bag;
                                finished[i] = !readers.get(i).next();
                                if (finished[i]) {
                                    break;
                                }
                                cur = readers.get(i).getCurrentKey();
                            }
                        }

                        if (bag == null) {
                            bags[i] = new InternalCachedBag(numInputs);
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
        private int minIndex;
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

        public void setCurrentKeyIndex(int curKeyIndex) {
            this.minIndex = curKeyIndex;
        }

        @Override
        public boolean hasNextBatch() {
            Object cur = null;
            try {
                for (int i = 0; i < numInputs; i++) {
                    if (!finished[i]) {
                        cur = readers.get(i).getCurrentKey();
                        if (comparator.compare(min, cur) == 0
                                && (!min.isNull() || (min.isNull() && i == minIndex))) {
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
                for (int i = 0; i < numInputs; i++) {
                    if (!finished[i]) {
                        cur = readers.get(i).getCurrentKey();
                        int batchCount = 0;
                        while (comparator.compare(min, cur) == 0 && (!min.isNull() ||
                                min.isNull() && i==minIndex)) {
                            Iterator<Object> iter = readers.get(i).getCurrentValues().iterator();
                            while (iter.hasNext() && batchCount < batchSize) {
                                bags[i].add(pkgr.getValueTuple(keyWritable, (NullableTuple) iter.next(), i));
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
                for (int i = 0; i < numInputs; i++) {
                    if (!finished[i]) {
                        cur = readers.get(i).getCurrentKey();
                        while (comparator.compare(min, cur) == 0 && (!min.isNull() ||
                                min.isNull() && i==minIndex)) {
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

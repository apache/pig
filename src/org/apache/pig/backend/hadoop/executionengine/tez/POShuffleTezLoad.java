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

package org.apache.pig.backend.hadoop.executionengine.tez;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.JobCreationException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POPackage;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.InternalCachedBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.io.NullableTuple;
import org.apache.pig.impl.io.PigNullableWritable;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.library.api.KeyValuesReader;
import org.apache.tez.runtime.library.input.ShuffledMergedInput;

public class POShuffleTezLoad extends POPackage implements TezLoad {

    private static final long serialVersionUID = 1L;

    List<String> inputKeys = new ArrayList<String>();

    List<ShuffledMergedInput> inputs = new ArrayList<ShuffledMergedInput>();
    List<KeyValuesReader> readers = new ArrayList<KeyValuesReader>();
    List<Boolean> finished = new ArrayList<Boolean>();

    private boolean[] readOnce;

    Result res;

    protected static final TupleFactory tf = TupleFactory.getInstance();

    WritableComparator comparator = null;

    public POShuffleTezLoad(OperatorKey k, POPackage pack) {
        super(k);
        setPkgr(pack.getPkgr());
        this.setNumInps(pack.getNumInps());
    }

    @Override
    public void attachInputs(Map<String, LogicalInput> inputs, Configuration conf)
            throws ExecException {
        readOnce = new boolean[numInputs];
        for (int i = 0; i < numInputs; i++) {
            readOnce[i] = false;
        }

        try {
            comparator = ReflectionUtils.newInstance(
                    TezDagBuilder.comparatorForKeyType(pkgr.getKeyType()), conf);
        } catch (JobCreationException e) {
            throw new ExecException(e);
        }
        try {
            // TODO: Only take the inputs which are actually specified.
            for (LogicalInput input : inputs.values()) {
                ShuffledMergedInput smInput = (ShuffledMergedInput) input;
                this.inputs.add(smInput);
                this.readers.add(smInput.getReader());
            }

            for (int i = 0; i < numInputs; i++) {
                finished.add(!readers.get(i).next());
            }
        } catch (IOException e) {
            throw new ExecException(e);
        }
    }

    @Override
    public Result getNextTuple() throws ExecException {
        Result res = pkgr.getNext();
        while (res.returnStatus == POStatus.STATUS_EOP) {
            PigNullableWritable minimum = null;
            boolean newData = false;
            try {
                for (int i = 0; i < numInputs; i++) {
                    if (!finished.get(i)) {
                        newData = true;
                        PigNullableWritable current = (PigNullableWritable) readers
                                .get(i).getCurrentKey();
                        if (minimum == null
                                || comparator.compare(minimum, current) > 0) {
                            minimum = current;
                        }
                    }
                }
            } catch (IOException e) {
                throw new ExecException(e);
            }

            if (!newData) {
                return new Result(POStatus.STATUS_EOP, null);
            }

            key = pkgr.getKey(minimum);

            DataBag[] bags = new DataBag[numInputs];

            try {
                for (int i = 0; i < numInputs; i++) {
                    if (!finished.get(i)) {
                        PigNullableWritable current = (PigNullableWritable) readers
                                .get(i).getCurrentKey();
                        if (comparator.compare(minimum, current) == 0) {
                            DataBag bag = new InternalCachedBag(numInputs);
                            Iterable<Object> vals = readers.get(i).getCurrentValues();
                            for (Object val : vals) {
                                NullableTuple nTup = (NullableTuple) val;
                                int index = nTup.getIndex();
                                Tuple tup = pkgr.getValueTuple(key, nTup, index);
                                bag.add(tup);
                            }
                            finished.set(i, !readers.get(i).next());
                            bags[i] = bag;
                        } else {
                            bags[i] = new InternalCachedBag(numInputs);
                        }
                    } else {
                        bags[i] = new InternalCachedBag(numInputs);
                    }
                }
            } catch (IOException e) {
                throw new ExecException(e);
            }
            pkgr.attachInput(key, bags, readOnce);
            res = pkgr.getNext();
        }
        return res;
    }

    public void setInputKeys(List<String> inputKeys) {
        this.inputKeys = inputKeys;
    }

    public void addInputKey(String inputKey) {
        inputKeys.add(inputKey);
    }

    @Override
    public boolean supportsMultipleInputs() {
        return true;
    }

}

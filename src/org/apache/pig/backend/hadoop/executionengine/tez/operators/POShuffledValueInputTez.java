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

package org.apache.pig.backend.hadoop.executionengine.tez.operators;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.JobControlCompiler;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.tez.TezInput;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.library.api.KeyValuesReader;

/**
 * POShuffledValueInputTez is used read tuples from a Tez Intermediate output from a shuffle edge
 * To be used with POValueOutputTez.
 * TODO: To be removed after PIG-3775 and TEZ-661
 */
public class POShuffledValueInputTez extends PhysicalOperator implements TezInput {

    private static final long serialVersionUID = 1L;
    private static final Log LOG = LogFactory.getLog(POShuffledValueInputTez.class);
    private Set<String> inputKeys = new HashSet<String>();
    private transient boolean finished = false;
    private transient Iterator<KeyValuesReader> readers;
    private transient KeyValuesReader currentReader;
    private transient boolean hasNext;
    protected static final TupleFactory mTupleFactory = TupleFactory.getInstance();
    private transient Configuration conf;

    public POShuffledValueInputTez(OperatorKey k) {
        super(k);
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
    public void attachInputs(Map<String, LogicalInput> inputs,
            Configuration conf) throws ExecException {
        this.conf = conf;
        List<KeyValuesReader> readersList = new ArrayList<KeyValuesReader>();
        try {
            for (String inputKey : inputKeys) {
                LogicalInput input = inputs.get(inputKey);
                if (input == null) {
                    throw new ExecException("Input from vertex " + inputKey
                            + " is missing");
                }

                KeyValuesReader reader = (KeyValuesReader) input.getReader();
                readersList.add(reader);
                LOG.info("Attached input from vertex " + inputKey + " : input="
                        + input + ", reader=" + reader);
            }
            readers = readersList.iterator();
            currentReader = readers.next();
            hasNext = currentReader.next();
        } catch (Exception e) {
            throw new ExecException(e);
        }
    }

    @Override
    public Result getNextTuple() throws ExecException {
        try {
            if (finished) {
                return RESULT_EOP;
            }
            while (hasNext) {
                if (currentReader.getCurrentValues().iterator().hasNext()) {
                    Tuple origTuple = (Tuple)currentReader.getCurrentValues().iterator().next();
                    Tuple copy = mTupleFactory.newTuple(origTuple.getAll());
                    return new Result(POStatus.STATUS_OK, copy);
                }
                hasNext = currentReader.next();
            }
            if (readers.hasNext()) {
                currentReader = readers.next();
                hasNext = currentReader.next();
                if (hasNext) {
                    Tuple origTuple = (Tuple)currentReader.getCurrentValues().iterator().next();
                    Tuple copy = mTupleFactory.newTuple(origTuple.getAll());
                    return new Result(POStatus.STATUS_OK, copy);
                }
            }
            finished = true;
            // For certain operators (such as STREAM), we could still have some work
            // to do even after seeing the last input. These operators set a flag that
            // says all input has been sent and to run the pipeline one more time.
            if (Boolean.valueOf(conf.get(JobControlCompiler.END_OF_INP_IN_MAP, "false"))) {
                this.parentPlan.endOfAllInput = true;
            }
            return RESULT_EOP;
        } catch (IOException e) {
            throw new ExecException(e);
        }
    }

    public void addInputKey(String inputKey) {
        this.inputKeys.add(inputKey);
    }

    @Override
    public Tuple illustratorMarkup(Object in, Object out, int eqClassIndex) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void visit(PhyPlanVisitor v) throws VisitorException {
        v.visit(this);
    }

    @Override
    public boolean supportsMultipleInputs() {
        return false;
    }

    @Override
    public boolean supportsMultipleOutputs() {
        return false;
    }

    @Override
    public String name() {
        return "POShuffledValueInputTez - " + mKey.toString() + "\t<-\t " + inputKeys;
    }
}

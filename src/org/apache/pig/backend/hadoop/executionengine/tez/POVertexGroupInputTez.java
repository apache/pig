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
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.io.NullableTuple;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.library.api.KeyValuesReader;

/**
 * POVertexGroupInputTez is used on the backend to union tuples from Tez
 * ConcatenatedMergedInputs
 */
public class POVertexGroupInputTez extends PhysicalOperator implements TezLoad {

    private static final long serialVersionUID = 1L;

    private boolean hasNext;
    private String inputKey;
    private transient KeyValuesReader reader;

    public POVertexGroupInputTez(OperatorKey k) {
        super(k);
    }

    public String getInputKey() {
        return inputKey;
    }

    public void setInputKey(String inputKey) {
        this.inputKey = inputKey;
    }

    @Override
    public void attachInputs(Map<String, LogicalInput> inputs, Configuration conf)
            throws ExecException {
        try {
            LogicalInput input = inputs.get(inputKey);
            if (input == null) {
                throw new ExecException("Input VertexGroup " + inputKey + " is missing");
            }
            reader = (KeyValuesReader) input.getReader();
            hasNext = reader.next();
        } catch (Exception e) {
            throw new ExecException(e);
        }
    }

    @Override
    public Result getNextTuple() throws ExecException {
        try {
            while (hasNext) {
                if (reader.getCurrentValues().iterator().hasNext()) {
                    NullableTuple key = (NullableTuple) reader.getCurrentKey();
                    // The value is always null since the whole record is
                    // shuffled as the key. Call next() on iterator to just move
                    // it forward
                    reader.getCurrentValues().iterator().next();
                    return new Result(POStatus.STATUS_OK, key.getValueAsPigType());
                }
                hasNext = reader.next();
            }
            return RESULT_EOP;
        } catch (IOException e) {
            throw new ExecException(e);
        }
    }

    @Override
    public void addInputsToSkip(Set<String> inputsToSkip) {
        // TODO Auto-generated method stub
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
        return "POVertexGroupInputTez - " + inputKey+ "\t->\t " + mKey.toString();
    }
}

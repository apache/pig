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
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.JobControlCompiler;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLoad;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.tez.mapreduce.input.MRInput;
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.library.api.KeyValueReader;

/**
 * POSimpleTezLoad is used on the backend to read tuples from a Tez MRInput
 */
public class POSimpleTezLoad extends POLoad implements TezInput {

    private static final long serialVersionUID = 1L;
    private String inputKey;
    private MRInput input;
    private KeyValueReader reader;
    private transient Configuration conf;

    public POSimpleTezLoad(OperatorKey k) {
        super(k);
    }

    @Override
    public String[] getTezInputs() {
        return new String[] { inputKey };
    }

    @Override
    public void replaceInput(String oldInputKey, String newInputKey) {
        if (oldInputKey.equals(inputKey)) {
            inputKey = newInputKey;
        }
    }

    @Override
    public void addInputsToSkip(Set<String> inputsToSkip) {
    }

    @Override
    public void attachInputs(Map<String, LogicalInput> inputs,
            Configuration conf)
            throws ExecException {
        this.conf = conf;
        LogicalInput logInput = inputs.get(inputKey);
        if (logInput == null || !(logInput instanceof MRInput)) {
            throw new ExecException("POSimpleTezLoad only accepts MRInputs");
        }
        input = (MRInput) logInput;
        try {
            reader = input.getReader();
        } catch (IOException e) {
            throw new ExecException(e);
        }
    }

    /**
     * Previously, we reused the same Result object for all results, but we found
     * certain operators (e.g. POStream) save references to the Result object and
     * expect it to be constant.
     */
    @Override
    public Result getNextTuple() throws ExecException {
        try {
            Result res = new Result();
            if (!reader.next()) {
                res.result = null;
                res.returnStatus = POStatus.STATUS_EOP;
                // For certain operators (such as STREAM), we could still have some work
                // to do even after seeing the last input. These operators set a flag that
                // says all input has been sent and to run the pipeline one more time.
                if (Boolean.valueOf(conf.get(JobControlCompiler.END_OF_INP_IN_MAP, "false"))) {
                    this.parentPlan.endOfAllInput = true;
                }
            } else {
                Tuple next = (Tuple) reader.getCurrentValue();
                res.result = next;
                res.returnStatus = POStatus.STATUS_OK;
            }
            return res;
        } catch (IOException e) {
            throw new ExecException(e);
        }
    }

    public void setInputKey(String inputKey) {
        this.inputKey = inputKey;
    }
}

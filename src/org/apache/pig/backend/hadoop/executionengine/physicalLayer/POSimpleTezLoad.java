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

package org.apache.pig.backend.hadoop.executionengine.physicalLayer;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLoad;
import org.apache.pig.backend.hadoop.executionengine.tez.TezLoad;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.tez.mapreduce.input.MRInput;
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.library.api.KeyValueReader;

/**
 * POSimpleTezLoad is used on the backend to read tuples from a Tez MRInput
 */
public class POSimpleTezLoad extends POLoad implements TezLoad {
    /**
     *
     */
    private static final long serialVersionUID = 1L;
    private String inputKey;
    private MRInput input;
    private KeyValueReader reader;

    private Result res;

    public POSimpleTezLoad(OperatorKey k) {
        super(k);
        res = new Result();
    }

    @Override
    public void attachInputs(Map<String, LogicalInput> inputs,
            Configuration conf)
            throws ExecException {
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

    @Override
    public Result getNextTuple() throws ExecException {
        try {
            if (!reader.next()) {
                res.result = null;
                res.returnStatus = POStatus.STATUS_EOP;
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

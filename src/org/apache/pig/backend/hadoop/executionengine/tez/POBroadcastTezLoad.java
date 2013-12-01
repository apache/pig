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
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POPackage;
import org.apache.pig.impl.io.NullableTuple;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.library.broadcast.input.BroadcastKVReader;
import org.apache.tez.runtime.library.input.ShuffledUnorderedKVInput;

import com.google.common.collect.Lists;

/**
 * POBroadcastTezLoad is used on the backend to read tuples from a Tez ShuffledUnorderedKVInput
 */
public class POBroadcastTezLoad extends POPackage implements TezLoad {

    private static final long serialVersionUID = 1L;
    private static Result eopResult = new Result(POStatus.STATUS_EOP, null) ;

    private List<ShuffledUnorderedKVInput> inputs = Lists.newArrayList();
    @SuppressWarnings("rawtypes")
    private List<BroadcastKVReader> readers = Lists.newArrayList();
    private int currIdx = 0;

    public POBroadcastTezLoad(OperatorKey k) {
        super(k);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void attachInputs(Map<String, LogicalInput> inputs, Configuration conf)
            throws ExecException {
        try {
            for (LogicalInput input : inputs.values()) {
                ShuffledUnorderedKVInput suInput = (ShuffledUnorderedKVInput) input;
                this.inputs.add(suInput);
                this.readers.add((BroadcastKVReader) suInput.getReader());
            }
        } catch (Exception e) {
            throw new ExecException(e);
        }
    }

    @Override
    public Result getNextTuple() throws ExecException {
        // Drain inputs in order.
        while (currIdx < inputs.size()) {
            try {
                if (readers.get(currIdx).next()) {
                    Result res = new Result();
                    NullableTuple val = (NullableTuple) readers.get(currIdx).getCurrentValue();
                    res.result = val.getValueAsPigType();
                    res.returnStatus = POStatus.STATUS_OK;
                    return res;
                }
            } catch (IOException e) {
                throw new ExecException(e);
            }
            currIdx++;
        }
        return eopResult;
    }
}

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

import org.apache.hadoop.conf.Configuration;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POPackage;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.io.NullableTuple;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.library.api.KeyValuesReader;
import org.apache.tez.runtime.library.input.ShuffledMergedInput;

/**
 * POUnionTezLoad is used on the backend to union tuples from Tez ShuffledMergedInputs
 */
public class POUnionTezLoad extends POShuffleTezLoad {

    private static final long serialVersionUID = 1L;
    private static Result eopResult = new Result(POStatus.STATUS_EOP, null);
    private int currIdx = 0;
    private int tuplCnt = 0;
    private Result res;

    public POUnionTezLoad(POPackage pack) {
        super(pack);
    }

    @Override
    public void attachInputs(Map<String, LogicalInput> inputs, Configuration conf)
            throws ExecException {
        try {
            for (String key : inputKeys) {
                LogicalInput input = inputs.get(key);
                if (input instanceof ShuffledMergedInput) {
                    ShuffledMergedInput suInput = (ShuffledMergedInput) input;
                    this.inputs.add(suInput);
                    this.readers.add((KeyValuesReader) suInput.getReader());
                }
            }

            // We need to adjust numInputs because it's possible for both
            // ShuffledMergedInputs and non-ShuffledMergedInputs to be attached
            // to the same vertex. If so, we're only interested in
            // ShuffledMergedInputs. So we ignore the others.
            this.numInputs = this.inputs.size();
        } catch (Exception e) {
            throw new ExecException(e);
        }
    }

    @Override
    public Result getNextTuple() throws ExecException {
        // Return the same tuple as previous if the tuple count is > 0.
        if (tuplCnt > 0) {
            tuplCnt -= 1;
            return res;
        }

        // Drain inputs in order.
        while (currIdx < numInputs) {
            try {
                if (readers.get(currIdx).next()) {
                    // If there are multiple tuples for the current key, we
                    // increase the tuple count and return the same tuple as
                    // many times as tuplCnt.
                    for (Object val : readers.get(currIdx).getCurrentValues()) {
                        if (tuplCnt == 0) {
                            res = new Result();
                            NullableTuple nTup = (NullableTuple) val;
                            res.result = (Tuple) nTup.getValueAsPigType();;
                            res.returnStatus = POStatus.STATUS_OK;
                        }
                        tuplCnt += 1;
                    }
                    if (tuplCnt > 0) {
                        tuplCnt -= 1;
                        return res;
                    }
                }
            } catch (IOException e) {
                throw new ExecException(e);
            }
            currIdx++;
        }
        return eopResult;
    }
}

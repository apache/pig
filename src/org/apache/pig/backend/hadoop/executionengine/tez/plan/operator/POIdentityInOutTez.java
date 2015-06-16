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
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLocalRearrange;
import org.apache.pig.backend.hadoop.executionengine.tez.runtime.TezInput;
import org.apache.pig.backend.hadoop.executionengine.tez.runtime.TezOutput;
import org.apache.pig.impl.io.NullablePartitionWritable;
import org.apache.pig.impl.io.PigNullableWritable;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.api.LogicalOutput;
import org.apache.tez.runtime.api.Reader;
import org.apache.tez.runtime.library.api.KeyValueReader;
import org.apache.tez.runtime.library.api.KeyValueWriter;
import org.apache.tez.runtime.library.api.KeyValuesReader;

/**
 * POIdentityInOutTez is used to pass through tuples as is to next vertex from
 * previous vertex's POLocalRearrangeTez. For eg: In case of Order By, the
 * partition vertex which just applies the WeightedRangePartitioner on the
 * previous vertex data uses POIdentityInOutTez.
 */
@InterfaceAudience.Private
public class POIdentityInOutTez extends POLocalRearrangeTez implements TezInput, TezOutput {

    private static final long serialVersionUID = 1L;
    private static final Log LOG = LogFactory.getLog(POIdentityInOutTez.class);
    private String inputKey;
    private transient KeyValueReader reader;
    private transient KeyValuesReader shuffleReader;
    private transient boolean shuffleInput;
    private transient boolean finished = false;

    public POIdentityInOutTez(OperatorKey k, POLocalRearrange inputRearrange) {
        super(inputRearrange);
        this.mKey = k;
    }

    public void setInputKey(String inputKey) {
        this.inputKey = inputKey;
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
            Configuration conf) throws ExecException {
        LogicalInput input = inputs.get(inputKey);
        if (input == null) {
            throw new ExecException("Input from vertex " + inputKey + " is missing");
        }
        try {
            Reader r = input.getReader();
            if (r instanceof KeyValueReader) {
                reader = (KeyValueReader) r;
            } else {
                shuffleInput = true;
                shuffleReader = (KeyValuesReader) r;
            }
            LOG.info("Attached input from vertex " + inputKey + " : input=" + input + ", reader=" + r);
        } catch (Exception e) {
            throw new ExecException(e);
        }
    }

    @Override
    public void attachOutputs(Map<String, LogicalOutput> outputs,
            Configuration conf) throws ExecException {
        LogicalOutput output = outputs.get(outputKey);
        if (output == null) {
            throw new ExecException("Output to vertex " + outputKey + " is missing");
        }
        try {
            writer = (KeyValueWriter) output.getWriter();
            LOG.info("Attached output to vertex " + outputKey + " : output=" + output + ", writer=" + writer);
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
            if (shuffleInput) {
                while (shuffleReader.next()) {
                    Object curKey = shuffleReader.getCurrentKey();
                    Iterable<Object> vals = shuffleReader.getCurrentValues();
                    if (isSkewedJoin) {
                        NullablePartitionWritable wrappedKey = new NullablePartitionWritable(
                                (PigNullableWritable) curKey);
                        wrappedKey.setPartition(-1);
                        curKey = wrappedKey;
                    }
                    for (Object val : vals) {
                        writer.write(curKey, val);
                    }
                }
            } else {
                while (reader.next()) {
                    if (isSkewedJoin) {
                        NullablePartitionWritable wrappedKey = new NullablePartitionWritable(
                                (PigNullableWritable) reader.getCurrentKey());
                        // Skewed join wraps key with NullablePartitionWritable
                        // The partitionIndex in NullablePartitionWritable is not serialized.
                        // So setting it here instead of the previous vertex POLocalRearrangeTez.
                        // Serializing it would add overhead for MR as well.
                        wrappedKey.setPartition(-1);
                        writer.write(wrappedKey, reader.getCurrentValue());
                    } else {
                        writer.write(reader.getCurrentKey(),
                                reader.getCurrentValue());
                    }
                }
            }
            finished = true;
            return RESULT_EOP;
        } catch (IOException e) {
            throw new ExecException(e);
        }
    }

    @Override
    public String name() {
        return "POIdentityInOutTez - " + mKey.toString() + "\t<-\t " + inputKey + "\t->\t " + outputKey;
    }

}

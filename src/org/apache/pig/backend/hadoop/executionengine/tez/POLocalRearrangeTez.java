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
import org.apache.pig.backend.hadoop.HDataType;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLocalRearrange;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.io.NullablePartitionWritable;
import org.apache.pig.impl.io.NullableTuple;
import org.apache.pig.impl.io.PigNullableWritable;
import org.apache.pig.impl.plan.NodeIdGenerator;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.tez.runtime.api.LogicalOutput;
import org.apache.tez.runtime.library.api.KeyValueWriter;
import org.apache.tez.runtime.library.output.OnFileSortedOutput;
import org.apache.tez.runtime.library.output.OnFileUnorderedKVOutput;

/**
 * POLocalRearrangeTez is used to write to a Tez OnFileSortedOutput
 * (shuffle) or OnFileUnorderedKVOutput (broadcast)
 */
public class POLocalRearrangeTez extends POLocalRearrange implements TezOutput {

    private static final long serialVersionUID = 1L;
    protected static Result empty = new Result(POStatus.STATUS_NULL, null);

    protected String outputKey;
    protected transient KeyValueWriter writer;

    // Tez union is implemented as LR + Pkg
    private boolean isUnion = false;
    private boolean isSkewedJoin = false;

    public POLocalRearrangeTez(OperatorKey k) {
        super(k);
    }

    public POLocalRearrangeTez(OperatorKey k, int rp) {
        super(k, rp);
    }

    public POLocalRearrangeTez(POLocalRearrange copy) {
        super(copy);
    }

    public String getOutputKey() {
        return outputKey;
    }

    public void setOutputKey(String outputKey) {
        this.outputKey = outputKey;
    }

    public boolean isUnion() {
        return isUnion;
    }

    public void setUnion(boolean isUnion) {
        this.isUnion = isUnion;
    }

    public boolean isSkewedJoin() {
        return isSkewedJoin;
    }

    public void setSkewedJoin(boolean isSkewedJoin) {
        this.isSkewedJoin = isSkewedJoin;
    }

    @Override
    public void attachOutputs(Map<String, LogicalOutput> outputs,
            Configuration conf) throws ExecException {
        LogicalOutput logicalOut = outputs.get(outputKey);
        if (logicalOut == null) {
            throw new ExecException(
                    "POLocalRearrangeTez only accepts OnFileSortedOutput (shuffle)"
                            + " or OnFileUnorderedKVOutput(broadcast). key =  "
                            + outputKey + ", outputs = " + outputs
                            + ", operator = " + this);
        }
        try {
            if (logicalOut instanceof OnFileSortedOutput) {
                writer = ((OnFileSortedOutput) logicalOut).getWriter();
            } else if (logicalOut instanceof OnFileUnorderedKVOutput) {
                writer = ((OnFileUnorderedKVOutput) logicalOut).getWriter();
            } else {
                throw new ExecException(
                        "POLocalRearrangeTez only accepts OnFileSortedOutput (shuffle)"
                                + " or OnFileUnorderedKVOutput(broadcast). key =  "
                                + outputKey + ", outputs = " + outputs
                                + ", operator = " + this);
            }
        } catch (ExecException e) {
            throw e;
        } catch (Exception e) {
            throw new ExecException(e);
        }
    }

    @Override
    public Result getNextTuple() throws ExecException {
        Result res = super.getNextTuple();
        if (writer == null) { // In the case of combiner
            return res;
        }

        try {
            switch (res.returnStatus) {
            case POStatus.STATUS_OK:
                if (illustrator == null) {
                    Tuple result = (Tuple) res.result;
                    Byte index = (Byte)result.get(0);
                    PigNullableWritable key = null;
                    NullableTuple val = null;
                    if (isUnion) {
                        // Use the entire tuple as both key and value
                        key = HDataType.getWritableComparableTypes(result.get(1), keyType);
                        val = new NullableTuple((Tuple)result.get(1));
                    } else if (isSkewedJoin) {
                        // Skewed join uses NullablePartitionWritable as key
                        Byte tupleKeyIdx = 2;
                        Byte tupleValIdx = 3;

                        Integer partitionIndex = -1;
                        tupleKeyIdx--;
                        tupleValIdx--;

                        key = HDataType.getWritableComparableTypes(result.get(tupleKeyIdx), keyType);
                        NullablePartitionWritable wrappedKey = new NullablePartitionWritable(key);
                        wrappedKey.setIndex(index);
                        wrappedKey.setPartition(partitionIndex);
                        key = wrappedKey;
                        val = new NullableTuple((Tuple)result.get(tupleValIdx));
                    } else {
                        key = HDataType.getWritableComparableTypes(result.get(1), keyType);
                        val = new NullableTuple((Tuple)result.get(2));
                    }

                    // Both the key and the value need the index.  The key needs it so
                    // that it can be sorted on the index in addition to the key
                    // value.  The value needs it so that POPackage can properly
                    // assign the tuple to its slot in the projection.
                    key.setIndex(index);
                    val.setIndex(index);
                    writer.write(key, val);
                } else {
                    illustratorMarkup(res.result, res.result, 0);
                }
                res = empty;
                break;
            case POStatus.STATUS_EOP:
            case POStatus.STATUS_ERR:
            case POStatus.STATUS_NULL:
            default:
                break;
            }
        } catch (IOException ioe) {
            int errCode = 2135;
            String msg = "Received error from POLocalRearrage function." + ioe.getMessage();
            throw new ExecException(msg, errCode, ioe);
        }
        return inp;
    }

    /**
     * Make a deep copy of this operator.
     * @throws CloneNotSupportedException
     */
    @Override
    public POLocalRearrangeTez clone() throws CloneNotSupportedException {
        POLocalRearrangeTez clone = new POLocalRearrangeTez(new OperatorKey(
                mKey.scope, NodeIdGenerator.getGenerator().getNextNodeId(
                        mKey.scope)), requestedParallelism);
        deepCopyTo(clone);
        clone.setOutputKey(outputKey);
        return clone;
    }

    @Override
    public String name() {
        return super.name() + "\t->\t " + outputKey;
    }
}

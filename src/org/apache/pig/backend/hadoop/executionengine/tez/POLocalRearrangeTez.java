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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.HDataType;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLocalRearrange;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.io.NullableTuple;
import org.apache.pig.impl.io.PigNullableWritable;
import org.apache.pig.impl.plan.NodeIdGenerator;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.tez.runtime.api.LogicalOutput;
import org.apache.tez.runtime.library.api.KeyValueWriter;

/**
 * POLocalRearrangeTez is used to write to a Tez OnFileSortedOutput
 * (shuffle) or OnFileUnorderedKVOutput (broadcast)
 */
public class POLocalRearrangeTez extends POLocalRearrange implements TezOutput {

    private static final long serialVersionUID = 1L;
    private static final Log LOG = LogFactory.getLog(POLocalRearrangeTez.class);

    protected String outputKey;
    protected transient KeyValueWriter writer;

    protected boolean connectedToPackage = true;
    protected boolean isSkewedJoin = false;

    public POLocalRearrangeTez(OperatorKey k) {
        super(k);
    }

    public POLocalRearrangeTez(OperatorKey k, int rp) {
        super(k, rp);
    }

    public POLocalRearrangeTez(POLocalRearrange copy) {
        super(copy);
        if (copy instanceof POLocalRearrangeTez) {
            POLocalRearrangeTez copyTez = (POLocalRearrangeTez) copy;
            this.isSkewedJoin = copyTez.isSkewedJoin;
            this.connectedToPackage = copyTez.connectedToPackage;
            this.outputKey = copyTez.outputKey;
        }
    }

    public String getOutputKey() {
        return outputKey;
    }

    public void setOutputKey(String outputKey) {
        this.outputKey = outputKey;
    }

    public boolean isConnectedToPackage() {
        return connectedToPackage;
    }

    public void setConnectedToPackage(boolean connectedToPackage) {
        this.connectedToPackage = connectedToPackage;
    }

    public boolean isSkewedJoin() {
        return isSkewedJoin;
    }

    public void setSkewedJoin(boolean isSkewedJoin) {
        this.isSkewedJoin = isSkewedJoin;
    }

    @Override
    public String[] getTezOutputs() {
        return new String[] { outputKey };
    }

    @Override
    public void replaceOutput(String oldOutputKey, String newOutputKey) {
        if (oldOutputKey.equals(outputKey)) {
            outputKey = newOutputKey;
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
        res = super.getNextTuple();
        if (writer == null) { // In the case of combiner
            return res;
        }

        try {
            switch (res.returnStatus) {
            case POStatus.STATUS_OK:
                if (illustrator == null) {
                    Tuple result = (Tuple) res.result;
                    Byte index = (Byte) result.get(0);
                    PigNullableWritable key = HDataType.getWritableComparableTypes(result.get(1), keyType);
                    NullableTuple val = new NullableTuple((Tuple)result.get(2));

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
                res = RESULT_EMPTY;
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
        clone.isSkewedJoin = isSkewedJoin;
        clone.connectedToPackage = connectedToPackage;
        clone.setOutputKey(outputKey);
        return clone;
    }

    @Override
    public String name() {
        return super.name() + "\t->\t " + outputKey;
    }
}

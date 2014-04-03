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
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.tez.mapreduce.output.MROutput;
import org.apache.tez.runtime.api.LogicalOutput;
import org.apache.tez.runtime.library.api.KeyValueWriter;

/**
 * POStoreTez is used to write to a Tez MROutput
 */
public class POStoreTez extends POStore implements TezOutput {

    private static final long serialVersionUID = 1L;
    private transient MROutput output;
    private transient KeyValueWriter writer;
    private String outputKey;

    public POStoreTez(OperatorKey k) {
        super(k);
        this.outputKey = k.toString();
    }

    public POStoreTez(POStore copy) {
        super(copy);
        this.outputKey = copy.getOperatorKey().toString();
    }

    @Override
    public void visit(PhyPlanVisitor v) throws VisitorException {
        v.visitPOStoreTez(this);
    }
    
    public String getOutputKey() {
        return outputKey;
    }

    public void setOutputKey(String outputKey) {
        this.outputKey = outputKey;
    }

    @Override
    public String[] getTezOutputs() {
        return new String[] { outputKey };
    }

    @Override
    public void replaceOutput(String oldOutputKey, String newOutputKey) {
    }

    @Override
    public void attachOutputs(Map<String, LogicalOutput> outputs,
            Configuration conf)
            throws ExecException {
        LogicalOutput logicalOut = outputs.get(outputKey);
        if (logicalOut == null || !(logicalOut instanceof MROutput)) {
            throw new ExecException("POStoreTez only accepts MROutput. key =  "
                    + getOperatorKey() + ", outputs = " + outputs);
        }
        output = (MROutput) logicalOut;
        try {
            writer = output.getWriter();
        } catch (IOException e) {
            throw new ExecException(e);
        }
    }

    @Override
    public Result getNextTuple() throws ExecException {
        Result res = processInput();
        try {
            switch (res.returnStatus) {
            case POStatus.STATUS_OK:
                if (illustrator == null) {
                    // TODO: What if the storeFunc.putNext does not write null
                    // for the key? But this seems to be the behaviour in MR also
                    writer.write(null, res.result);
                } else
                    illustratorMarkup(res.result, res.result, 0);
                res = empty;

                if (outputRecordCounter != null) {
                    outputRecordCounter.increment(1);
                }
                break;
            case POStatus.STATUS_EOP:
            case POStatus.STATUS_ERR:
            case POStatus.STATUS_NULL:
            default:
                break;
            }
        } catch (IOException ioe) {
            int errCode = 2135;
            String msg = "Received error from store function." + ioe.getMessage();
            throw new ExecException(msg, errCode, ioe);
        }
        return res;
    }

}

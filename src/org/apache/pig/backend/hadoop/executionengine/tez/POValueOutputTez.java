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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Writable;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.tez.runtime.api.LogicalOutput;
import org.apache.tez.runtime.library.api.KeyValueWriter;

public class POValueOutputTez extends PhysicalOperator implements TezOutput {

    private static final long serialVersionUID = 1L;
    private static final Log LOG = LogFactory.getLog(POValueOutputTez.class);
    // TODO Change this to outputKey and write only once
    // when a shared edge support is available in Tez
    protected Set<String> outputKeys = new HashSet<String>();
    // TODO Change this to value only writer after implementing
    // value only input output
    protected transient List<KeyValueWriter> writers;

    public static EmptyWritable EMPTY_KEY = new EmptyWritable();

    public POValueOutputTez(OperatorKey k) {
        super(k);
    }

    @Override
    public String[] getTezOutputs() {
        return outputKeys.toArray(new String[outputKeys.size()]);
    }

    @Override
    public void replaceOutput(String oldOutputKey, String newOutputKey) {
        if (outputKeys.remove(oldOutputKey)) {
            outputKeys.add(oldOutputKey);
        }
    }

    @Override
    public void attachOutputs(Map<String, LogicalOutput> outputs,
            Configuration conf) throws ExecException {
        writers = new ArrayList<KeyValueWriter>();
        for (String outputKey : outputKeys) {
            LogicalOutput output = outputs.get(outputKey);
            if (output == null) {
                throw new ExecException("Output to vertex " + outputKey
                        + " is missing");
            }
            try {
                KeyValueWriter writer = (KeyValueWriter) output.getWriter();
                writers.add(writer);
                LOG.info("Attached output to vertex " + outputKey + " : output=" + output + ", writer=" + writer);
            } catch (Exception e) {
                throw new ExecException(e);
            }
        }
    }

    public void addOutputKey(String outputKey) {
        outputKeys.add(outputKey);
    }

    public void removeOutputKey(String outputKey) {
        outputKeys.remove(outputKey);
    }

    @Override
    public Result getNextTuple() throws ExecException {
        Result inp;
        while (true) {
            inp = processInput();
            if (inp.returnStatus == POStatus.STATUS_EOP
                    || inp.returnStatus == POStatus.STATUS_ERR) {
                break;
            }
            if (inp.returnStatus == POStatus.STATUS_NULL) {
                continue;
            }
            for (KeyValueWriter writer : writers) {
                try {
                    writer.write(EMPTY_KEY, inp.result);
                } catch (IOException e) {
                    throw new ExecException(e);
                }
            }
            return RESULT_EMPTY;
        }
        return inp;
    }

    @Override
    public Tuple illustratorMarkup(Object in, Object out, int eqClassIndex) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void visit(PhyPlanVisitor v) throws VisitorException {
        v.visitValueOutputTez(this);
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
        return "POValueOutputTez - " + mKey.toString() + "\t->\t " + outputKeys;
    }

    public static class EmptyWritable implements Writable {

        @Override
        public void write(DataOutput out) throws IOException {
        }

        @Override
        public void readFields(DataInput in) throws IOException {
        }
    }

    //TODO: Remove after PIG-3775/TEZ-661
    public static class EmptyWritableComparator implements RawComparator<EmptyWritable> {

        @Override
        public int compare(EmptyWritable o1, EmptyWritable o2) {
            return 0;
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            // 0 - Reverses the input order. 0 groups all values into
            // single record on reducer which is additional overhead.
            // -1, 1 - Returns input in random order. But comparator is invoked way more
            // times than 0. Compared to 1, -1 invokes comparator even more.
            // Going with 0 for now. After TEZ-661 this will not be required any more.
            return 0;
        }

    }

}

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
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.tez.runtime.TezInput;
import org.apache.pig.backend.hadoop.executionengine.tez.runtime.TezOutput;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.api.LogicalOutput;
import org.apache.tez.runtime.library.api.KeyValueWriter;
import org.apache.tez.runtime.library.api.KeyValuesReader;

/**
 * POCounterStatsTez is used to group counters from previous vertex POCounterTez tasks
 */
public class POCounterStatsTez extends PhysicalOperator implements TezInput, TezOutput {

    private static final long serialVersionUID = 1L;
    private static final Log LOG = LogFactory.getLog(POCounterStatsTez.class);
    private String inputKey;
    private String outputKey;
    // TODO: Even though we expect only one record from POCounter, because of Shuffle we have
    // KeyValuesReader. After TEZ-661, switch to unsorted shuffle
    private transient KeyValuesReader reader;
    private transient KeyValueWriter writer;
    private transient boolean finished = false;

    public POCounterStatsTez(OperatorKey k) {
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
        LogicalInput input = inputs.get(inputKey);
        if (input == null) {
            throw new ExecException("Input from vertex " + inputKey + " is missing");
        }
        try {
            reader = (KeyValuesReader) input.getReader();
            LOG.info("Attached input from vertex " + inputKey + " : input=" + input + ", reader=" + reader);
        } catch (Exception e) {
            throw new ExecException(e);
        }
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
        try {
            if (finished) {
                return RESULT_EOP;
            }
            Map<Integer, Long> counterRecords = new HashMap<Integer, Long>();
            Integer key = null;
            Long value = null;
            // Read count of records per task
            while (reader.next()) {
                key = ((IntWritable)reader.getCurrentKey()).get();
                for (Object val : reader.getCurrentValues()) {
                    value = ((LongWritable)val).get();
                    counterRecords.put(key, value);
                }
            }

            // BinInterSedes only takes String for map key
            Map<String, Long> counterOffsets = new HashMap<String, Long>();
            // Create a map to contain task ids and beginning offset of record count
            // based on total record count of all tasks
            // For eg: If Task 0 has 5 records, Task 1 has 10 records and Task 2 has 3 records
            // map will contain {0=0, 1=5, 2=15}
            Long prevTasksCount = counterRecords.get(0);
            counterOffsets.put("0", 0L);
            for (int i = 1; i < counterRecords.size(); i++) {
                counterOffsets.put("" + i, prevTasksCount);
                prevTasksCount += counterRecords.get(i);
            }

            Tuple tuple = mTupleFactory.newTuple(1);
            tuple.set(0, counterOffsets);
            writer.write(POValueOutputTez.EMPTY_KEY, tuple);
            finished = true;
            return RESULT_EOP;
        } catch (IOException e) {
            throw new ExecException(e);
        }
    }

    public void setInputKey(String inputKey) {
        this.inputKey = inputKey;
    }

    public void setOutputKey(String outputKey) {
        this.outputKey = outputKey;
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
        return "PORankStatsTez - " + mKey.toString() + "\t<-\t " + inputKey + "\t->\t " + outputKey;
    }
}

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
package org.apache.pig.backend.hadoop.executionengine.tez.operators;

import java.io.IOException;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POCounter;
import org.apache.pig.backend.hadoop.executionengine.tez.POValueOutputTez;
import org.apache.pig.backend.hadoop.executionengine.tez.TezOutput;
import org.apache.pig.backend.hadoop.executionengine.tez.TezTaskConfigurable;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.tez.runtime.api.LogicalOutput;
import org.apache.tez.runtime.api.TezProcessorContext;
import org.apache.tez.runtime.library.api.KeyValueWriter;

public class POCounterTez extends POCounter implements TezOutput, TezTaskConfigurable {

    private static final long serialVersionUID = 1L;
    private static final Log LOG = LogFactory.getLog(POCounterTez.class);

    private String tuplesOutputKey;
    private String statsOutputKey;

    private transient KeyValueWriter tuplesWriter;
    private transient KeyValueWriter statsWriter;
    private transient long totalTaskRecords = 0;

    public POCounterTez(POCounter copy) {
        super(copy);
    }

    @Override
    public void initialize(TezProcessorContext processorContext)
            throws ExecException {
        this.setTaskId(processorContext.getTaskIndex());
    }

    public void setTuplesOutputKey(String tuplesOutputKey) {
        this.tuplesOutputKey = tuplesOutputKey;
    }

    public void setStatsOutputKey(String statsOutputKey) {
        this.statsOutputKey = statsOutputKey;
    }

    @Override
    public String[] getTezOutputs() {
        return new String[] { tuplesOutputKey, statsOutputKey };
    }

    @Override
    public void replaceOutput(String oldOutputKey, String newOutputKey) {
        if (oldOutputKey.equals(tuplesOutputKey)) {
            tuplesOutputKey = newOutputKey;
        } else if (oldOutputKey.equals(statsOutputKey)) {
            statsOutputKey = newOutputKey;
        }
    }

    @Override
    public void attachOutputs(Map<String, LogicalOutput> outputs,
            Configuration conf) throws ExecException {
        LogicalOutput output = outputs.get(tuplesOutputKey);
        if (output == null) {
            throw new ExecException("Output to vertex " + tuplesOutputKey + " is missing");
        }
        try {
            tuplesWriter = (KeyValueWriter) output.getWriter();
            LOG.info("Attached output to vertex " + tuplesOutputKey + " : output=" + output + ", writer=" + tuplesWriter);
        } catch (Exception e) {
            throw new ExecException(e);
        }

        output = outputs.get(statsOutputKey);
        if (output == null) {
            throw new ExecException("Output to vertex " + statsOutputKey + " is missing");
        }
        try {
            statsWriter = (KeyValueWriter) output.getWriter();
            LOG.info("Attached output to vertex " + statsOutputKey + " : output=" + output + ", writer=" + statsWriter);
        } catch (Exception e) {
            throw new ExecException(e);
        }
    }

    @Override
    public Result getNextTuple() throws ExecException {
        Result inp = null;
        try {
            while (true) {
                inp = processInput();
                if (inp.returnStatus == POStatus.STATUS_EOP
                        || inp.returnStatus == POStatus.STATUS_ERR)
                    break;
                if (inp.returnStatus == POStatus.STATUS_NULL) {
                    continue;
                }

                tuplesWriter.write(POValueOutputTez.EMPTY_KEY,
                        addCounterValue(inp).result);
                if (isRowNumber()) {
                    incrementReduceCounter(POCounter.ONE);
                }
            }

            statsWriter.write(new IntWritable(this.getTaskId()), new LongWritable(totalTaskRecords));

        } catch (IOException e) {
            throw new ExecException(e);
        }
        return RESULT_EOP;
    }

    @Override
    protected void incrementReduceCounter(Long increment) {
        totalTaskRecords += increment;
    }

    @Override
    public void visit(PhyPlanVisitor v) throws VisitorException {
        super.visit(v);
        v.visit(this);
    }

    @Override
    public String name() {
        return "POCounterTez - " + mKey.toString() + "\t->\t " + tuplesOutputKey + "," + statsOutputKey;
    }

}

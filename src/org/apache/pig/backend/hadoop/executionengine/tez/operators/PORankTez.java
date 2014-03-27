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
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.PORank;
import org.apache.pig.backend.hadoop.executionengine.tez.ObjectCache;
import org.apache.pig.backend.hadoop.executionengine.tez.TezLoad;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.library.api.KeyValueReader;

public class PORankTez extends PORank implements TezLoad {

    private static final long serialVersionUID = 1L;
    private static final Log LOG = LogFactory.getLog(PORankTez.class);

    private String tuplesInputKey;
    private String statsInputKey;
    private transient boolean isInputCached;
    private transient KeyValueReader reader;
    private transient Map<Integer, Long> counterOffsets;

    public PORankTez(PORank copy) {
        super(copy);
    }

    public void setTuplesInputKey(String tuplesInputKey) {
        this.tuplesInputKey = tuplesInputKey;
    }

    public void setStatsInputKey(String statsInputKey) {
        this.statsInputKey = statsInputKey;
    }

    @Override
    public void addInputsToSkip(Set<String> inputsToSkip) {
        String cacheKey = "rankstats-" + getOperatorKey().toString();
        Object cacheValue = ObjectCache.getInstance().retrieve(cacheKey);
        if (cacheValue != null) {
            isInputCached = true;
            inputsToSkip.add(statsInputKey);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void attachInputs(Map<String, LogicalInput> inputs,
            Configuration conf) throws ExecException {
        LogicalInput input = inputs.get(tuplesInputKey);
        if (input == null) {
            throw new ExecException("Input from vertex " + tuplesInputKey + " is missing");
        }
        try {
            reader = (KeyValueReader) input.getReader();
            LOG.info("Attached input from vertex " + tuplesInputKey + " : input=" + input + ", reader=" + reader);
        } catch (Exception e) {
            throw new ExecException(e);
        }

        String cacheKey = "rankstats-" + getOperatorKey().toString();
        if (isInputCached) {
            counterOffsets = (Map<Integer, Long>) ObjectCache.getInstance().retrieve(cacheKey);
            LOG.info("Found counter stats for PORankTez in Tez cache. cachekey=" + cacheKey);
            return;
        }
        input = inputs.get(statsInputKey);
        if (input == null) {
            throw new ExecException("Input from vertex " + statsInputKey + " is missing");
        }
        try {
            KeyValueReader reader = (KeyValueReader) input.getReader();
            LOG.info("Attached input from vertex " + statsInputKey + " : input=" + input + ", reader=" + reader);
            reader.next();
            // POCounterStatsTez produces a HashMap which contains
            // mapping of task id and the offset of record count in each task based on total record count
            Map<String, Long> counterOffsetsTemp = (Map<String, Long>) ((Tuple)reader.getCurrentValue()).get(0);
            counterOffsets = new HashMap<Integer, Long>(counterOffsetsTemp.size(), 1);
            for (Entry<String, Long> entry : counterOffsetsTemp.entrySet()) {
                counterOffsets.put(Integer.valueOf(entry.getKey()), entry.getValue());
            }
            ObjectCache.getInstance().cache(cacheKey, counterOffsets);
            LOG.info("Cached PORankTez counter stats in Tez ObjectRegistry with vertex scope. cachekey=" + cacheKey);
        } catch (Exception e) {
            throw new ExecException(e);
        }
    }

    @Override
    public Result getNextTuple() throws ExecException {
        Result inp = null;

        try {
            while (reader.next()) {
                inp = new Result(POStatus.STATUS_OK, reader.getCurrentValue());
                return addRank(inp);
            }
        } catch (IOException e) {
            throw new ExecException(e);
        }

        return RESULT_EOP;
    }

    @Override
    protected Long getRankCounterOffset(Integer taskId) {
        if (illustrator != null) {
            return 0L;
        }
        return counterOffsets.get(taskId);
    }

    @Override
    public void visit(PhyPlanVisitor v) throws VisitorException {
        super.visit(v);
        v.visit(this);
    }

    @Override
    public String name() {
        return "PORankTez - " + mKey.toString() + "\t<-\t " + tuplesInputKey + "," + statsInputKey;
    }

}

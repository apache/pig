/*
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

package org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.tez.runtime.ObjectCache;
import org.apache.pig.backend.hadoop.executionengine.tez.runtime.TezInput;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.util.Pair;
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.library.api.KeyValueReader;

public class POMergeCogroupTez extends POMergeCogroup implements TezInput {

    private static final Log LOG = LogFactory.getLog(POMergeJoinTez.class);
    private static final long serialVersionUID = 1L;
    private String inputKey;
    private transient String cacheKey;
    private transient KeyValueReader reader;
    private transient List<Pair<Integer, Tuple>> index;

    public POMergeCogroupTez(OperatorKey k, List<PhysicalOperator> inpPOs, POLocalRearrange[] lrs, int parallel) {
        super(k, inpPOs, lrs, parallel);
    }

    public POMergeCogroupTez(POMergeCogroup copy) {
        super(copy);
    }

    public void setInputKey(String inputKey) {
        this.inputKey = inputKey;
    }

    @Override
    public String[] getTezInputs() {
        return new String[] { this.inputKey };
    }

    @Override
    public void replaceInput(String oldInputKey, String newInputKey) {
        this.inputKey = newInputKey;
    }

    @Override
    public void addInputsToSkip(Set<String> inputsToSkip) {
        cacheKey = "mergecogrp-" + inputKey;
        Object cacheValue = ObjectCache.getInstance().retrieve(cacheKey);
        if (cacheValue != null) {
            inputsToSkip.add(inputKey);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void attachInputs(Map<String, LogicalInput> inputs, Configuration conf) throws ExecException {
        Object cacheValue = ObjectCache.getInstance().retrieve(cacheKey);
        if (cacheValue != null) {
            this.index = (LinkedList<Pair<Integer, Tuple>>) cacheValue;
            return;
        }

        LogicalInput input = inputs.get(inputKey);
        if (input == null) {
            throw new ExecException("Input from vertex " + inputKey + " is missing");
        }

        try {
            input = inputs.get(this.inputKey);
            reader = (KeyValueReader) input.getReader();
            LOG.info(
                    "Attached input from vertex " + this.inputKey + " : input=" + input + ", reader=" + reader);
            index = new LinkedList<>();
            while (reader.next()) {
                Tuple origTuple = (Tuple) reader.getCurrentValue();
                Tuple copyTuple = mTupleFactory.newTuple(origTuple.getAll());
                addTupleToIndex(copyTuple, index);
            }
            ObjectCache.getInstance().cache(cacheKey, this.index);
        }
        catch (Exception e) {
            throw new ExecException(e);
        }
    }

    @Override
    public String name() {
        return super.name().replace("MergeCogroup", "MergeCogroupTez") + "\t<-\t " + this.inputKey;
    }

    @Override
    protected List<Pair<Integer, Tuple>> readIndex() {
        return this.index;
    }

    @Override
    public POMergeCogroupTez clone() throws CloneNotSupportedException {
        return (POMergeCogroupTez) super.clone();
    }
}
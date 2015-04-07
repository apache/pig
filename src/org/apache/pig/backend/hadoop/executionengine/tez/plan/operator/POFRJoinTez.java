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
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POFRJoin;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLocalRearrange;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POMergeJoin.TuplesToSchemaTupleList;
import org.apache.pig.backend.hadoop.executionengine.tez.runtime.ObjectCache;
import org.apache.pig.backend.hadoop.executionengine.tez.runtime.TezInput;
import org.apache.pig.data.SchemaTupleBackend;
import org.apache.pig.data.SchemaTupleClassGenerator.GenContext;
import org.apache.pig.data.SchemaTupleFactory;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.io.NullableTuple;
import org.apache.pig.impl.io.PigNullableWritable;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.library.api.KeyValueReader;

import com.google.common.collect.Lists;

/**
 * POFRJoinTez is used on the backend to load replicated table from Tez
 * ShuffleUnorderedKVInput and load fragmented table from data pipeline.
 */
public class POFRJoinTez extends POFRJoin implements TezInput {

    private static final Log log = LogFactory.getLog(POFRJoinTez.class);
    private static final long serialVersionUID = 1L;

    private List<String> inputKeys;

    // For replicated tables
    private transient List<LogicalInput> replInputs;
    private transient List<KeyValueReader> replReaders;
    private transient boolean isInputCached;
    private transient String cacheKey;

    public POFRJoinTez(POFRJoin copy, List<String> inputKeys) throws ExecException {
       super(copy);
       this.inputKeys = inputKeys;
    }

    @Override
    public String[] getTezInputs() {
        return inputKeys.toArray(new String[inputKeys.size()]);
    }

    @Override
    public void replaceInput(String oldInputKey, String newInputKey) {
        if (inputKeys.remove(oldInputKey)) {
            inputKeys.add(newInputKey);
        }
    }

    @Override
    public void addInputsToSkip(Set<String> inputsToSkip) {
        cacheKey = "replicatemap-" + inputKeys.toString();
        Object cacheValue = ObjectCache.getInstance().retrieve(cacheKey);
        if (cacheValue != null) {
            isInputCached = true;
            inputsToSkip.addAll(inputKeys);
        }
    }

    @Override
    public void attachInputs(Map<String, LogicalInput> inputs, Configuration conf)
            throws ExecException {
        if (isInputCached) {
            return;
        }
        try {
            this.replInputs = Lists.newArrayList();
            this.replReaders = Lists.newArrayList();
            for (String key : inputKeys) {
                LogicalInput input = inputs.get(key);
                if (!this.replInputs.contains(input)) {
                    this.replInputs.add(input);
                    this.replReaders.add((KeyValueReader) input.getReader());
                }
            }
        } catch (Exception e) {
            throw new ExecException(e);
        }
    }

    /**
     * Builds the HashMaps by reading replicated inputs from broadcast edges
     *
     * @throws ExecException
     */
    @Override
    protected void setUpHashMap() throws ExecException {

        // Re-check again in case of Split + union + replicate join
        // where same POFRJoinTez occurs in different Split sub-plans
        Object cacheValue = ObjectCache.getInstance().retrieve(cacheKey);
        if (cacheValue != null) {
            replicates = (TupleToMapKey[]) cacheValue;
            log.info("Found " + (replicates.length - 1) + " replication hash tables in Tez cache. cachekey=" + cacheKey);
            return;
        }

        log.info("Building replication hash table");

        SchemaTupleFactory[] inputSchemaTupleFactories = new SchemaTupleFactory[inputSchemas.length];
        SchemaTupleFactory[] keySchemaTupleFactories = new SchemaTupleFactory[inputSchemas.length];

        for (int i = 0; i < inputSchemas.length; i++) {
            Schema schema = inputSchemas[i];
            if (schema != null) {
                log.debug("Using SchemaTuple for FR Join Schema: " + schema);
                inputSchemaTupleFactories[i] =
                        SchemaTupleBackend.newSchemaTupleFactory(schema, false, GenContext.FR_JOIN);
            }
            schema = keySchemas[i];
            if (schema != null) {
                log.debug("Using SchemaTuple for FR Join key Schema: " + schema);
                keySchemaTupleFactories[i] =
                        SchemaTupleBackend.newSchemaTupleFactory(schema, false, GenContext.FR_JOIN);
            }
        }

        long time1 = System.currentTimeMillis();

        replicates[fragment] = null;
        int inputIdx = 0;
        // We need to adjust the index because the number of replInputs is
        // one less than the number of inputSchemas. The inputSchemas
        // includes the fragmented table.
        int schemaIdx = 1;
        while (inputIdx < replInputs.size()) {
            SchemaTupleFactory inputSchemaTupleFactory = inputSchemaTupleFactories[schemaIdx];
            SchemaTupleFactory keySchemaTupleFactory = keySchemaTupleFactories[schemaIdx];

            TupleToMapKey replicate = new TupleToMapKey(4000, keySchemaTupleFactory);
            POLocalRearrange lr = LRs[schemaIdx];

            try {
                while (replReaders.get(inputIdx).next()) {
                    if (getReporter() != null) {
                        getReporter().progress();
                    }

                    PigNullableWritable key = (PigNullableWritable) replReaders.get(inputIdx).getCurrentKey();
                    if (isKeyNull(key.getValueAsPigType())) continue;
                    NullableTuple val = (NullableTuple) replReaders.get(inputIdx).getCurrentValue();

                    // POFRJoin#getValueTuple() is reused to construct valTuple,
                    // and it takes an indexed Tuple as parameter. So we need to
                    // construct one here.
                    Tuple retTuple = mTupleFactory.newTuple(3);
                    retTuple.set(0, key.getIndex());
                    retTuple.set(1, key.getValueAsPigType());
                    retTuple.set(2, val.getValueAsPigType());
                    Tuple valTuple = getValueTuple(lr, retTuple);

                    Tuple keyTuple = mTupleFactory.newTuple(1);
                    keyTuple.set(0, key.getValueAsPigType());
                    if (replicate.get(keyTuple) == null) {
                        replicate.put(keyTuple, new TuplesToSchemaTupleList(1, inputSchemaTupleFactory));
                    }
                    replicate.get(keyTuple).add(valTuple);
                }
            } catch (IOException e) {
                throw new ExecException(e);
            }
            replicates[schemaIdx] = replicate;
            inputIdx++;
            schemaIdx++;
        }

        long time2 = System.currentTimeMillis();
        log.info((replicates.length - 1) + " replication hash tables built. Time taken: " + (time2 - time1));

        ObjectCache.getInstance().cache(cacheKey, replicates);
        log.info("Cached replicate hash tables in Tez ObjectRegistry with vertex scope. cachekey=" + cacheKey);
    }

    @Override
    public String name() {
        StringBuffer inputs = new StringBuffer();
        for (int i = 0; i < inputKeys.size(); i++) {
            if (i > 0) inputs.append(",");
            inputs.append(inputKeys.get(i));
        }
        return super.name() + "\t<-\t " + inputs.toString();
    }

    public List<String> getInputKeys() {
        return inputKeys;
    }
}

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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.data.DataType;
import org.apache.pig.data.SchemaTupleBackend;
import org.apache.pig.data.SchemaTupleClassGenerator;
import org.apache.pig.data.SchemaTupleFactory;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import java.util.List;
import java.util.Map;

public class POFRJoinSpark extends POFRJoin {
    private static final Log log = LogFactory.getLog(POFRJoinSpark.class);

    private Map<String, List<Tuple>> broadcasts;

    public POFRJoinSpark(POFRJoin copy) throws ExecException {
        super(copy);
    }

    @Override
    protected void setUpHashMap() throws ExecException {
        log.info("Building replication hash table");

        SchemaTupleFactory[] inputSchemaTupleFactories = new SchemaTupleFactory[inputSchemas.length];
        SchemaTupleFactory[] keySchemaTupleFactories = new SchemaTupleFactory[inputSchemas.length];
        for (int i = 0; i < inputSchemas.length; i++) {
            addSchemaToFactories(inputSchemas[i], inputSchemaTupleFactories, i);
            addSchemaToFactories(keySchemas[i], keySchemaTupleFactories, i);
        }

        replicates[fragment] = null;
        int i = -1;
        long start = System.currentTimeMillis();
        for (int k = 0; k < inputSchemas.length; ++k) {
            ++i;

            SchemaTupleFactory inputSchemaTupleFactory = inputSchemaTupleFactories[i];
            SchemaTupleFactory keySchemaTupleFactory = keySchemaTupleFactories[i];

            if (i == fragment) {
                replicates[i] = null;
                continue;
            }

            TupleToMapKey replicate = new TupleToMapKey(1000, keySchemaTupleFactory);

            log.debug("Completed setup. Trying to build replication hash table");
            List<Tuple> tuples = broadcasts.get(parentPlan.getPredecessors(this).get(i).getOperatorKey().toString());

            POLocalRearrange localRearrange = LRs[i];

            for (Tuple t : tuples) {
                localRearrange.attachInput(t);
                Result res = localRearrange.getNextTuple();
                if (getReporter() != null) {
                    getReporter().progress();
                }
                Tuple tuple = (Tuple) res.result;
                if (isKeyNull(tuple.get(1))) continue;
                Tuple key = mTupleFactory.newTuple(1);
                key.set(0, tuple.get(1));
                Tuple value = getValueTuple(localRearrange, tuple);

                if (replicate.get(key) == null) {
                    replicate.put(key, new POMergeJoin.TuplesToSchemaTupleList(1, inputSchemaTupleFactory));
                }

                replicate.get(key).add(value);

            }
            replicates[i] = replicate;
        }
        long end = System.currentTimeMillis();
        log.debug("Hash Table built. Time taken: " + (end - start));
    }

    @Override
    public String name() {
        return getAliasString() + "FRJoinSpark[" + DataType.findTypeName(resultType)
                + "]" + " - " + mKey.toString();
    }

    private void addSchemaToFactories(Schema schema, SchemaTupleFactory[] schemaTupleFactories, int index) {
        if (schema != null) {
            log.debug("Using SchemaTuple for FR Join Schema: " + schema);
            schemaTupleFactories[index] = SchemaTupleBackend.newSchemaTupleFactory(schema, false, SchemaTupleClassGenerator.GenContext.FR_JOIN);
        }
    }

    public void attachInputs(Map<String, List<Tuple>> broadcasts) {
        this.broadcasts = broadcasts;
    }
}

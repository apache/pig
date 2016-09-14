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
package org.apache.pig.backend.hadoop.executionengine.spark.converter;

import java.io.Serializable;

import scala.Tuple2;
import scala.runtime.AbstractFunction1;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLocalRearrange;
import org.apache.pig.data.Tuple;

/**
 * Used by JoinGroupSparkConverter and ReduceByConverter to convert incoming locally rearranged tuple, which is of the
 * form Tuple(index, key, value) into Tuple2<key, Tuple(key, value)>
 */
public class LocalRearrangeFunction extends
        AbstractFunction1<Tuple, Tuple2<IndexedKey, Tuple>> implements Serializable {
    private static final Log LOG = LogFactory
            .getLog(LocalRearrangeFunction.class);
    private final POLocalRearrange lra;

    private boolean useSecondaryKey;
    private boolean[] secondarySortOrder;

    public LocalRearrangeFunction(POLocalRearrange lra, boolean useSecondaryKey, boolean[] secondarySortOrder) {
        this.useSecondaryKey = useSecondaryKey;
        this.secondarySortOrder = secondarySortOrder;
        this.lra = lra;
    }

    //in:Tuple(index,key,value)
    //out:<IndexedKey, value>
    @Override
    public Tuple2<IndexedKey, Tuple> apply(Tuple t) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("LocalRearrangeFunction in " + t);
        }
        Result result;
        try {
            lra.setInputs(null);
            lra.attachInput(t);
            result = lra.getNextTuple();

            if (result == null) {
                throw new RuntimeException(
                        "Null response found for LocalRearange on tuple: "
                                + t);
            }

            switch (result.returnStatus) {
                case POStatus.STATUS_OK:
                    // (index, key, value without keys)
                    Tuple resultTuple = (Tuple) result.result;
                    Object key = resultTuple.get(1);
                    IndexedKey indexedKey = new IndexedKey((Byte) resultTuple.get(0), key);
                    if( useSecondaryKey) {
                        indexedKey.setUseSecondaryKey(useSecondaryKey);
                        indexedKey.setSecondarySortOrder(secondarySortOrder);
                    }
                    Tuple2<IndexedKey, Tuple> out = new Tuple2<IndexedKey, Tuple>(indexedKey,
                            (Tuple) resultTuple.get(2));
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("LocalRearrangeFunction out " + out);
                    }
                    return out;
                default:
                    throw new RuntimeException(
                            "Unexpected response code from operator "
                                    + lra + " : " + result);
            }
        } catch (ExecException e) {
            throw new RuntimeException(
                    "Couldn't do LocalRearange on tuple: " + t, e);
        }
    }

}


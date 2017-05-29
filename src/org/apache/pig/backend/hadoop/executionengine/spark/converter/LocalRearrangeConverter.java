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

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

import scala.runtime.AbstractFunction1;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.spark.SparkUtil;
import org.apache.pig.data.Tuple;
import org.apache.spark.rdd.RDD;

@SuppressWarnings({ "serial" })
public class LocalRearrangeConverter implements
        RDDConverter<Tuple, Tuple, PhysicalOperator> {
    private static final Log LOG = LogFactory
            .getLog(LocalRearrangeConverter.class);

    @Override
    public RDD<Tuple> convert(List<RDD<Tuple>> predecessors,
            PhysicalOperator physicalOperator) throws IOException {
        SparkUtil.assertPredecessorSize(predecessors, physicalOperator, 1);
        RDD<Tuple> rdd = predecessors.get(0);
        // call local rearrange to get key and value
        return rdd.map(new LocalRearrangeFunction(physicalOperator),
                SparkUtil.getManifest(Tuple.class));

    }

    private static class LocalRearrangeFunction extends
            AbstractFunction1<Tuple, Tuple> implements Serializable {

        private final PhysicalOperator physicalOperator;

        public LocalRearrangeFunction(PhysicalOperator physicalOperator) {
            this.physicalOperator = physicalOperator;
        }

        @Override
        public Tuple apply(Tuple t) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("LocalRearrangeFunction in " + t);
            }
            Result result;
            try {
                physicalOperator.setInputs(null);
                physicalOperator.attachInput(t);
                result = physicalOperator.getNextTuple();

                if (result == null) {
                    throw new RuntimeException(
                            "Null response found for LocalRearange on tuple: "
                                    + t);
                }

                switch (result.returnStatus) {
                case POStatus.STATUS_OK:
                    // (index, key, value without keys)
                    Tuple resultTuple = (Tuple) result.result;
                    if (LOG.isDebugEnabled())
                        LOG.debug("LocalRearrangeFunction out " + resultTuple);
                    return resultTuple;
                default:
                    throw new RuntimeException(
                            "Unexpected response code from operator "
                                    + physicalOperator + " : " + result);
                }
            } catch (ExecException e) {
                throw new RuntimeException(
                        "Couldn't do LocalRearange on tuple: " + t, e);
            }
        }

    }

}

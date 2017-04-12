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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POFRJoinSpark;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POFRJoin;
import org.apache.pig.backend.hadoop.executionengine.spark.SparkPigContext;
import org.apache.pig.backend.hadoop.executionengine.spark.SparkUtil;
import org.apache.pig.data.Tuple;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.rdd.RDD;

@SuppressWarnings("serial")
public class FRJoinConverter implements
        RDDConverter<Tuple, Tuple, POFRJoin> {
    private static final Log LOG = LogFactory.getLog(FRJoinConverter.class);

    private Set<String> replicatedInputs;

    public RDD<Tuple> convert(List<RDD<Tuple>> predecessors,
                              POFRJoin poFRJoin) throws IOException {
        SparkUtil.assertPredecessorSizeGreaterThan(predecessors, poFRJoin, 1);
        RDD<Tuple> rdd = predecessors.get(0);

        attachReplicatedInputs((POFRJoinSpark) poFRJoin);

        FRJoinFunction frJoinFunction = new FRJoinFunction(poFRJoin);
        return rdd.toJavaRDD().mapPartitions(frJoinFunction, true).rdd();
    }

    private void attachReplicatedInputs(POFRJoinSpark poFRJoin) {
        Map<String, List<Tuple>> replicatedInputMap = new HashMap<>();

        for (String replicatedInput : replicatedInputs) {
            replicatedInputMap.put(replicatedInput, SparkPigContext.get().getBroadcastedVars().get(replicatedInput).value());
        }

        poFRJoin.attachInputs(replicatedInputMap);
    }

    private static class FRJoinFunction implements
            FlatMapFunction<Iterator<Tuple>, Tuple>, Serializable {

        private POFRJoin poFRJoin;
        private FRJoinFunction(POFRJoin poFRJoin) {
            this.poFRJoin = poFRJoin;
        }

        @Override
        public Iterable<Tuple> call(final Iterator<Tuple> input) throws Exception {

            return new Iterable<Tuple>() {

                @Override
                public Iterator<Tuple> iterator() {
                    return new OutputConsumerIterator(input) {

                        @Override
                        protected void attach(Tuple tuple) {
                            poFRJoin.setInputs(null);
                            poFRJoin.attachInput(tuple);
                        }

                        @Override
                        protected Result getNextResult() throws ExecException {
                            return poFRJoin.getNextTuple();
                        }

                        @Override
                        protected void endOfInput() {
                        }
                    };
                }
            };
        }

    }

    public void setReplicatedInputs(Set<String> replicatedInputs) {
        this.replicatedInputs = replicatedInputs;
    }
}
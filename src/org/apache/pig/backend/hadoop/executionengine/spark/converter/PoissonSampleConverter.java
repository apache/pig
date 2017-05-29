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

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.spark.operator.POPoissonSampleSpark;
import org.apache.pig.backend.hadoop.executionengine.spark.SparkUtil;
import org.apache.pig.data.Tuple;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.rdd.RDD;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public class PoissonSampleConverter implements RDDConverter<Tuple, Tuple, POPoissonSampleSpark> {

    @Override
    public RDD<Tuple> convert(List<RDD<Tuple>> predecessors,
                              POPoissonSampleSpark po) throws IOException {
        SparkUtil.assertPredecessorSize(predecessors, po, 1);
        RDD<Tuple> rdd = predecessors.get(0);
        PoissionSampleFunction poissionSampleFunction = new PoissionSampleFunction(po);
        return rdd.toJavaRDD().mapPartitions(poissionSampleFunction, false).rdd();
    }

    private static class PoissionSampleFunction implements FlatMapFunction<Iterator<Tuple>, Tuple> {

        private final POPoissonSampleSpark po;

        public PoissionSampleFunction(POPoissonSampleSpark po) {
            this.po = po;
        }

        @Override
        public Iterable<Tuple> call(final Iterator<Tuple> tuples) {

            return new Iterable<Tuple>() {

                public Iterator<Tuple> iterator() {
                    return new OutputConsumerIterator(tuples) {

                        @Override
                        protected void attach(Tuple tuple) {
                            po.setInputs(null);
                            po.attachInput(tuple);
                        }

                        @Override
                        protected Result getNextResult() throws ExecException {
                            return po.getNextTuple();
                        }

                        @Override
                        protected void endOfInput() {
                            po.setEndOfInput(true);
                        }
                    };
                }
            };
        }
    }
}

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
import java.util.Iterator;
import java.util.List;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POMergeCogroup;
import org.apache.pig.backend.hadoop.executionengine.spark.SparkUtil;
import org.apache.pig.data.Tuple;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.rdd.RDD;


public class MergeCogroupConverter implements RDDConverter<Tuple, Tuple, POMergeCogroup> {
    @Override
    public RDD<Tuple> convert(List<RDD<Tuple>> predecessors,
                              POMergeCogroup physicalOperator) {
        SparkUtil.assertPredecessorSize(predecessors, physicalOperator, 1);
        RDD<Tuple> rdd = predecessors.get(0);
        MergeCogroupFunction mergeCogroupFunction = new MergeCogroupFunction(physicalOperator);
        return rdd.toJavaRDD().mapPartitions(mergeCogroupFunction, true).rdd();
    }

    private static class MergeCogroupFunction implements
            FlatMapFunction<Iterator<Tuple>, Tuple>, Serializable {

        private POMergeCogroup poMergeCogroup;

        @Override
        public Iterable<Tuple> call(final Iterator<Tuple> input) throws Exception {
            return new Iterable<Tuple>() {

                @Override
                public Iterator<Tuple> iterator() {
                    return new OutputConsumerIterator(input) {

                        @Override
                        protected void attach(Tuple tuple) {
                            poMergeCogroup.setInputs(null);
                            poMergeCogroup.attachInput(tuple);
                        }

                        @Override
                        protected Result getNextResult() throws ExecException {
                            return poMergeCogroup.getNextTuple();
                        }

                        @Override
                        protected void endOfInput() {
                            poMergeCogroup.getParentPlan().endOfAllInput = true;
                        }
                    };
                }
            };
        }

        private MergeCogroupFunction(POMergeCogroup poMergeCogroup) {
            this.poMergeCogroup = poMergeCogroup;
        }
    }
}

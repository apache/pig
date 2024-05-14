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
import java.util.Iterator;
import java.util.List;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLimit;
import org.apache.pig.backend.hadoop.executionengine.spark.FlatMapFunctionAdapter;
import org.apache.pig.backend.hadoop.executionengine.spark.SparkUtil;
import org.apache.pig.data.Tuple;
import org.apache.spark.rdd.PartitionCoalescer;
import org.apache.spark.rdd.RDD;

import scala.Option;

@SuppressWarnings({ "serial" })
public class LimitConverter implements RDDConverter<Tuple, Tuple, POLimit> {

    @Override
    public RDD<Tuple> convert(List<RDD<Tuple>> predecessors, POLimit poLimit)
            throws IOException {
        SparkUtil.assertPredecessorSize(predecessors, poLimit, 1);
        RDD<Tuple> rdd = predecessors.get(0);
        LimitFunction limitFunction = new LimitFunction(poLimit);
        RDD<Tuple> rdd2 = rdd.coalesce(1, false, Option.<PartitionCoalescer>empty(), null);
        return rdd2.toJavaRDD().mapPartitions(SparkUtil.flatMapFunction(limitFunction), false).rdd();
    }

    private static class LimitFunction implements FlatMapFunctionAdapter<Iterator<Tuple>, Tuple> {

        private final POLimit poLimit;

        public LimitFunction(POLimit poLimit) {
            this.poLimit = poLimit;
        }

        @Override
        public Iterator<Tuple> call(final Iterator<Tuple> tuples) {
            return new OutputConsumerIterator(tuples) {

                @Override
                protected void attach(Tuple tuple) {
                    poLimit.setInputs(null);
                    poLimit.attachInput(tuple);
                }

                @Override
                protected Result getNextResult() throws ExecException {
                    return poLimit.getNextTuple();
                }

                @Override
                protected void endOfInput() {
                }
            };
        }
    }
}

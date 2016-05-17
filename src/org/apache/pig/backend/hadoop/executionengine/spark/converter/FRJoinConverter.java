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
import java.util.Iterator;
import java.util.List;

import scala.Tuple2;
import scala.runtime.AbstractFunction1;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POFRJoin;
import org.apache.pig.backend.hadoop.executionengine.spark.SparkUtil;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.rdd.RDD;

import com.google.common.base.Optional;

@SuppressWarnings("serial")
public class FRJoinConverter implements
        RDDConverter<Tuple, Tuple, POFRJoin> {
    private static final Log LOG = LogFactory.getLog(FRJoinConverter.class);

    public RDD<Tuple> convert(List<RDD<Tuple>> predecessors,
                              POFRJoin poFRJoin) throws IOException {
        SparkUtil.assertPredecessorSize(predecessors, poFRJoin, 1);
        RDD<Tuple> rdd = predecessors.get(0);
        FRJoinFunction frJoinFunction = new FRJoinFunction(poFRJoin);
        return rdd.toJavaRDD().mapPartitions(frJoinFunction, true).rdd();
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
}
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
import java.util.Iterator;

import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POMergeJoin;
import org.apache.pig.backend.hadoop.executionengine.spark.SparkUtil;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.rdd.RDD;

import scala.Tuple2;
import scala.runtime.AbstractFunction1;


@SuppressWarnings("serial")
public class MergeJoinConverter implements
        POConverter<Tuple, Tuple, POMergeJoin> {

    @Override
    public RDD<Tuple> convert(List<RDD<Tuple>> predecessors,
                              POMergeJoin poMergeJoin) throws IOException {

        SparkUtil.assertPredecessorSize(predecessors, poMergeJoin, 2);

        RDD<Tuple> rdd1 = predecessors.get(0);
        RDD<Tuple> rdd2 = predecessors.get(1);

        // make (key, value) pairs, key has type Object, value has type Tuple
        RDD<Tuple2<Object, Tuple>> rdd1Pair = rdd1.map(new ExtractKeyFunction(
                poMergeJoin, 0), SparkUtil.<Object, Tuple>getTuple2Manifest());
        RDD<Tuple2<Object, Tuple>> rdd2Pair = rdd2.map(new ExtractKeyFunction(
                poMergeJoin, 1), SparkUtil.<Object, Tuple>getTuple2Manifest());

        JavaPairRDD<Object, Tuple> prdd1 = new JavaPairRDD<Object, Tuple>(
                rdd1Pair, SparkUtil.getManifest(Object.class),
                SparkUtil.getManifest(Tuple.class));
        JavaPairRDD<Object, Tuple> prdd2 = new JavaPairRDD<Object, Tuple>(
                rdd2Pair, SparkUtil.getManifest(Object.class),
                SparkUtil.getManifest(Tuple.class));

        JavaPairRDD<Object, Tuple2<Tuple, Tuple>> jrdd = prdd1
                .join(prdd2);

        // map to get JavaRDD<Tuple> from join() output, which is
        // JavaPairRDD<Object, Tuple2<Tuple, Tuple>> by
        // ignoring the key (of type Object) and appending the values (the
        // Tuples)
        JavaRDD<Tuple> result = jrdd
                .mapPartitions(new ToValueFunction());

        return result.rdd();
    }

    private static class ExtractKeyFunction extends
            AbstractFunction1<Tuple, Tuple2<Object, Tuple>> implements
            Serializable {

        private final POMergeJoin poMergeJoin;
        private final int LR_index; // 0 for left table, 1 for right table

        public ExtractKeyFunction(POMergeJoin poMergeJoin, int LR_index) {
            this.poMergeJoin = poMergeJoin;
            this.LR_index = LR_index;
        }

        @Override
        public Tuple2<Object, Tuple> apply(Tuple tuple) {
            poMergeJoin.getLRs()[LR_index].attachInput(tuple);

            try {
                Result lrOut = poMergeJoin.getLRs()[LR_index].getNextTuple();
                if(lrOut.returnStatus!= POStatus.STATUS_OK){
                    int errCode = 2167;
                    String errMsg = "LocalRearrange used to extract keys from " +
                            "tuple is not configured correctly";
                    throw new ExecException(errMsg,errCode, PigException.BUG);
                }

                // If tuple is (AA, 5) and key index is $1, then it lrOut is 0 5 (AA),
                // so get(1) returns key
                Object key = ((Tuple) lrOut.result).get(1);
                Tuple value = tuple;
                Tuple2<Object, Tuple> tuple_KeyValue = new Tuple2<Object, Tuple>(key,
                        value);

                return tuple_KeyValue;

            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static class ToValueFunction
            implements FlatMapFunction<Iterator<Tuple2<Object, Tuple2<Tuple, Tuple>>>, Tuple>, Serializable {

        private class Tuple2TransformIterable implements Iterable<Tuple> {

            Iterator<Tuple2<Object, Tuple2<Tuple, Tuple>>> in;

            Tuple2TransformIterable(
                    Iterator<Tuple2<Object, Tuple2<Tuple, Tuple>>> input) {
                in = input;
            }

            public Iterator<Tuple> iterator() {
                return new IteratorTransform<Tuple2<Object, Tuple2<Tuple, Tuple>>, Tuple>(
                        in) {
                    @Override
                    protected Tuple transform(
                            Tuple2<Object, Tuple2<Tuple, Tuple>> next) {
                        try {

                            Tuple leftTuple = next._2()._1();
                            Tuple rightTuple = next._2()._2();

                            TupleFactory tf = TupleFactory.getInstance();
                            Tuple result = tf.newTuple(leftTuple.size()
                                    + rightTuple.size());

                            // concatenate the two tuples together to make a
                            // resulting tuple
                            for (int i = 0; i < leftTuple.size(); i++)
                                result.set(i, leftTuple.get(i));
                            for (int i = 0; i < rightTuple.size(); i++)
                                result.set(i + leftTuple.size(),
                                        rightTuple.get(i));

                            return result;

                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }
                };
            }
        }

        @Override
        public Iterable<Tuple> call(
                Iterator<Tuple2<Object, Tuple2<Tuple, Tuple>>> input) {
            return new Tuple2TransformIterable(input);
        }
    }
}
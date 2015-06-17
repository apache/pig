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

    @Override
    public RDD<Tuple> convert(List<RDD<Tuple>> predecessors,
                              POFRJoin poFRJoin) throws IOException {

        SparkUtil.assertPredecessorSizeGreaterThan(predecessors, poFRJoin, 1);
        JavaPairRDD<IndexedKey, Tuple2<Tuple, Tuple>> joinedPairRDD;
        int lr_idx = 0;
        // RDD<Tuple> -> RDD<Tuple2<IndexedKey, Tuple>> -> JavaPairRDD<IndexedKey, Tuple>
        JavaPairRDD<IndexedKey, Tuple> pairRDD1 = getPairRDD(predecessors.get(lr_idx),
                poFRJoin, lr_idx);
        lr_idx ++;
        // RDD transformations to support multiple join inputs:
        //  join().mapPartitions().join().mapPartitions,...
        while (true) {
            JavaPairRDD<IndexedKey, Tuple> pairRDD2 = getPairRDD(predecessors.get(lr_idx),
                    poFRJoin, lr_idx);

            joinedPairRDD = join(pairRDD1, pairRDD2, poFRJoin);
            if (++ lr_idx == predecessors.size()) {
                break;
            }
            // join() outputs tuples of the form (key, (v, w)), or in other words
            // (key, (tuple from table1, tuple from table2, tuple from table3,...)
            // We need to convert these to the form (key, (tuple)) to
            // prepare it for the next join, i.e.
            // RDD<Tuple2<IndexedKey, Tuple2<Tuple, Tuple>>> ->
            // RDD<Tuple2<IndexedKey, Tuple> -> JavaPairRDD<IndexedKey, Tuple>
            JavaRDD<Tuple2<IndexedKey, Tuple>> resultRDD = joinedPairRDD
                    .mapPartitions(new ToKeyValueFunction());
            pairRDD1 = new JavaPairRDD<IndexedKey, Tuple>(
                    resultRDD.rdd(), SparkUtil.getManifest(IndexedKey.class),
                    SparkUtil.getManifest(Tuple.class));
        }

        // map to get JavaRDD<Tuple> from join() output (which is
        // JavaPairRDD<IndexedKey, Tuple2<Tuple, Tuple>>, i.e. tuples are separated)
        // by ignoring the key (of type IndexedKey) and concatenating the values
        // (i.e. the tuples)
        JavaRDD<Tuple> result = joinedPairRDD.mapPartitions(new ToValueFunction());

        return result.rdd();
    }

    private JavaPairRDD<IndexedKey, Tuple2<Tuple, Tuple>> join(
            JavaPairRDD<IndexedKey, Tuple> pairRDD1,
            JavaPairRDD<IndexedKey, Tuple> pairRDD2,
            POFRJoin pofrJoin) {
        if (pofrJoin.isLeftOuterJoin()) {
            return leftOuterJoin(pairRDD1, pairRDD2);
        } else {
            return pairRDD1.join(pairRDD2);
        }
    }

    private JavaPairRDD<IndexedKey, Tuple2<Tuple, Tuple>> leftOuterJoin(
            JavaPairRDD<IndexedKey, Tuple> pairRDD1,
            JavaPairRDD<IndexedKey, Tuple> pairRDD2) {

        // leftouterjoin() returns RDD containing pairs of the form
        // (k, (v, optional(w)))
        JavaPairRDD<IndexedKey, Tuple2<Tuple, Optional<Tuple>>> pairRDD =
                pairRDD1.leftOuterJoin(pairRDD2);
        return pairRDD.mapToPair(new AbsentToEmptyTupleFunction(
                ((Tuple) pairRDD2.first()._2()).size()));
    }

    private static JavaPairRDD<IndexedKey, Tuple> getPairRDD(RDD<Tuple> rdd,
                                                         POFRJoin poFRJoin,
                                                         int lr_idx) {
        RDD<Tuple2<IndexedKey, Tuple>> keyValRdd = rdd.map(
                new ExtractKeyFunction(poFRJoin, lr_idx),
                SparkUtil.<IndexedKey, Tuple>getTuple2Manifest());
        JavaPairRDD<IndexedKey, Tuple> pairRDD = new JavaPairRDD<IndexedKey, Tuple>(
                keyValRdd, SparkUtil.getManifest(IndexedKey.class),
                SparkUtil.getManifest(Tuple.class));
        return pairRDD;
    }


    private static class ExtractKeyFunction extends
            AbstractFunction1<Tuple, Tuple2<IndexedKey, Tuple>> implements
            Serializable {

        private final POFRJoin poFRJoin;
        private final int lr_index;

        public ExtractKeyFunction(POFRJoin poFRJoin, int lr_index) {
            this.poFRJoin = poFRJoin;
            this.lr_index = lr_index;
        }

        @Override
        public Tuple2<IndexedKey, Tuple> apply(Tuple tuple) {
            poFRJoin.getLRs()[lr_index].attachInput(tuple);

            try {
                // Join key is modeled in POFRJoin using Local Rearrange operators
                // referenced inside POFRJoin, one for each input to POFRJoin.
                // We locally rearrange the incoming tuple by running it through
                // the POLocalRearrange operator identified by lr_index.
                Result lrOut = poFRJoin.getLRs()[lr_index].getNextTuple();
                if(lrOut.returnStatus!= POStatus.STATUS_OK){
                    int errCode = 2167;
                    String errMsg = "LocalRearrange used to extract keys from tuple isn't configured correctly";
                    throw new ExecException(errMsg,errCode, PigException.BUG);
                }

                // If tuple is (AA, 5) and key index is $1, then
                // lrOut is 0 5 (AA), so get(1) returns key
                Object index = ((Tuple) lrOut.result).get(0);
                Object key = ((Tuple) lrOut.result).get(1);
                Tuple value = tuple;
                Tuple2<IndexedKey, Tuple> tuple_KeyValue = new Tuple2<IndexedKey, Tuple>(new IndexedKey((Byte) index, key),
                        value);

                return tuple_KeyValue;

            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static class ToValueFunction
            implements FlatMapFunction<Iterator<Tuple2<IndexedKey, Tuple2<Tuple, Tuple>>>, Tuple>,
            Serializable {

        private class Tuple2TransformIterable implements Iterable<Tuple> {

            Iterator<Tuple2<IndexedKey, Tuple2<Tuple, Tuple>>> in;

            Tuple2TransformIterable(
                    Iterator<Tuple2<IndexedKey, Tuple2<Tuple, Tuple>>> input) {
                in = input;
            }

            public Iterator<Tuple> iterator() {
                return new IteratorTransform<Tuple2<IndexedKey, Tuple2<Tuple, Tuple>>, Tuple>(
                        in) {
                    @Override
                    protected Tuple transform(
                            Tuple2<IndexedKey, Tuple2<Tuple, Tuple>> next) {
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
                Iterator<Tuple2<IndexedKey, Tuple2<Tuple, Tuple>>> input) {
            return new Tuple2TransformIterable(input);
        }
    }

        private static class ToKeyValueFunction
                implements FlatMapFunction<Iterator<Tuple2<IndexedKey, Tuple2<Tuple, Tuple>>>,
                Tuple2<IndexedKey, Tuple>>, Serializable {

            private class Tuple2TransformIterable implements
                    Iterable<Tuple2<IndexedKey, Tuple>> {

                Iterator<Tuple2<IndexedKey, Tuple2<Tuple, Tuple>>> in;

                Tuple2TransformIterable(
                        Iterator<Tuple2<IndexedKey, Tuple2<Tuple, Tuple>>> input) {
                    in = input;
                }

                public Iterator<Tuple2<IndexedKey, Tuple>> iterator() {
                    return new IteratorTransform<Tuple2<IndexedKey, Tuple2<Tuple, Tuple>>,
                            Tuple2<IndexedKey, Tuple>>(
                            in) {
                        @Override
                        protected Tuple2<IndexedKey, Tuple> transform(
                                Tuple2<IndexedKey, Tuple2<Tuple, Tuple>> next) {
                            try {

                                Tuple leftTuple = next._2()._1();
                                Tuple rightTuple = next._2()._2();

                                TupleFactory tf = TupleFactory.getInstance();
                                Tuple value = tf.newTuple(leftTuple.size()
                                        + rightTuple.size());

                                // append the two tuples together to make a resulting tuple
                                for (int i = 0; i < leftTuple.size(); i++)
                                    value.set(i, leftTuple.get(i));
                                for (int i = 0; i < rightTuple.size(); i++)
                                    value.set(i + leftTuple.size(),
                                            rightTuple.get(i));

                                Tuple2<IndexedKey, Tuple> result = new Tuple2<IndexedKey, Tuple>(
                                        next._1(),
                                        value);
                                return result;

                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        }
                    };
                }
            }


            @Override
            public Iterable<Tuple2<IndexedKey, Tuple>> call(
                    Iterator<Tuple2<IndexedKey, Tuple2<Tuple, Tuple>>> input) {
            return new Tuple2TransformIterable(input);
        }
    }

    private static class AbsentToEmptyTupleFunction implements
            PairFunction<Tuple2<IndexedKey, Tuple2<Tuple, Optional<Tuple>>>,
                    IndexedKey, Tuple2<Tuple, Tuple>>, Serializable {

        private int rightTupleSize;

        public AbsentToEmptyTupleFunction(int rightTupleSize) {
            this.rightTupleSize = rightTupleSize;
        }

        // When w is absent in the input tuple (key, (v, optional(w))),
        // the output tuple will contain an empty tuple in it's place.
        @Override
        public Tuple2<IndexedKey, Tuple2<Tuple, Tuple>> call(
                Tuple2<IndexedKey, Tuple2<Tuple, Optional<Tuple>>> input) {
            final IndexedKey key = input._1();
            Tuple2<IndexedKey, Tuple2<Tuple, Tuple>> result;
            Tuple2<Tuple, Optional<Tuple>> inval = input._2();
            if (inval._2().isPresent()) {
                result = new Tuple2<IndexedKey, Tuple2<Tuple, Tuple>>(
                        key,
                        new Tuple2<Tuple, Tuple>(
                                inval._1(),
                                inval._2().get())
                );
            } else {
                result = new Tuple2<IndexedKey, Tuple2<Tuple, Tuple>>(
                        key,
                        new Tuple2<Tuple, Tuple>(
                                inval._1(),
                                TupleFactory.getInstance().newTuple(rightTupleSize))
                );
            }
            return result;
        }
    }
}
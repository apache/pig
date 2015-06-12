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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.spark.SparkUtil;
import org.apache.pig.backend.hadoop.executionengine.spark.operator.POGlobalRearrangeSpark;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.spark.HashPartitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.rdd.CoGroupedRDD;
import org.apache.spark.rdd.RDD;

import scala.Product2;
import scala.Tuple2;
import scala.collection.JavaConversions;
import scala.collection.Seq;
import scala.runtime.AbstractFunction1;

@SuppressWarnings({ "serial" })
public class GlobalRearrangeConverter implements
        RDDConverter<Tuple, Tuple, POGlobalRearrangeSpark> {
    private static final Log LOG = LogFactory
            .getLog(GlobalRearrangeConverter.class);

    private static final TupleFactory tf = TupleFactory.getInstance();
 
    @Override
    public RDD<Tuple> convert(List<RDD<Tuple>> predecessors,
                              POGlobalRearrangeSpark op) throws IOException {
        SparkUtil.assertPredecessorSizeGreaterThan(predecessors,
                op, 0);
        int parallelism = SparkUtil.getParallelism(predecessors,
                op);

//         TODO: Figure out the tradeoff of using CoGroupRDD (even for 1 input),
//         vs using groupBy (like we do in this commented code), vs using
//         reduceByKey(). This is a pending task in Pig on Spark Milestone 1
//         Once we figure that out, we can allow custom partitioning for
//         secondary sort case as well.
//        if (predecessors.size() == 1) {
//            // GROUP BY
//            JavaPairRDD<Object, Iterable<Tuple>> prdd;
//            if (op.isUseSecondaryKey()) {
//                prdd = handleSecondarySort(predecessors.get(0), op, parallelism);
//            } else {
//                JavaRDD<Tuple> jrdd = predecessors.get(0).toJavaRDD();
//                prdd = jrdd.groupBy(new GetKeyFunction(op), parallelism);
//                prdd.groupByKey(new CustomPartitioner(op.getCustomPartitioner(),
//                        parallelism));
//            }
//            JavaRDD<Tuple> jrdd2 = prdd.map(new GroupTupleFunction(op));
//            return jrdd2.rdd();
//
//        if (predecessors.size() == 1 && op.isUseSecondaryKey()) {
//            return handleSecondarySort(predecessors.get(0), op, parallelism);
//        }
        List<RDD<Tuple2<IndexedKey, Tuple>>> rddPairs = new ArrayList<RDD<Tuple2<IndexedKey, Tuple>>>();

        if (predecessors.size() == 1 && op.isUseSecondaryKey()) {
            rddPairs.add(handleSecondarySort(predecessors.get(0), op, parallelism).rdd());
        } else {
            for (RDD<Tuple> rdd : predecessors) {
                JavaRDD<Tuple> jrdd = JavaRDD.fromRDD(rdd, SparkUtil.getManifest(Tuple.class));
                JavaRDD<Tuple2<IndexedKey, Tuple>> rddPair = jrdd.map(new ToKeyValueFunction());
                rddPairs.add(rddPair.rdd());
            }
        }

        // Something's wrong with the type parameters of CoGroupedRDD
        // key and value are the same type ???
        CoGroupedRDD<Object> coGroupedRDD = new CoGroupedRDD<Object>(
                (Seq<RDD<? extends Product2<Object, ?>>>) (Object) (JavaConversions
                        .asScalaBuffer(rddPairs).toSeq()),
                SparkUtil.getPartitioner(op.getCustomPartitioner(), parallelism)
        );

        RDD<Tuple2<IndexedKey, Seq<Seq<Tuple>>>> rdd =
            (RDD<Tuple2<IndexedKey, Seq<Seq<Tuple>>>>) (Object) coGroupedRDD;
        return rdd.toJavaRDD().map(new ToGroupKeyValueFunction()).rdd();
    }

    private JavaRDD<Tuple2<IndexedKey,Tuple>> handleSecondarySort(
            RDD<Tuple> rdd, POGlobalRearrangeSpark op, int parallelism) {

        RDD<Tuple2<Tuple, Object>> rddPair = rdd.map(new ToKeyNullValueFunction(),
                SparkUtil.<Tuple, Object>getTuple2Manifest());

        JavaPairRDD<Tuple, Object> pairRDD = new JavaPairRDD<Tuple, Object>(rddPair,
                SparkUtil.getManifest(Tuple.class),
                SparkUtil.getManifest(Object.class));

        //first sort the tuple by secondary key if enable useSecondaryKey sort
        JavaPairRDD<Tuple, Object> sorted = pairRDD.repartitionAndSortWithinPartitions(
                new HashPartitioner(parallelism),
                new PigSecondaryKeyComparatorSpark(op.getSecondarySortOrder()));
        JavaRDD<Tuple> jrdd = sorted.keys();
        JavaRDD<Tuple2<IndexedKey,Tuple>> jrddPair = jrdd.map(new ToKeyValueFunction(op));
        return jrddPair;
    }

    private static class RemoveValueFunction implements
            FlatMapFunction<Iterator<Tuple2<Tuple, Object>>, Tuple>, Serializable {

        private class Tuple2TransformIterable implements Iterable<Tuple> {

            Iterator<Tuple2<Tuple, Object>> in;

            Tuple2TransformIterable(Iterator<Tuple2<Tuple, Object>> input) {
                in = input;
            }

            public Iterator<Tuple> iterator() {
                return new IteratorTransform<Tuple2<Tuple, Object>, Tuple>(in) {
                    @Override
                    protected Tuple transform(Tuple2<Tuple, Object> next) {
                        return next._1();
                    }
                };
            }
        }

        @Override
        public Iterable<Tuple> call(Iterator<Tuple2<Tuple, Object>> input) {
            return new Tuple2TransformIterable(input);
        }
    }

    private static class ToKeyNullValueFunction extends
            AbstractFunction1<Tuple, Tuple2<Tuple, Object>> implements
            Serializable {

        @Override
        public Tuple2<Tuple, Object> apply(Tuple t) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("ToKeyNullValueFunction in " + t);
            }

            Tuple2<Tuple, Object> out = new Tuple2<Tuple, Object>(t, null);
            if (LOG.isDebugEnabled()) {
                LOG.debug("ToKeyNullValueFunction out " + out);
            }

            return out;
        }
    }

    private static class PigSecondaryKeyComparatorSpark implements Comparator, Serializable {
        private static final long serialVersionUID = 1L;

        private static boolean[] secondarySortOrder;

        public PigSecondaryKeyComparatorSpark(boolean[] pSecondarySortOrder) {
            secondarySortOrder = pSecondarySortOrder;
        }

        @Override
        public int compare(Object o1, Object o2) {
            Tuple t1 = (Tuple) o1;
            Tuple t2 = (Tuple) o2;
            try {
                if ((t1.size() < 3) || (t2.size() < 3)) {
                    throw new RuntimeException("tuple size must bigger than 3, tuple[0] stands for index, tuple[1]" +
                            "stands for the compound key, tuple[3] stands for the value");
                }
                Tuple compoundKey1 = (Tuple) t1.get(1);
                Tuple compoundKey2 = (Tuple) t2.get(1);
                if ((compoundKey1.size() < 2) || (compoundKey2.size() < 2)) {
                    throw new RuntimeException("compoundKey size must bigger than, compoundKey[0] stands for firstKey," +
                            "compoundKey[1] stands for secondaryKey");
                }
                Object secondaryKey1 = compoundKey1.get(1);
                Object secondaryKey2 = compoundKey2.get(1);
                int res = compareSecondaryKeys(secondaryKey1, secondaryKey2, secondarySortOrder);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("t1:" + t1 + "t2:" + t2 + " res:" + res);
                }
                return res;
            } catch (ExecException e) {
                throw new RuntimeException("Fail to get the compoundKey", e);
            }
        }

        private int compareSecondaryKeys(Object o1, Object o2, boolean[] asc) {
            int rc = 0;
            if (o1 != null && o2 != null && o1 instanceof Tuple && o2 instanceof Tuple) {
                // objects are Tuples, we may need to apply sort order inside them
                Tuple t1 = (Tuple) o1;
                Tuple t2 = (Tuple) o2;
                int sz1 = t1.size();
                int sz2 = t2.size();
                if (sz2 < sz1) {
                    return 1;
                } else if (sz2 > sz1) {
                    return -1;
                } else {
                    for (int i = 0; i < sz1; i++) {
                        try {
                            rc = DataType.compare(t1.get(i), t2.get(i));
                            if (rc != 0 && asc != null && asc.length > 1 && !asc[i])
                                rc *= -1;
                            if ((t1.get(i) == null) || (t2.get(i) == null)) {
                                if (LOG.isDebugEnabled()) {
                                    LOG.debug("t1.get(i) is:" + t1.get(i) + " t2.get(i) is:" + t2.get(i));
                                }
                            }
                            if (rc != 0) break;
                        } catch (ExecException e) {
                            throw new RuntimeException("Unable to compare tuples", e);
                        }
                    }
                }
            } else {
                // objects are NOT Tuples, delegate to DataType.compare()
                rc = DataType.compare(o1, o2);
            }
            // apply sort order for keys that are not tuples or for whole tuples
            if (asc != null && asc.length == 1 && !asc[0])
                rc *= -1;
            return rc;
        }
    }

    /**
     * Function that extract keys from locally rearranged tuples.
     */
    private static class GetKeyFunction implements Function<Tuple, Object>, Serializable {
        public final POGlobalRearrangeSpark glrSpark;

        public GetKeyFunction(POGlobalRearrangeSpark globalRearrangeSpark) {
            this.glrSpark = globalRearrangeSpark;
        }

        public Object call(Tuple t) {
            try {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("GetKeyFunction in " + t);
                }

                Object key;
                if ((glrSpark != null) && (glrSpark.isUseSecondaryKey())) {
                    key = ((Tuple) t.get(1)).get(0);
                } else {
                    key = t.get(1);
                }

                if (LOG.isDebugEnabled()) {
                    LOG.debug("GetKeyFunction out " + key);
                }

                return key;
            } catch (ExecException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Function that converts elements of PairRDD to regular RDD.
     * - Input (PairRDD) contains elements of the form
     * Tuple2<key, Iterable<(index, key, value)>>.
     * - Output (regular RDD) contains elements of the form
     * Tuple<(key, iterator to (index, key, value))>
     */
    private static class GroupTupleFunction
            implements Function<Tuple2<Object, Iterable<Tuple>>, Tuple>,
            Serializable {
        public final POGlobalRearrangeSpark glrSpark;

        public GroupTupleFunction(POGlobalRearrangeSpark globalRearrangeSpark) {
            this.glrSpark = globalRearrangeSpark;
        }

        public Tuple call(Tuple2<Object, Iterable<Tuple>> v1) {
            try {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("GroupTupleFunction in " + v1);
                }

                Tuple tuple = tf.newTuple(2);
                tuple.set(0, v1._1()); // key
                // Note that v1._2() is (index, key, value) tuple, and
                // v1._2().iterator() is iterator to Seq<Tuple> (aka bag of values)
                tuple.set(1, v1._2().iterator());
                if (LOG.isDebugEnabled()) {
                    LOG.debug("GroupTupleFunction out " + tuple);
                }

                return tuple;
            } catch (ExecException e) {
                throw new RuntimeException(e);
            }
        }
    }


    /**
     * Converts incoming locally rearranged tuple, which is of the form
     * (index, key, value) into Tuple2<key, value>
     */
    private static class ToKeyValueFunction implements
            Function<Tuple, Tuple2<IndexedKey, Tuple>>, Serializable {

        private POGlobalRearrangeSpark glrSpark = null;

        public ToKeyValueFunction(POGlobalRearrangeSpark glrSpark) {
            this.glrSpark = glrSpark;
        }

        public ToKeyValueFunction() {

        }

        @Override
        public Tuple2<IndexedKey, Tuple> call(Tuple t) {
            try {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("ToKeyValueFunction in " + t);
                }

                Object key = null;
                if ((glrSpark != null) && (glrSpark.isUseSecondaryKey())) {
                    key = ((Tuple) t.get(1)).get(0);
                } else {
                    key = t.get(1);
                }

                Tuple2<IndexedKey, Tuple> out = new Tuple2<IndexedKey, Tuple>(
                        new IndexedKey((Byte) t.get(0), key),
                        (Tuple) t.get(2));
                if (LOG.isDebugEnabled()) {
                    LOG.debug("ToKeyValueFunction out " + out);
                }

                return out;
            } catch (ExecException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Converts cogroup output where each element is {key, bag[]} represented
     * as Tuple2<Object, Seq<Seq<Tuple>>> into tuple of form
     * (key, Iterator<(index, key, value)>)
     */
    private static class ToGroupKeyValueFunction implements
            Function<Tuple2<IndexedKey, Seq<Seq<Tuple>>>, Tuple>, Serializable {

        @Override
        public Tuple call(Tuple2<IndexedKey, Seq<Seq<Tuple>>> input) {
            try {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("ToGroupKeyValueFunction in " + input);
                }

                final Object key = input._1().getKey();
                Object obj = input._2();
                // XXX this is a hack for Spark 1.1.0: the type is an Array, not Seq
                Seq<Tuple>[] bags = (Seq<Tuple>[])obj;
                int i = 0;
                List<Iterator<Tuple>> tupleIterators = new ArrayList<Iterator<Tuple>>();
                for (int j = 0; j < bags.length; j ++) {
                    Seq<Tuple> bag = bags[j];
                    Iterator<Tuple> iterator = JavaConversions
                            .asJavaCollection(bag).iterator();
                    final int index = i;
                    tupleIterators.add(new IteratorTransform<Tuple, Tuple>(
                            iterator) {
                        @Override
                        protected Tuple transform(Tuple next) {
                            try {
                                Tuple tuple = tf.newTuple(3);
                                tuple.set(0, index);
                                tuple.set(1, key);
                                tuple.set(2, next);
                                return tuple;
                            } catch (ExecException e) {
                                throw new RuntimeException(e);
                            }
                        }
                    });
                    ++ i;
                }

                Tuple out = tf.newTuple(2);
                out.set(0, key);
                out.set(1, new IteratorUnion<Tuple>(tupleIterators.iterator()));
                if (LOG.isDebugEnabled()) {
                    LOG.debug("ToGroupKeyValueFunction out " + out);
                }

                return out;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static class IteratorUnion<T> implements Iterator<T> {

        private final Iterator<Iterator<T>> iterators;

        private Iterator<T> current;

        public IteratorUnion(Iterator<Iterator<T>> iterators) {
            super();
            this.iterators = iterators;
        }

        @Override
        public boolean hasNext() {
            if (current != null && current.hasNext()) {
                return true;
            } else if (iterators.hasNext()) {
                current = iterators.next();
                return hasNext();
            } else {
                return false;
            }
        }

        @Override
        public T next() {
            return current.next();
        }

        @Override
        public void remove() {
            current.remove();
        }

    }
}

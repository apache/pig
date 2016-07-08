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
import java.util.Iterator;
import java.util.List;

import scala.Product2;
import scala.Tuple2;
import scala.collection.JavaConversions;
import scala.collection.Seq;
import scala.runtime.AbstractFunction1;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.JobConf;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigMapReduce;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLocalRearrange;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POPackage;
import org.apache.pig.backend.hadoop.executionengine.spark.KryoSerializer;
import org.apache.pig.backend.hadoop.executionengine.spark.SparkUtil;
import org.apache.pig.backend.hadoop.executionengine.spark.operator.POGlobalRearrangeSpark;
import org.apache.pig.backend.hadoop.executionengine.spark.operator.POJoinGroupSpark;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.io.NullableTuple;
import org.apache.pig.impl.io.PigNullableWritable;
import org.apache.spark.HashPartitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.rdd.CoGroupedRDD;
import org.apache.spark.rdd.RDD;


public class JoinGroupSparkConverter implements RDDConverter<Tuple, Tuple, POJoinGroupSpark> {
    private static final Log LOG = LogFactory
            .getLog(JoinGroupSparkConverter.class);
    private byte[] confBytes;

    public JoinGroupSparkConverter(byte[] confBytes) {
        this.confBytes = confBytes;
    }

    @Override
    public RDD<Tuple> convert(List<RDD<Tuple>> predecessors, POJoinGroupSpark op) throws IOException {
        SparkUtil.assertPredecessorSizeGreaterThan(predecessors,
                op, 0);
        List<POLocalRearrange> lraOps = op.getLraOps();
        POGlobalRearrangeSpark glaOp = op.getGlaOp();
        POPackage pkgOp = op.getPkgOp();
        int parallelism = SparkUtil.getParallelism(predecessors, glaOp);
        List<RDD<Tuple2<IndexedKey, Tuple>>> rddAfterLRA = new ArrayList<RDD<Tuple2<IndexedKey, Tuple>>>();
        boolean useSecondaryKey = glaOp.isUseSecondaryKey();

        for (int i = 0; i < predecessors.size(); i++) {
            RDD<Tuple> rdd = predecessors.get(i);
            rddAfterLRA.add(rdd.map(new LocalRearrangeFunction(lraOps.get(i), glaOp),
                    SparkUtil.<IndexedKey, Tuple>getTuple2Manifest()));
        }
        if (rddAfterLRA.size() == 1 && useSecondaryKey) {
            rddAfterLRA.set(0, handleSecondarySort(rddAfterLRA.get(0), parallelism));
        }

        CoGroupedRDD<Object> coGroupedRDD = new CoGroupedRDD<Object>(
                (Seq<RDD<? extends Product2<Object, ?>>>) (Object) (JavaConversions
                        .asScalaBuffer(rddAfterLRA).toSeq()),
                SparkUtil.getPartitioner(glaOp.getCustomPartitioner(), parallelism),
		SparkUtil.getManifest(Object.class));

        RDD<Tuple2<IndexedKey, Seq<Seq<Tuple>>>> rdd =
                (RDD<Tuple2<IndexedKey, Seq<Seq<Tuple>>>>) (Object) coGroupedRDD;
        return rdd.toJavaRDD().map(new GroupPkgFunction(pkgOp, this.confBytes)).rdd();
    }


    private RDD<Tuple2<IndexedKey, Tuple>> handleSecondarySort(
            RDD<Tuple2<IndexedKey, Tuple>> rdd, int parallelism) {

        JavaPairRDD<IndexedKey, Tuple> pairRDD = JavaPairRDD.fromRDD(rdd, SparkUtil.getManifest(IndexedKey.class), SparkUtil.getManifest(Tuple.class));

        //first sort the tuple by secondary key if enable useSecondaryKey sort
        JavaPairRDD<IndexedKey, Tuple> sorted = pairRDD.repartitionAndSortWithinPartitions(
                new HashPartitioner(parallelism));
        JavaPairRDD<IndexedKey, Tuple> sortByKey = sorted.sortByKey();
        return sortByKey.mapToPair(new RemoveSecondaryKey()).rdd();
    }

    private static class RemoveSecondaryKey implements
            PairFunction<Tuple2<IndexedKey, Tuple>, IndexedKey, Tuple>,
            Serializable {

        @Override
        public Tuple2<IndexedKey, Tuple> call(Tuple2<IndexedKey, Tuple> t) throws Exception {
            IndexedKey key = t._1();
            Tuple compoundKey = (Tuple) key.getKey();
            if (compoundKey.size() < 2) {
                throw new RuntimeException("compoundKey.size() should be more than 2");
            }
            IndexedKey newKey = new IndexedKey(key.getIndex(), compoundKey.get(0));
            return new Tuple2<IndexedKey, Tuple>(newKey, t._2());
        }
    }


    private static class LocalRearrangeFunction extends
            AbstractFunction1<Tuple, Tuple2<IndexedKey, Tuple>> implements Serializable {

        private final POLocalRearrange lra;

        private boolean useSecondaryKey;
        private boolean[] secondarySortOrder;

        public LocalRearrangeFunction(POLocalRearrange lra, POGlobalRearrangeSpark glaOp) {
            if( glaOp.isUseSecondaryKey()) {
                this.useSecondaryKey = glaOp.isUseSecondaryKey();
                this.secondarySortOrder = glaOp.getSecondarySortOrder();
            }
            this.lra = lra;
        }

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

    /**
     * Send cogroup output where each element is {key, bag[]} to PoPackage
     * then call PoPackage#getNextTuple to get the result
     */
    private static class GroupPkgFunction implements
            Function<Tuple2<IndexedKey, Seq<Seq<Tuple>>>, Tuple>, Serializable {

        private final POPackage pkgOp;
        private byte[] confBytes;
        private JobConf jobConf = null;

        public GroupPkgFunction(POPackage pkgOp, byte[] confBytes) {
            this.pkgOp = pkgOp;
            this.confBytes = confBytes;
        }

        void initializeJobConf() {
            jobConf = KryoSerializer.deserializeJobConf(this.confBytes);
            jobConf.set("pig.cachedbag.type", "default");
            PigMapReduce.sJobConfInternal.set(jobConf);
        }

        @Override
        public Tuple call(final Tuple2<IndexedKey, Seq<Seq<Tuple>>> input) {
            if( jobConf == null) {
                initializeJobConf();
            }

            try {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("GroupPkgFunction in " + input);
                }

                final PigNullableWritable key = new PigNullableWritable() {

                    public Object getValueAsPigType() {
                        IndexedKey keyTuple = input._1();
                        return keyTuple.getKey();
                    }
                };
                Object obj = input._2();
                // XXX this is a hack for Spark 1.1.0: the type is an Array, not Seq
                Seq<Tuple>[] bags = (Seq<Tuple>[]) obj;
                int i = 0;
                List<Iterator<NullableTuple>> tupleIterators = new ArrayList<Iterator<NullableTuple>>();
                for (int j = 0; j < bags.length; j++) {
                    Seq<Tuple> bag = bags[j];
                    Iterator<Tuple> iterator = JavaConversions
                            .asJavaCollection(bag).iterator();
                    final int index = i;
                    tupleIterators.add(new IteratorTransform<Tuple, NullableTuple>(
                            iterator) {
                        @Override
                        protected NullableTuple transform(Tuple next) {
                            NullableTuple nullableTuple = new NullableTuple(next);
                            nullableTuple.setIndex((byte) index);
                            return nullableTuple;
                        }
                    });
                    ++i;
                }


                pkgOp.setInputs(null);
                pkgOp.attachInput(key, new IteratorUnion<NullableTuple>(tupleIterators.iterator()));
                Result result = pkgOp.getNextTuple();
                if (result == null) {
                    throw new RuntimeException(
                            "Null response found for Package on key: " + key);
                }
                Tuple out;
                switch (result.returnStatus) {
                    case POStatus.STATUS_OK:
                        // (key, {(value)...})
                        out = (Tuple) result.result;
                        break;
                    case POStatus.STATUS_NULL:
                        out = null;
                        break;
                    default:
                        throw new RuntimeException(
                                "Unexpected response code from operator "
                                        + pkgOp + " : " + result + " "
                                        + result.returnStatus);
                }
                if (LOG.isDebugEnabled()) {
                    LOG.debug("GroupPkgFunction out " + out);
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

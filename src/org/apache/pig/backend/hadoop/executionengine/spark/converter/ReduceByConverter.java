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

import scala.Tuple2;
import scala.runtime.AbstractFunction1;
import scala.runtime.AbstractFunction2;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.spark.SparkUtil;
import org.apache.pig.backend.hadoop.executionengine.spark.operator.POReduceBySpark;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DefaultBagFactory;
import org.apache.pig.data.DefaultTuple;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.spark.HashPartitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.rdd.PairRDDFunctions;
import org.apache.spark.rdd.RDD;

@SuppressWarnings({"serial"})
public class ReduceByConverter implements RDDConverter<Tuple, Tuple, POReduceBySpark> {
    private static final Log LOG = LogFactory.getLog(ReduceByConverter.class);

    private static final TupleFactory tf = TupleFactory.getInstance();

    @Override
    public RDD<Tuple> convert(List<RDD<Tuple>> predecessors, POReduceBySpark op) throws IOException {
        SparkUtil.assertPredecessorSize(predecessors, op, 1);
        int parallelism = SparkUtil.getParallelism(predecessors, op);

        RDD<Tuple> rdd = predecessors.get(0);

        JavaRDD<Tuple2<IndexedKey, Tuple>> rddPair;
        if (op.isUseSecondaryKey()) {
            rddPair = handleSecondarySort(rdd, op, parallelism);
        } else {
            JavaRDD<Tuple> jrdd = JavaRDD.fromRDD(rdd, SparkUtil.getManifest(Tuple.class));
            rddPair = jrdd.map(new ToKeyValueFunction(op));
        }
        PairRDDFunctions<IndexedKey, Tuple> pairRDDFunctions
                = new PairRDDFunctions<>(rddPair.rdd(),
                SparkUtil.getManifest(IndexedKey.class),
                SparkUtil.getManifest(Tuple.class), null);

        RDD<Tuple2<IndexedKey, Tuple>> tupleRDD = pairRDDFunctions.reduceByKey(
                SparkUtil.getPartitioner(op.getCustomPartitioner(), parallelism),
                new MergeValuesFunction(op));
        LOG.debug("Custom Partitioner and parallelims used : " + op.getCustomPartitioner() + ", " + parallelism);

        return tupleRDD.map(new ToTupleFunction(op), SparkUtil.getManifest(Tuple.class));
    }

    private JavaRDD<Tuple2<IndexedKey, Tuple>> handleSecondarySort(
            RDD<Tuple> rdd, POReduceBySpark op, int parallelism) {

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
        JavaRDD<Tuple2<IndexedKey, Tuple>> jrddPair = jrdd.map(new ToKeyValueFunction(op));
        return jrddPair;
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

    /**
     * Converts incoming locally rearranged tuple, which is of the form
     * (index, key, value) into Tuple2<key, Tuple(key, value)>
     */
    private static class ToKeyValueFunction implements
            Function<Tuple, Tuple2<IndexedKey, Tuple>>, Serializable {

        private POReduceBySpark poReduce = null;

        public ToKeyValueFunction(POReduceBySpark poReduce) {
            this.poReduce = poReduce;
        }

        @Override
        public Tuple2<IndexedKey, Tuple> call(Tuple t) {
            try {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("ToKeyValueFunction in " + t);
                }

                Object key;
                if ((poReduce != null) && (poReduce.isUseSecondaryKey())) {
                    key = ((Tuple) t.get(1)).get(0);
                } else {
                    key = t.get(1);
                }

                Tuple tupleWithKey = tf.newTuple();
                tupleWithKey.append(key);
                tupleWithKey.append(t.get(2));

                Tuple2<IndexedKey, Tuple> out = new Tuple2<IndexedKey, Tuple>(new IndexedKey((Byte) t.get(0), key), tupleWithKey);
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
     * Given two input tuples, this function outputs the resultant tuple.
     * Additionally, it packages the input tuples to ensure the Algebraic Functions can work on them.
     */
    private static final class MergeValuesFunction extends AbstractFunction2<Tuple, Tuple, Tuple>
            implements Serializable {
        private final POReduceBySpark poReduce;

        public MergeValuesFunction(POReduceBySpark poReduce) {
            this.poReduce = poReduce;
        }

        @Override
        public Tuple apply(Tuple v1, Tuple v2) {
            LOG.debug("MergeValuesFunction in : " + v1 + " , " + v2);
            Tuple result = tf.newTuple(2);
            DataBag bag = DefaultBagFactory.getInstance().newDefaultBag();
            Tuple t = new DefaultTuple();
            try {
                // Package the input tuples so they can be processed by Algebraic functions.
                Object key = v1.get(0);
                if (key == null) {
                    key = "";
                } else {
                    result.set(0, key);
                }
                bag.add((Tuple) v1.get(1));
                bag.add((Tuple) v2.get(1));
                t.append(key);
                t.append(bag);

                poReduce.getPkgr().attachInput(key, new DataBag[]{(DataBag) t.get(1)}, new boolean[]{true});
                Tuple packagedTuple = (Tuple) poReduce.getPkgr().getNext().result;

                // Perform the operation
                LOG.debug("MergeValuesFunction packagedTuple : " + t);
                poReduce.attachInput(packagedTuple);
                Result r = poReduce.getNext(poReduce.getResultType());

                // Ensure output is consistent with the output of KeyValueFunction
                // If we return r.result, the result will be something like this:
                // (ABC,(2),(3)) - A tuple with key followed by values.
                // But, we want the result to look like this:
                // (ABC,((2),(3))) - A tuple with key and a value tuple (containing values).
                // Hence, the construction of a new value tuple

                Tuple valueTuple = tf.newTuple();
                for (Object o : ((Tuple) r.result).getAll()) {
                    if (!o.equals(key)) {
                        valueTuple.append(o);
                    }
                }
                result.set(1,valueTuple);
                LOG.debug("MergeValuesFunction out : " + result);
                return result;
            } catch (ExecException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * This function transforms the Tuple to ensure it is packaged as per requirements of the Operator's packager.
     */
    private static final class ToTupleFunction extends AbstractFunction1<Tuple2<IndexedKey, Tuple>, Tuple>
            implements Serializable {

        private final POReduceBySpark poReduce;

        public ToTupleFunction(POReduceBySpark poReduce) {
            this.poReduce = poReduce;
        }

        @Override
        public Tuple apply(Tuple2<IndexedKey, Tuple> v1) {
            LOG.debug("ToTupleFunction in : " + v1);
            DataBag bag = DefaultBagFactory.getInstance().newDefaultBag();
            Tuple t = new DefaultTuple();
            Tuple packagedTuple = null;
            try {
                Object key = v1._2().get(0);
                bag.add((Tuple) v1._2().get(1));
                t.append(key);
                t.append(bag);
                poReduce.getPkgr().attachInput(key, new DataBag[]{(DataBag) t.get(1)}, new boolean[]{true});
                packagedTuple = (Tuple) poReduce.getPkgr().getNext().result;
            } catch (ExecException e) {
                throw new RuntimeException(e);
            }
            LOG.debug("ToTupleFunction out : " + packagedTuple);
            return packagedTuple;
        }
    }
}

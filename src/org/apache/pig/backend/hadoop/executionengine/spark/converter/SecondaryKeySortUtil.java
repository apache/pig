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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Objects;

import scala.Tuple2;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POPackage;
import org.apache.pig.backend.hadoop.executionengine.spark.SparkUtil;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.io.NullableTuple;
import org.apache.pig.impl.io.PigNullableWritable;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.rdd.RDD;

/**
 * Provide utility functions which is used by ReducedByConverter and JoinGroupSparkConverter.
 */
public class SecondaryKeySortUtil {
    private static final Log LOG = LogFactory
            .getLog(SecondaryKeySortUtil.class);

    public static RDD<Tuple> handleSecondarySort(
            RDD<Tuple2<IndexedKey, Tuple>> rdd, POPackage pkgOp) {
        JavaPairRDD<IndexedKey, Tuple> pairRDD = JavaPairRDD.fromRDD(rdd, SparkUtil.getManifest(IndexedKey.class),
                SparkUtil.getManifest(Tuple.class));

        int partitionNums = pairRDD.partitions().size();
        //repartition to group tuples with same indexedkey to same partition
        JavaPairRDD<IndexedKey, Tuple> sorted = pairRDD.repartitionAndSortWithinPartitions(
                new IndexedKeyPartitioner(partitionNums));
        //Package tuples with same indexedkey as the result: (key,(val1,val2,val3,...))
        return sorted.mapPartitions(new AccumulateByKey(pkgOp), true).rdd();
    }

    //Package tuples with same indexedkey as the result: (key,(val1,val2,val3,...))
    //Send (key,Iterator) to POPackage, use POPackage#getNextTuple to get the result
    private static class AccumulateByKey implements FlatMapFunction<Iterator<Tuple2<IndexedKey, Tuple>>, Tuple>,
            Serializable {
        private POPackage pkgOp;

        public AccumulateByKey(POPackage pkgOp) {
            this.pkgOp = pkgOp;
        }

        @Override
        public Iterable<Tuple> call(final Iterator<Tuple2<IndexedKey, Tuple>> it) throws Exception {
            return new Iterable<Tuple>() {
                Object curKey = null;
                ArrayList curValues = new ArrayList();
                boolean initialized = false;

                @Override
                public Iterator<Tuple> iterator() {
                    return new Iterator<Tuple>() {

                        @Override
                        public boolean hasNext() {
                            return it.hasNext() || curKey != null;
                        }

                        @Override
                        public Tuple next() {
                            while (it.hasNext()) {
                                Tuple2<IndexedKey, Tuple> t = it.next();
                                //key changes, restruct the last tuple by curKey, curValues and return
                                Object tMainKey = null;
                                try {
                                    tMainKey = ((Tuple) (t._1()).getKey()).get(0);

                                    //If the key has changed and we've seen at least 1 already
                                    if (initialized &&
                                            ((curKey == null && tMainKey != null) ||
                                                    (curKey != null && !curKey.equals(tMainKey)))){
                                        Tuple result = restructTuple(curKey, new ArrayList(curValues));
                                        curValues.clear();
                                        curKey = tMainKey;
                                        curValues.add(t._2());
                                        return result;
                                    }
                                    curKey = tMainKey;
                                    //if key does not change, just append the value to the same key
                                    curValues.add(t._2());
                                    initialized = true;

                                } catch (ExecException e) {
                                    throw new RuntimeException("AccumulateByKey throw exception: ", e);
                                }
                            }
                            if (curKey == null) {
                                throw new RuntimeException("AccumulateByKey curKey is null");
                            }

                            //if we get here, this should be the last record
                            Tuple res = restructTuple(curKey, curValues);
                            curKey = null;
                            return res;
                        }


                        @Override
                        public void remove() {
                            // Not implemented.
                            // throw Unsupported Method Invocation Exception.
                            throw new UnsupportedOperationException();
                        }
                    };
                }
            };
        }

        private Tuple restructTuple(final Object curKey, final ArrayList<Tuple> curValues) {
            try {
                Tuple retVal = null;
                PigNullableWritable retKey = new PigNullableWritable() {

                    public Object getValueAsPigType() {
                        return curKey;
                    }
                };

                //Here restruct a tupleIterator, later POPackage#tupIter will use it.
                final Iterator<Tuple> tupleItearator = curValues.iterator();
                Iterator<NullableTuple> iterator = new Iterator<NullableTuple>() {
                    public boolean hasNext() {
                        return tupleItearator.hasNext();
                    }

                    public NullableTuple next() {
                        Tuple t = tupleItearator.next();
                        return new NullableTuple(t);
                    }

                    public void remove() {
                        throw new UnsupportedOperationException();
                    }
                };
                pkgOp.setInputs(null);
                pkgOp.attachInput(retKey, iterator);
                Result res = pkgOp.getNextTuple();
                if (res.returnStatus == POStatus.STATUS_OK) {
                    retVal = (Tuple) res.result;
                }
                if (LOG.isDebugEnabled()) {
                    LOG.debug("AccumulateByKey out: " + retVal);
                }
                return retVal;
            } catch (ExecException e) {
                throw new RuntimeException("AccumulateByKey#restructTuple throws exception: ", e);
            }
        }
    }

    //Group tuples with same IndexKey into same partition
    private static class IndexedKeyPartitioner extends Partitioner {
        private int partition;

        public IndexedKeyPartitioner(int partition) {
            this.partition = partition;
        }

        @Override
        public int getPartition(Object obj) {
            IndexedKey indexedKey = (IndexedKey) obj;
            Tuple key = (Tuple) indexedKey.getKey();

            int hashCode = 0;
            try {
                hashCode = Objects.hashCode(key.get(0));
            } catch (ExecException e) {
                throw new RuntimeException("IndexedKeyPartitioner#getPartition: ", e);
            }
            return Math.abs(hashCode) % partition;
        }

        @Override
        public int numPartitions() {
            return partition;
        }
    }
}

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
import java.util.Map;
import java.util.HashMap;

import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.hadoop.executionengine.spark.SparkPigContext;
import org.apache.pig.data.DataBag;
import org.apache.pig.impl.builtin.PartitionSkewedKeys;
import org.apache.pig.impl.util.Pair;
import org.apache.spark.Partitioner;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;
import scala.runtime.AbstractFunction1;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLocalRearrange;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POSkewedJoin;
import org.apache.pig.backend.hadoop.executionengine.spark.SparkUtil;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.plan.NodeIdGenerator;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.util.MultiMap;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.rdd.RDD;

public class SkewedJoinConverter implements
        RDDConverter<Tuple, Tuple, POSkewedJoin>, Serializable {

    private static Log log = LogFactory.getLog(SkewedJoinConverter.class);

    private POLocalRearrange[] LRs;
    private POSkewedJoin poSkewedJoin;

    private String skewedJoinPartitionFile;

    public void setSkewedJoinPartitionFile(String partitionFile) {
        skewedJoinPartitionFile = partitionFile;
    }


    @Override
    public RDD<Tuple> convert(List<RDD<Tuple>> predecessors,
                              POSkewedJoin poSkewedJoin) throws IOException {

        SparkUtil.assertPredecessorSize(predecessors, poSkewedJoin, 2);
        LRs = new POLocalRearrange[2];
        this.poSkewedJoin = poSkewedJoin;

        createJoinPlans(poSkewedJoin.getJoinPlans());

        // extract the two RDDs
        RDD<Tuple> rdd1 = predecessors.get(0);
        RDD<Tuple> rdd2 = predecessors.get(1);

        Broadcast<List<Tuple>> keyDist = SparkPigContext.get().getBroadcastedVars().get(skewedJoinPartitionFile);

        // if no keyDist,  we need  defaultParallelism
        Integer defaultParallelism = SparkPigContext.get().getParallelism(predecessors, poSkewedJoin);

        // with partition id
        SkewPartitionIndexKeyFunction skewFun = new SkewPartitionIndexKeyFunction(this, keyDist, defaultParallelism);
        RDD<Tuple2<PartitionIndexedKey, Tuple>> skewIdxKeyRDD = rdd1.map(skewFun,
                SparkUtil.<PartitionIndexedKey, Tuple>getTuple2Manifest());

        // Tuple2 RDD to Pair RDD
        JavaPairRDD<PartitionIndexedKey, Tuple> skewIndexedJavaPairRDD = new JavaPairRDD<PartitionIndexedKey, Tuple>(
                skewIdxKeyRDD, SparkUtil.getManifest(PartitionIndexedKey.class),
                SparkUtil.getManifest(Tuple.class));

        // with partition id
        StreamPartitionIndexKeyFunction streamFun = new StreamPartitionIndexKeyFunction(this, keyDist, defaultParallelism);
        JavaRDD<Tuple2<PartitionIndexedKey, Tuple>> streamIdxKeyJavaRDD = rdd2.toJavaRDD().flatMap(streamFun);

        // Tuple2 RDD to Pair RDD
        JavaPairRDD<PartitionIndexedKey, Tuple> streamIndexedJavaPairRDD = new JavaPairRDD<PartitionIndexedKey, Tuple>(
                streamIdxKeyJavaRDD.rdd(), SparkUtil.getManifest(PartitionIndexedKey.class),
                SparkUtil.getManifest(Tuple.class));

        JavaRDD<Tuple> result = doJoin(skewIndexedJavaPairRDD,
                streamIndexedJavaPairRDD,
                buildPartitioner(keyDist, defaultParallelism),
                keyDist);

        // return type is RDD<Tuple>, so take it from JavaRDD<Tuple>
        return result.rdd();
    }

    private void createJoinPlans(MultiMap<PhysicalOperator, PhysicalPlan> inpPlans) throws PlanException {

        int i = -1;
        for (PhysicalOperator inpPhyOp : inpPlans.keySet()) {
            ++i;
            POLocalRearrange lr = new POLocalRearrange(genKey());
            try {
                lr.setIndex(i);
            } catch (ExecException e) {
                throw new PlanException(e.getMessage(), e.getErrorCode(), e.getErrorSource(), e);
            }
            lr.setResultType(DataType.TUPLE);
            lr.setKeyType(DataType.TUPLE);//keyTypes.get(i).size() > 1 ? DataType.TUPLE : keyTypes.get(i).get(0));
            lr.setPlans(inpPlans.get(inpPhyOp));
            LRs[i] = lr;
        }
    }

    private OperatorKey genKey() {
        return new OperatorKey(poSkewedJoin.getOperatorKey().scope, NodeIdGenerator.getGenerator().getNextNodeId(poSkewedJoin.getOperatorKey().scope));
    }

    /**
     * @param <L> be generic because it can be Optional<Tuple> or Tuple
     * @param <R> be generic because it can be Optional<Tuple> or Tuple
     */
    private static class ToValueFunction<L, R> implements
            FlatMapFunction<Iterator<Tuple2<PartitionIndexedKey, Tuple2<L, R>>>, Tuple>, Serializable {

        private boolean[] innerFlags;
        private int[] schemaSize;

        private final Broadcast<List<Tuple>> keyDist;

        transient private boolean initialized = false;
        transient protected Map<Tuple, Pair<Integer, Integer>> reducerMap;

        public ToValueFunction(boolean[] innerFlags, int[] schemaSize, Broadcast<List<Tuple>> keyDist) {
            this.innerFlags = innerFlags;
            this.schemaSize = schemaSize;
            this.keyDist = keyDist;
        }

        private class Tuple2TransformIterable implements Iterable<Tuple> {

            Iterator<Tuple2<PartitionIndexedKey, Tuple2<L, R>>> in;

            Tuple2TransformIterable(
                    Iterator<Tuple2<PartitionIndexedKey, Tuple2<L, R>>> input) {
                in = input;
            }

            public Iterator<Tuple> iterator() {
                return new IteratorTransform<Tuple2<PartitionIndexedKey, Tuple2<L, R>>, Tuple>(
                        in) {
                    @Override
                    protected Tuple transform(
                            Tuple2<PartitionIndexedKey, Tuple2<L, R>> next) {
                        try {

                            L left = next._2._1;
                            R right = next._2._2;

                            TupleFactory tf = TupleFactory.getInstance();
                            Tuple result = tf.newTuple();

                            Tuple leftTuple = tf.newTuple();
                            if (!innerFlags[0]) {
                                // left should be Optional<Tuple>
                                Optional<Tuple> leftOption = (Optional<Tuple>) left;
                                if (!leftOption.isPresent()) {
                                    // Add an empty left record for RIGHT OUTER JOIN.
                                    // Notice: if it is a skewed, only join the first reduce key
                                    if (isFirstReduceKey(next._1)) {
                                        for (int i = 0; i < schemaSize[0]; i++) {
                                            leftTuple.append(null);
                                        }
                                    } else {
                                        return this.next();
                                    }
                                } else {
                                    leftTuple = leftOption.get();
                                }
                            } else {
                                leftTuple = (Tuple) left;
                            }
                            for (int i = 0; i < leftTuple.size(); i++) {
                                result.append(leftTuple.get(i));
                            }

                            Tuple rightTuple = tf.newTuple();
                            if (!innerFlags[1]) {
                                // right should be Optional<Tuple>
                                Optional<Tuple> rightOption = (Optional<Tuple>) right;
                                if (!rightOption.isPresent()) {
                                    for (int i = 0; i < schemaSize[1]; i++) {
                                        rightTuple.append(null);
                                    }
                                } else {
                                    rightTuple = rightOption.get();
                                }
                            } else {
                                rightTuple = (Tuple) right;
                            }
                            for (int i = 0; i < rightTuple.size(); i++) {
                                result.append(rightTuple.get(i));
                            }

                            if (log.isDebugEnabled()) {
                                log.debug("MJC: Result = " + result.toDelimitedString(" "));
                            }

                            return result;
                        } catch (Exception e) {
                            log.warn(e);
                        }
                        return null;
                    }
                };
            }
        }

        @Override
        public Iterable<Tuple> call(
                Iterator<Tuple2<PartitionIndexedKey, Tuple2<L, R>>> input) {
            return new Tuple2TransformIterable(input);
        }

        private boolean isFirstReduceKey(PartitionIndexedKey pKey) {
            // non-skewed key
            if (pKey.getPartitionId() == -1) {
                return true;
            }

            if (!initialized) {
                Integer[] reducers = new Integer[1];
                reducerMap = loadKeyDistribution(keyDist, reducers);
                initialized = true;
            }

            Pair<Integer, Integer> indexes = reducerMap.get(pKey.getKey());
            if (indexes != null && pKey.getPartitionId() != indexes.first) {
                // return false only when the key is skewed
                // and it is not the first reduce key.
                return false;
            }

            return true;
        }
    }

    /**
     * Utility function.
     * 1. Get parallelism
     * 2. build a key distribution map from the broadcasted key distribution file
     *
     * @param keyDist
     * @param totalReducers
     * @return
     */
    private static Map<Tuple, Pair<Integer, Integer>> loadKeyDistribution(Broadcast<List<Tuple>> keyDist,
                                                                          Integer[] totalReducers) {
        Map<Tuple, Pair<Integer, Integer>> reducerMap = new HashMap<>();
        totalReducers[0] = -1; // set a default value

        if (keyDist == null || keyDist.value() == null || keyDist.value().size() == 0) {
            // this could happen if sampling is empty
            log.warn("Empty dist file: ");
            return reducerMap;
        }

        try {
            final TupleFactory tf = TupleFactory.getInstance();

            Tuple t = keyDist.value().get(0);

            Map<String, Object> distMap = (Map<String, Object>) t.get(0);
            DataBag partitionList = (DataBag) distMap.get(PartitionSkewedKeys.PARTITION_LIST);

            totalReducers[0] = Integer.valueOf("" + distMap.get(PartitionSkewedKeys.TOTAL_REDUCERS));

            Iterator<Tuple> it = partitionList.iterator();
            while (it.hasNext()) {
                Tuple idxTuple = it.next();
                Integer maxIndex = (Integer) idxTuple.get(idxTuple.size() - 1);
                Integer minIndex = (Integer) idxTuple.get(idxTuple.size() - 2);
                // Used to replace the maxIndex with the number of reducers
                if (maxIndex < minIndex) {
                    maxIndex = totalReducers[0] + maxIndex;
                }

                // remove the last 2 fields of the tuple, i.e: minIndex and maxIndex and store
                // it in the reducer map
                Tuple keyTuple = tf.newTuple();
                for (int i = 0; i < idxTuple.size() - 2; i++) {
                    keyTuple.append(idxTuple.get(i));
                }

                // number of reducers
                Integer cnt = maxIndex - minIndex;
                reducerMap.put(keyTuple, new Pair(minIndex, cnt));
            }

        } catch (ExecException e) {
            log.warn(e.getMessage());
        }

        return reducerMap;
    }

    private static class PartitionIndexedKey extends IndexedKey {
        // for user defined partitioner
        int partitionId;

        public PartitionIndexedKey(byte index, Object key) {
            super(index, key);
            partitionId = -1;
        }

        public PartitionIndexedKey(byte index, Object key, int pid) {
            super(index, key);
            partitionId = pid;
        }

        public int getPartitionId() {
            return partitionId;
        }

        private void setPartitionId(int pid) {
            partitionId = pid;
        }

        @Override
        public String toString() {
            return "PartitionIndexedKey{" +
                    "index=" + getIndex() +
                    ", partitionId=" + getPartitionId() +
                    ", key=" + getKey() +
                    '}';
        }
    }

    /**
     * append a Partition id to the records from skewed table.
     * so that the SkewedJoinPartitioner can send skewed records to different reducer
     * <p>
     * see: https://wiki.apache.org/pig/PigSkewedJoinSpec
     */
    private static class SkewPartitionIndexKeyFunction extends
            AbstractFunction1<Tuple, Tuple2<PartitionIndexedKey, Tuple>> implements
            Serializable {

        private final SkewedJoinConverter poSkewedJoin;

        private final Broadcast<List<Tuple>> keyDist;
        private final Integer defaultParallelism;

        transient private boolean initialized = false;
        transient protected Map<Tuple, Pair<Integer, Integer>> reducerMap;
        transient private Integer parallelism = -1;
        transient private Map<Tuple, Integer> currentIndexMap;

        public SkewPartitionIndexKeyFunction(SkewedJoinConverter poSkewedJoin,
                                             Broadcast<List<Tuple>> keyDist,
                                             Integer defaultParallelism) {
            this.poSkewedJoin = poSkewedJoin;
            this.keyDist = keyDist;
            this.defaultParallelism = defaultParallelism;
        }

        @Override
        public Tuple2<PartitionIndexedKey, Tuple> apply(Tuple tuple) {
            // attach tuple to LocalRearrange
            poSkewedJoin.LRs[0].attachInput(tuple);

            try {
                Result lrOut = poSkewedJoin.LRs[0].getNextTuple();

                // If tuple is (AA, 5) and key index is $1, then it lrOut is 0 5
                // (AA), so get(1) returns key
                Byte index = (Byte) ((Tuple) lrOut.result).get(0);
                Object key = ((Tuple) lrOut.result).get(1);

                Tuple keyTuple = (Tuple) key;
                int partitionId = getPartitionId(keyTuple);
                PartitionIndexedKey pIndexKey = new PartitionIndexedKey(index, keyTuple, partitionId);

                // make a (key, value) pair
                Tuple2<PartitionIndexedKey, Tuple> tuple_KeyValue = new Tuple2<PartitionIndexedKey, Tuple>(
                        pIndexKey,
                        tuple);

                return tuple_KeyValue;
            } catch (Exception e) {
                System.out.print(e);
                return null;
            }
        }

        private Integer getPartitionId(Tuple keyTuple) {
            if (!initialized) {
                Integer[] reducers = new Integer[1];
                reducerMap = loadKeyDistribution(keyDist, reducers);
                parallelism = reducers[0];

                if (parallelism <= 0) {
                    parallelism = defaultParallelism;
                }

                currentIndexMap = Maps.newHashMap();

                initialized = true;
            }

            // for partition table, compute the index based on the sampler output
            Pair<Integer, Integer> indexes;
            Integer curIndex = -1;

            indexes = reducerMap.get(keyTuple);

            // if the reducerMap does not contain the key return -1 so that the
            // partitioner will do the default hash based partitioning
            if (indexes == null) {
                return -1;
            }

            if (currentIndexMap.containsKey(keyTuple)) {
                curIndex = currentIndexMap.get(keyTuple);
            }

            if (curIndex >= (indexes.first + indexes.second) || curIndex == -1) {
                curIndex = indexes.first;
            } else {
                curIndex++;
            }

            // set it in the map
            currentIndexMap.put(keyTuple, curIndex);
            return (curIndex % parallelism);
        }

    }

    /**
     * POPartitionRearrange is not used in spark mode now,
     * Here, use flatMap and CopyStreamWithPidFunction to copy the
     * stream records to the multiple reducers
     * <p>
     * see: https://wiki.apache.org/pig/PigSkewedJoinSpec
     */
    private static class StreamPartitionIndexKeyFunction implements FlatMapFunction<Tuple, Tuple2<PartitionIndexedKey, Tuple>> {

        private SkewedJoinConverter poSkewedJoin;
        private final Broadcast<List<Tuple>> keyDist;
        private final Integer defaultParallelism;

        private transient boolean initialized = false;
        protected transient Map<Tuple, Pair<Integer, Integer>> reducerMap;
        private transient Integer parallelism;

        public StreamPartitionIndexKeyFunction(SkewedJoinConverter poSkewedJoin,
                                               Broadcast<List<Tuple>> keyDist,
                                               Integer defaultParallelism) {
            this.poSkewedJoin = poSkewedJoin;
            this.keyDist = keyDist;
            this.defaultParallelism = defaultParallelism;
        }

        public Iterable<Tuple2<PartitionIndexedKey, Tuple>> call(Tuple tuple) throws Exception {
            if (!initialized) {
                Integer[] reducers = new Integer[1];
                reducerMap = loadKeyDistribution(keyDist, reducers);
                parallelism = reducers[0];
                if (parallelism <= 0) {
                    parallelism = defaultParallelism;
                }
                initialized = true;
            }

            // streamed table
            poSkewedJoin.LRs[1].attachInput(tuple);
            Result lrOut = poSkewedJoin.LRs[1].getNextTuple();

            Byte index = (Byte) ((Tuple) lrOut.result).get(0);
            Tuple key = (Tuple) ((Tuple) lrOut.result).get(1);

            ArrayList<Tuple2<PartitionIndexedKey, Tuple>> l = new ArrayList();
            Pair<Integer, Integer> indexes = reducerMap.get(key);

            // For non skewed keys, we set the partition index to be -1
            // so that the partitioner will do the default hash based partitioning
            if (indexes == null) {
                indexes = new Pair<>(-1, 0);
            }

            for (Integer reducerIdx = indexes.first, cnt = 0; cnt <= indexes.second; reducerIdx++, cnt++) {
                if (reducerIdx >= parallelism) {
                    reducerIdx = 0;
                }

                // set the partition index
                int partitionId = reducerIdx.intValue();
                PartitionIndexedKey pIndexKey = new PartitionIndexedKey(index, key, partitionId);

                l.add(new Tuple2(pIndexKey, tuple));
            }

            return l;
        }
    }

    /**
     * user defined spark partitioner for skewed join
     */
    private static class SkewedJoinPartitioner extends Partitioner {
        private int numPartitions;

        public SkewedJoinPartitioner(int parallelism) {
            numPartitions = parallelism;
        }

        @Override
        public int numPartitions() {
            return numPartitions;
        }

        @Override
        public int getPartition(Object IdxKey) {
            if (IdxKey instanceof PartitionIndexedKey) {
                int partitionId = ((PartitionIndexedKey) IdxKey).getPartitionId();
                if (partitionId >= 0) {
                    return partitionId;
                }
            }

            //else: by default using hashcode
            Tuple key = (Tuple) ((PartitionIndexedKey) IdxKey).getKey();


            int code = key.hashCode() % numPartitions;
            if (code >= 0) {
                return code;
            } else {
                return code + numPartitions;
            }
        }
    }

    /**
     * use parallelism from keyDist or the default parallelism to
     * create user defined partitioner
     *
     * @param keyDist
     * @param defaultParallelism
     * @return
     */
    private SkewedJoinPartitioner buildPartitioner(Broadcast<List<Tuple>> keyDist, Integer defaultParallelism) {
        Integer parallelism = -1;
        Integer[] reducers = new Integer[1];
        loadKeyDistribution(keyDist, reducers);
        parallelism = reducers[0];
        if (parallelism <= 0) {
            parallelism = defaultParallelism;
        }

        return new SkewedJoinPartitioner(parallelism);
    }

    /**
     * do all kinds of Join (inner, left outer, right outer, full outer)
     *
     * @param skewIndexedJavaPairRDD
     * @param streamIndexedJavaPairRDD
     * @param partitioner
     * @return
     */
    private JavaRDD<Tuple> doJoin(
            JavaPairRDD<PartitionIndexedKey, Tuple> skewIndexedJavaPairRDD,
            JavaPairRDD<PartitionIndexedKey, Tuple> streamIndexedJavaPairRDD,
            SkewedJoinPartitioner partitioner,
            Broadcast<List<Tuple>> keyDist) {

        boolean[] innerFlags = poSkewedJoin.getInnerFlags();
        int[] schemaSize = {0, 0};
        for (int i = 0; i < 2; i++) {
            if (poSkewedJoin.getSchema(i) != null) {
                schemaSize[i] = poSkewedJoin.getSchema(i).size();
            }
        }

        ToValueFunction toValueFun = new ToValueFunction(innerFlags, schemaSize, keyDist);

        if (innerFlags[0] && innerFlags[1]) {
            // inner join
            JavaPairRDD<PartitionIndexedKey, Tuple2<Tuple, Tuple>> resultKeyValue = skewIndexedJavaPairRDD.
                    join(streamIndexedJavaPairRDD, partitioner);

            return resultKeyValue.mapPartitions(toValueFun);
        } else if (innerFlags[0] && !innerFlags[1]) {
            // left outer join
            JavaPairRDD<PartitionIndexedKey, Tuple2<Tuple, Optional<Tuple>>> resultKeyValue = skewIndexedJavaPairRDD.
                    leftOuterJoin(streamIndexedJavaPairRDD, partitioner);

            return resultKeyValue.mapPartitions(toValueFun);
        } else if (!innerFlags[0] && innerFlags[1]) {
            // right outer join
            JavaPairRDD<PartitionIndexedKey, Tuple2<Optional<Tuple>, Tuple>> resultKeyValue = skewIndexedJavaPairRDD.
                    rightOuterJoin(streamIndexedJavaPairRDD, partitioner);

            return resultKeyValue.mapPartitions(toValueFun);
        } else {
            // full outer join
            JavaPairRDD<PartitionIndexedKey, Tuple2<Optional<Tuple>, Optional<Tuple>>> resultKeyValue = skewIndexedJavaPairRDD.
                    fullOuterJoin(streamIndexedJavaPairRDD, partitioner);

            return resultKeyValue.mapPartitions(toValueFun);
        }
    }

}

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
package org.apache.pig.backend.hadoop.executionengine.spark;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import scala.Product2;
import scala.Tuple2;
import scala.collection.JavaConversions;
import scala.collection.Seq;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

import org.apache.hadoop.mapred.JobConf;
import org.apache.pig.PigConstants;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MRConfiguration;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POProject;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POSort;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.util.PlanHelper;
import org.apache.pig.backend.hadoop.executionengine.spark.plan.SparkOperator;
import org.apache.pig.backend.hadoop.executionengine.util.MapRedUtil;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.plan.NodeIdGenerator;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.spark.HashPartitioner;
import org.apache.spark.Partitioner;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.rdd.RDD;

public class SparkUtil {

    private static ThreadLocal<Integer> SPARK_REDUCERS = null;
    private static ConcurrentHashMap<String, Broadcast<List<Tuple>>> broadcastedVars = new ConcurrentHashMap() ;

    public static <T> ClassTag<T> getManifest(Class<T> clazz) {
        return ClassTag$.MODULE$.apply(clazz);
    }

    @SuppressWarnings("unchecked")
    public static <K, V> ClassTag<Tuple2<K, V>> getTuple2Manifest() {
        return (ClassTag<Tuple2<K, V>>) (Object) getManifest(Tuple2.class);
    }

    @SuppressWarnings("unchecked")
    public static <K, V> ClassTag<Product2<K, V>> getProduct2Manifest() {
        return (ClassTag<Product2<K, V>>) (Object) getManifest(Product2.class);
    }

    public static JobConf newJobConf(PigContext pigContext, PhysicalPlan physicalPlan, SparkEngineConf sparkEngineConf) throws
            IOException {
        JobConf jobConf = new JobConf(
                ConfigurationUtil.toConfiguration(pigContext.getProperties()));
        //Serialize the thread local variable UDFContext#udfConfs, UDFContext#clientSysProps and PigContext#packageImportList
        //inside SparkEngineConf separately
        jobConf.set("spark.engine.conf",ObjectSerializer.serialize(sparkEngineConf));
        //Serialize the PigContext so it's available in Executor thread.
        jobConf.set("pig.pigContext", ObjectSerializer.serialize(pigContext));
        // Serialize the thread local variable inside PigContext separately
        // Although after PIG-4920, we store udf.import.list in SparkEngineConf
        // but we still need store it in jobConf because it will be used in PigOutputFormat#setupUdfEnvAndStores
        jobConf.set("udf.import.list",
                ObjectSerializer.serialize(PigContext.getPackageImportList()));
        Random rand = new Random();
        jobConf.set(MRConfiguration.JOB_APPLICATION_ATTEMPT_ID, Integer.toString(rand.nextInt()));
        jobConf.set(PigConstants.LOCAL_CODE_DIR,
                System.getProperty("java.io.tmpdir"));
        jobConf.set(MRConfiguration.JOB_ID, UUID.randomUUID().toString());

        LinkedList<POStore> stores = PlanHelper.getPhysicalOperators(
                physicalPlan, POStore.class);
        POStore firstStore = stores.getFirst();
        if (firstStore != null) {
            MapRedUtil.setupStreamingDirsConfSingle(firstStore, pigContext,
                    jobConf);
        }
        return jobConf;
    }

    public static <T> Seq<T> toScalaSeq(List<T> list) {
        return JavaConversions.asScalaBuffer(list);
    }

    public static void assertPredecessorSize(List<RDD<Tuple>> predecessors,
            PhysicalOperator physicalOperator, int size) {
        if (predecessors.size() != size) {
            throw new RuntimeException("Should have " + size
                    + " predecessors for " + physicalOperator.getClass()
                    + ". Got : " + predecessors.size());
        }
    }

    public static void assertPredecessorSizeGreaterThan(
            List<RDD<Tuple>> predecessors, PhysicalOperator physicalOperator,
            int size) {
        if (predecessors.size() <= size) {
            throw new RuntimeException("Should have greater than" + size
                    + " predecessors for " + physicalOperator.getClass()
                    + ". Got : " + predecessors.size());
        }
    }

    public static int getParallelism(List<RDD<Tuple>> predecessors,
            PhysicalOperator physicalOperator) {
        if (SPARK_REDUCERS != null) {
            return getSparkReducers();
        }

        int parallelism = physicalOperator.getRequestedParallelism();
        if (parallelism <= 0) {
            // Parallelism wasn't set in Pig, so set it to whatever Spark thinks
            // is reasonable.
            parallelism = predecessors.get(0).context().defaultParallelism();
        }

        return parallelism;
    }

    public static Partitioner getPartitioner(String customPartitioner, int parallelism) {
        if (customPartitioner == null) {
            return new HashPartitioner(parallelism);
        } else {
            return new MapReducePartitionerWrapper(customPartitioner, parallelism);
        }
    }

    // createIndexerSparkNode is a utility to create an indexer spark node with baseSparkOp
    static public void createIndexerSparkNode(SparkOperator baseSparkOp, String scope, NodeIdGenerator nig) throws PlanException, ExecException {
        List<PhysicalPlan> eps = new ArrayList<PhysicalPlan>();
        PhysicalPlan ep = new PhysicalPlan();
        POProject prj = new POProject(new OperatorKey(scope,
                nig.getNextNodeId(scope)));
        prj.setStar(true);
        prj.setOverloaded(false);
        prj.setResultType(DataType.TUPLE);
        ep.add(prj);
        eps.add(ep);

        List<Boolean> ascCol = new ArrayList<Boolean>();
        ascCol.add(true);

        int requestedParallelism = baseSparkOp.requestedParallelism;
        POSort sort = new POSort(new OperatorKey(scope, nig.getNextNodeId(scope)), requestedParallelism, null, eps, ascCol, null);
        //POSort is added to sort the index tuples genereated by MergeJoinIndexer.More detail, see PIG-4601
        baseSparkOp.physicalPlan.addAsLeaf(sort);
    }

    static public ConcurrentHashMap<String, Broadcast<List<Tuple>>> getBroadcastedVars() {
        return broadcastedVars;
    }


    public static int getSparkReducers() {
        return SPARK_REDUCERS.get();
    }

    public static void setSparkReducers(int sparkReducers) {
        SPARK_REDUCERS.set(sparkReducers);
    }
}
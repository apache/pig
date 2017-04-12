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

import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.data.Tuple;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.rdd.RDD;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * singleton class like PigContext
 */
public class SparkPigContext {

    private static SparkPigContext context =  null;
    private static ThreadLocal<Integer> PIG_DEFAULT_PARALLELISM = null;
    private static ConcurrentHashMap<String, Broadcast<List<Tuple>>> broadcastedVars = new ConcurrentHashMap() ;

    public static SparkPigContext get(){
        if( context == null){
            context = new SparkPigContext();
        }
        return context;
    }
    public static int getPigDefaultParallelism() {
        return PIG_DEFAULT_PARALLELISM.get();
    }


    public static int getParallelism(List<RDD<Tuple>> predecessors,
                                     PhysicalOperator physicalOperator) {
        if (PIG_DEFAULT_PARALLELISM != null) {
           return getPigDefaultParallelism();
        }

        int parallelism = physicalOperator.getRequestedParallelism();
        if (parallelism <= 0) {
            //Spark automatically sets the number of "map" tasks to run on each file according to its size (though
            // you can control it through optional parameters to SparkContext.textFile, etc), and for distributed
            //"reduce" operations, such as groupByKey and reduceByKey, it uses the largest parent RDD's number of
            // partitions.
            int maxParallism = 0;
            for (int i = 0; i < predecessors.size(); i++) {
                int tmpParallelism = predecessors.get(i).getNumPartitions();
                if (tmpParallelism > maxParallism) {
                    maxParallism = tmpParallelism;
                }
            }
            parallelism = maxParallism;
        }
        return parallelism;
    }

    public static void setPigDefaultParallelism(int pigDefaultParallelism) {
        PIG_DEFAULT_PARALLELISM.set(pigDefaultParallelism);
    }

     public static ConcurrentHashMap<String, Broadcast<List<Tuple>>> getBroadcastedVars() {
        return broadcastedVars;
    }
}

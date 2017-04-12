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

import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POBroadcastSpark;
import org.apache.pig.backend.hadoop.executionengine.spark.SparkPigContext;
import org.apache.pig.backend.hadoop.executionengine.spark.SparkUtil;
import org.apache.pig.data.Tuple;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.rdd.RDD;

import java.util.List;

public class BroadcastConverter implements RDDConverter<Tuple, Tuple, POBroadcastSpark> {

    private final JavaSparkContext sc;

    public BroadcastConverter(JavaSparkContext sc) {
        this.sc = sc;
    }

    @Override
    public RDD<Tuple> convert(List<RDD<Tuple>> predecessors, POBroadcastSpark po) {
        SparkUtil.assertPredecessorSize(predecessors, po, 1);
        RDD<Tuple> rdd = predecessors.get(0);

        // Just collect the predecessor RDD, and broadcast it
        JavaRDD<Tuple> javaRDD = new JavaRDD<>(rdd, SparkUtil.getManifest(Tuple.class));
        Broadcast<List<Tuple>> broadcastedRDD = sc.broadcast(javaRDD.collect());

        // Save the broadcast variable to broadcastedVars map, so that this
        // broadcasted variable can be referenced by the driver client.
        SparkPigContext.get().getBroadcastedVars().put(po.getBroadcastedVariableName(), broadcastedRDD);

        return rdd;
    }

}

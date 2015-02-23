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
import java.util.List;
import scala.Product2;
import scala.Tuple2;
import scala.collection.JavaConversions;
import scala.collection.Seq;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

import org.apache.hadoop.mapred.JobConf;

import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.pig.impl.util.UDFContext;

import org.apache.spark.rdd.RDD;

public class SparkUtil {

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

    public static JobConf newJobConf(PigContext pigContext) throws IOException {
        JobConf jobConf = new JobConf(
                ConfigurationUtil.toConfiguration(pigContext.getProperties()));
        jobConf.set("pig.pigContext", ObjectSerializer.serialize(pigContext));
        UDFContext.getUDFContext().serialize(jobConf);
        jobConf.set("udf.import.list",
                ObjectSerializer.serialize(PigContext.getPackageImportList()));
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
        int parallelism = physicalOperator.getRequestedParallelism();
        if (parallelism <= 0) {
            // Parallelism wasn't set in Pig, so set it to whatever Spark thinks
            // is reasonable.
            parallelism = predecessors.get(0).context().defaultParallelism();
        }
        return parallelism;
    }

}

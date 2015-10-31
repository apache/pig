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
import java.util.ArrayList;
import java.util.List;

import org.apache.pig.tools.pigstats.PigStatsUtil;
import org.apache.pig.tools.pigstats.spark.SparkCounters;
import org.apache.pig.tools.pigstats.spark.SparkPigStatusReporter;
import org.apache.pig.tools.pigstats.spark.SparkStatsUtil;
import scala.Tuple2;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.pig.PigConfiguration;
import org.apache.pig.StoreFuncInterface;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.JobControlCompiler;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigOutputFormat;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.backend.hadoop.executionengine.spark.SparkUtil;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.rdd.PairRDDFunctions;
import org.apache.spark.rdd.RDD;

import com.google.common.collect.Lists;

/**
 * Converter that takes a POStore and stores it's content.
 */
@SuppressWarnings({ "serial" })
public class StoreConverter implements
        RDDConverter<Tuple, Tuple2<Text, Tuple>, POStore> {

  private static final Log LOG = LogFactory.getLog(StoreConverter.class);

    private PigContext pigContext;

    public StoreConverter(PigContext pigContext) {
        this.pigContext = pigContext;
    }

    @Override
    public RDD<Tuple2<Text, Tuple>> convert(List<RDD<Tuple>> predecessors,
            POStore op) throws IOException {
        SparkUtil.assertPredecessorSize(predecessors, op, 1);
        RDD<Tuple> rdd = predecessors.get(0);

        SparkPigStatusReporter.getInstance().createCounter(SparkStatsUtil.SPARK_STORE_COUNTER_GROUP,
                SparkStatsUtil.getStoreSparkCounterName(op));

        // convert back to KV pairs
        JavaRDD<Tuple2<Text, Tuple>> rddPairs = rdd.toJavaRDD().map(
                buildFromTupleFunction(op));

        PairRDDFunctions<Text, Tuple> pairRDDFunctions = new PairRDDFunctions<Text, Tuple>(
                rddPairs.rdd(), SparkUtil.getManifest(Text.class),
                SparkUtil.getManifest(Tuple.class), null);

        JobConf jobConf = SparkUtil.newJobConf(pigContext);
        POStore poStore = configureStorer(jobConf, op);

        if ("true".equalsIgnoreCase(jobConf
                .get(PigConfiguration.PIG_OUTPUT_LAZY))) {
            Job storeJob = new Job(jobConf);
            LazyOutputFormat.setOutputFormatClass(storeJob,
                    PigOutputFormat.class);
            jobConf = (JobConf) storeJob.getConfiguration();
            jobConf.setOutputKeyClass(Text.class);
            jobConf.setOutputValueClass(Tuple.class);
            String fileName = poStore.getSFile().getFileName();
            Path filePath = new Path(fileName);
            FileOutputFormat.setOutputPath(jobConf,filePath);
            pairRDDFunctions.saveAsNewAPIHadoopDataset(jobConf);
        } else {
            pairRDDFunctions.saveAsNewAPIHadoopFile(poStore.getSFile()
                    .getFileName(), Text.class, Tuple.class,
                    PigOutputFormat.class, jobConf);
        }

        RDD<Tuple2<Text, Tuple>> retRdd = rddPairs.rdd();
        if (LOG.isDebugEnabled())
            LOG.debug("RDD lineage: " + retRdd.toDebugString());
        return retRdd;
    }


    private static POStore configureStorer(JobConf jobConf,
            PhysicalOperator op) throws IOException {
        ArrayList<POStore> storeLocations = Lists.newArrayList();
        POStore poStore = (POStore) op;
        storeLocations.add(poStore);
        StoreFuncInterface sFunc = poStore.getStoreFunc();
        sFunc.setStoreLocation(poStore.getSFile().getFileName(),
                new org.apache.hadoop.mapreduce.Job(jobConf));
        poStore.setInputs(null);
        poStore.setParentPlan(null);

        jobConf.set(JobControlCompiler.PIG_MAP_STORES,
                ObjectSerializer.serialize(Lists.newArrayList()));
        jobConf.set(JobControlCompiler.PIG_REDUCE_STORES,
                ObjectSerializer.serialize(storeLocations));
        return poStore;
    }

    private static class FromTupleFunction implements
            Function<Tuple, Tuple2<Text, Tuple>> {

        private static Text EMPTY_TEXT = new Text();
        private String counterGroupName;
        private String counterName;
        private SparkCounters sparkCounters;


        public Tuple2<Text, Tuple> call(Tuple v1) {
            if (sparkCounters != null) {
                sparkCounters.increment(counterGroupName, counterName, 1L);
            }
            return new Tuple2<Text, Tuple>(EMPTY_TEXT, v1);
        }

        public void setCounterGroupName(String counterGroupName) {
            this.counterGroupName = counterGroupName;
        }

        public void setCounterName(String counterName) {
            this.counterName = counterName;
        }

        public void setSparkCounters(SparkCounters sparkCounter) {
            this.sparkCounters = sparkCounter;
        }
    }

    private FromTupleFunction buildFromTupleFunction(POStore op) {
        FromTupleFunction ftf = new FromTupleFunction();
        if (!op.isTmpStore()) {
            ftf.setCounterGroupName(SparkStatsUtil.SPARK_STORE_COUNTER_GROUP);
            ftf.setCounterName(SparkStatsUtil.getStoreSparkCounterName(op));
            SparkPigStatusReporter counterReporter = SparkPigStatusReporter.getInstance();
            ftf.setSparkCounters(counterReporter.getCounters());
        }
        return ftf;
    }
}

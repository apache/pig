package org.apache.pig.backend.hadoop.executionengine.spark.converter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
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
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;

import scala.Tuple2;

import com.google.common.collect.Lists;

/**
 * Converter that takes a POStore and stores it's content.
 */
@SuppressWarnings({ "serial" })
public class StoreConverter implements
        POConverter<Tuple, Tuple2<Text, Tuple>, POStore> {

    private static final FromTupleFunction FROM_TUPLE_FUNCTION = new FromTupleFunction();

    private PigContext pigContext;

    public StoreConverter(PigContext pigContext) {
        this.pigContext = pigContext;
    }

    @Override
    public RDD<Tuple2<Text, Tuple>> convert(List<RDD<Tuple>> predecessors,
            POStore physicalOperator) throws IOException {
        SparkUtil.assertPredecessorSize(predecessors, physicalOperator, 1);
        RDD<Tuple> rdd = predecessors.get(0);
        // convert back to KV pairs
        JavaRDD<Tuple2<Text, Tuple>> rddPairs = rdd.toJavaRDD().map(FROM_TUPLE_FUNCTION);

        PairRDDFunctions<Text, Tuple> pairRDDFunctions = new PairRDDFunctions<Text, Tuple>(
                rddPairs.rdd(), SparkUtil.getManifest(Text.class),
                SparkUtil.getManifest(Tuple.class), null);

        JobConf storeJobConf = SparkUtil.newJobConf(pigContext);
        POStore poStore = configureStorer(storeJobConf, physicalOperator);

        if ("true".equalsIgnoreCase(storeJobConf
                .get(PigConfiguration.PIG_OUTPUT_LAZY))) {
            Job storeJob = new Job(storeJobConf);
            LazyOutputFormat.setOutputFormatClass(storeJob,
                    PigOutputFormat.class);
            storeJobConf = (JobConf) storeJob.getConfiguration();
            storeJobConf.setOutputKeyClass(Text.class);
            storeJobConf.setOutputValueClass(Tuple.class);
            String fileName = poStore.getSFile().getFileName();
            Path filePath = new Path(fileName);
            FileOutputFormat.setOutputPath(storeJobConf,filePath);
            pairRDDFunctions.saveAsNewAPIHadoopDataset(storeJobConf);
        } else {
            pairRDDFunctions.saveAsNewAPIHadoopFile(poStore.getSFile()
                    .getFileName(), Text.class, Tuple.class,
                    PigOutputFormat.class, storeJobConf);
        }
        return rddPairs.rdd();
    }

    private static POStore configureStorer(JobConf jobConf,
            PhysicalOperator physicalOperator) throws IOException {
        ArrayList<POStore> storeLocations = Lists.newArrayList();
        POStore poStore = (POStore) physicalOperator;
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

        public Tuple2<Text, Tuple> call(Tuple v1) {
            return new Tuple2<Text, Tuple>(EMPTY_TEXT, v1);
        }
    }
}

package org.apache.pig.backend.hadoop.executionengine.spark.converter;

import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;
import java.util.List;


import org.apache.hadoop.mapred.JobConf;
import org.apache.pig.FuncSpec;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigMapReduce;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POUserFunc;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POForEach;
import org.apache.pig.backend.hadoop.executionengine.spark.KryoSerializer;
import org.apache.pig.backend.hadoop.executionengine.spark.SparkUtil;
import org.apache.pig.backend.hadoop.executionengine.util.MapRedUtil;
import org.apache.pig.data.SchemaTupleBackend;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.rdd.RDD;

/**
 * Convert that is able to convert an RRD to another RRD using a POForEach
 */
@SuppressWarnings({"serial" })
public class ForEachConverter implements POConverter<Tuple, Tuple, POForEach> {
    private byte[] confBytes;

    public ForEachConverter(byte[] confBytes) {
        this.confBytes = confBytes;
    }

    @Override
    public RDD<Tuple> convert(List<RDD<Tuple>> predecessors,
            POForEach physicalOperator) {
        SparkUtil.assertPredecessorSize(predecessors, physicalOperator, 1);
        RDD<Tuple> rdd = predecessors.get(0);
        ForEachFunction forEachFunction = new ForEachFunction(physicalOperator, this.confBytes);
        return rdd.toJavaRDD().mapPartitions(forEachFunction, true).rdd();
    }

    private static class ForEachFunction implements
            FlatMapFunction<Iterator<Tuple>, Tuple>, Serializable {

        private POForEach poForEach;
        private byte[] confBytes;
        private transient JobConf jobConf;

        private ForEachFunction(POForEach poForEach, byte[] confBytes) {
            this.poForEach = poForEach;
            this.confBytes = confBytes;
        }

        void initializeJobConf() {
            if (this.jobConf == null) {
                this.jobConf = KryoSerializer.deserializeJobConf(this.confBytes);
                PigMapReduce.sJobConfInternal.set(jobConf);
                try {
                    MapRedUtil.setupUDFContext(jobConf);
                    PigContext pc = (PigContext) ObjectSerializer.deserialize(jobConf.get("pig.pigContext"));
                    SchemaTupleBackend.initialize(jobConf, pc);

                } catch (IOException ioe) {
                    String msg = "Problem while configuring UDFContext from ForEachConverter.";
                    throw new RuntimeException(msg, ioe);
                }
            }
        }

        public Iterable<Tuple> call(final Iterator<Tuple> input) {
            initializeJobConf();
            PhysicalOperator[] planLeafOps= poForEach.getPlanLeafOps();
            if (planLeafOps != null) {
                for (PhysicalOperator op : planLeafOps) {
                    if (op.getClass() == POUserFunc.class) {
                        POUserFunc udf = (POUserFunc) op;
                          udf.setFuncInputSchema();
                    }
                }
            }


            return new Iterable<Tuple>() {

                @Override
                public Iterator<Tuple> iterator() {
                    return new POOutputConsumerIterator(input) {

                        protected void attach(Tuple tuple) {
                            poForEach.setInputs(null);
                            poForEach.attachInput(tuple);
                        }

                        protected Result getNextResult() throws ExecException {
                            return poForEach.getNextTuple();
                        }
                    };
                }
            };
        }
    }
}

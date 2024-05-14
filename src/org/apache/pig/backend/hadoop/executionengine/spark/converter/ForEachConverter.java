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
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.mapred.JobConf;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.ProgressableReporter;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POUserFunc;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POForEach;
import org.apache.pig.backend.hadoop.executionengine.spark.FlatMapFunctionAdapter;
import org.apache.pig.backend.hadoop.executionengine.spark.KryoSerializer;
import org.apache.pig.backend.hadoop.executionengine.spark.SparkUtil;
import org.apache.pig.data.SchemaTupleBackend;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.spark.rdd.RDD;

/**
 * Convert that is able to convert an RRD to another RRD using a POForEach
 */
@SuppressWarnings({"serial" })
public class ForEachConverter implements RDDConverter<Tuple, Tuple, POForEach> {

    private JobConf jobConf;

    public ForEachConverter(JobConf jobConf) {
        this.jobConf = jobConf;
    }

    @Override
    public RDD<Tuple> convert(List<RDD<Tuple>> predecessors,
            POForEach physicalOperator) {

        byte[] confBytes = KryoSerializer.serializeJobConf(jobConf);

        SparkUtil.assertPredecessorSize(predecessors, physicalOperator, 1);
        RDD<Tuple> rdd = predecessors.get(0);
        ForEachFunction forEachFunction = new ForEachFunction(physicalOperator, confBytes);

        return rdd.toJavaRDD().mapPartitions(SparkUtil.flatMapFunction(forEachFunction), true).rdd();
    }

    private static class ForEachFunction implements
            FlatMapFunctionAdapter<Iterator<Tuple>, Tuple>, Serializable {

        private POForEach poForEach;
        private byte[] confBytes;
        private transient JobConf jobConf;

        private ForEachFunction(POForEach poForEach, byte[] confBytes) {
            this.poForEach = poForEach;
            this.confBytes = confBytes;
        }

        @Override
        public Iterator<Tuple> call(final Iterator<Tuple> input) {

            initialize();

            // Initialize a reporter as the UDF might want to report progress.
            PhysicalOperator.setReporter(new ProgressableReporter());
            PhysicalOperator[] planLeafOps= poForEach.getPlanLeafOps();
            if (planLeafOps != null) {
                for (PhysicalOperator op : planLeafOps) {
                    if (op.getClass() == POUserFunc.class) {
                        POUserFunc udf = (POUserFunc) op;
                          udf.setFuncInputSchema();
                    }
                }
            }
            return new OutputConsumerIterator(input) {

                @Override
                protected void attach(Tuple tuple) {
                    poForEach.setInputs(null);
                    poForEach.attachInput(tuple);
                }

                @Override
                protected Result getNextResult() throws ExecException {
                    return poForEach.getNextTuple();
                }

                @Override
                protected void endOfInput() {
                }
            };
        }

        private void initialize() {
            if (this.jobConf == null) {
                try {
                    this.jobConf = KryoSerializer.deserializeJobConf(this.confBytes);
                    PigContext pc = (PigContext) ObjectSerializer.deserialize(jobConf.get("pig.pigContext"));
                    SchemaTupleBackend.initialize(jobConf, pc);
                } catch (IOException e) {
                    throw new RuntimeException("Couldn't initialize ForEachConverter");
                }
            }
        }
    }
}

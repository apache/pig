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
import java.util.List;
import java.util.UUID;

import scala.runtime.AbstractFunction1;

import org.apache.hadoop.mapred.JobConf;
import org.apache.pig.PigConstants;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MRConfiguration;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigMapReduce;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POFilter;
import org.apache.pig.backend.hadoop.executionengine.spark.KryoSerializer;
import org.apache.pig.backend.hadoop.executionengine.spark.SparkUtil;
import org.apache.pig.backend.hadoop.executionengine.util.MapRedUtil;
import org.apache.pig.data.SchemaTupleBackend;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.spark.rdd.RDD;

/**
 * Converter that converts an RDD to a filtered RRD using POFilter
 */
@SuppressWarnings({ "serial" })
public class FilterConverter implements RDDConverter<Tuple, Tuple, POFilter> {

    private byte[] confBytes;

    public FilterConverter(byte[] confBytes) {
        this.confBytes = confBytes;
    }

    @Override
    public RDD<Tuple> convert(List<RDD<Tuple>> predecessors,
            POFilter physicalOperator) {
        SparkUtil.assertPredecessorSize(predecessors, physicalOperator, 1);
        RDD<Tuple> rdd = predecessors.get(0);
        FilterFunction filterFunction = new FilterFunction(physicalOperator, confBytes);
        return rdd.filter(filterFunction);
    }

    private static class FilterFunction extends
            AbstractFunction1<Tuple, Object> implements Serializable {

        private POFilter poFilter;
        private byte[] confBytes;
        private transient JobConf jobConf;

        private FilterFunction(POFilter poFilter, byte[] confBytes) {
            this.poFilter = poFilter;
            this.confBytes = confBytes;
        }

        @Override
        public Boolean apply(Tuple v1) {
            initializeJobConf();
            Result result;
            try {
                poFilter.setInputs(null);
                poFilter.attachInput(v1);
                result = poFilter.getNextTuple();
            } catch (ExecException e) {
                throw new RuntimeException("Couldn't filter tuple", e);
            }

            if (result == null) {
                return false;
            }

            switch (result.returnStatus) {
            case POStatus.STATUS_OK:
                return true;
            case POStatus.STATUS_EOP: // TODO: probably also ok for EOS,
                                      // END_OF_BATCH
                return false;
            default:
                throw new RuntimeException(
                        "Unexpected response code from filter: " + result);
            }
        }

        void initializeJobConf() {
            if (this.jobConf != null) {
                return;
            }
            this.jobConf = KryoSerializer.deserializeJobConf(this.confBytes);
            PigMapReduce.sJobConfInternal.set(jobConf);
            try {
                MapRedUtil.setupUDFContext(jobConf);
                PigContext pc = (PigContext) ObjectSerializer.deserialize(jobConf.get("pig.pigContext"));
                SchemaTupleBackend.initialize(jobConf, pc);
                // Although Job ID and task index are not really applicable for spark,
                // set them here to overcome PIG-4827
                jobConf.set(MRConfiguration.JOB_ID, UUID.randomUUID().toString());
                jobConf.set(PigConstants.TASK_INDEX, "0");
            } catch (IOException ioe) {
                throw new RuntimeException("Problem while configuring UDFContext from FilterConverter.", ioe);
            }
        }
    }
}

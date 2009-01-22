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
package org.apache.pig.backend.hadoop.executionengine.mapReduceLayer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.TargetedTuple;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.io.PigNullableWritable;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.impl.util.ObjectSerializer;

/**
 * This class is the static Mapper class used by Pig
 * to execute Pig map only jobs. It gets a TargetedTuple
 * as input to the map function. Using the targets in it,
 * attaches the tuple as input to the target operators.
 * 
 * The map plan is then executed. The result is then collected
 * into the output collector provide by Hadoop which is configured
 * to write to the store file.
 * 
 * There is a reporter running in a separate thread that keeps
 * reporting progress to let the job tracker know that we are 
 * alive. The sleep time for the reporter thread can be configured
 * per job via the "pig.reporter.sleep.time". By default it uses
 * a 10 sec sleep time
 */

public class PigMapOnly {

    public static class Map extends PigMapBase implements
            Mapper<Text, Tuple, PigNullableWritable, Writable> {

        @Override
        public void collect(OutputCollector<PigNullableWritable, Writable> oc, Tuple tuple) throws ExecException, IOException {
            oc.collect(null, tuple);
        }
    }
}

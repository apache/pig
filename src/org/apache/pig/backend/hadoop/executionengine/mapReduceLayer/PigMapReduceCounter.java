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

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigMapOnly.Map;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POCounter;
import org.apache.pig.data.Tuple;
import org.apache.pig.tools.pigstats.PigStatusReporter;

public class PigMapReduceCounter {

    /**
     * This class is the used only for simple RANK operation, namely row number mode.
     **/
    public static class PigMapCounter extends PigMapBase {

        private static final Log log = LogFactory.getLog(PigMapCounter.class);
        public static String taskID;
        public static Context context;
        private PhysicalOperator pOperator;

        /**
         * Here is set up the task id, in order to be attached to each tuple
         **/
        public void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);

            taskID = String.valueOf(context.getTaskAttemptID().getTaskID().getId());

            pOperator = mp.getLeaves().get(0);

            while(true) {
                if(pOperator instanceof POCounter){
                    ((POCounter) pOperator).setTaskId(taskID);
                    ((POCounter) pOperator).resetLocalCounter();
                    break;
                } else {
                    pOperator = mp.getPredecessors(pOperator).get(0);
                }
            }
        }

        /**
         * While tuples are collected, they are counted one by one by a global counter per task.
         **/
        @Override
        public void collect(Context context, Tuple tuple)
        throws InterruptedException, IOException {
            context.write(null, tuple);
            try {
                PigStatusReporter reporter = PigStatusReporter.getInstance();
                if (reporter != null) {
                    reporter.getCounter(
                            JobControlCompiler.PIG_MAP_RANK_NAME
                            + context.getJobID().toString(), taskID)
                            .increment(1);
                }
            } catch (Exception ex) {
                log.error("Error on incrementer of PigMapCounter");
            }
        }
    }

    /**
     * This class is the used for RANK BY operations, independently if it is dense or not.
     **/
    public static class PigReduceCounter extends PigMapReduce.Reduce {

        private static final Log log = LogFactory.getLog(PigReduceCounter.class);
        public static String taskID;
        public static Context context;
        public static List<PhysicalOperator> leaves;
        public static PhysicalOperator leaf;

        /**
         * Here is set up the task id, in order to be attached to each tuple
         **/
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);

            taskID = String.valueOf(context.getTaskAttemptID().getTaskID().getId());

            leaf = rp.getLeaves().get(0);

            while(true) {
                if(leaf instanceof POCounter){
                    ((POCounter) leaf).setTaskId(taskID);
                    ((POCounter) leaf).resetLocalCounter();
                    break;
                } else {
                    leaf = rp.getPredecessors(leaf).get(0);
                }
            }

            this.context = context;
        }

        /**
         * On this case, global counters are accessed during reduce phase (immediately after a
         * sorting phase) and the increment for global counters are dependent if it is dense rank
         * or not.
         * If it is a dense rank, increment is done by 1. if it is not increment depends on the size
         * of the size of bag in the tuple.
         * @param increment is the value to add to the corresponding global counter.
         **/
        public static void incrementCounter(Long increment) {
            try {
                PigStatusReporter reporter = PigStatusReporter.getInstance();
                if (reporter != null) {

                    if(leaf instanceof POCounter){
                        reporter.getCounter(
                                JobControlCompiler.PIG_MAP_RANK_NAME
                                + context.getJobID().toString(), taskID).increment(increment);

                    }

                }
            } catch (Exception ex) {
                log.error("Error on incrementer of PigReduceCounter");
            }

        }
    }
}

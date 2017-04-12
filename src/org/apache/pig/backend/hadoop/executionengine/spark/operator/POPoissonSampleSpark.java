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
package org.apache.pig.backend.hadoop.executionengine.spark.operator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POPoissonSample;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.builtin.PoissonSampleLoader;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;

public class POPoissonSampleSpark extends POPoissonSample {
    private static final Log LOG = LogFactory.getLog(POPoissonSampleSpark.class);
    //TODO verify can be removed?
    //private static final long serialVersionUID = 1L;
    // Only for Spark
    private transient boolean endOfInput = false;

    public boolean isEndOfInput() {
        return endOfInput;
    }

    public void setEndOfInput(boolean isEndOfInput) {
        endOfInput = isEndOfInput;
    }

    public POPoissonSampleSpark(OperatorKey k, int rp, int sr, float hp, long tm) {
        super(k, rp, sr, hp, tm);
    }

    @Override
    public Result getNextTuple() throws ExecException {
        if (!initialized) {
            numRowsSampled = 0;
            avgTupleMemSz = 0;
            rowNum = 0;
            skipInterval = -1;
            if (totalMemory == null) {
                // Initialize in backend to get memory of task
                totalMemory = Runtime.getRuntime().maxMemory();
            }
            long availRedMem = (long) (totalMemory * heapPerc);
            memToSkipPerSample = availRedMem/sampleRate;
            initialized = true;
        }
        if (numRowSplTupleReturned) {
            // row num special row has been returned after all inputs
            // were read, nothing more to read
            return RESULT_EOP;
        }

        Result res;
        res = processInput();

        // if reaches at the end, pick a record and return
        if (this.isEndOfInput()) {
            // if skip enough, and the last record is OK.
            if ( numSkipped == skipInterval
                    && res.returnStatus == POStatus.STATUS_OK) {
                return createNumRowTuple((Tuple) res.result);
            } else if (newSample != null) {
                return createNumRowTuple((Tuple) newSample.result);
            }
        }

        // just return to read next record from input
        if (res.returnStatus == POStatus.STATUS_NULL) {
            return new Result(POStatus.STATUS_NULL, null);
        } else if (res.returnStatus == POStatus.STATUS_EOP
                    || res.returnStatus == POStatus.STATUS_ERR) {
            return res;
        }

        // got a 'OK' record
        rowNum++;

        if (numSkipped < skipInterval) {
            numSkipped++;

            // skip this tuple, and continue to read from input
            return new Result(POStatus.STATUS_EOP, null);
        }

        // pick this record as sampled
        newSample = res;
        numSkipped = 0;
        Result pickedSample = newSample;
        updateSkipInterval((Tuple) pickedSample.result);

        if( LOG.isDebugEnabled()) {
            LOG.debug("pickedSample:");
            if (pickedSample.result != null) {
                for (int i = 0; i < ((Tuple) pickedSample.result).size(); i++) {
                    LOG.debug("the " + i + " ele:" + ((Tuple) pickedSample.result).get(i));
                }
            }
        }
        return pickedSample;
    }

    @Override
    public String name() {
        return getAliasString() + "PoissonSampleSpark - " + mKey.toString();
    }
}

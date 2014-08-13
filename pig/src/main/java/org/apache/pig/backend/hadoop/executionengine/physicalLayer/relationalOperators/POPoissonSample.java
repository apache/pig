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
package org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.builtin.PoissonSampleLoader;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;

public class POPoissonSample extends PhysicalOperator {

    private static final long serialVersionUID = 1L;

    private static final TupleFactory tf = TupleFactory.getInstance();
    private static Result eop = new Result(POStatus.STATUS_EOP, null);

    // num of rows sampled so far
    private int numRowsSampled = 0;

    // average size of tuple in memory, for tuples sampled
    private long avgTupleMemSz = 0;

    // current row number
    private long rowNum = 0;

    // number of tuples to skip after each sample
    private long skipInterval = -1;

    // bytes in input to skip after every sample.
    // divide this by avgTupleMemSize to get skipInterval
    private long memToSkipPerSample = 0;

    // has the special row with row number information been returned
    private boolean numRowSplTupleReturned = false;

    // 17 is not a magic number. It can be obtained by using a poisson
    // cumulative distribution function with the mean set to 10 (empirically,
    // minimum number of samples) and the confidence set to 95%
    public static final int DEFAULT_SAMPLE_RATE = 17;

    private int sampleRate = 0;

    private float heapPerc = 0f;

    // new Sample result
    private Result newSample = null;

    public POPoissonSample(OperatorKey k, int rp, int sr, float hp) {
        super(k, rp, null);
        numRowsSampled = 0;
        avgTupleMemSz = 0;
        rowNum = 0;
        skipInterval = -1;
        memToSkipPerSample = 0;
        numRowSplTupleReturned = false;
        newSample = null;
        sampleRate = sr;
        heapPerc = hp;
    }

    @Override
    public Tuple illustratorMarkup(Object in, Object out, int eqClassIndex) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void visit(PhyPlanVisitor v) throws VisitorException {
        v.visitPoissonSample(this);
    }

    @Override
    public Result getNextTuple() throws ExecException {
        if (numRowSplTupleReturned) {
            // row num special row has been returned after all inputs
            // were read, nothing more to read
            return eop;
        }

        Result res = null;
        if (skipInterval == -1) {
            // select first tuple as sample and calculate
            // number of tuples to be skipped
            while (true) {
                res = processInput();
                if (res.returnStatus == POStatus.STATUS_NULL) {
                    continue;
                } else if (res.returnStatus == POStatus.STATUS_EOP) {
                    if (this.parentPlan.endOfAllInput) {
                        return eop;
                    } else {
                        continue;
                    }
                } else if (res.returnStatus == POStatus.STATUS_ERR) {
                    return res;
                }

                if (res.result == null) {
                    continue;
                }
                long availRedMem = (long) (Runtime.getRuntime().maxMemory() * heapPerc);
                memToSkipPerSample = availRedMem/sampleRate;
                updateSkipInterval((Tuple)res.result);

                rowNum++;
                newSample = res;
                break;
            }
        }

        // skip tuples
        for (long numSkipped  = 0; numSkipped < skipInterval; numSkipped++) {
            res = processInput();
            if (res.returnStatus == POStatus.STATUS_NULL) {
                continue;
            } else if (res.returnStatus == POStatus.STATUS_EOP) {
                if (this.parentPlan.endOfAllInput) {
                    return createNumRowTuple((Tuple)newSample.result);
                } else {
                    return res;
                }
            } else if (res.returnStatus == POStatus.STATUS_ERR){
                return res;
            }
            rowNum++;
        }

        // skipped enough, get new sample
        while (true) {
            res = processInput();
            if (res.returnStatus == POStatus.STATUS_NULL) {
                continue;
            } else if (res.returnStatus == POStatus.STATUS_EOP) {
                if (this.parentPlan.endOfAllInput) {
                    return createNumRowTuple((Tuple)newSample.result);
                } else {
                    return res;
                }
            } else if (res.returnStatus == POStatus.STATUS_ERR){
                return res;
            }

            if (res.result == null) {
                continue;
            }
            updateSkipInterval((Tuple)res.result);
            Result currentSample = newSample;

            rowNum++;
            newSample = res;
            return currentSample;
        }
    }

    @Override
    public boolean supportsMultipleInputs() {
        return false;
    }

    @Override
    public boolean supportsMultipleOutputs() {
        return false;
    }

    @Override
    public String name() {
        return getAliasString() + "PoissonSample - " + mKey.toString();
    }

    /**
     * Update the average tuple size base on newly sampled tuple t
     * and recalculate skipInterval
     * @param t - tuple
     */
    private void updateSkipInterval(Tuple t) {
        avgTupleMemSz =
            ((avgTupleMemSz*numRowsSampled) + t.getMemorySize())/(numRowsSampled + 1);
        skipInterval = memToSkipPerSample/avgTupleMemSz;

        // skipping fewer number of rows the first few times, to reduce the
        // probability of first tuples size (if much smaller than rest)
        // resulting in very few samples being sampled. Sampling a little extra
        // is OK
        if(numRowsSampled < 5) {
            skipInterval = skipInterval/(10-numRowsSampled);
        }
        ++numRowsSampled;
    }

    /**
     * @param sample - sample tuple
     * @return - Tuple appended with special marker string column, num-rows column
     * @throws ExecException
     */
    private Result createNumRowTuple(Tuple sample) throws ExecException {
        int sz = (sample == null) ? 0 : sample.size();
        Tuple t = tf.newTuple(sz + 2);

        if (sample != null) {
            for (int i=0; i<sample.size(); i++){
                t.set(i, sample.get(i));
            }
        }

        t.set(sz, PoissonSampleLoader.NUMROWS_TUPLE_MARKER);
        t.set(sz + 1, rowNum);
        numRowSplTupleReturned = true;
        return new Result(POStatus.STATUS_OK, t);
    }
}

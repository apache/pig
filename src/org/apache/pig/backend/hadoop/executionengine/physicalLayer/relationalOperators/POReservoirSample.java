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

import java.util.List;

import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.builtin.PoissonSampleLoader;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.util.UDFContext;

public class POReservoirSample extends PhysicalOperator {

    private static final long serialVersionUID = 1L;

    // number of samples to be sampled
    protected long numSamples;

    private transient int nextSampleIdx = 0;

    private transient long rowProcessed = 0;

    private transient boolean sampleCollectionDone = false;

    //array to store the result
    private transient Result[] samples = null;

    // last sample result
    private transient Result lastSample = null;

    private transient RandomDataGenerator randGen;

    public POReservoirSample(OperatorKey k) {
        this(k, -1, null);
    }

    public POReservoirSample(OperatorKey k, int rp) {
        this(k, rp, null);
    }

    public POReservoirSample(OperatorKey k, List<PhysicalOperator> inp) {
        this(k, -1, inp);
    }

    public POReservoirSample(OperatorKey k, int rp, List<PhysicalOperator> inp) {
        super(k, rp, inp);
    }

    public POReservoirSample(OperatorKey k, int rp, List<PhysicalOperator> inp, long numSamples) {
        super(k, rp, inp);
        this.numSamples = numSamples;
    }

    @Override
    public Tuple illustratorMarkup(Object in, Object out, int eqClassIndex) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void visit(PhyPlanVisitor v) throws VisitorException {
        v.visitReservoirSample(this);
    }

    @Override
    public Result getNextTuple() throws ExecException {
        if (sampleCollectionDone){
            return getSample();
        }
        //else collect samples
        if (samples == null) {
            samples = new Result[(int)numSamples];
            long taskIdHashCode = UDFContext.getUDFContext().getJobConf().get(JobContext.TASK_ID).hashCode();
            long randomSeed = ((long)taskIdHashCode << 32) | (taskIdHashCode & 0xffffffffL);
            randGen = new RandomDataGenerator();
            randGen.reSeed(randomSeed);
        }

        // populate the samples array with first numSamples tuples
        Result res = null;
        while (rowProcessed < numSamples) {
            res = processInput();
            if (res.returnStatus == POStatus.STATUS_OK) {
                samples[(int)rowProcessed] = res;
                rowProcessed++;
            } else if (res.returnStatus == POStatus.STATUS_NULL) {
                continue;
            } else if (res.returnStatus == POStatus.STATUS_EOP) {
                if (this.parentPlan.endOfAllInput) {
                    break;
                } else {
                    // In case of Split can get EOP in between.
                    // Return here instead of setting lastSample to EOP in getSample
                    return res;
                }
            } else {
                break;
            }
        }

        if (res == null || res.returnStatus != POStatus.STATUS_EOP) {
            while (true) {
                // pick this as sample
                res = processInput();
                if (res.returnStatus == POStatus.STATUS_NULL) {
                    continue;
                } else if (res.returnStatus != POStatus.STATUS_OK) {
                    break;
                }

                // collect samples until input is exhausted
                long rand = randGen.nextLong(0, rowProcessed + 1);
                if (rand < numSamples) {
                    samples[(int)rand] = res;
                }
                rowProcessed++;
            }
        }

        if (res.returnStatus == POStatus.STATUS_EOP) {
            if (this.parentPlan.endOfAllInput) {
                sampleCollectionDone = true;
            } else {
                // In case of Split can get EOP in between.
                return res;
            }
        }

        return getSample();
    }

    private Result getSample() throws ExecException {
        if (lastSample == null) {
            lastSample = retrieveSample();
        }
        if (lastSample.returnStatus==POStatus.STATUS_EOP) {
            return lastSample;
        }

        Result currentSample = retrieveSample();
        // If this is the last sample, tag with number of rows
        if (currentSample.returnStatus == POStatus.STATUS_EOP) {
            lastSample = createNumRowTuple((Tuple)lastSample.result);
        } else if (currentSample.returnStatus == POStatus.STATUS_NULL) {
            return currentSample;
        }
        Result result = lastSample;
        lastSample = currentSample;
        return result;
    }

    private Result retrieveSample() throws ExecException {
        if(nextSampleIdx < Math.min(rowProcessed, samples.length)){
            if (illustrator != null) {
                illustratorMarkup(samples[nextSampleIdx].result, samples[nextSampleIdx].result, 0);
            }
            Result res = samples[nextSampleIdx];
            samples[nextSampleIdx++] = null; //Free memory
            if (res == null) { // Input data has lesser rows than numSamples
                return RESULT_EMPTY;
            }
            return res;
        }
        else{
            samples = null; // Free memory
            return RESULT_EOP;
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
        return getAliasString() + "ReservoirSample - " + mKey.toString();
    }

    /**
     * @param sample - sample tuple
     * @return - Tuple appended with special marker string column, num-rows column
     * @throws ExecException
     */
    private Result createNumRowTuple(Tuple sample) throws ExecException {
        int sz = (sample == null) ? 0 : sample.size();
        Tuple t = mTupleFactory.newTuple(sz + 2);

        if (sample != null) {
            for (int i=0; i<sample.size(); i++){
                t.set(i, sample.get(i));
            }
        }

        t.set(sz, PoissonSampleLoader.NUMROWS_TUPLE_MARKER);
        t.set(sz + 1, rowProcessed);
        return new Result(POStatus.STATUS_OK, t);
    }

}

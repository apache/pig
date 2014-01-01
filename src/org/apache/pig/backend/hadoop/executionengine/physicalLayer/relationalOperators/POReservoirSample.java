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
import java.util.Random;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;

public class POReservoirSample extends PhysicalOperator {

    private static final long serialVersionUID = 1L;

    // number of samples to be sampled
    protected int numSamples;

    private transient int nextSampleIdx= 0;

    //array to store the result
    private transient Result[] samples = null;

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

    public POReservoirSample(OperatorKey k, int rp, List<PhysicalOperator> inp, int numSamples) {
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
    }

    @Override
    public Result getNextTuple() throws ExecException {
        if(samples != null){
            return getSample();
        }
        //else collect samples
        samples = new Result[numSamples];

        // populate the samples array with first numSamples tuples
        Result res = null;
        int rowProcessed = 0;
        while (rowProcessed<numSamples) {
            res = processInput();
            if(res.returnStatus == POStatus.STATUS_OK) {
                samples[rowProcessed] = res;
                rowProcessed++;
            } else if (res.returnStatus == POStatus.STATUS_NULL) {
                continue;
            }
            else {
                break;
            }
        }

        int rowNum = numSamples+1;
        Random randGen = new Random();

        if(res.returnStatus == POStatus.STATUS_OK){ // did not exhaust all tuples
            while(true){
                // pick this as sample
                Result sampleResult = processInput();
                if(sampleResult.returnStatus == POStatus.STATUS_NULL) {
                    continue;
                } else if (sampleResult.returnStatus != POStatus.STATUS_OK) {
                    break;
                }

                // collect samples until input is exhausted
                int rand = randGen.nextInt(rowNum);
                if(rand < numSamples){
                    samples[rand] = sampleResult;
                }
                rowNum++;
            }
        }

        return getSample();
    }

    private Result getSample() throws ExecException {
        if(nextSampleIdx < samples.length){
            if (illustrator != null) {
                illustratorMarkup(samples[nextSampleIdx].result, samples[nextSampleIdx].result, 0);
            }
            return samples[nextSampleIdx++];
        }
        else{
            Result res = new Result();
            res.returnStatus = POStatus.STATUS_EOP;
            return res;
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
}

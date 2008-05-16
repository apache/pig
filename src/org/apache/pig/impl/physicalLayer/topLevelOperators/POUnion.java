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
package org.apache.pig.impl.physicalLayer.topLevelOperators;

import java.util.BitSet;
import java.util.List;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.OperatorKey;
import org.apache.pig.impl.physicalLayer.POStatus;
import org.apache.pig.impl.physicalLayer.Result;
import org.apache.pig.impl.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.impl.plan.VisitorException;

/**
 * The union operator that combines the two inputs into a single
 * stream. Note that this doesn't eliminate duplicate tuples.
 * The Operator will also be added to every map plan which processes
 * more than one input. This just pulls out data from the piepline
 * using the proposed single threaded shared execution model. By shared
 * execution I mean, one input to the Union operator is called
 * once and the execution moves to the next non-drained input till
 * all the inputs are drained.
 *
 */
public class POUnion extends PhysicalOperator<PhyPlanVisitor> {
    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    //Used for efficiently shifting between non-drained
    //inputs
    BitSet done;
    
    //The index of the last input that was read
    int lastInd = 0;

    public POUnion(OperatorKey k) {
        this(k, -1, null);
    }

    public POUnion(OperatorKey k, int rp) {
        this(k, rp, null);
    }

    public POUnion(OperatorKey k, List<PhysicalOperator> inp) {
        this(k, -1, inp);
    }

    public POUnion(OperatorKey k, int rp, List<PhysicalOperator> inp) {
        super(k, rp, inp);
    }

    @Override
    public void setInputs(List<PhysicalOperator> inputs) {
        super.setInputs(inputs);
        done = new BitSet(inputs.size());
    }

    @Override
    public void visit(PhyPlanVisitor v) throws VisitorException {
        v.visitUnion(this);
    }

    @Override
    public String name() {
        return "Union - " + mKey.toString();
    }

    @Override
    public boolean supportsMultipleInputs() {
        return true;
    }

    @Override
    public boolean supportsMultipleOutputs() {
        return false;
    }

    public void clearDone() {
        done.clear();
    }

    /**
     * The code below, tries to follow our single threaded 
     * shared execution model with execution being passed
     * around each non-drained input
     */
    @Override
    public Result getNext(Tuple t) throws ExecException {
        while(true){
            if (done.nextClearBit(0) >= inputs.size()) {
                res = new Result();
                res.returnStatus = POStatus.STATUS_EOP;
                clearDone();
                return res;
            }
            if(lastInd >= inputs.size() || done.nextClearBit(lastInd) >= inputs.size())
                lastInd = 0;
            int ind = done.nextClearBit(lastInd);
            Result res;
            
            while(true){
                if(reporter!=null) reporter.progress();
                res = inputs.get(ind).getNext(t);
                if(res.returnStatus == POStatus.STATUS_NULL)
                    continue;
                
                lastInd = ind + 1;
                
                if(res.returnStatus == POStatus.STATUS_ERR)
                    return new Result();
                
                if (res.returnStatus == POStatus.STATUS_OK)
                    return res;
                
                if (res.returnStatus == POStatus.STATUS_EOP) {
                    done.set(ind);
                    break;
                }
            }
        }
    }
}

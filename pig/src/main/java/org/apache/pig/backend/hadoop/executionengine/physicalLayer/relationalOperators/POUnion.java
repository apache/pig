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

import java.util.BitSet;
import java.util.LinkedList;
import java.util.List;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.util.IdentityHashSet;
import org.apache.pig.pen.util.ExampleTuple;

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
public class POUnion extends PhysicalOperator {
    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    //Used for efficiently shifting between non-drained
    //inputs
    BitSet done;

    boolean nextReturnEOP = false ;
    private static Result eopResult = new Result(POStatus.STATUS_EOP, null) ;
    
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
        if (inputs != null) {
            done = new BitSet(inputs.size());
        }
        else {
            done = new BitSet(0) ;
        }
    }

    @Override
    public void visit(PhyPlanVisitor v) throws VisitorException {
        v.visitUnion(this);
    }

    @Override
    public String name() {
        return getAliasString() + "Union" + "[" + DataType.findTypeName(resultType)
                + "]" + " - " + mKey.toString();
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
    public Result getNextTuple() throws ExecException {

        if (nextReturnEOP) {
            nextReturnEOP = false ;
            return eopResult ;
        }

        // Case 1 : Normal connected plan
        if (!isInputAttached()) {
            
            if (inputs == null || inputs.size()==0) {
                // Neither does this Union have predecessors nor
                // was any input attached! This can happen when we have
                // a plan like below
                // POUnion
                // |
                // |--POLocalRearrange
                // |    |
                // |    |-POUnion (root 2)--> This union's getNext() can lead the code here
                // |
                // |--POLocalRearrange (root 1)
                
                // The inner POUnion above is a root in the plan which has 2 roots.
                // So these 2 roots would have input coming from different input
                // sources (dfs files). So certain maps would be working on input only
                // meant for "root 1" above and some maps would work on input
                // meant only for "root 2". In the former case, "root 2" would
                // neither get input attached to it nor does it have predecessors
                // which is the case which can lead us here.
                return eopResult;
            }
          
            while(true){
                if (done.nextClearBit(0) >= inputs.size()) {
                    clearDone();
                    return eopResult ;
                }
                if(lastInd >= inputs.size() || done.nextClearBit(lastInd) >= inputs.size())
                    lastInd = 0;
                int ind = done.nextClearBit(lastInd);
                Result res;

                while(true){
                    if(getReporter()!=null) {
                        getReporter().progress();
                    }
                    res = inputs.get(ind).getNextTuple();
                    lastInd = ind + 1;

                    if(res.returnStatus == POStatus.STATUS_OK || 
                            res.returnStatus == POStatus.STATUS_NULL || res.returnStatus == POStatus.STATUS_ERR) {
                        illustratorMarkup(res.result, res.result, ind);
                        return res;
                    }

                    if (res.returnStatus == POStatus.STATUS_EOP) {
                        done.set(ind);
                        break;
                    }
                }
            }
        }
        // Case 2 : Input directly injected
        else {
            res.result = input;
            res.returnStatus = POStatus.STATUS_OK;
            detachInput();
            nextReturnEOP = true ;
            illustratorMarkup(res.result, res.result, 0);
            return res;
        }


    }
    
    @Override
    public Tuple illustratorMarkup(Object in, Object out, int eqClassIndex) {
        if(illustrator != null) {
            if (illustrator.getEquivalenceClasses() == null) {
                int size = (inputs == null ? 1 : inputs.size());
                LinkedList<IdentityHashSet<Tuple>> equivalenceClasses = new LinkedList<IdentityHashSet<Tuple>>();
                for (int i = 0; i < size; ++i) {
                    IdentityHashSet<Tuple> equivalenceClass = new IdentityHashSet<Tuple>();
                    equivalenceClasses.add(equivalenceClass);
                }
                illustrator.setEquivalenceClasses(equivalenceClasses, this);
            }
            ExampleTuple tIn = (ExampleTuple) in;
            illustrator.getEquivalenceClasses().get(eqClassIndex).add(tIn);
            illustrator.addData((Tuple) out);
        }
        return null;
    }
}

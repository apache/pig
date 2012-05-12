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

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.plan.NodeIdGenerator;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.pen.util.ExampleTuple;

public class POLimit extends PhysicalOperator {
	   /**
     * 
     */
    private static final long serialVersionUID = 1L;

    // Counts for outputs processed
    private long soFar = 0;
    
    // Number of limited outputs
    long mLimit;

    // The expression plan
    PhysicalPlan expressionPlan;

    public POLimit(OperatorKey k) {
        this(k, -1, null);
    }

    public POLimit(OperatorKey k, int rp) {
        this(k, rp, null);
    }

    public POLimit(OperatorKey k, List<PhysicalOperator> inputs) {
        this(k, -1, inputs);
    }

    public POLimit(OperatorKey k, int rp, List<PhysicalOperator> inputs) {
        super(k, rp, inputs);
    }
    
    public void setLimit(long limit) {
    	mLimit = limit;
    }
    
    public long getLimit() {
    	return mLimit;
    }

    public PhysicalPlan getLimitPlan() {
        return expressionPlan;
    }

    public void setLimitPlan(PhysicalPlan expressionPlan) {
        this.expressionPlan = expressionPlan;
    }

    /**
     * Counts the number of tuples processed into static variable soFar, if the number of tuples processed reach the 
     * limit, return EOP; Otherwise, return the tuple 
     */
    @Override
    public Result getNext(Tuple t) throws ExecException {
        // if it is the first time, evaluate the expression. Otherwise reuse the computed value.
        if (this.getLimit() < 0 && expressionPlan != null) {
            PhysicalOperator expression = expressionPlan.getLeaves().get(0);
            long variableLimit;
            Result returnValue;
            switch (expression.getResultType()) {
            case DataType.LONG:
                returnValue = expression.getNext(dummyLong);
                if (returnValue.returnStatus != POStatus.STATUS_OK || returnValue.result == null)
                    throw new RuntimeException("Unable to evaluate Limit expression: "
                            + returnValue);
                variableLimit = (Long) returnValue.result;
                break;
            case DataType.INTEGER:
                returnValue = expression.getNext(dummyInt);
                if (returnValue.returnStatus != POStatus.STATUS_OK || returnValue.result == null)
                    throw new RuntimeException("Unable to evaluate Limit expression: "
                            + returnValue);
                variableLimit = (Integer) returnValue.result;
                break;
            default:
                throw new RuntimeException("Limit requires an integer parameter");
            }
            if (variableLimit <= 0)
                throw new RuntimeException("Limit requires a positive integer parameter");
            this.setLimit(variableLimit);
        }
        Result inp = null;
        while (true) {
            inp = processInput();
            if (inp.returnStatus == POStatus.STATUS_EOP || inp.returnStatus == POStatus.STATUS_ERR)
                break;
            
            illustratorMarkup(inp.result, null, 0);
            // illustrator ignore LIMIT before the post processing
            if ((illustrator == null || illustrator.getOriginalLimit() != -1) && soFar>=mLimit)
            	inp.returnStatus = POStatus.STATUS_EOP;
            
            soFar++;
            break;
        }

        return inp;
    }

    @Override
    public String name() {
        return getAliasString() + "Limit - " + mKey.toString();
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
    public void visit(PhyPlanVisitor v) throws VisitorException {
        v.visitLimit(this);
    }

    @Override
    public void reset() {
        soFar = 0;
    }

    @Override
    public POLimit clone() throws CloneNotSupportedException {
        POLimit newLimit = new POLimit(new OperatorKey(this.mKey.scope,
            NodeIdGenerator.getGenerator().getNextNodeId(this.mKey.scope)),
            this.requestedParallelism, this.inputs);
        newLimit.mLimit = this.mLimit;
        newLimit.expressionPlan = this.expressionPlan.clone();
        newLimit.addOriginalLocation(alias, getOriginalLocations());
        return newLimit;
    }
    
    @Override
    public Tuple illustratorMarkup(Object in, Object out, int eqClassIndex) {
        if(illustrator != null) {
            ExampleTuple tIn = (ExampleTuple) in;
            illustrator.getEquivalenceClasses().get(eqClassIndex).add(tIn);
            illustrator.addData((Tuple) in);
        }
        return (Tuple) in;
    }
}

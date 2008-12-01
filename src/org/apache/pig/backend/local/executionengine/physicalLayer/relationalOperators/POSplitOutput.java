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
package org.apache.pig.backend.local.executionengine.physicalLayer.relationalOperators;

import java.util.Iterator;
import java.util.List;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.pen.util.ExampleTuple;

public class POSplitOutput extends PhysicalOperator {

    /**
     * POSplitOutput reads from POSplit using an iterator
     */
    private static final long serialVersionUID = 1L;
    
    PhysicalOperator compOp;
    PhysicalPlan compPlan;
    Iterator<Tuple> it;
    
    public POSplitOutput(OperatorKey k, int rp, List<PhysicalOperator> inp) {
	super(k, rp, inp);
	// TODO Auto-generated constructor stub
    }

    public POSplitOutput(OperatorKey k, int rp) {
	super(k, rp);
	// TODO Auto-generated constructor stub
    }

    public POSplitOutput(OperatorKey k, List<PhysicalOperator> inp) {
	super(k, inp);
	// TODO Auto-generated constructor stub
    }

    public POSplitOutput(OperatorKey k) {
	super(k);
	// TODO Auto-generated constructor stub
    }

    @Override
    public void visit(PhyPlanVisitor v) throws VisitorException {
	// TODO Auto-generated method stub

    }
    
    public Result getNext(Tuple t) throws ExecException {
	if(it == null) {
	    PhysicalOperator op = getInputs().get(0);
	    Result res = getInputs().get(0).getNext(t);
	    if(res.returnStatus == POStatus.STATUS_OK)
		it = (Iterator<Tuple>) res.result;
	}
	Result res = null;
	Result inp = new Result();
	while(true) {
	    if(it.hasNext())
		inp.result = it.next();
	    else {
		inp.returnStatus = POStatus.STATUS_EOP;
		return inp;
	    }
	    inp.returnStatus = POStatus.STATUS_OK;

	    compPlan.attachInput((Tuple) inp.result);

	    res = compOp.getNext(dummyBool);
	    if (res.returnStatus != POStatus.STATUS_OK 
		    && res.returnStatus != POStatus.STATUS_NULL) 
		return res;

	    if (res.result != null && (Boolean) res.result == true) {
		if(lineageTracer != null) {
		    ExampleTuple tIn = (ExampleTuple) inp.result;
		    lineageTracer.insert(tIn);
		    lineageTracer.union(tIn, tIn);
		}
		return inp;
	    }
	}
        
    }

    @Override
    public String name() {
	// TODO Auto-generated method stub
	return "POSplitOutput " + mKey.toString();
    }

    @Override
    public boolean supportsMultipleInputs() {
	// TODO Auto-generated method stub
	return false;
    }

    @Override
    public boolean supportsMultipleOutputs() {
	// TODO Auto-generated method stub
	return false;
    }
    
    public void setPlan(PhysicalPlan compPlan) {
	this.compPlan = compPlan;
	this.compOp = compPlan.getLeaves().get(0);
    }

}

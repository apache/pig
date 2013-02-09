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

import java.util.ArrayList;
import java.util.List;

import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.util.MultiMap;

/**
 * The PhysicalOperator that represents a skewed join. It must have two inputs.
 * This operator does not do any actually work, it is only a place holder. When it is
 * translated into MR plan, a POSkewedJoin is translated into a sampling job and a join 
 * job.
 * 
 *
 */
public class POSkewedJoin extends PhysicalOperator  {

	
	private static final long serialVersionUID = 1L;
	private boolean[] mInnerFlags;
	
	// The schema is used only by the MRCompiler to support outer join
	transient private List<Schema> inputSchema = new ArrayList<Schema>();
	
	// physical plans to retrive join keys
	// the key of this <code>MultiMap</code> is the PhysicalOperator that corresponds to an input
	// the value is a list of <code>PhysicalPlan</code> to retrieve each join key for this input
    private MultiMap<PhysicalOperator, PhysicalPlan> mJoinPlans;

    public POSkewedJoin(OperatorKey k)  {
        this(k,-1,null, null);
    }

    public POSkewedJoin(OperatorKey k, int rp) {
        this(k, rp, null, null);
    }

    public POSkewedJoin(OperatorKey k, List<PhysicalOperator> inp, boolean []flags) {
        this(k, -1, inp, flags);
    }

    public POSkewedJoin(OperatorKey k, int rp, List<PhysicalOperator> inp, boolean []flags) {
        super(k,rp,inp);
        if (flags != null) {
        	// copy the inner flags
        	mInnerFlags = new boolean[flags.length];
        	for (int i = 0; i < flags.length; i++) {
        		mInnerFlags[i] = flags[i];
        	}
        }
    }
    
    public boolean[] getInnerFlags() {
    	return mInnerFlags;
    }
    
    public MultiMap<PhysicalOperator, PhysicalPlan> getJoinPlans() {
    	return mJoinPlans;
    }
    
    public void setJoinPlans(MultiMap<PhysicalOperator, PhysicalPlan> joinPlans) {
        mJoinPlans = joinPlans;
    }    
    
	@Override
	public void visit(PhyPlanVisitor v) throws VisitorException {
		v.visitSkewedJoin(this);
	}

    @Override
    public String name() {
        return getAliasString() + "SkewedJoin["
                + DataType.findTypeName(resultType) + "]" + " - "
                + mKey.toString();
    }

	@Override
	public boolean supportsMultipleInputs() {		
		return true;
	}

	@Override
	public boolean supportsMultipleOutputs() {	
		return false;
	}
	
	public void addSchema(Schema s) {
		inputSchema.add(s);
	}
	
	public Schema getSchema(int i) {
		return inputSchema.get(i);
	}

	@Override
  public Tuple illustratorMarkup(Object in, Object out, int eqClassIndex) {
	    return null;
	}
}

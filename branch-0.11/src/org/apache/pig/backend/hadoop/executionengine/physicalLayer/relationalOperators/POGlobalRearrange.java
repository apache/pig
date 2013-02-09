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
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.impl.plan.VisitorException;

/**
 * Dummy operator to test MRCompiler.
 * This will be a local operator and its
 * getNext methods have to be implemented 
 *
 */

//We intentionally skip type checking in backend for performance reasons
@SuppressWarnings("unchecked")
public class POGlobalRearrange extends PhysicalOperator {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    
    /* As, GlobalRearrange decides the map reduce boundary, we add custom
     * partitioner here
     */
    protected String customPartitioner;

    public String getCustomPartitioner() {
		return customPartitioner;
	}

	public void setCustomPartitioner(String customPartitioner) {
		this.customPartitioner = customPartitioner;
	}

    public POGlobalRearrange(OperatorKey k) {
        this(k, -1, null);
    }

    public POGlobalRearrange(OperatorKey k, int rp) {
        this(k, rp, null);
    }
    
    public POGlobalRearrange(OperatorKey k, List inp) {
        this(k, -1, null);
    }

    public POGlobalRearrange(OperatorKey k, int rp, List inp) {
        super(k, rp, inp);
    }
    
    @Override
    public void visit(PhyPlanVisitor v) throws VisitorException {
        v.visitGlobalRearrange(this);
    }

    @Override
    public String name() {
        return getAliasString() + "Global Rearrange" + "["
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

    @Override
    public boolean isBlocking() {
        return true;
    }

    @Override
    public Result getNext(Tuple t) throws ExecException {
        // TODO Auto-generated method stub
        return super.getNext(t);
    }
    
    @Override
    public Tuple illustratorMarkup(Object in, Object out, int eqClassIndex) {
      return null;
    }
}

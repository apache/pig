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
package org.apache.pig.impl.logicalLayer;

import java.util.ArrayList;
import java.util.List;

import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.parser.QueryParser;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLimit;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.ProjectionMap;
import org.apache.pig.impl.plan.RequiredFields;
import org.apache.pig.impl.plan.VisitorException;

public class LOLimit extends LogicalOperator {
    private static final long serialVersionUID = 2L;
    private long mLimit;
    /**
     * 
     * @param plan
     *            Logical plan this operator is a part of.
     * @param k
     *            Operator key to assign to this node.
     * @param limit
     *            Number of limited outputs
     */

    public LOLimit(LogicalPlan plan, OperatorKey k, long limit) {
        super(plan, k);
        mLimit = limit;
    }

    public LogicalOperator getInput() {
        return mPlan.getPredecessors(this).get(0);
    }

    public long getLimit() {
        return mLimit;
    }

    public void setLimit(long limit) {
    	mLimit = limit;
    }
    @Override
    public Schema getSchema() throws FrontendException {
        if (!mIsSchemaComputed) {
            try {
                mSchema = getInput().getSchema();
                mIsSchemaComputed = true;
            } catch (FrontendException ioe) {
                mSchema = null;
                mIsSchemaComputed = false;
                throw ioe;
            }
        }
        return mSchema;
    }

    @Override
    public String name() {
        return "Limit (" + mLimit + ") " + mKey.scope + "-" + mKey.id;
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
    public void visit(LOVisitor v) throws VisitorException {
        v.visit(this);
    }

    @Override
    public byte getType() {
        return DataType.BAG ;
    }
    
    // Shouldn't this be clone?
    public LOLimit duplicate()
    {
    	return new LOLimit(mPlan, OperatorKey.genOpKey(mKey.scope), mLimit);
    }

    /**
     * @see org.apache.pig.impl.plan.Operator#clone()
     * Do not use the clone method directly. Operators are cloned when logical plans
     * are cloned using {@link LogicalPlanCloner}
     */
    @Override
    protected Object clone() throws CloneNotSupportedException {
        LOLimit limitClone = (LOLimit)super.clone();
        return limitClone;
    }
    
    @Override
    public ProjectionMap getProjectionMap() {
        Schema outputSchema;
        try {
            outputSchema = getSchema();
        } catch (FrontendException fee) {
            return null;
        }
        
        if(outputSchema == null) {
            return null;
        }
        
        Schema inputSchema = null;        
        
        List<LogicalOperator> predecessors = (ArrayList<LogicalOperator>)mPlan.getPredecessors(this);
        if(predecessors != null) {
            try {
                inputSchema = predecessors.get(0).getSchema();
            } catch (FrontendException fee) {
                return null;
            }
        } else {
            return null;
        }
        
        if(inputSchema == null) {
            return null;
        }
        
        if(Schema.equals(inputSchema, outputSchema, false, true)) {
            //there is a one is to one mapping between input and output schemas
            return new ProjectionMap(false);
        } else {
            //problem - input and output schemas for a distinct have to match!
            return null;
        }
    }
    
    @Override
    public List<RequiredFields> getRequiredFields() {
        List<RequiredFields> requiredFields = new ArrayList<RequiredFields>();
        requiredFields.add(new RequiredFields(false, true));
        return requiredFields;
    }

}

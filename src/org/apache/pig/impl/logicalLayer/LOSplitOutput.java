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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.pig.PigException;
import org.apache.pig.impl.logicalLayer.LogicalOperator;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.ProjectionMap;
import org.apache.pig.impl.plan.RequiredFields;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.util.Pair;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.optimizer.SchemaRemover;


public class LOSplitOutput extends LogicalOperator {
    private static final long serialVersionUID = 2L;

    protected int mIndex;
    private LogicalPlan mCondPlan;
    
    /**
     * @param plan
     *            LogicalPlan this operator is a part of.
     * @param key
     *            OperatorKey for this operator
     * @param index
     *            index of this output in the split
     * @param condPlan
     *            logical plan containing the condition for this split output
     */
    public LOSplitOutput(
            LogicalPlan plan,
            OperatorKey key,
            int index,
            LogicalPlan condPlan) {
        super(plan, key);
        this.mIndex = index;
        this.mCondPlan = condPlan;
    }

    public LogicalPlan getConditionPlan() {
        return mCondPlan;
    }
    
    @Override
    public String name() {
        return "SplitOutput[" + getAlias() + "] " + mKey.scope + "-" + mKey.id;
    }

    @Override
    public Schema getSchema() throws FrontendException{
        if (!mIsSchemaComputed) {
            // get our parent's schema
            try {
                LogicalOperator input = mPlan.getPredecessors(this).get(0);
                if (null == input) {
                    int errCode = 1006;
                    String msg = "Could not find operator in plan";
                    throw new FrontendException(msg, errCode, PigException.INPUT, false, null);
                }
                mSchema = input.getSchema();
                mIsSchemaComputed = true;
            } catch (FrontendException fe) {
                mSchema = null;
                mIsSchemaComputed = false;
                throw fe;
            }
        }
        return mSchema;
    }

    public void visit(LOVisitor v) throws VisitorException{
        v.visit(this);
    }

    @Override
    public boolean supportsMultipleInputs() {
        return false;
    }

    public int getReadFrom() {
        return mIndex;
    }

    public byte getType() {
        return DataType.BAG ;
    }

    public void unsetSchema() throws VisitorException{
        SchemaRemover sr = new SchemaRemover(mCondPlan);
        sr.visit();
        super.unsetSchema();
    }

    /**
     * @see org.apache.pig.impl.plan.Operator#clone()
     * Do not use the clone method directly. Operators are cloned when logical plans
     * are cloned using {@link LogicalPlanCloner}
     */
    @Override
    protected Object clone() throws CloneNotSupportedException {
        LOSplitOutput splitOutputClone = (LOSplitOutput)super.clone();
        LogicalPlanCloneHelper lpCloner = new LogicalPlanCloneHelper(mCondPlan);
        splitOutputClone.mCondPlan = lpCloner.getClonedPlan();
        return splitOutputClone;
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
            //problem - input and output schemas for a split output have to match!
            return null;
        }
    }

    @Override
    public List<RequiredFields> getRequiredFields() {
        List<RequiredFields> requiredFields = new ArrayList<RequiredFields>();
        Set<Pair<Integer, Integer>> fields = new HashSet<Pair<Integer, Integer>>();
        TopLevelProjectFinder projectFinder = new TopLevelProjectFinder(
                mCondPlan);
        try {
            projectFinder.visit();
        } catch (VisitorException ve) {
            requiredFields.clear();
            requiredFields.add(null);
            return requiredFields;
        }
        Set<LOProject> projectStarSet = projectFinder.getProjectStarSet();

        if (projectStarSet != null) {
            requiredFields.add(new RequiredFields(true));
            return requiredFields;
        } else {
            for (LOProject project : projectFinder.getProjectSet()) {
                for (int inputColumn : project.getProjection()) {
                    fields.add(new Pair<Integer, Integer>(0,
                            inputColumn));
                }
            }
            if(fields.size() == 0) {
                requiredFields.add(new RequiredFields(false, true));
            } else {                
                requiredFields.add(new RequiredFields(new ArrayList<Pair<Integer, Integer>>(fields)));
            }
            return (requiredFields.size() == 0? null: requiredFields);
        }
    }

}

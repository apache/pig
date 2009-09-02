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
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Iterator;
import java.util.Set;

import org.apache.pig.FuncSpec;
import org.apache.pig.PigException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.plan.Operator;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.plan.ProjectionMap;
import org.apache.pig.impl.plan.RequiredFields;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.plan.PlanVisitor;
import org.apache.pig.impl.util.Pair;
import org.apache.pig.data.DataType;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class LOSort extends RelationalOperator {
    private static final long serialVersionUID = 2L;

    private List<Boolean> mAscCols;
    private FuncSpec mSortFunc;
    private boolean mIsStar = false;
    private long limit;
    private List<LogicalPlan> mSortColPlans;
    private static Log log = LogFactory.getLog(LOSort.class);

    /**
     * @param plan
     *            LogicalPlan this operator is a part of.
     * @param key
     *            OperatorKey for this operator
     * @param sortColPlans
     *            Array of column numbers that will be used for sorting data.
     * @param ascCols
     *            Array of booleans. Should be same size as sortCols. True
     *            indicates sort ascending (default), false sort descending. If
     *            this array is null, then all columns will be sorted ascending.
     * @param sortFunc
     *            the user defined sorting function
     */
    public LOSort(
            LogicalPlan plan,
            OperatorKey key,
            List<LogicalPlan> sortColPlans,
            List<Boolean> ascCols,
            FuncSpec sortFunc) {
        super(plan, key);
        mSortColPlans = sortColPlans;
        mAscCols = ascCols;
        mSortFunc = sortFunc;
        limit = -1;
    }

    public LogicalOperator getInput() {
        return mPlan.getPredecessors(this).get(0);
    }
    
    public List<LogicalPlan> getSortColPlans() {
        return mSortColPlans;
    }

    public void setSortColPlans(List<LogicalPlan> sortPlans) {
        mSortColPlans = sortPlans;
    }

    public List<Boolean> getAscendingCols() {
        return mAscCols;
    }

    public void setAscendingCols(List<Boolean> ascCols) {
        mAscCols = ascCols;
    }

    public FuncSpec getUserFunc() {
        return mSortFunc;
    }

    public void setUserFunc(FuncSpec func) {
        mSortFunc = func;
    }

    public boolean isStar() {
        return mIsStar;
    }

    public void setStar(boolean b) {
        mIsStar = b;
    }

    public void setLimit(long l)
    {
    	limit = l;
    }
    
    public long getLimit()
    {
    	return limit;
    }
    
    public boolean isLimited()
    {
    	return (limit!=-1);
    }

    @Override
    public String name() {
        return "SORT " + mKey.scope + "-" + mKey.id;
    }

    @Override
    public Schema getSchema() throws FrontendException {
        if (!mIsSchemaComputed) {
            // get our parent's schema
            Collection<LogicalOperator> s = mPlan.getPredecessors(this);
            ArrayList<Schema.FieldSchema> fss = new ArrayList<Schema.FieldSchema>();
            try {
                LogicalOperator op = s.iterator().next();
                if (null == op) {
                    int errCode = 1006;
                    String msg = "Could not find operator in plan";                    
                    throw new FrontendException(msg, errCode, PigException.INPUT, false, null);
                }
                if(op instanceof ExpressionOperator) {
                    Schema.FieldSchema fs = new Schema.FieldSchema(((ExpressionOperator)op).getFieldSchema());
                    if(DataType.isSchemaType(fs.type)) {
                        mSchema = fs.schema;
                    } else {
                        fss.add(fs);
                        mSchema = new Schema(fss);
                    }
                } else {
                    mSchema = op.getSchema();
                }
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
    public boolean supportsMultipleInputs() {
        return false;
    }

    public void visit(LOVisitor v) throws VisitorException {
        v.visit(this);
    }

    public byte getType() {
        return DataType.BAG ;
    }

    /**
     * @see org.apache.pig.impl.logicalLayer.LogicalOperator#clone()
     * Do not use the clone method directly. Operators are cloned when logical plans
     * are cloned using {@link LogicalPlanCloner}
     */
    @Override
    protected Object clone() throws CloneNotSupportedException {
        LOSort clone = (LOSort) super.clone();
        
        // deep copy sort related attributes
        if(mAscCols != null) {
            clone.mAscCols = new ArrayList<Boolean>();
            for (Iterator<Boolean> it = mAscCols.iterator(); it.hasNext();) {
                clone.mAscCols.add(new Boolean(it.next()));
            }
        }
        
        if(mSortFunc != null)
            clone.mSortFunc = mSortFunc.clone();
        
        if(mSortColPlans != null) {
            clone.mSortColPlans = new ArrayList<LogicalPlan>();
            for (Iterator<LogicalPlan> it = mSortColPlans.iterator(); it.hasNext();) {
                LogicalPlanCloneHelper lpCloneHelper = new LogicalPlanCloneHelper(it.next());
                clone.mSortColPlans.add(lpCloneHelper.getClonedPlan());            
            }
        }
        return clone;
    }
    
    @Override
    public ProjectionMap getProjectionMap() {
        if(mIsProjectionMapComputed) return mProjectionMap;
        mIsProjectionMapComputed = true;

        Schema outputSchema;
        try {
            outputSchema = getSchema();
        } catch (FrontendException fee) {
            mProjectionMap = null;
            return mProjectionMap;
        }

        Schema inputSchema = null;        
        
        List<LogicalOperator> predecessors = (ArrayList<LogicalOperator>)mPlan.getPredecessors(this);
        if(predecessors != null) {
            try {
                inputSchema = predecessors.get(0).getSchema();
            } catch (FrontendException fee) {
                mProjectionMap = null;
                return mProjectionMap;
            }
        } else {
            mProjectionMap = null;
            return mProjectionMap;
        }

        if(Schema.equals(inputSchema, outputSchema, false, true)) {
            //there is a one is to one mapping between input and output schemas
            mProjectionMap = new ProjectionMap(false);
            return mProjectionMap;
        } else {
            //problem - input and output schemas for a sort have to match!
            mProjectionMap = null;
            return mProjectionMap;
        }
    }
    
    @Override
    public List<RequiredFields> getRequiredFields() {
        List<RequiredFields> requiredFields = new ArrayList<RequiredFields>();
        Set<Pair<Integer, Integer>> fields = new HashSet<Pair<Integer, Integer>>();
        Set<LOProject> projectSet = new HashSet<LOProject>();
        boolean orderByStar = false;

        for (LogicalPlan plan : getSortColPlans()) {
            TopLevelProjectFinder projectFinder = new TopLevelProjectFinder(
                    plan);
            try {
                projectFinder.visit();
            } catch (VisitorException ve) {
                requiredFields.clear();
                requiredFields.add(null);
                return requiredFields;
            }
            projectSet.addAll(projectFinder.getProjectSet());
            if(projectFinder.getProjectStarSet() != null) {
                orderByStar = true;
            }
        }

        if(orderByStar) {
            requiredFields.add(new RequiredFields(true));
            return requiredFields;
        } else {
            for (LOProject project : projectSet) {
                for (int inputColumn : project.getProjection()) {
                    fields.add(new Pair<Integer, Integer>(0, inputColumn));
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

    /* (non-Javadoc)
     * @see org.apache.pig.impl.plan.Operator#rewire(org.apache.pig.impl.plan.Operator, org.apache.pig.impl.plan.Operator)
     */
    @Override
    public void rewire(Operator oldPred, int oldPredIndex, Operator newPred, boolean useOldPred) throws PlanException {
        super.rewire(oldPred, oldPredIndex, newPred, useOldPred);
        LogicalOperator previous = (LogicalOperator) oldPred;
        LogicalOperator current = (LogicalOperator) newPred;
        for(LogicalPlan plan: mSortColPlans) {
            try {
                ProjectFixerUpper projectFixer = new ProjectFixerUpper(
                        plan, previous, oldPredIndex, current, useOldPred, this);
                projectFixer.visit();
            } catch (VisitorException ve) {
                int errCode = 2144;
                String msg = "Problem while fixing project inputs during rewiring.";
                throw new PlanException(msg, errCode, PigException.BUG, ve);
            }
        }
    }
    
    @Override
    public List<RequiredFields> getRelevantInputs(int output, int column) {
        if (output!=0)
            return null;
        
        if (column<0)
            return null;
        
        // if we have schema information, check if output column is valid
        if (mSchema!=null)
        {
            if (column >= mSchema.size())
                return null;
        }
        
        ArrayList<Pair<Integer, Integer>> inputList = new ArrayList<Pair<Integer, Integer>>();
        inputList.add(new Pair<Integer, Integer>(0, column));
        List<RequiredFields> result = new ArrayList<RequiredFields>();
        result.add(new RequiredFields(inputList));
        return result;
    }
}

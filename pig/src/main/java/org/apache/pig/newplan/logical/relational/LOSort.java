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
package org.apache.pig.newplan.logical.relational;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.pig.FuncSpec;
import org.apache.pig.SortColInfo;
import org.apache.pig.SortColInfo.Order;
import org.apache.pig.SortInfo;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.PlanVisitor;
import org.apache.pig.newplan.ReverseDependencyOrderWalker;
import org.apache.pig.newplan.logical.expression.LogicalExpressionPlan;
import org.apache.pig.newplan.logical.expression.ProjectExpression;
import org.apache.pig.newplan.logical.visitor.ResetProjectionAttachedRelationalOpVisitor;

public class LOSort extends LogicalRelationalOperator{
    private List<Boolean> mAscCols;
    private FuncSpec mSortFunc;
    private boolean mIsStar = false;
    private long limit = -1;
    private List<LogicalExpressionPlan> mSortColPlans;
    
    public LOSort(OperatorPlan plan) {
        super("LOSort", plan);
    }

    public LOSort(OperatorPlan plan, List<LogicalExpressionPlan> sortColPlans,
            List<Boolean> ascCols,
            FuncSpec sortFunc ) {
        this( plan );
        mSortColPlans = sortColPlans;
        mAscCols = ascCols;
        mSortFunc = sortFunc;
    }

    public List<LogicalExpressionPlan> getSortColPlans() {
        return mSortColPlans;
    }

    public void setSortColPlans(List<LogicalExpressionPlan> sortPlans) {
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
    public LogicalSchema getSchema() throws FrontendException {
        if (schema!=null)
            return schema;
        
        LogicalRelationalOperator input = null;
        input = (LogicalRelationalOperator)plan.getPredecessors(this).get(0);
        
        schema = input.getSchema();
        return schema;
    }

    @Override
    public void accept(PlanVisitor v) throws FrontendException {
        if (!(v instanceof LogicalRelationalNodesVisitor)) {
            throw new FrontendException("Expected LogicalPlanVisitor", 2223);
        }
        ((LogicalRelationalNodesVisitor)v).visit(this);
        
    }
    
    public SortInfo getSortInfo() throws FrontendException {
        LogicalSchema schema = this.getSchema();
        List<SortColInfo> sortColInfoList = new ArrayList<SortColInfo>();
        for (int i = 0; i < mSortColPlans.size(); i++) {
            
            //get the single project from the sort plans
            LogicalExpressionPlan lp = mSortColPlans.get(i);
            Iterator<Operator> opsIterator = lp.getOperators();
            List<Operator> opsList = new ArrayList<Operator>();
            while(opsIterator.hasNext()) {
                opsList.add(opsIterator.next());
            }
            if(opsList.size() != 1 || !(opsList.get(0) instanceof ProjectExpression)) {
                throw new FrontendException(this, "Unsupported operator in inner plan: " + opsList.get(0), 2237);
            }
            ProjectExpression project = (ProjectExpression) opsList.get(0);
            
            //create SortColInfo from the project
            if(project.isProjectStar()){
                //there is no input schema, that is why project-star is still here
                // we don't know how many columns are represented by this
                //so don't add further columns to sort list
                return new SortInfo(sortColInfoList);
            } 
            if(project.isRangeProject()){
                if(project.getEndCol() < 0){
                    //stop here for 
                    // same reason as project-star condition above 
                    //(unkown number of columns this represents)
                    return new SortInfo(sortColInfoList);
                }
                //expand the project-range into multiple SortColInfos
                for(int cnum = project.getStartCol(); cnum < project.getEndCol(); cnum++){
                    sortColInfoList.add(
                            new SortColInfo(null, cnum, getOrder(mAscCols,i))
                    );
                }
            }
            else{
                int sortColIndex = project.getColNum();
                String sortColName = (schema == null) ? null :
                    schema.getField(sortColIndex).alias;

                sortColInfoList.add(
                        new SortColInfo(sortColName, sortColIndex, getOrder(mAscCols,i))
                );
            }
        }
        return new SortInfo(sortColInfoList);
    }

    private Order getOrder(List<Boolean> mAscCols2, int i) {
        return mAscCols.get(i) ? 
                SortColInfo.Order.ASCENDING : SortColInfo.Order.DESCENDING;
    }

    @Override
    public boolean isEqual(Operator other) throws FrontendException {
        if (other != null && other instanceof LOSort) {
            LOSort otherSort = (LOSort)other;
            if (!mAscCols.equals(otherSort.getAscendingCols()))
                return false;
            if (!mSortFunc.equals(otherSort.getUserFunc()))
                return false;
            if (mIsStar!=otherSort.isStar())
                return false;
            if (limit!=otherSort.getLimit())
                return false;
            if (!mSortColPlans.equals(otherSort.getSortColPlans()))
                return false;
        }
        return checkEquality((LogicalRelationalOperator)other);
    }
    
    public Operator getInput(LogicalPlan plan) {
        return plan.getPredecessors(this).get(0);
    }

    public static LOSort createCopy(LOSort sort) throws FrontendException {
        LOSort newSort = new LOSort(sort.getPlan(), null,
                                    sort.getAscendingCols(),
                                    sort.getUserFunc());

        List<LogicalExpressionPlan> newSortColPlans =
            new ArrayList<LogicalExpressionPlan>(sort.getSortColPlans().size());

        for(LogicalExpressionPlan lep:sort.getSortColPlans() ) {
            LogicalExpressionPlan new_lep = lep.deepCopy();

            // Resetting the attached LOSort operator of the ProjectExpression
            // to the newSort 
            new ResetProjectionAttachedRelationalOpVisitor(
                new_lep, newSort ).visit();
            newSortColPlans.add(new_lep);
        }
        newSort.setSortColPlans(newSortColPlans);
        return newSort;
    }
}

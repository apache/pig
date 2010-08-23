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
import org.apache.pig.SortInfo;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.PlanVisitor;
import org.apache.pig.newplan.logical.expression.LogicalExpressionPlan;
import org.apache.pig.newplan.logical.expression.ProjectExpression;

public class LOSort extends LogicalRelationalOperator{
    private List<Boolean> mAscCols;
    private FuncSpec mSortFunc;
    private boolean mIsStar = false;
    private long limit;
    private List<LogicalExpressionPlan> mSortColPlans;

    public LOSort(OperatorPlan plan, List<LogicalExpressionPlan> sortColPlans,
            List<Boolean> ascCols,
            FuncSpec sortFunc ) {
        super("LOSort", plan);
        mSortColPlans = sortColPlans;
        mAscCols = ascCols;
        mSortFunc = sortFunc;
        limit = -1;
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
            LogicalExpressionPlan lp = mSortColPlans.get(i);
            Iterator<Operator> opsIterator = lp.getOperators();
            List<Operator> opsList = new ArrayList<Operator>();
            while(opsIterator.hasNext()) {
                opsList.add(opsIterator.next());
            }
            if(opsList.size() != 1 || !(opsList.get(0) instanceof ProjectExpression)) {
                throw new FrontendException("Unsupported operator in inner plan: " + opsList.get(0), 2237);
            }
            ProjectExpression project = (ProjectExpression) opsList.get(0);
            int sortColIndex = project.getColNum();
            String sortColName = (schema == null) ? null :
                schema.getField(sortColIndex).alias;
            sortColInfoList.add(new SortColInfo(sortColName, sortColIndex, 
                    mAscCols.get(i)? SortColInfo.Order.ASCENDING :
                        SortColInfo.Order.DESCENDING));
        }
        return new SortInfo(sortColInfoList);
    }

    @Override
    public boolean isEqual(Operator other) throws FrontendException {
        if (other != null && other instanceof LOSort) {
            LOSort otherSort = (LOSort)other;
            if (!mAscCols.equals(otherSort.getAscendingCols()))
                return false;
            if (mSortFunc.equals(otherSort.getUserFunc()))
                return false;
            if (mIsStar!=otherSort.isStar())
                return false;
            if (limit!=otherSort.getLimit())
                return false;
            if (mSortColPlans.equals(otherSort.getSortColPlans()))
                return false;
        }
        return checkEquality((LogicalRelationalOperator)other);
    }
}

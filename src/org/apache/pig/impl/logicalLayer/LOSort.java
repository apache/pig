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
import java.util.List;
import java.util.Map;

import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.plan.PlanVisitor;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class LOSort extends LogicalOperator {
    private static final long serialVersionUID = 2L;

    private LogicalOperator mInput;
    private List<Boolean> mAscCols;
    private String mSortFunc;
    private boolean mIsStar = false;
    private List<LogicalPlan> mSortColPlans;
	private static Log log = LogFactory.getLog(LOSort.class);

    /**
     * @param plan
     *            LogicalPlan this operator is a part of.
     * @param key
     *            OperatorKey for this operator
     * @param input
     *            Input to sort
     * @param sortCols
     *            Array of column numbers that will be used for sorting data.
     * @param ascCols
     *            Array of booleans. Should be same size as sortCols. True
     *            indicates sort ascending (default), false sort descending. If
     *            this array is null, then all columns will be sorted ascending.
     * @param sorFunc
     *            the user defined sorting function
     */
    public LOSort(LogicalPlan plan, OperatorKey key, LogicalOperator input,
            List<LogicalPlan> sortColPlans, List<Boolean> ascCols, String sortFunc) {
        super(plan, key);
        mInput = input;
        mSortColPlans = sortColPlans;
        mAscCols = ascCols;
        mSortFunc = sortFunc;
    }

    public LogicalOperator getInput() {
        return mInput;
    }
    
    public List<LogicalPlan> getSortColPlans() {
        return mSortColPlans;
    }

    public List<Boolean> getAscendingCols() {
        return mAscCols;
    }

    public String getUserFunc() {
        return mSortFunc;
    }

    public void setUserFunc(String func) {
        mSortFunc = func;
    }

    public void setStar(boolean b) {
        mIsStar = b;
    }

    @Override
    public String name() {
        return "SORT " + mKey.scope + "-" + mKey.id;
    }

    @Override
    public Schema getSchema() throws FrontendException {
        if (!mIsSchemaComputed && (null == mSchema)) {
            // get our parent's schema
            Collection<LogicalOperator> s = mPlan.getPredecessors(this);
            try {
                LogicalOperator op = s.iterator().next();
                if (null == op) {
                    throw new FrontendException("Could not find operator in plan");
                }
                mSchema = op.getSchema();
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

    @Override
    public boolean supportsMultipleOutputs() {
        return false;
    }

    public void visit(LOVisitor v) throws VisitorException {
        v.visit(this);
    }
}

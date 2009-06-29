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

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.ArrayList;

import org.apache.pig.FuncSpec;
import org.apache.pig.impl.plan.DepthFirstWalker;
import org.apache.pig.impl.plan.VisitorException;

/**
 * A visitor to track the UDFs in a plan.
 */
public class UDFFinder extends LOVisitor {

    List<FuncSpec> mUDFList = new ArrayList<FuncSpec>();

    /**
     * 
     * @param plan
     *            logical plan to query the presence of UDFs
     */
    public UDFFinder(LogicalPlan plan) {
        super(plan, new DepthFirstWalker<LogicalOperator, LogicalPlan>(plan));
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.apache.pig.impl.logicalLayer.LOVisitor#visit(org.apache.pig.impl.
     * logicalLayer.LOUserFunc)
     */
    @Override
    protected void visit(LOUserFunc udf) throws VisitorException {
        mUDFList.add(udf.getFuncSpec());
    }

    /**
     * 
     * @return list of function specifications found in the plan
     */
    public List<FuncSpec> getUDFList() {
        return mUDFList;
    }

    /**
     * 
     * @return set of function specifications found in the plan
     */
    public Set<FuncSpec> getUDFSet() {
        return new HashSet<FuncSpec>(mUDFList);
    }

    /**
     * 
     * @return true if the plan had any UDFs; false otherwise
     */
    public boolean foundAnyUDF() {
        return (mUDFList.size() == 0 ? false : true);
    }

}
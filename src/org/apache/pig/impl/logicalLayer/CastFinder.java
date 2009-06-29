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
 * A visitor to track the casts in a plan.
 */
public class CastFinder extends LOVisitor {

    List<LOCast> mCastList = new ArrayList<LOCast>();

    /**
     * 
     * @param plan
     *            logical plan to query the presence of UDFs
     */
    public CastFinder(LogicalPlan plan) {
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
    protected void visit(LOCast cast) throws VisitorException {
        mCastList.add(cast);
    }

    /**
     * 
     * @return list of casts found in the plan
     */
    public List<LOCast> getCastList() {
        return mCastList;
    }

    /**
     * 
     * @return set of casts found in the plan
     */
    public Set<LOCast> getCastSet() {
        return new HashSet<LOCast>(mCastList);
    }

    /**
     * 
     * @return true if the plan contained any Casts; false otherwise
     */
    public boolean foundAnyCast() {
        return (mCastList.size() == 0 ? false : true);
    }

}
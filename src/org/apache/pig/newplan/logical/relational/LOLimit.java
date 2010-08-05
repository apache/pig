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

import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.PlanVisitor;

public class LOLimit extends LogicalRelationalOperator {

    private long mLimit;
    
    private static final long serialVersionUID = 2L;
    //private static Log log = LogFactory.getLog(LOFilter.class);

        
    public LOLimit(LogicalPlan plan, long limit) {
        super("LOLimit", plan);
        mLimit = limit;
    }

    public long getLimit() {
        return mLimit;
    }

    public void setLimit(long limit) {
        mLimit = limit;
    }
    
    @Override
    public LogicalSchema getSchema() throws FrontendException {
        LogicalRelationalOperator input = null;
        input = (LogicalRelationalOperator)plan.getPredecessors(this).get(0);
        
        return input.getSchema();
    }   
    
    @Override
    public void accept(PlanVisitor v) throws FrontendException {
        if (!(v instanceof LogicalRelationalNodesVisitor)) {
            throw new FrontendException("Expected LogicalPlanVisitor", 2223);
        }
        ((LogicalRelationalNodesVisitor)v).visit(this);
    }
    
    @Override
    public boolean isEqual(Operator other) throws FrontendException{
        if (other != null && other instanceof LOLimit && ((LOLimit)other).getLimit() == mLimit)
            return checkEquality((LogicalRelationalOperator)other);
        else
            return false;
    }
}

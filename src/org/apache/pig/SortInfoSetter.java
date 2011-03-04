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

package org.apache.pig;

import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.LOConst;
import org.apache.pig.impl.logicalLayer.LOLimit;
import org.apache.pig.impl.logicalLayer.LOSort;
import org.apache.pig.impl.logicalLayer.LOSplit;
import org.apache.pig.impl.logicalLayer.LOSplitOutput;
import org.apache.pig.impl.logicalLayer.LOStore;
import org.apache.pig.impl.logicalLayer.LOVisitor;
import org.apache.pig.impl.logicalLayer.LogicalOperator;
import org.apache.pig.impl.logicalLayer.LogicalPlan;
import org.apache.pig.impl.plan.DependencyOrderWalker;
import org.apache.pig.impl.plan.VisitorException;

// TODO: remove this class. Added just to make test cases compile. extracted from pigserver
public class SortInfoSetter extends LOVisitor{

    public SortInfoSetter(LogicalPlan plan) {
        super(plan, new DependencyOrderWalker<LogicalOperator, LogicalPlan>(plan));
    }

    @Override
    protected void visit(LOStore store) throws VisitorException {

        LogicalOperator storePred = store.getPlan().getPredecessors(store).get(0);
        if(storePred == null){
            int errCode = 2051;
            String msg = "Did not find a predecessor for Store." ;
            throw new VisitorException(msg, errCode, PigException.BUG);
        }

        SortInfo sortInfo = null;
        if(storePred instanceof LOLimit) {
            storePred = store.getPlan().getPredecessors(storePred).get(0);
        } else if (storePred instanceof LOSplitOutput) {
            LOSplitOutput splitOutput = (LOSplitOutput)storePred;
            // We assume this is the LOSplitOutput we injected for this case:
            // b = order a by $0; store b into '1'; store b into '2';
            // In this case, we should mark both '1' and '2' as sorted
            LogicalPlan conditionPlan = splitOutput.getConditionPlan();
            if (conditionPlan.getRoots().size()==1) {
                LogicalOperator root = conditionPlan.getRoots().get(0);
                if (root instanceof LOConst) {
                    Object value = ((LOConst)root).getValue();
                    if (value instanceof Boolean && (Boolean)value==true) {
                        LogicalOperator split = splitOutput.getPlan().getPredecessors(splitOutput).get(0);
                        if (split instanceof LOSplit) {
                            storePred = store.getPlan().getPredecessors(split).get(0);
                        }
                    }
                }
            }
        }
        // if this predecessor is a sort, get
        // the sort info.
        if(storePred instanceof LOSort) {
            try {
                sortInfo = ((LOSort)storePred).getSortInfo();
            } catch (FrontendException e) {
                throw new VisitorException(e);
            }
        }
        store.setSortInfo(sortInfo);
    }
}

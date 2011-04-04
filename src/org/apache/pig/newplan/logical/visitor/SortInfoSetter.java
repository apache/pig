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

package org.apache.pig.newplan.logical.visitor;

import org.apache.pig.PigException;
import org.apache.pig.SortInfo;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.newplan.DependencyOrderWalker;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.logical.expression.ConstantExpression;
import org.apache.pig.newplan.logical.expression.LogicalExpressionPlan;
import org.apache.pig.newplan.logical.relational.LOLimit;
import org.apache.pig.newplan.logical.relational.LOSort;
import org.apache.pig.newplan.logical.relational.LOSplit;
import org.apache.pig.newplan.logical.relational.LOSplitOutput;
import org.apache.pig.newplan.logical.relational.LOStore;
import org.apache.pig.newplan.logical.relational.LogicalRelationalNodesVisitor;

public class SortInfoSetter extends LogicalRelationalNodesVisitor {

    public SortInfoSetter(OperatorPlan plan) throws FrontendException {
        super(plan, new DependencyOrderWalker(plan));
    }

    @Override
    public void visit(LOStore store) throws FrontendException {
        
        Operator storePred = store.getPlan().getPredecessors(store).get(0);
        if(storePred == null){
            int errCode = 2051;
            String msg = "Did not find a predecessor for Store." ;
            throw new VisitorException(store, msg, errCode, PigException.BUG);    
        }
        
        SortInfo sortInfo = null;
        if(storePred instanceof LOLimit) {
            storePred = store.getPlan().getPredecessors(storePred).get(0);
        } else if (storePred instanceof LOSplitOutput) {
            LOSplitOutput splitOutput = (LOSplitOutput)storePred;
            // We assume this is the LOSplitOutput we injected for this case:
            // b = order a by $0; store b into '1'; store b into '2';
            // In this case, we should mark both '1' and '2' as sorted
            LogicalExpressionPlan conditionPlan = splitOutput.getFilterPlan();
            if (conditionPlan.getSinks().size()==1) {
                Operator root = conditionPlan.getSinks().get(0);
                if (root instanceof ConstantExpression) {
                    Object value = ((ConstantExpression)root).getValue();
                    if (value instanceof Boolean && (Boolean)value==true) {
                        Operator split = splitOutput.getPlan().getPredecessors(splitOutput).get(0);
                        if (split instanceof LOSplit)
                            storePred = store.getPlan().getPredecessors(split).get(0);
                    }
                }
            }
        }
        // if this predecessor is a sort, get
        // the sort info.
        if( storePred instanceof LOSort ) {
            sortInfo = ((LOSort)storePred).getSortInfo();
        }
        store.setSortInfo(sortInfo);
    }
}

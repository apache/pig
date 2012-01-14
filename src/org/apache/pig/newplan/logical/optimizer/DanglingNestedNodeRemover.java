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

package org.apache.pig.newplan.logical.optimizer;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.DependencyOrderWalker;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.logical.relational.LOForEach;
import org.apache.pig.newplan.logical.relational.LOGenerate;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.newplan.logical.relational.LogicalRelationalNodesVisitor;

public class DanglingNestedNodeRemover extends LogicalRelationalNodesVisitor {

    public DanglingNestedNodeRemover(OperatorPlan plan)
            throws FrontendException {
        super(plan, new DependencyOrderWalker(plan));
    }

    @Override
    public void visit(LOForEach foreach) throws FrontendException {
        LogicalPlan innerPlan = foreach.getInnerPlan();
        List<Operator> opsToRemove = new ArrayList<Operator>();
        Iterator<Operator> ops = innerPlan.getOperators();
        
        while (ops.hasNext()) {
            Operator op = ops.next();
            // Check if op leads to LOGenerate, otherwise, candidate to remove
            Operator currentOp = op;
            boolean endWithNoLOGenerate = false;
            while (!(currentOp instanceof LOGenerate)) {
                if (innerPlan.getSuccessors(currentOp)==null) {
                    endWithNoLOGenerate = true;
                    break;
                }
                currentOp = innerPlan.getSuccessors(currentOp).get(0);
            }
            if (endWithNoLOGenerate)
                opsToRemove.add(op);
        }
        
        for (Operator op : opsToRemove) {
            innerPlan.removeAndReconnect(op);
        }
    }
}

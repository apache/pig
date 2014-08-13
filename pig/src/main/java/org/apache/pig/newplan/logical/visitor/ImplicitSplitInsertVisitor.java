/**
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

import java.util.List;

import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.util.Pair;
import org.apache.pig.newplan.DependencyOrderWalker;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.logical.expression.ConstantExpression;
import org.apache.pig.newplan.logical.expression.LogicalExpressionPlan;
import org.apache.pig.newplan.logical.optimizer.AllSameRalationalNodesVisitor;
import org.apache.pig.newplan.logical.optimizer.SchemaResetter;
import org.apache.pig.newplan.logical.optimizer.UidResetter;
import org.apache.pig.newplan.logical.relational.LOSplit;
import org.apache.pig.newplan.logical.relational.LOSplitOutput;
import org.apache.pig.newplan.logical.relational.LOStore;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.newplan.logical.relational.LogicalRelationalOperator;
import org.apache.pig.newplan.logical.relational.LogicalSchema;


public class ImplicitSplitInsertVisitor extends AllSameRalationalNodesVisitor {

    public ImplicitSplitInsertVisitor(LogicalPlan plan) throws FrontendException {
        super(plan, new DependencyOrderWalker(plan));
    }

    // Look to see if this is a non-split node with two outputs.  If so
    // it matches.
    private boolean nodeHasTwoOutputs(LogicalRelationalOperator op) {
        if (op instanceof LOSplit || op instanceof LOStore) {
            return false;
        }
        List<Operator> succs = plan.getSuccessors(op);
        if (succs != null && succs.size() >= 2) {
            return true;
        } else {
            return false;
        }
    }

    @Override
    public void execute(LogicalRelationalOperator op) throws FrontendException {
        if(!nodeHasTwoOutputs(op) )  {
            return;
        }

        // For two successors of op here is a pictorial
        // representation of the change required:
        // BEFORE:
        // Succ1  Succ2
        //  \       /
        //      op

        //  SHOULD BECOME:

        // AFTER:
        // Succ1          Succ2
        //   |              |
        // SplitOutput SplitOutput
        //      \       /
        //        Split
        //          |
        //          op

        List<Operator> succs = plan.getSuccessors(op);
        if (succs == null || succs.size() < 2) {
            throw new FrontendException("Invalid match in ImplicitSplitInserter rule.", 2243);
        }
        LOSplit splitOp = new LOSplit(plan);
        splitOp.setAlias(((LogicalRelationalOperator) op).getAlias());
        Operator[] sucs = succs.toArray(new Operator[0]);
        plan.add(splitOp);
        plan.connect(op, splitOp);
        for (Operator suc : sucs) {
            // position is remembered in order to maintain the order of the successors
            Pair<Integer, Integer> pos = plan.disconnect(op, suc);
            LogicalExpressionPlan filterPlan = new LogicalExpressionPlan();
            new ConstantExpression(filterPlan, Boolean.valueOf(true));
            LOSplitOutput splitOutput = new LOSplitOutput((LogicalPlan) plan, filterPlan);
            splitOutput.setAlias(splitOp.getAlias());
            plan.add(splitOutput);
            plan.connect(splitOp, splitOutput);
            plan.connect(splitOutput, pos.first, suc, pos.second);
        }

        // Since we adjust the uid layout, clear all cached uids
        UidResetter uidResetter = new UidResetter(plan);
        uidResetter.visit();

        // Manually regenerate schema
        SchemaResetter schemaResetter = new SchemaResetter(plan, true);
        schemaResetter.visit();
    }
}

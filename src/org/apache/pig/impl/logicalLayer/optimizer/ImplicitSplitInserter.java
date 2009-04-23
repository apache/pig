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

package org.apache.pig.impl.logicalLayer.optimizer;

import java.util.ArrayList;
import java.util.List;

import org.apache.pig.PigException;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.LOConst;
import org.apache.pig.impl.logicalLayer.LOSplitOutput;
import org.apache.pig.impl.logicalLayer.LogicalOperator;
import org.apache.pig.impl.logicalLayer.LogicalPlan;
import org.apache.pig.impl.logicalLayer.LOSplit;
import org.apache.pig.impl.logicalLayer.LOStore;
import org.apache.pig.impl.plan.DepthFirstWalker;
import org.apache.pig.impl.plan.NodeIdGenerator;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.optimizer.OptimizerException;

public class ImplicitSplitInserter extends LogicalTransformer {

    public ImplicitSplitInserter(LogicalPlan plan) {
        super(plan, new DepthFirstWalker<LogicalOperator, LogicalPlan>(plan));
    }

    @Override
    public boolean check(List<LogicalOperator> nodes) throws OptimizerException {
        // Look to see if this is a non-split node with two outputs.  If so
        // it matches.
        if((nodes == null) || (nodes.size() <= 0)) {
            int errCode = 2052;
            String msg = "Internal error. Cannot retrieve operator from null or empty list.";
            throw new OptimizerException(msg, errCode, PigException.BUG);
        }
        try {
            LogicalOperator op = nodes.get(0);
            List<LogicalOperator> succs = mPlan.getSuccessors(op);
            if (succs == null || succs.size() < 2) return false;
            if (op instanceof LOSplit) return false;
            if (op instanceof LOStore) return false;
            return true;
        } catch (Exception e) {
            int errCode = 2048;
            String msg = "Error while performing checks to introduce split operators.";
            throw new OptimizerException(msg, errCode, PigException.BUG, e);
        }
    }

    @Override
    public void transform(List<LogicalOperator> nodes)
            throws OptimizerException {
        if((nodes == null) || (nodes.size() <= 0)) {
            int errCode = 2052;
            String msg = "Internal error. Cannot retrieve operator from null or empty list.";
            throw new OptimizerException(msg, errCode, PigException.BUG);
        }
        try {
            // Insert a split and its corresponding SplitOutput nodes into the plan
            // between node 0 and 1 / 2.
            String scope = nodes.get(0).getOperatorKey().scope;
            NodeIdGenerator idGen = NodeIdGenerator.getGenerator();
            LOSplit splitOp = new LOSplit(mPlan, new OperatorKey(scope, 
                    idGen.getNextNodeId(scope)), new ArrayList<LogicalOperator>());
            splitOp.setAlias(nodes.get(0).getAlias());
            mPlan.add(splitOp);
            
            // Find all the successors and connect appropriately with split
            // and splitoutput operators.  Keep our own copy
            // of the list, as we're changing the graph by doing these calls 
            // and that will change the list of predecessors.
            List<LogicalOperator> succs = 
                new ArrayList<LogicalOperator>(mPlan.getSuccessors(nodes.get(0)));
            int index = -1;
            // For two successors of nodes.get(0) here is a pictorial
            // representation of the change required:
            // BEFORE:
            // Succ1  Succ2
            //  \       /
            //  nodes.get(0)
            
            //  SHOULD BECOME:
            
            // AFTER:
            // Succ1          Succ2
            //   |              |
            // SplitOutput SplitOutput
            //      \       /
            //        Split
            //          |
            //        nodes.get(0)
            
            // Here is how this will be accomplished.
            // First (the same) Split Operator will be "inserted between" nodes.get(0)
            // and all its successors. The "insertBetween" API is used which makes sure
            // the ordering of operators in the graph is preserved. So we get the following: 
            // Succ1        Succ2
            //    |          |
            //   Split     Split
            //      \      /  
            //      nodes.get(0)
            
            // Then all but the first connection between nodes.get(0) and the Split 
            // Operator are removed using "disconnect" - so we get the following:
            // Succ1          Succ2
            //      \       /
            //        Split
            //          |
            //        nodes.get(0)
            
            // Now a new SplitOutputOperator is "inserted between" the Split operator
            // and the successors. So we get:
            // Succ1          Succ2
            //   |              |
            // SplitOutput SplitOutput
            //      \       /
            //        Split
            //          |
            //        nodes.get(0)
            
            
            for (LogicalOperator succ : succs) {
                mPlan.insertBetween(nodes.get(0), splitOp, succ);
            }
            
            for(int i = 1; i < succs.size(); i++) {
                mPlan.disconnect(nodes.get(0), splitOp); 
            }

            for (LogicalOperator succ : succs) {
                LogicalPlan condPlan = new LogicalPlan();
                LOConst cnst = new LOConst(mPlan, new OperatorKey(scope, 
                        idGen.getNextNodeId(scope)), new Boolean(true));
                cnst.setType(DataType.BOOLEAN);
                condPlan.add(cnst);
                LOSplitOutput splitOutput = new LOSplitOutput(mPlan, 
                        new OperatorKey(scope, idGen.getNextNodeId(scope)), ++index, condPlan);
                splitOp.addOutput(splitOutput);
                mPlan.add(splitOutput);
                mPlan.insertBetween(splitOp, splitOutput, succ);
                splitOutput.setAlias(splitOp.getAlias());
                // Patch up the contained plans of succ
                fixUpContainedPlans(nodes.get(0), splitOutput, succ, null);
            }
            
        } catch (Exception e) {
            int errCode = 2047;
            String msg = "Internal error. Unable to introduce split operators.";
            throw new OptimizerException(msg, errCode, PigException.BUG, e);
        }
    }
}

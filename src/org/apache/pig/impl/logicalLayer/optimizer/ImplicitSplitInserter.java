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

import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.LOConst;
import org.apache.pig.impl.logicalLayer.LOPrinter;
import org.apache.pig.impl.logicalLayer.LOSplitOutput;
import org.apache.pig.impl.logicalLayer.LogicalOperator;
import org.apache.pig.impl.logicalLayer.LogicalPlan;
import org.apache.pig.impl.logicalLayer.LOSplit;
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
        LogicalOperator op = nodes.get(0);
        List<LogicalOperator> succs = mPlan.getSuccessors(op);
        if (succs == null || succs.size() < 2) return false;
        if (op instanceof LOSplit) return false;
        return true;
    }

    @Override
    public void transform(List<LogicalOperator> nodes)
            throws OptimizerException {
        // Insert a split and its corresponding SplitOutput nodes into the plan
        // between node 0 and 1 / 2.
        String scope = nodes.get(0).getOperatorKey().scope;
        NodeIdGenerator idGen = NodeIdGenerator.getGenerator();
        LOSplit splitOp = new LOSplit(mPlan, new OperatorKey(scope, 
                idGen.getNextNodeId(scope)), new ArrayList<LogicalOperator>());
        try {
            mPlan.add(splitOp);
            
            // Find all the successors and connect appropriately with split
            // and splitoutput operators.  Keep our own copy
            // of the list, as we're changing the graph by doing these calls 
            // and that will change the list of predecessors.
            List<LogicalOperator> succs = 
                new ArrayList<LogicalOperator>(mPlan.getSuccessors(nodes.get(0)));
            int index = -1;
            boolean nodeConnectedToSplit = false;
            for (LogicalOperator succ : succs) {
                if(!nodeConnectedToSplit) {
                    mPlan.insertBetween(nodes.get(0), splitOp, succ);
                    // nodes.get(0) should be connected to Split (only once) and
                    // split -> splitoutput -> successor - this is for the first successor  
                    // for the next successor we just want to connect in the order 
                    // split -> splitoutput -> successor without involving nodes.get(0)
                    // in the above call we have connected
                    // nodes.get(0) to split (we will set the flag
                    // to true later in this loop iteration). Hence in subsequent 
                    // iterations we will only disconnect nodes.get(0) from its
                    // successor and connect the split-splitoutput chain
                    // to the successor
                } else {
                    mPlan.disconnect(nodes.get(0), succ);                    
                }
                LogicalPlan condPlan = new LogicalPlan();
                LOConst cnst = new LOConst(mPlan, new OperatorKey(scope, 
                        idGen.getNextNodeId(scope)), new Boolean(true));
                cnst.setType(DataType.BOOLEAN);
                condPlan.add(cnst);
                LOSplitOutput splitOutput = new LOSplitOutput(mPlan, 
                        new OperatorKey(scope, idGen.getNextNodeId(scope)), ++index, condPlan);
                splitOp.addOutput(splitOutput);
                mPlan.add(splitOutput);
                
                if(!nodeConnectedToSplit) {
                    // node.get(0) should be connected to Split (only once) and
                    // split to splitoutput to successor - this is for the first successor  
                    // for the next successor we just want to connect in the order 
                    // split - splitoutput - successor.
                    // the call below is in the first successor case
                    mPlan.insertBetween(splitOp, splitOutput, succ);    
                    nodeConnectedToSplit = true;
                } else {
                    mPlan.connect(splitOp, splitOutput);
                    mPlan.connect(splitOutput, succ);
                }
                // Patch up the contained plans of succ
                fixUpContainedPlans(nodes.get(0), splitOutput, succ, null);
            }
        } catch (Exception e) {
            throw new OptimizerException(e);
        }
    }
}

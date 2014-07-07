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
package org.apache.pig.backend.hadoop.executionengine.tez;

import java.io.PrintStream;
import java.util.List;
import java.util.Map.Entry;

import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PlanPrinter;
import org.apache.pig.backend.hadoop.executionengine.tez.TezOperator.VertexGroupInfo;
import org.apache.pig.impl.plan.DependencyOrderWalker;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;

/**
 * A visitor to print out the Tez plan.
 */
public class TezPrinter extends TezOpPlanVisitor {

    private PrintStream mStream = null;
    private boolean isVerbose = true;

    /**
     * @param ps PrintStream to output plan information to
     * @param plan tez plan to print
     */
    public TezPrinter(PrintStream ps, TezOperPlan plan) {
        super(plan, new DependencyOrderWalker<TezOperator, TezOperPlan>(plan));
        mStream = ps;
    }

    public void setVerbose(boolean verbose) {
        isVerbose = verbose;
    }

    @Override
    public void visitTezOp(TezOperator tezOper) throws VisitorException {
        if (tezOper.isVertexGroup()) {
            VertexGroupInfo info = tezOper.getVertexGroupInfo();
            mStream.println("Tez vertex group "
                    + tezOper.getOperatorKey().toString() + "\t<-\t "
                    + info.getInputs() + "\t->\t " + info.getOutput());
            mStream.println("# No plan on vertex group");
        } else {
            mStream.println("Tez vertex " + tezOper.getOperatorKey().toString());
        }
        if (tezOper.inEdges.size() > 0) {
            for (Entry<OperatorKey, TezEdgeDescriptor> inEdge : tezOper.inEdges.entrySet()) {
                //TODO: Print other edge properties like custom partitioner
                if (!inEdge.getValue().combinePlan.isEmpty()) {
                    mStream.println("# Combine plan on edge <" + inEdge.getKey() + ">");
                    PlanPrinter<PhysicalOperator, PhysicalPlan> printer =
                            new PlanPrinter<PhysicalOperator, PhysicalPlan>(
                                    inEdge.getValue().combinePlan, mStream);
                    printer.setVerbose(isVerbose);
                    printer.visit();
                    mStream.println();
                }
            }
        }
        if (tezOper.plan != null && tezOper.plan.size() > 0) {
            mStream.println("# Plan on vertex");
            PlanPrinter<PhysicalOperator, PhysicalPlan> printer =
                    new PlanPrinter<PhysicalOperator, PhysicalPlan>(tezOper.plan, mStream);
            printer.setVerbose(isVerbose);
            printer.visit();
            mStream.println();
        }
    }

    /**
     * This class prints the Tez Vertex Graph
     */
    public static class TezGraphPrinter extends TezOpPlanVisitor {

        StringBuffer buf;

        public TezGraphPrinter(TezOperPlan plan) {
            super(plan, new DependencyOrderWalker<TezOperator, TezOperPlan>(plan));
            buf = new StringBuffer();
        }

        @Override
        public void visitTezOp(TezOperator tezOper) throws VisitorException {
            if (tezOper.isVertexGroup()) {
                buf.append("Tez vertex group " + tezOper.getOperatorKey().toString());
            } else {
                buf.append("Tez vertex " + tezOper.getOperatorKey().toString());
            }
            List<TezOperator> succs = mPlan.getSuccessors(tezOper);
            if (succs != null) {
                buf.append("\t->\t");
                for (TezOperator op : succs) {
                    if (op.isVertexGroup()) {
                        buf.append("Tez vertex group " + op.getOperatorKey().toString()).append(",");
                    } else {
                        buf.append("Tez vertex " + op.getOperatorKey().toString()).append(",");
                    }
                }
            }
            buf.append("\n");
        }

        @Override
        public String toString() {
            buf.append("\n");
            return buf.toString();
        }
    }
}

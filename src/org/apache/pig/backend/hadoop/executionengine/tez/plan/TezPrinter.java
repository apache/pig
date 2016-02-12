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
package org.apache.pig.backend.hadoop.executionengine.tez.plan;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PlanPrinter;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.TezOperator.VertexGroupInfo;
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
        super(plan, new DependencyOrderWalker<TezOperator, TezOperPlan>(plan, true));
        mStream = ps;
    }

    public void setVerbose(boolean verbose) {
        isVerbose = verbose;
    }

    @Override
    public void visitTezOp(TezOperator tezOper) throws VisitorException {
        if (tezOper.isVertexGroup()) {
            VertexGroupInfo info = tezOper.getVertexGroupInfo();
            mStream.print("Tez vertex group "
                    + tezOper.getOperatorKey().toString());
            if (info!=null) {
                mStream.println("\t<-\t " + info.getInputs() + "\t->\t " + info.getOutput());
            } else {
                mStream.println();
            }
            mStream.println("# No plan on vertex group");
        } else {
            mStream.println("Tez vertex " + tezOper.getOperatorKey().toString());
        }
        if (tezOper.inEdges.size() > 0) {
            List<OperatorKey> inEdges = new ArrayList<OperatorKey>(tezOper.inEdges.keySet());
            Collections.sort(inEdges);
            for (OperatorKey inEdge : inEdges) {
                //TODO: Print other edge properties like custom partitioner
                TezEdgeDescriptor edgeDesc = tezOper.inEdges.get(inEdge);
                if (!edgeDesc.combinePlan.isEmpty()) {
                    mStream.println("# Combine plan on edge <" + inEdge + ">");
                    PlanPrinter<PhysicalOperator, PhysicalPlan> printer =
                            new PlanPrinter<PhysicalOperator, PhysicalPlan>(
                                    edgeDesc.combinePlan, mStream);
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
        } else if (!tezOper.isVertexGroup()) {
            // For things like NativeTezOper
            mStream.println("" + tezOper);
        }
    }

    /**
     * This class prints the Tez Vertex Graph
     */
    public static class TezVertexGraphPrinter extends TezOpPlanVisitor {

        StringBuilder buf;

        public TezVertexGraphPrinter(TezOperPlan plan) {
            super(plan, new DependencyOrderWalker<TezOperator, TezOperPlan>(plan, true));
            buf = new StringBuilder();
        }

        @Override
        public void visitTezOp(TezOperator tezOper) throws VisitorException {
            writePlan(mPlan, tezOper, buf);
        }

        public static void writePlan(TezOperPlan plan, TezOperator tezOper, StringBuilder buf) {
            if (tezOper.isVertexGroup()) {
                buf.append("Tez vertex group " + tezOper.getOperatorKey().toString());
            } else {
                buf.append("Tez vertex " + tezOper.getOperatorKey().toString());
            }
            List<TezOperator> succs = plan.getSuccessors(tezOper);
            if (succs != null) {
                Collections.sort(succs);
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

    /**
     * This class prints the Tez DAG Graph
     */
    public static class TezDAGGraphPrinter extends TezPlanContainerVisitor {

        StringBuilder buf;

        public TezDAGGraphPrinter(TezPlanContainer plan) {
            super(plan, new DependencyOrderWalker<TezPlanContainerNode, TezPlanContainer>(plan, true));
            buf = new StringBuilder();
        }

        @Override
        public void visitTezPlanContainerNode(TezPlanContainerNode tezPlanContainerNode) throws VisitorException {
            writePlan(mPlan, tezPlanContainerNode, buf);
        }

        public static void writePlan(TezPlanContainer mPlan, TezPlanContainerNode tezPlanContainerNode, StringBuilder buf) {
            buf.append("Tez DAG " + tezPlanContainerNode.getOperatorKey().toString());
            List<TezPlanContainerNode> succs = mPlan.getSuccessors(tezPlanContainerNode);
            if (succs != null) {
                Collections.sort(succs);
                buf.append("\t->\t");
                for (TezPlanContainerNode op : succs) {
                    buf.append("Tez DAG " + op.getOperatorKey().toString()).append(",");
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

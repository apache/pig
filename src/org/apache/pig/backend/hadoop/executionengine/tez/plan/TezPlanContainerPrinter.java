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

import org.apache.pig.backend.hadoop.executionengine.tez.plan.TezPrinter.TezDAGGraphPrinter;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.TezPrinter.TezVertexGraphPrinter;
import org.apache.pig.impl.plan.DependencyOrderWalker;
import org.apache.pig.impl.plan.VisitorException;

public class TezPlanContainerPrinter extends TezPlanContainerVisitor {
    private PrintStream mStream = null;
    private boolean isVerbose = true;

    /**
     * @param ps PrintStream to output plan information to
     * @param plan tez plan to print
     */
    public TezPlanContainerPrinter(PrintStream ps, TezPlanContainer planContainer) {
        super(planContainer, new DependencyOrderWalker<TezPlanContainerNode, TezPlanContainer>(planContainer));
        mStream = ps;
        mStream.println("#--------------------------------------------------");
        mStream.println("# There are " + planContainer.size() + " DAGs in the session");
        mStream.println("#--------------------------------------------------");
        printContainerPlan(planContainer);
    }

    private void printContainerPlan(TezPlanContainer planContainer) {
        try {
            if (planContainer.size() > 1) {
                TezDAGGraphPrinter graphPrinter = new TezDAGGraphPrinter(planContainer);
                graphPrinter.visit();
                mStream.print(graphPrinter.toString());
            }
        } catch (VisitorException e) {
            throw new RuntimeException(e);
        }
    }

    public void setVerbose(boolean verbose) {
        isVerbose = verbose;
    }

    @Override
    public void visitTezPlanContainerNode(TezPlanContainerNode tezPlanContainerNode) throws VisitorException {
        mStream.println("#--------------------------------------------------");
        mStream.println("# TEZ DAG plan: " + tezPlanContainerNode.getOperatorKey());
        mStream.println("#--------------------------------------------------");
        TezVertexGraphPrinter graphPrinter = new TezVertexGraphPrinter(tezPlanContainerNode.getTezOperPlan());
        graphPrinter.visit();
        mStream.print(graphPrinter.toString());
        TezPrinter printer = new TezPrinter(mStream, tezPlanContainerNode.getTezOperPlan());
        printer.setVerbose(isVerbose);
        printer.visit();
    }
}


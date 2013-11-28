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

import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.util.MultiMap;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.PlanVisitor;
import org.apache.pig.newplan.logical.expression.LogicalExpressionPlan;
import org.apache.pig.newplan.logical.relational.LOCogroup;
import org.apache.pig.newplan.logical.relational.LOFilter;
import org.apache.pig.newplan.logical.relational.LOForEach;
import org.apache.pig.newplan.logical.relational.LOGenerate;
import org.apache.pig.newplan.logical.relational.LOJoin;
import org.apache.pig.newplan.logical.relational.LOLimit;
import org.apache.pig.newplan.logical.relational.LORank;
import org.apache.pig.newplan.logical.relational.LOSort;
import org.apache.pig.newplan.logical.relational.LOSplitOutput;
import org.apache.pig.newplan.logical.relational.LogicalPlan;

/**
 * A visitor mechanism printing out the logical plan.
 */
public class LogicalPlanPrinter extends PlanVisitor {

    private PrintStream mStream = null;
    private byte[] TAB1 = "    ".getBytes();
    private byte[] TABMore = "|   ".getBytes();
    private byte[] Bar = "|\n".getBytes();
    private byte[] LSep = "|---".getBytes();
    private byte[] USep = "|   |\n".getBytes();
    static public String SEPERATE = "\t";

    protected ArrayList<byte[]> tabs;
    protected boolean reverse = false;

    /**
     * @param ps PrintStream to output plan information to
     * @param plan Logical plan to print
     */
    public LogicalPlanPrinter(OperatorPlan plan, PrintStream ps) throws FrontendException {
        this(plan, ps, new ArrayList<byte[]>());
    }

    private LogicalPlanPrinter(OperatorPlan plan, PrintStream ps, ArrayList<byte[]> tabs) throws FrontendException {
        super(plan, null);
        mStream = ps;
        this.tabs = tabs;
        if (plan instanceof LogicalPlan) {
            reverse = false;
        }
        else {
            reverse = true;
        }
    }

    @Override
    public void visit() throws FrontendException {
        try {
            depthFirstLP();
        } catch (IOException e) {
            throw new FrontendException(e);
        }
    }

    protected void depthFirstLP() throws FrontendException, IOException {
        List<Operator> leaves;
        if(reverse) {
            leaves = plan.getSources();
        } else {
            leaves = plan.getSinks();
        }
        for (Operator leaf : leaves) {
            writeWithTabs((leaf.toString()+"\n").getBytes());
            depthFirst(leaf);
        }
    }

    private void writeWithTabs(byte[] data) throws IOException {
        for(byte[] tab : tabs) {
            mStream.write(tab);
        }
        mStream.write(data);
    }

    private void depthFirst(Operator node) throws FrontendException, IOException {
        printNodePlan(node);
        List<Operator> operators;

        if(reverse) {
            operators = plan.getSuccessors(node);
        } else {
            operators =  plan.getPredecessors(node);
        }
        if (operators == null)
            return;

        List<Operator> predecessors =  new ArrayList<Operator>(operators);

        int i = 0;
        for (Operator pred : predecessors) {
            i++;
            writeWithTabs(Bar);
            writeWithTabs(LSep);
            mStream.write((pred.toString()+"\n").getBytes());
            if (i < predecessors.size()) {
                tabs.add(TABMore);
            } else {
                tabs.add(TAB1);
            }
            depthFirst(pred);
            tabs.remove(tabs.size() - 1);
        }
    }

    private void printPlan(OperatorPlan lp) throws VisitorException, IOException {
        writeWithTabs(USep);
        tabs.add(TABMore);
        if(lp!=null) {
            LogicalPlanPrinter printer = new LogicalPlanPrinter(lp, mStream, tabs);
            printer.visit();
        }
        tabs.remove(tabs.size() - 1);
    }

    private void printNodePlan(Operator node) throws FrontendException, IOException {
        if(node instanceof LOFilter){
            printPlan(((LOFilter)node).getFilterPlan());
        }
        else if(node instanceof LOLimit){
            printPlan(((LOLimit)node).getLimitPlan());
        }
        else if(node instanceof LOForEach){
            printPlan(((LOForEach)node).getInnerPlan());        
        }
        else if(node instanceof LOCogroup){
            MultiMap<Integer, LogicalExpressionPlan> plans = ((LOCogroup)node).getExpressionPlans();
            for (int i : plans.keySet()) {
                // Visit the associated plans
                for (OperatorPlan plan : plans.get(i)) {
                    printPlan(plan);
                }
            }
        }
        else if(node instanceof LOJoin){
            MultiMap<Integer, LogicalExpressionPlan> plans = ((LOJoin)node).getExpressionPlans();
            for (int i: plans.keySet()) {
                // Visit the associated plans
                for (OperatorPlan plan : plans.get(i)) {
                    printPlan(plan);
                }
            }
        }
        else if(node instanceof LORank){
            // Visit fields for rank
            for (OperatorPlan plan : ((LORank)node).getRankColPlans())
                printPlan(plan);
        }
        else if(node instanceof LOSort){
            for (OperatorPlan plan : ((LOSort)node).getSortColPlans())
                printPlan(plan);
        }
        else if(node instanceof LOSplitOutput){
            printPlan(((LOSplitOutput)node).getFilterPlan());
        }
        else if(node instanceof LOGenerate){
            for (OperatorPlan plan : ((LOGenerate)node).getOutputPlans()) {
                printPlan(plan);
            }
        }
    }
}



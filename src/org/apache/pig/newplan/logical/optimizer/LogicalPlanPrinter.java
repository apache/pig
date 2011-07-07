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

import java.io.ByteArrayOutputStream;
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
import org.apache.pig.newplan.logical.relational.LOSort;
import org.apache.pig.newplan.logical.relational.LOSplitOutput;
import org.apache.pig.newplan.logical.relational.LogicalPlan;

/**
 * A visitor mechanism printing out the logical plan.
 */
public class LogicalPlanPrinter extends PlanVisitor {

    private PrintStream mStream = null;
    private String TAB1 = "    ";
    private String TABMore = "|   ";
    private String LSep = "|\n|---";
    private String USep = "|   |\n|   ";
    static public String SEPERATE = "\t";

    /**
     * @param ps PrintStream to output plan information to
     * @param plan Logical plan to print
     */
    public LogicalPlanPrinter(OperatorPlan plan, PrintStream ps) throws FrontendException {
        super(plan, null);
        mStream = ps;
    }

    @Override
    public void visit() throws FrontendException {
        try {
            if (plan instanceof LogicalPlan) {
                mStream.write(depthFirstLP().getBytes());
            }
            else {
                mStream.write(reverseDepthFirstLP().getBytes());
            }
        } catch (IOException e) {
            throw new FrontendException(e);
        }
    }

    protected String depthFirstLP() throws FrontendException, IOException {
        StringBuilder sb = new StringBuilder();
        List<Operator> leaves = plan.getSinks();
        for (Operator leaf : leaves) {
            sb.append(depthFirst(leaf));
            sb.append("\n");
        }
        return sb.toString();
    }
    
    private String depthFirst(Operator node) throws FrontendException, IOException {
        String nodeString = printNode(node);
        
        List<Operator> originalPredecessors =  plan.getPredecessors(node);
        if (originalPredecessors == null)
            return nodeString;
        
        StringBuffer sb = new StringBuffer(nodeString);
        List<Operator> predecessors =  new ArrayList<Operator>(originalPredecessors);
        
        int i = 0;
        for (Operator pred : predecessors) {
            i++;
            String DFStr = depthFirst(pred);
            if (DFStr != null) {
                sb.append(LSep);
                if (i < predecessors.size())
                    sb.append(shiftStringByTabs(DFStr, 2));
                else
                    sb.append(shiftStringByTabs(DFStr, 1));
            }
        }
        return sb.toString();
    }
    
    protected String reverseDepthFirstLP() throws FrontendException, IOException {
        StringBuilder sb = new StringBuilder();
        List<Operator> roots = plan.getSources();
        for (Operator root : roots) {
            sb.append(reverseDepthFirst(root));
            sb.append("\n");
        }
        return sb.toString();
    }
    
    private String reverseDepthFirst(Operator node) throws FrontendException, IOException {
        String nodeString = printNode(node);
        
        List<Operator> originalSuccessors =  plan.getSuccessors(node);
        if (originalSuccessors == null)
            return nodeString;
        
        StringBuffer sb = new StringBuffer(nodeString);
        List<Operator> successors =  new ArrayList<Operator>(originalSuccessors);
        
        int i = 0;
        for (Operator succ : successors) {
            i++;
            String DFStr = reverseDepthFirst(succ);
            if (DFStr != null) {
                sb.append(LSep);
                if (i < successors.size())
                    sb.append(shiftStringByTabs(DFStr, 2));
                else
                    sb.append(shiftStringByTabs(DFStr, 1));
            }
        }
        return sb.toString();
    }
    
    private String planString(OperatorPlan lp) throws VisitorException, IOException {
        StringBuilder sb = new StringBuilder();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(baos);
        if(lp!=null) {
            LogicalPlanPrinter printer = new LogicalPlanPrinter(lp, ps);
            printer.visit();
        }
        else
            return "";
        sb.append(USep);
        sb.append(shiftStringByTabs(baos.toString(), 2));
        return sb.toString();
    }
    
    private String printNode(Operator node) throws FrontendException, IOException {
        StringBuilder sb = new StringBuilder(node.toString()+"\n");
        
        if(node instanceof LOFilter){
            sb.append(planString(((LOFilter)node).getFilterPlan()));
        }
        else if(node instanceof LOLimit){
            sb.append(planString(((LOLimit)node).getLimitPlan()));
        }
        else if(node instanceof LOForEach){
            sb.append(planString(((LOForEach)node).getInnerPlan()));        
        }
        else if(node instanceof LOCogroup){
            MultiMap<Integer, LogicalExpressionPlan> plans = ((LOCogroup)node).getExpressionPlans();
            for (int i : plans.keySet()) {
                // Visit the associated plans
                for (OperatorPlan plan : plans.get(i)) {
                    sb.append(planString(plan));
                }
            }
        }
        else if(node instanceof LOJoin){
            MultiMap<Integer, LogicalExpressionPlan> plans = ((LOJoin)node).getExpressionPlans();
            for (int i: plans.keySet()) {
                // Visit the associated plans
                for (OperatorPlan plan : plans.get(i)) {
                    sb.append(planString(plan));
                }
            }
        }
        else if(node instanceof LOSort){
            for (OperatorPlan plan : ((LOSort)node).getSortColPlans())
                sb.append(planString(plan));
        }
        else if(node instanceof LOSplitOutput){
            sb.append(planString(((LOSplitOutput)node).getFilterPlan()));
        }
        else if(node instanceof LOGenerate){
            for (OperatorPlan plan : ((LOGenerate)node).getOutputPlans()) {
                sb.append(planString(plan));
            }
        }
        return sb.toString();
    }

    private String shiftStringByTabs(String DFStr, int TabType) {
        StringBuilder sb = new StringBuilder();
        String[] spl = DFStr.split("\n");

        String tab = (TabType == 1) ? TAB1 : TABMore;

        sb.append(spl[0] + "\n");
        for (int i = 1; i < spl.length; i++) {
            sb.append(tab);
            sb.append(spl[i]);
            sb.append("\n");
        }
        return sb.toString();
    }
}

        

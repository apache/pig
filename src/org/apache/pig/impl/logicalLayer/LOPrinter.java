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
package org.apache.pig.impl.logicalLayer;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.io.PrintStream;
import java.io.OutputStream;
import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.util.Collection;
import java.util.Collections;

import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.plan.DepthFirstWalker;
import org.apache.pig.impl.plan.DependencyOrderWalker;
import org.apache.pig.impl.util.MultiMap;
import org.apache.pig.impl.plan.VisitorException;

/**
 * A visitor mechanism printing out the logical plan.
 */
public class LOPrinter extends LOVisitor {

    private PrintStream mStream = null;
    private String TAB1 = "    ";
    private String TABMore = "|   ";
    private String LSep = "|\n|---";
    private String USep = "|   |\n|   ";
    private int levelCntr = -1;
    private OutputStream printer;
    private boolean isVerbose = true;

    /**
     * @param ps PrintStream to output plan information to
     * @param plan Logical plan to print
     */
    public LOPrinter(PrintStream ps, LogicalPlan plan) {
        //super(plan, new DependencyOrderWalker(plan));
        super(plan, new DepthFirstWalker(plan));
        mStream = ps;
    }

    @Override
    public void visit() throws VisitorException {
        try {
            mStream.write(depthFirstLP().getBytes());
        } catch (IOException e) {
            throw new VisitorException(e);
        }
    }

    public void setVerbose(boolean verbose) {
        isVerbose = verbose;
    }

    public void print(OutputStream printer) throws VisitorException, IOException {
        this.printer = printer;
        printer.write(depthFirstLP().getBytes());
    }


    protected String depthFirstLP() throws VisitorException, IOException {
        StringBuilder sb = new StringBuilder();
        List<LogicalOperator> leaves = mPlan.getLeaves();
        Collections.sort(leaves);
        for (LogicalOperator leaf : leaves) {
            sb.append(depthFirst(leaf));
            sb.append("\n");
        }
        //sb.delete(sb.length() - "\n".length(), sb.length());
        //sb.delete(sb.length() - "\n".length(), sb.length());
        return sb.toString();
    }
    
    private String planString(LogicalPlan lp) throws VisitorException, IOException {
        StringBuilder sb = new StringBuilder();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        if(lp!=null)
            lp.explain(baos, mStream);
        else
            return "";
        sb.append(USep);
        sb.append(shiftStringByTabs(baos.toString(), 2));
        return sb.toString();
    }
    
    private String planString(
            List<LogicalPlan> logicalPlanList) throws VisitorException, IOException {
        StringBuilder sb = new StringBuilder();
        if(logicalPlanList!=null)
            for (LogicalPlan lp : logicalPlanList) {
                sb.append(planString(lp));
            }
        return sb.toString();
    }

    private String depthFirst(LogicalOperator node) throws VisitorException, IOException {
        StringBuilder sb = new StringBuilder(node.name());
        if(node instanceof ExpressionOperator) {
            sb.append(" FieldSchema: ");
            try {
                sb.append(((ExpressionOperator)node).getFieldSchema());
            } catch (Exception e) {
                sb.append("Caught Exception: " + e.getMessage());
            }
        } else {
            sb.append(" Schema: ");
            try {
                sb.append(node.getSchema());
            } catch (Exception e) {
                sb.append("Caught exception: " + e.getMessage());
            }
        }
        sb.append(" Type: " + DataType.findTypeName(node.getType()));
        sb.append("\n");

        if (isVerbose) {
            if(node instanceof LOFilter){
                sb.append(planString(((LOFilter)node).getComparisonPlan()));
            }
            else if(node instanceof LOForEach){
                sb.append(planString(((LOForEach)node).getForEachPlans()));        
            }
            else if(node instanceof LOGenerate){
                sb.append(planString(((LOGenerate)node).getGeneratePlans())); 
                
            }
            else if(node instanceof LOCogroup){
                MultiMap<LogicalOperator, LogicalPlan> plans = ((LOCogroup)node).getGroupByPlans();
                for (LogicalOperator lo : plans.keySet()) {
                    // Visit the associated plans
                    for (LogicalPlan plan : plans.get(lo)) {
                        sb.append(planString(plan));
                    }
                }
            }
            else if(node instanceof LOFRJoin){
                MultiMap<LogicalOperator, LogicalPlan> plans = ((LOFRJoin)node).getJoinColPlans();
                for (LogicalOperator lo : plans.keySet()) {
                    // Visit the associated plans
                    for (LogicalPlan plan : plans.get(lo)) {
                        sb.append(planString(plan));
                    }
                }
            }
            else if(node instanceof LOSort){
                sb.append(planString(((LOSort)node).getSortColPlans())); 
            }
            else if(node instanceof LOSplitOutput){
                sb.append(planString(((LOSplitOutput)node).getConditionPlan()));
            }
            else if (node instanceof LOProject) {
                sb.append("Input: ");
                sb.append(((LOProject)node).getExpression().name());
            }
        }
        
        List<LogicalOperator> originalPredecessors =  mPlan.getPredecessors(node);
        if (originalPredecessors == null)
            return sb.toString();
        
        List<LogicalOperator> predecessors =  new ArrayList<LogicalOperator>(originalPredecessors);
        
        Collections.sort(predecessors);
        int i = 0;
        for (LogicalOperator pred : predecessors) {
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

    private void dispTabs() {
        for (int i = 0; i < levelCntr; i++)
            System.out.print(TAB1);
    }
}

        

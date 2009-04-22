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
package org.apache.pig.impl.plan.optimizer;

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
public class RulePlanPrinter extends RulePlanVisitor {

    private PrintStream mStream = null;
    private String TAB1 = "    ";
    private String TABMore = "|   ";
    private String LSep = "|\n|---";
    private String USep = "|   |\n|   ";
    private int levelCntr = -1;
    private OutputStream printer;

    /**
     * @param ps PrintStream to output plan information to
     * @param plan Logical plan to print
     */
    public RulePlanPrinter(PrintStream ps, RulePlan plan) {
        //super(plan, new DependencyOrderWalker(plan));
        super(plan, new DepthFirstWalker(plan));
        mStream = ps;
    }

    @Override
    public void visit() throws VisitorException {
        try {
            mStream.write(depthFirst().getBytes());
        } catch (IOException e) {
            throw new VisitorException(e);
        }
    }

    public void print(OutputStream printer) throws VisitorException, IOException {
        this.printer = printer;
        printer.write(depthFirst().getBytes());
    }


    protected String depthFirst() throws VisitorException, IOException {
        StringBuilder sb = new StringBuilder();
        List<RuleOperator> leaves = mPlan.getLeaves();
        Collections.sort(leaves);
        for (RuleOperator leaf : leaves) {
            sb.append(depthFirst(leaf));
            sb.append("\n");
        }
        //sb.delete(sb.length() - "\n".length(), sb.length());
        //sb.delete(sb.length() - "\n".length(), sb.length());
        return sb.toString();
    }
    
    private String planString(RulePlan rulePlan) throws VisitorException, IOException {
        StringBuilder sb = new StringBuilder();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        if(rulePlan!=null)
            rulePlan.explain(baos, mStream);
        else
            return "";
        sb.append(USep);
        sb.append(shiftStringByTabs(baos.toString(), 2));
        return sb.toString();
    }
    
    private String planString(
            List<RulePlan> rulePlans) throws VisitorException, IOException {
        StringBuilder sb = new StringBuilder();
        if(rulePlans!=null)
            for (RulePlan rulePlan : rulePlans) {
                sb.append(planString(rulePlan));
            }
        return sb.toString();
    }

    private String depthFirst(RuleOperator node) throws VisitorException, IOException {
        StringBuilder sb = new StringBuilder(node.name());
        sb.append("\n");
        
        List<RuleOperator> originalPredecessors =  mPlan.getPredecessors(node);
        if (originalPredecessors == null)
            return sb.toString();
        
        List<RuleOperator> predecessors =  new ArrayList<RuleOperator>(originalPredecessors);
        
        Collections.sort(predecessors);
        int i = 0;
        for (RuleOperator pred : predecessors) {
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

        

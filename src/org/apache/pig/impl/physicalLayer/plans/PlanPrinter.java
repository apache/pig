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
package org.apache.pig.impl.physicalLayer.plans;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.pig.impl.physicalLayer.topLevelOperators.POFilter;
import org.apache.pig.impl.physicalLayer.topLevelOperators.POForEach;
import org.apache.pig.impl.physicalLayer.topLevelOperators.POGenerate;
import org.apache.pig.impl.physicalLayer.topLevelOperators.POGlobalRearrange;
import org.apache.pig.impl.physicalLayer.topLevelOperators.POLoad;
import org.apache.pig.impl.physicalLayer.topLevelOperators.POLocalRearrange;
import org.apache.pig.impl.physicalLayer.topLevelOperators.POPackage;
import org.apache.pig.impl.physicalLayer.topLevelOperators.POSort;
import org.apache.pig.impl.physicalLayer.topLevelOperators.POStore;
import org.apache.pig.impl.physicalLayer.topLevelOperators.POUnion;
import org.apache.pig.impl.plan.DepthFirstWalker;
import org.apache.pig.impl.plan.Operator;
import org.apache.pig.impl.plan.OperatorPlan;
import org.apache.pig.impl.plan.PlanVisitor;
import org.apache.pig.impl.plan.VisitorException;

public class PlanPrinter<O extends Operator, P extends OperatorPlan<O>> extends
        PlanVisitor<O, P> {

    String TAB1 = "    ";

    String TABMore = "|   ";

    String LSep = "|\n|---";
    
    String USep = "|   |\n|   ";

    int levelCntr = -1;

    OutputStream printer;

    public PlanPrinter(P plan) {
        super(plan, new DepthFirstWalker<O, P>(plan));
    }

    @Override
    public void visit() throws VisitorException {
        try {
            System.out.write(depthFirstPP().getBytes());
        } catch (IOException e) {
            throw new VisitorException(e.getMessage());
        }
    }

    public void print(OutputStream printer) throws VisitorException, IOException {
        this.printer = printer;
        printer.write(depthFirstPP().getBytes());
    }

    protected void breadthFirst() throws VisitorException {
        List<O> leaves = mPlan.getLeaves();
        Set<O> seen = new HashSet<O>();
        breadthFirst(leaves, seen);
    }

    private void breadthFirst(Collection<O> predecessors, Set<O> seen)
            throws VisitorException {
        ++levelCntr;
        dispTabs();

        List<O> newPredecessors = new ArrayList<O>();
        for (O pred : predecessors) {
            if (seen.add(pred)) {
                List<O> predLst = mPlan.getPredecessors(pred);
                if (predLst != null)
                    newPredecessors.addAll(predLst);

                pred.visit(this);
            }
        }
        if (newPredecessors.size() > 0) {
            System.out.println();
            breadthFirst(newPredecessors, seen);
        }
    }

    protected String depthFirstPP() throws VisitorException {
        StringBuilder sb = new StringBuilder();
        List<O> leaves = mPlan.getLeaves();
        Collections.sort(leaves);
        for (O leaf : leaves) {
            sb.append(depthFirst(leaf));
            sb.append("\n");
        }
        sb.delete(sb.length() - "\n".length(), sb.length());
        sb.delete(sb.length() - "\n".length(), sb.length());
        return sb.toString();
    }
    
    private String planString(PhysicalPlan pp){
        StringBuilder sb = new StringBuilder();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        if(pp!=null)
            pp.explain(baos);
        else
            return "";
        sb.append(USep);
        sb.append(shiftStringByTabs(baos.toString(), 2));
        return sb.toString();
    }
    
    private String planString(List<ExprPlan> lep){
        StringBuilder sb = new StringBuilder();
        if(lep!=null)
            for (ExprPlan ep : lep) {
                sb.append(planString(ep));
            }
        return sb.toString();
    }

    private String depthFirst(O node) throws VisitorException {
        StringBuilder sb = new StringBuilder(node.name() + "\n");
        if(node instanceof POFilter){
            sb.append(planString(((POFilter)node).getPlan()));
        }
        else if(node instanceof POForEach){
            sb.append(planString(((POForEach)node).getPlan()));        
        }
        else if(node instanceof POGenerate){
            sb.append(planString(((POGenerate)node).getInputPlans())); 
            
        }
        else if(node instanceof POLocalRearrange){
            sb.append(planString(((POLocalRearrange)node).getPlans()));
        }
        else if(node instanceof POSort){
            sb.append(planString(((POSort)node).getSortPlans())); 
        }
        
        List<O> predecessors = mPlan.getPredecessors(node);
        
        if (predecessors == null)
            return sb.toString();
        
        Collections.sort(predecessors);
        int i = 0;
        for (O pred : predecessors) {
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

    public void visitLoad(POLoad op) {
        System.out.print(op.name() + "   ");
    }

    public void visitStore(POStore op) {
        System.out.print(op.name() + "   ");
    }

    public void visitFilter(POFilter op) {
        System.out.print(op.name() + "   ");
    }

    public void visitLocalRearrange(POLocalRearrange op) {
        System.out.print(op.name() + "   ");
    }

    public void visitGlobalRearrange(POGlobalRearrange op) {
        System.out.print(op.name() + "   ");
    }

    public void visitPackage(POPackage op) {
        System.out.print(op.name() + "   ");
    }

    public void visitStartMap(POUnion op) {
        System.out.print(op.name() + "   ");
    }
    
    /*public static void main(String[] args) throws PlanException, ExecException {
        ExprPlan ep = GenPhyOp.arithPlan();
        GreaterThanExpr gt = GenPhyOp.compGreaterThanExpr();
        ConstantExpression ce = GenPhyOp.exprConst();
        ce.setValue(50);
        ep.add(ce);
        ep.addAsLeaf(gt);
        
        POFilter fil = GenPhyOp.topFilterOp();
        fil.setPlan(ep);
        
        PhysicalPlan php = new PhysicalPlan();
        php.add(fil);
        int fields[] = {0};
        Tuple sample = TupleFactory.getInstance().newTuple();
        sample.append("S");
//        POForEach fe = GenPhyOp.topForEachOPWithPlan(fields, sample);
        POForEach fe = GenPhyOp.topForEachOp();
        php.add(fe);
        php.connect(fil, fe);
        php.explain(System.out);
    }*/
}

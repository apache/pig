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
package org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.pig.data.Tuple;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.BinaryExpressionOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.ExpressionOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POBinCond;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.UnaryComparisonOperator;
import org.apache.pig.impl.plan.OperatorPlan;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.util.MultiMap;

/**
 * 
 * The base class for all types of physical plans. 
 * This extends the Operator Plan.
 *
 */
public class PhysicalPlan extends OperatorPlan<PhysicalOperator> implements Cloneable {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    
    // marker to indicate whether all input for this plan
    // has been sent - this is currently only used in POStream
    // to know if all map() calls and reduce() calls are finished
    // and that there is no more input expected.
    public boolean endOfAllInput = false;

    private MultiMap<PhysicalOperator, PhysicalOperator> opmap = null;
    
    public PhysicalPlan() {
        super();
    }
    
    public void attachInput(Tuple t){
        List<PhysicalOperator> roots = getRoots();
        for (PhysicalOperator operator : roots) {
            operator.attachInput(t);
		}
    }
    
    public void detachInput(){
        for(PhysicalOperator op : getRoots())
            op.detachInput();
    }
    /**
     * Write a visual representation of the Physical Plan
     * into the given output stream
     * @param out : OutputStream to which the visual representation is written
     */
    public void explain(OutputStream out) {
        explain(out, true);
    }

    /**
     * Write a visual representation of the Physical Plan
     * into the given output stream
     * @param out : OutputStream to which the visual representation is written
     * @param verbose : Amount of information to print
     */
    public void explain(OutputStream out, boolean verbose){
        PlanPrinter<PhysicalOperator, PhysicalPlan> mpp = new PlanPrinter<PhysicalOperator, PhysicalPlan>(
                this);
        mpp.setVerbose(verbose);

        try {
            mpp.print(out);
        } catch (VisitorException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    /**
     * Write a visual representation of the Physical Plan
     * into the given printstream
     * @param ps : PrintStream to which the visual representation is written
     * @param format : Format to print in
     * @param verbose : Amount of information to print
     */
    public void explain(PrintStream ps, String format, boolean verbose) {
        ps.println("#-----------------------------------------------");
        ps.println("# Physical Plan:");
        ps.println("#-----------------------------------------------");

        if (format.equals("text")) {
            explain((OutputStream)ps, verbose);
            ps.println("");
        } else if (format.equals("dot")) {
            DotPOPrinter pp = new DotPOPrinter(this, ps);
            pp.setVerbose(verbose);
            pp.dump();
        }
        ps.println("");
  }
    
    @Override
    public void connect(PhysicalOperator from, PhysicalOperator to)
            throws PlanException {
        
        super.connect(from, to);
        to.setInputs(getPredecessors(to));
    }
    
    /*public void connect(List<PhysicalOperator> from, PhysicalOperator to) throws IOException{
        if(!to.supportsMultipleInputs()){
            throw new IOException("Invalid Operation on " + to.name() + ". It doesn't support multiple inputs.");
        }
        
    }*/
    
    @Override
    public void remove(PhysicalOperator op) {
        op.setInputs(null);
        List<PhysicalOperator> sucs = getSuccessors(op);
        if(sucs!=null && sucs.size()!=0){
            for (PhysicalOperator suc : sucs) {
                // successor could have multiple inputs
                // for example = POUnion - remove op from
                // its list of inputs - if after removal
                // there are no other inputs, set successor's
                // inputs to null
                List<PhysicalOperator> succInputs = suc.getInputs();
                succInputs.remove(op);
                if(succInputs.size() == 0)
                    suc.setInputs(null);
                else
                    suc.setInputs(succInputs);
            }
        }
        super.remove(op);
    }
    
    /* (non-Javadoc)
     * @see org.apache.pig.impl.plan.OperatorPlan#replace(org.apache.pig.impl.plan.Operator, org.apache.pig.impl.plan.Operator)
     */
    @Override
    public void replace(PhysicalOperator oldNode, PhysicalOperator newNode)
            throws PlanException {
        List<PhysicalOperator> oldNodeSuccessors = getSuccessors(oldNode);
        super.replace(oldNode, newNode);
        if(oldNodeSuccessors != null) {
            for (PhysicalOperator preds : oldNodeSuccessors) {
                List<PhysicalOperator> inputs = preds.getInputs();
                // now replace oldNode with newNode in
                // the input list of oldNode's successors
                for(int i = 0; i < inputs.size(); i++) {
                    if(inputs.get(i) == oldNode) {
                        inputs.set(i, newNode);
                    }
                }    
            }
        }
        
    }

    /* (non-Javadoc)
     * @see org.apache.pig.impl.plan.OperatorPlan#add(org.apache.pig.impl.plan.Operator)
    @Override
    public void add(PhysicalOperator op) {
        // attach this plan as the plan the operator is part of
        //op.setParentPlan(this);
        super.add(op);
    }
*/

    public boolean isEmpty() {
        return (mOps.size() == 0);
    }

    @Override
    public String toString() {
        if(isEmpty())
            return "Empty Plan!";
        else{
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            explain(baos, true);
            return baos.toString();
        }
    }

    @Override
    public PhysicalPlan clone() throws CloneNotSupportedException {
        PhysicalPlan clone = new PhysicalPlan();

        // Get all the nodes in this plan, and clone them.  As we make
        // clones, create a map between clone and original.  Then walk the
        // connections in this plan and create equivalent connections in the
        // clone.
        Map<PhysicalOperator, PhysicalOperator> matches = 
            new HashMap<PhysicalOperator, PhysicalOperator>(mOps.size());
        for (PhysicalOperator op : mOps.keySet()) {
            PhysicalOperator c = op.clone();
            clone.add(c);
            if (opmap != null)
                opmap.put(op, c);
            matches.put(op, c);
        }

        // Build the edges
        for (PhysicalOperator op : mFromEdges.keySet()) {
            PhysicalOperator cloneFrom = matches.get(op);
            if (cloneFrom == null) {
                String msg = "Unable to find clone for op " + op.name();
                throw new CloneNotSupportedException(msg);
            }
            Collection<PhysicalOperator> toOps = mFromEdges.get(op);
            for (PhysicalOperator toOp : toOps) {
                PhysicalOperator cloneTo = matches.get(toOp);
                if (cloneTo == null) {
                    String msg = "Unable to find clone for op " + toOp.name();
                    throw new CloneNotSupportedException(msg);
                }
                try {
                    clone.connect(cloneFrom, cloneTo);
                } catch (PlanException pe) {
                    CloneNotSupportedException cnse = new CloneNotSupportedException();
                    cnse.initCause(pe);
                    throw cnse;
                }
            }
        }

        // Fix up all the inputs in the operators themselves.
        for (PhysicalOperator op : mOps.keySet()) {
            List<PhysicalOperator> inputs = op.getInputs();
            if (inputs == null || inputs.size() == 0) continue;
            List<PhysicalOperator> newInputs = 
                new ArrayList<PhysicalOperator>(inputs.size());
            PhysicalOperator cloneOp = matches.get(op);
            if (cloneOp == null) {
                String msg = "Unable to find clone for op " + op.name();
                throw new CloneNotSupportedException(msg);
            }
            for (PhysicalOperator iOp : inputs) {
                PhysicalOperator cloneIOp = matches.get(iOp);
                if (cloneIOp == null) {
                    String msg = "Unable to find clone for op " + iOp.name();
                    throw new CloneNotSupportedException(msg);
                }
                newInputs.add(cloneIOp);
            }
            cloneOp.setInputs(newInputs);
        }
        
        for (PhysicalOperator op : mOps.keySet()) {
            if (op instanceof UnaryComparisonOperator) {
                UnaryComparisonOperator orig = (UnaryComparisonOperator)op;
                UnaryComparisonOperator cloneOp = (UnaryComparisonOperator)matches.get(op);
                cloneOp.setExpr((ExpressionOperator)matches.get(orig.getExpr()));
                cloneOp.setOperandType(orig.getOperandType());
            } else if (op instanceof BinaryExpressionOperator) {
                BinaryExpressionOperator orig = (BinaryExpressionOperator)op;
                BinaryExpressionOperator cloneOp = (BinaryExpressionOperator)matches.get(op);
                cloneOp.setRhs((ExpressionOperator)matches.get(orig.getRhs()));
                cloneOp.setLhs((ExpressionOperator)matches.get(orig.getLhs()));
            } else if (op instanceof POBinCond) {
                POBinCond orig = (POBinCond)op;
                POBinCond cloneOp = (POBinCond)matches.get(op);
                cloneOp.setRhs((ExpressionOperator)matches.get(orig.getRhs()));
                cloneOp.setLhs((ExpressionOperator)matches.get(orig.getLhs()));
                cloneOp.setCond((ExpressionOperator)matches.get(orig.getCond()));
            }
        }

        return clone;
    }
    
    public void setOpMap(MultiMap<PhysicalOperator, PhysicalOperator> opmap) {
        this.opmap = opmap;
    }
    
    public void resetOpMap()
    {
        opmap = null;
    }
}

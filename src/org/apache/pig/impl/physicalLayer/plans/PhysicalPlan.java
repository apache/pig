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
import java.util.List;

import org.apache.pig.data.Tuple;
import org.apache.pig.impl.physicalLayer.topLevelOperators.PhysicalOperator;
import org.apache.pig.impl.plan.OperatorPlan;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.plan.VisitorException;

/**
 * 
 * The base class for all types of physical plans. 
 * This extends the Operator Plan.
 *
 * @param <E>
 */
public class PhysicalPlan<E extends PhysicalOperator> extends OperatorPlan<E> {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public PhysicalPlan() {
        super();
    }
    
    public void attachInput(Tuple t){
        List<E> roots = getRoots();
        for (E operator : roots)
            operator.attachInput(t);
    }
    
    /**
     * Write a visual representation of the Physical Plan
     * into the given output stream
     * @param out : OutputStream to which the visual representation is written
     */
    public void explain(OutputStream out){
        PlanPrinter<E, PhysicalPlan<E>> mpp = new PlanPrinter<E, PhysicalPlan<E>>(
                this);

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
    
    @Override
    public void connect(E from, E to)
            throws PlanException {
        super.connect(from, to);
        to.setInputs(getPredecessors(to));
    }
    
    /*public void connect(List<E> from, E to) throws IOException{
        if(!to.supportsMultipleInputs()){
            throw new IOException("Invalid Operation on " + to.name() + ". It doesn't support multiple inputs.");
        }
        
    }*/
    
    @Override
    public void remove(E op) {
        op.setInputs(null);
        List<E> sucs = getSuccessors(op);
        if(sucs!=null && sucs.size()!=0){
            for (E suc : sucs) {
                suc.setInputs(null);
            }
        }
        super.remove(op);
    }

    public boolean isEmpty() {
        return (mOps.size() == 0);
    }

    @Override
    public String toString() {
        if(isEmpty())
            return "Empty Plan!";
        else{
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            explain(baos);
            return baos.toString();
        }
    }

    @Override
    public boolean equals(Object obj) {
        // TODO Auto-generated method stub
        return super.equals(obj);
    }
    
    
    
}

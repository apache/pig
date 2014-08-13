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
package org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators;

import java.util.ArrayList;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.data.DataType;
import org.apache.pig.data.NonSpillableDataBag;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;

/**
 * Implements a specialized form of POProject which is
 * used *ONLY* in the following case:
 * This project is Project(*) introduced after a relational operator
 * to supply a bag as output (as an expression). This project is either
 * providing the bag as input to a successor expression operator or is 
 * itself the leaf in a inner plan
 * If the predecessor relational operator sends an EOP
 * then send an empty bag first to signal "empty" output
 * and then send an EOP

 * NOTE: A Project(*) of return type BAG whose predecessor is
 * from an outside plan (i.e. not in the same inner plan as the project)
 * will NOT lead us here. So a query like:
 * a = load 'baginp.txt' as (b:bag{t:tuple()}); b = foreach a generate $0; dump b;
 * will go through a regular project (without the following flag)
 */
public class PORelationToExprProject extends POProject {
    
    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    boolean sendEmptyBagOnEOP = false;
    
    public PORelationToExprProject(OperatorKey k) {
        this(k,-1,0);
    }

    public PORelationToExprProject(OperatorKey k, int rp) {
        this(k, rp, 0);
    }
    
    public PORelationToExprProject(OperatorKey k, int rp, int col) {
        super(k, rp, col);
    }

    public PORelationToExprProject(OperatorKey k, int rp, ArrayList<Integer> cols) {
        super(k, rp, cols);
    }

    @Override
    public String name() {
        
        return "RelationToExpressionProject" + "[" + DataType.findTypeName(resultType) + "]" + ((isStar()) ? "[*]" : columns) + " - " + mKey.toString();
    }

    @Override
    public void visit(PhyPlanVisitor v) throws VisitorException {
        // for now the specialization in this class
        // does not affect the way visitors visit it - so
        // we can just use visitProject()
        v.visitProject(this);
    }
    
    /* (non-Javadoc)
     * @see org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator#reset()
     */
    @Override
    public void reset() {
        // the foreach in which this operator is
        // present is starting with a new set of inputs
        // if we see an EOP from the predecessor *first*
        // (.i.e we do not see any other input before and EOP
        // and only see an EOP - this can happen if a Filter
        // is the predecessor and it filters away all its input)
        // we should send an empty bag. Set a flag which can be 
        // checked if an EOP is encountered.
        sendEmptyBagOnEOP = true;
    }
    
    @Override
    public Result getNextDataBag() throws ExecException {
        Result input = processInputBag();
        
        // if this is called during accumulation, it is ok to have an empty bag
        // we need to send STATUS_OK so that the UDF can be called.
        if (isAccumulative()) {
            reset();
        }
        
        if(input.returnStatus!=POStatus.STATUS_OK) {
            if(input.returnStatus == POStatus.STATUS_NULL){
                return input;
            } else if (input.returnStatus == POStatus.STATUS_EOP && sendEmptyBagOnEOP)  {
                // we received an EOP from the predecessor
                // since the successor in the pipeline is
                // expecting a bag, send an empty bag
                input.result = new NonSpillableDataBag();
                input.returnStatus = POStatus.STATUS_OK;
                // we should send EOP the next time we are called
                // if the foreach in which this operator is present
                // calls this.getNext(bag) with new inputs then
                // this flag will be reset in this.reset()
            } else {
                // since we are sending down some result (empty bag or otherwise)
                // we should not be sending an empty bag on EOP any more UNLESS
                // we are processing new inputs (see reset())
                sendEmptyBagOnEOP = false;
                return input;
            }
        }
        Result r = consumeInputBag(input);
        // since we are sending down some result (empty bag or otherwise)
        // we should not be sending an empty bag on EOP any more UNLESS
        // we are processing new inputs (see reset())
        sendEmptyBagOnEOP = false;
        return(r);
    }
       
    @Override
    public PORelationToExprProject clone() throws CloneNotSupportedException {
        return (PORelationToExprProject) super.clone();
    }
    
}

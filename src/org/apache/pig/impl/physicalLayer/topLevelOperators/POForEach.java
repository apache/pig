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
package org.apache.pig.impl.physicalLayer.topLevelOperators;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DefaultBagFactory;
import org.apache.pig.data.DefaultTuple;
import org.apache.pig.data.IndexedTuple;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.OperatorKey;
import org.apache.pig.impl.physicalLayer.POStatus;
import org.apache.pig.impl.physicalLayer.Result;
import org.apache.pig.impl.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.impl.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.impl.plan.VisitorException;

/**
 * The foreach operator 
 * It has an embedded physical plan that
 * generates tuples as per the specification.
 */
public class POForEach extends PhysicalOperator<PhyPlanVisitor> {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    private Log log = LogFactory.getLog(getClass());

    PhysicalPlan<PhysicalOperator> plan;

    POGenerate gen;
    
    //Since the plan has a generate, this needs to be maintained
    //as the generate can potentially return multiple tuples for
    //same call.
    private boolean processingPlan = false;

    public POForEach(OperatorKey k) {
        this(k, -1, null);
    }

    public POForEach(OperatorKey k, int rp) {
        this(k, rp, null);
    }

    public POForEach(OperatorKey k, List<PhysicalOperator> inp) {
        this(k, -1, inp);
    }

    public POForEach(OperatorKey k, int rp, List<PhysicalOperator> inp) {
        super(k, rp, inp);
    }

    @Override
    public void visit(PhyPlanVisitor v) throws VisitorException {
        v.visitForEach(this);
    }

    @Override
    public String name() {
        return "For Each - " + mKey.toString();
    }

    @Override
    public boolean supportsMultipleInputs() {
        return false;
    }

    @Override
    public boolean supportsMultipleOutputs() {
        return false;
    }
    
    /**
     * Overridden since the attachment of the new input should cause the old
     * processing to end.
     */
    @Override
    public void attachInput(Tuple t) {
        super.attachInput(t);
        processingPlan = false;
    }
    
    /**
     * Calls getNext on the generate operator inside the nested
     * physical plan and returns it maintaining an additional state
     * to denote the begin and end of the nested plan processing.
     */
    @Override
    public Result getNext(Tuple t) throws ExecException {
        Result res = null;
        Result inp = null;
        //The nested plan is under processing
        //So return tuples that the generate oper
        //returns
        if(processingPlan){
            while(true) {
                res = gen.getNext(t);
                if(res.returnStatus==POStatus.STATUS_OK){
                    return res;
                }
                if(res.returnStatus==POStatus.STATUS_EOP){
                    processingPlan = false;
                    break;
                }
                if(res.returnStatus==POStatus.STATUS_ERR)
                    return res;
                if(res.returnStatus==POStatus.STATUS_NULL)
                    continue;
            }
        }
        //The nested plan processing is done or is
        //yet to begin. So process the input and start
        //nested plan processing on the input tuple
        //read
        while (true) {
            inp = processInput();
            if (inp.returnStatus == POStatus.STATUS_EOP || inp.returnStatus == POStatus.STATUS_ERR)
                return inp;
            if (inp.returnStatus == POStatus.STATUS_NULL)
                continue;
            
            plan.attachInput((Tuple) inp.result);
            
            res = gen.getNext(t);
            
            /*if (inp.returnStatus == POStatus.STATUS_EOP || inp.returnStatus == POStatus.STATUS_ERR)
                return inp;
            if(inp.returnStatus == POStatus.STATUS_NULL)
                continue;*/
            
            processingPlan = true;
            
            return res;
        }
    }

    public PhysicalPlan<PhysicalOperator> getPlan() {
        return plan;
    }

    public void setPlan(PhysicalPlan<PhysicalOperator> plan) {
        this.plan = plan;
        gen = (POGenerate) plan.getLeaves().get(0);
    }
}

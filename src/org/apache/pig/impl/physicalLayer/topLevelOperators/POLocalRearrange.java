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
import org.apache.pig.data.DefaultTuple;
import org.apache.pig.data.IndexedTuple;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.OperatorKey;
import org.apache.pig.impl.logicalLayer.parser.ParseException;
import org.apache.pig.impl.physicalLayer.POStatus;
import org.apache.pig.impl.physicalLayer.Result;
import org.apache.pig.impl.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.impl.physicalLayer.plans.PhysicalPlan;

/**
 * The local rearrange operator is a part of the co-group
 * implementation. It has an embedded physical plan that
 * generates tuples of the form (grpKey,(indxed inp Tuple)).
 *
 */
public class POLocalRearrange extends PhysicalOperator<PhyPlanVisitor> {

    private Log log = LogFactory.getLog(getClass());

    PhysicalPlan<PhysicalOperator> plan;

    // The position of this LR in the package operator
    int index;

    POGenerate gen;
    
    //Since the plan has a generate, this needs to be maintained
    //as the generate can potentially return multiple tuples for
    //same call.
    private boolean processingPlan = false;

    public POLocalRearrange(OperatorKey k) {
        this(k, -1, null);
    }

    public POLocalRearrange(OperatorKey k, int rp) {
        this(k, rp, null);
    }

    public POLocalRearrange(OperatorKey k, List<PhysicalOperator> inp) {
        this(k, -1, inp);
    }

    public POLocalRearrange(OperatorKey k, int rp, List<PhysicalOperator> inp) {
        super(k, rp, inp);
        index = -1;
    }

    @Override
    public void visit(PhyPlanVisitor v) throws ParseException {
        v.visitLocalRearrange(this);
    }

    @Override
    public String name() {
        return "Local Rearrange - " + mKey.toString();
    }

    @Override
    public boolean supportsMultipleInputs() {
        return false;
    }

    @Override
    public boolean supportsMultipleOutputs() {
        return false;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
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
     * physical plan. Converts the generated tuple into the proper
     * format, i.e, (key,{(value)})
     */
    @Override
    public Result getNext(Tuple t) throws ExecException {
        Result res = null;
        Result inp = null;
        //The nested plan is under processing
        //So return tuples that the generate oper
        //returns after converting them to the required
        //format
        if(processingPlan){
            while(true) {
                res = gen.getNext(t);
                if(res.returnStatus==POStatus.STATUS_OK){
                    res.result = constructLROutput((Tuple)res.result);
                    return res;
                }
                if(res.returnStatus==POStatus.STATUS_ERR)
                    return res;
                if(res.returnStatus==POStatus.STATUS_NULL)
                    continue;
                if(res.returnStatus==POStatus.STATUS_EOP){
                    processingPlan = false;
                    break;
                }
            }
        }
        //The nested plan processing is done or is
        //yet to begin. So process the input and start
        //nested plan processing on the input tuple
        //read
        while (true) {
            inp = processInput();
            if (inp.returnStatus == POStatus.STATUS_EOP || inp.returnStatus == POStatus.STATUS_ERR)
                break;
            if (inp.returnStatus == POStatus.STATUS_NULL)
                continue;
            
            plan.attachInput((Tuple) inp.result);
            
            res = gen.getNext(t);
            if (inp.returnStatus == POStatus.STATUS_EOP || inp.returnStatus == POStatus.STATUS_ERR)
                break;
            if(inp.returnStatus == POStatus.STATUS_NULL)
                continue;
            
            processingPlan = true;
            
            res.result = constructLROutput((Tuple)res.result);
            return res;
        }
        return inp;
    }
    
    private Tuple constructLROutput(Tuple genOut){
        //Strip the input tuple off its key which
        //will be the first field in the tuple
        Object key = genOut.getAll().remove(0);
        
        //Create the indexed tuple out of the value
        //that is remaining in the input tuple
        IndexedTuple it = new IndexedTuple(genOut, index);
        
        //Put the key and the indexed tuple
        //in a tuple and return
        Tuple outPut = new DefaultTuple();
        outPut.append(key);
        outPut.append(it);
        return outPut;
    }

    public PhysicalPlan<PhysicalOperator> getPlan() {
        return plan;
    }

    public void setPlan(PhysicalPlan<PhysicalOperator> plan) {
        this.plan = plan;
        gen = (POGenerate) plan.getLeaves().get(0);
    }
}

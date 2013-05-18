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
package org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.io.PigNullableWritable;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;

/**
 * The MapReduce Demultiplexer operator.
 * <p>
 * This operator is used when merging multiple Map-Reduce splittees
 * into a Map-only splitter during multi-query optimization. 
 * The reduce physical plans of the splittees become the inner plans 
 * of this operator.
 * <p>
 * Due to the recursive nature of multi-query optimization, this operator
 * may be contained in another demux operator.
 * <p>
 * The predecessor of this operator must be a POMultiQueryPackage
 * operator which passes the index (indicating which inner reduce plan to run)
 * along with other data to this operator.    
 */
public class PODemux extends PhysicalOperator {

    private static final long serialVersionUID = 1L;    
    
    private static int idxPart = 0x7F;
    
    private static Result empty = new Result(POStatus.STATUS_NULL, null);
    
    private static Result eop = new Result(POStatus.STATUS_EOP, null);
    
    /*
     * The list of sub-plans the inner plan is composed of
     */
    private ArrayList<PhysicalPlan> myPlans = new ArrayList<PhysicalPlan>();
    
    /*
     * Flag indicating when a new pull should start 
     */
    private boolean getNext = true;
    
    /*
     * Flag indicating when a new pull should start. 
     * It's used only when receiving the call
     * from reducer's close() method in the streaming case.
     */
    private boolean inpEOP = false;
    
    /*
     * The leaf of the current pipeline
     */
    private PhysicalOperator curLeaf = null;

    
    /**
     * The current pipeline plan
     */
    private PhysicalPlan curPlan = null;
    
    /*
     * Indicating if this operator is in a combiner. 
     * If not, this operator is in a reducer and the key
     * values must first be extracted from the tuple-wrap
     * before writing out to the disk
     */
    private boolean inCombiner = false;
    
    BitSet processedSet = new BitSet();
    
    /**
     * Constructs an operator with the specified key.
     * 
     * @param k the operator key
     */
    public PODemux(OperatorKey k) {
        this(k, -1, null);
    }

    /**
     * Constructs an operator with the specified key
     * and degree of parallelism.
     *  
     * @param k the operator key
     * @param rp the degree of parallelism requested
     */
    public PODemux(OperatorKey k, int rp) {
        this(k, rp, null);
    }

    /**
     * Constructs an operator with the specified key and inputs.
     *  
     * @param k the operator key
     * @param inp the inputs that this operator will read data from
     */
    public PODemux(OperatorKey k, List<PhysicalOperator> inp) {
        this(k, -1, inp);
    }

    /**
     * Constructs an operator with the specified key,
     * degree of parallelism and inputs.
     * 
     * @param k the operator key
     * @param rp the degree of parallelism requested 
     * @param inp the inputs that this operator will read data from
     */
    public PODemux(OperatorKey k, int rp, List<PhysicalOperator> inp) {
        super(k, rp, inp);
    }

    @Override
    public void visit(PhyPlanVisitor v) throws VisitorException {
        v.visitDemux(this);
    }

    @Override
    public String name() {
        return getAliasString() + "Demux [" + myPlans.size() + "] "+ mKey.toString();
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
     * Returns the list of inner plans.
     *  
     * @return the list of the nested plans
     */
    public List<PhysicalPlan> getPlans() {
        return myPlans;
    }
    
    /**
     * Appends the specified plan at the end of the list.
     * 
     * @param inPlan plan to be appended to the inner plan list
     */
    
    public void addPlan(PhysicalPlan inPlan) {  
        myPlans.add(inPlan);
        processedSet.set(myPlans.size()-1);
    }
   
    @Override
    public Result getNextTuple() throws ExecException {
        
        if (!inCombiner && this.parentPlan.endOfAllInput) {
            
            // If there is a stream in one of the inner plans, 
            // there could potentially be more to process - the 
            // reducer sets the flag stating that all map input has 
            // been sent already and runs the pipeline one more time
            // in its close() call.
            return getStreamCloseResult();                 
        
        } else { 
        
            if (getNext) {
                if(curPlan != null)
                    curPlan.detachInput();
                Result inp = processInput();
                
                if (inp.returnStatus == POStatus.STATUS_EOP) {
                    return inp;
                }
             
                curLeaf = attachInputWithIndex((Tuple)inp.result);
    
                getNext = false;
            }
            
            return runPipeline(curLeaf);      
        }
    }
    
    private Result runPipeline(PhysicalOperator leaf) throws ExecException {
       
        Result res = null;
        
        while (true) {
            
            res = leaf.getNextTuple();
            
            if (res.returnStatus == POStatus.STATUS_OK ||
                    res.returnStatus == POStatus.STATUS_EOP || 
                    res.returnStatus == POStatus.STATUS_ERR) {                
                break;
            } else if (res.returnStatus == POStatus.STATUS_NULL) {
                continue;
            } 
        }   
        
        if (res.returnStatus == POStatus.STATUS_EOP) {
            getNext = true;
        }
        return (res.returnStatus == POStatus.STATUS_OK || res.returnStatus == POStatus.STATUS_ERR) ? res : empty;
    }

    private Result getStreamCloseResult() throws ExecException {
        Result res = null;
       
        while (true) {
                       
            if (processedSet.cardinality() == myPlans.size()) {
                curLeaf = null;
                if(curPlan != null)
                    curPlan.detachInput();
                Result inp = processInput();
                if (inp.returnStatus == POStatus.STATUS_OK) {                
                    attachInputWithIndex((Tuple)inp.result);
                    inpEOP = false;
                } else if (inp.returnStatus == POStatus.STATUS_EOP){
                    inpEOP = true;
                } else if (inp.returnStatus == POStatus.STATUS_NULL) {
                    inpEOP = false;
                } else if (inp.returnStatus == POStatus.STATUS_ERR) {
                    return inp;
                }            
                processedSet.clear();
            }
            
            int idx = processedSet.nextClearBit(0);
            PhysicalOperator leaf = myPlans.get(idx).getLeaves().get(0);
            
            // a nested demux object is stored in multiple positions 
            // of the inner plan list, corresponding to the indexes of 
            // its inner plans; skip the object if it's already processed.
            if (curLeaf != null && leaf.getOperatorKey().equals(curLeaf.getOperatorKey())) {
                processedSet.set(idx++); 
                if (idx < myPlans.size()) {
                    continue;
                } else {
                    res = eop;
                }
            } else {
                curLeaf = leaf;
                res = leaf.getNextTuple();
               
                if (res.returnStatus == POStatus.STATUS_EOP)  {
                    processedSet.set(idx++);        
                    if (idx < myPlans.size()) {
                        continue;
                    }
                } else {
                    break;
                }
            
            }
            
            if (!inpEOP && res.returnStatus == POStatus.STATUS_EOP) {
                continue;
            } else {
                break;
            }
        }
        
        return res;              
    }    
    
    private PhysicalOperator attachInputWithIndex(Tuple res) throws ExecException {
        
        // unwrap the first field of the tuple to get the wrapped value which
        // is expected by the inner plans, as well as the index of the associated
        // inner plan.
        PigNullableWritable fld = (PigNullableWritable)res.get(0);        
        // choose an inner plan to run based on the index set by
        // the POLocalRearrange operator and passed to this operator
        // by POMultiQueryPackage
        int index = fld.getIndex();
        index &= idxPart;                      

        curPlan = myPlans.get(index);
        if (!(curPlan.getRoots().get(0) instanceof PODemux)) {                             
            res.set(0, fld.getValueAsPigType());
        }
        
        curPlan.attachInput(res);
        return curPlan.getLeaves().get(0);
    }
    
    /**
     * Sets a flag indicating if this operator is 
     * in a combiner. 
     * 
     * @param inCombiner true if this operator is in
     * a combiner; false if this operator is in a reducer
     */
    public void setInCombiner(boolean inCombiner) {
        this.inCombiner = inCombiner;
    }

    /**
     * Returns a flag indicating if this operator is 
     * in a combiner.
     * 
     * @return true if this operator is in a combiner;
     * otherwise this operator is in a reducer
     */
    public boolean isInCombiner() {
        return inCombiner;
    }
    
    @Override
    public Tuple illustratorMarkup(Object in, Object out, int eqClassIndex) {
        // nothing need to be done here
        return null;
    }
        
}

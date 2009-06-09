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
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.PigException;
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
    
    private Log log = LogFactory.getLog(getClass());
    
    /*
     * The base index of this demux. In the case of
     * a demux contained in another demux, the index
     * passed in must be shifted before it can be used.
     */
    private int baseIndex = 0;
    
    /*
     * The list of sub-plans the inner plan is composed of
     */
    private ArrayList<PhysicalPlan> myPlans = new ArrayList<PhysicalPlan>();
    
    /**
     * If the POLocalRearranges corresponding to the reduce plans in 
     * myPlans (the list of inner plans of the demux) have different key types
     * then the MultiQueryOptimizer converts all the keys to be of type tuple
     * by wrapping any non-tuple keys into Tuples (keys which are already tuples
     * are left alone).
     * The list below is a list of booleans indicating whether extra tuple wrapping
     * was done for the key in the corresponding POLocalRearranges and if we need
     * to "unwrap" the tuple to get to the key
     */
    private ArrayList<Boolean> isKeyWrapped = new ArrayList<Boolean>();
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
    
    /*
     * Indicating if all the inner plans have the same
     * map key type. If not, the keys passed in are 
     * wrapped inside tuples and need to be extracted
     * out during the reduce phase 
     */
    private boolean sameMapKeyType = true;
    
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
        return "Demux" + isKeyWrapped + "[" + baseIndex +"] - " + mKey.toString();
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
     * Sets the base index of this demux. 
     * 
     * @param idx the base index
     */
    public void setBaseIndex(int idx) {
        baseIndex = idx;
    }
    
    /**
     * Returns the base index of this demux
     * 
     * @return the base index
     */
    public int getBaseIndex() {
        return baseIndex;
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
    public void addPlan(PhysicalPlan inPlan, byte mapKeyType) {  
        myPlans.add(inPlan);
        processedSet.set(myPlans.size()-1);
        // if mapKeyType is already a tuple, we will NOT
        // be wrapping it in an extra tuple. If it is not
        // a tuple, we will wrap into in a tuple
        isKeyWrapped.add(mapKeyType == DataType.TUPLE ? false : true);
    }
   
    @Override
    public Result getNext(Tuple t) throws ExecException {
        
        if (!inCombiner && this.parentPlan.endOfAllInput) {
            
            // If there is a stream in one of the inner plans, 
            // there could potentially be more to process - the 
            // reducer sets the flag stating that all map input has 
            // been sent already and runs the pipeline one more time
            // in its close() call.
            return getStreamCloseResult();                 
        
        } else { 
        
            if (getNext) {
                
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
            
            res = leaf.getNext(dummyTuple);
            
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
                res = leaf.getNext(dummyTuple);
               
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
        
        // unwrap the key to get the wrapped value which
        // is expected by the inner plans
        PigNullableWritable key = (PigNullableWritable)res.get(0);        
    
        // choose an inner plan to run based on the index set by
        // the POLocalRearrange operator and passed to this operator
        // by POMultiQueryPackage
        int index = key.getIndex();
        index &= idxPart;
        index -= baseIndex;                         
        
        PhysicalPlan pl = myPlans.get(index);
        if (!(pl.getRoots().get(0) instanceof PODemux)) {                             
            if (!sameMapKeyType && !inCombiner && isKeyWrapped.get(index)) {                                       
                Tuple tup = (Tuple)key.getValueAsPigType();
                res.set(0, tup.get(0));
            } else {
                res.set(0, key.getValueAsPigType());
            }
        }
    
        myPlans.get(index).attachInput(res);
        return myPlans.get(index).getLeaves().get(0);
    }
    
    /**
     * Sets a flag indicating if all inner plans have 
     * the same map key type. 
     * 
     * @param sameMapKeyType true if all inner plans have 
     * the same map key type; otherwise false
     */
    public void setSameMapKeyType(boolean sameMapKeyType) {
        this.sameMapKeyType = sameMapKeyType;
    }

    /**
     * Returns a flag indicating if all inner plans 
     * have the same map key type 
     * 
     * @return true if all inner plans have 
     * the same map key type; otherwise false
     */
    public boolean isSameMapKeyType() {
        return sameMapKeyType;
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
        
}

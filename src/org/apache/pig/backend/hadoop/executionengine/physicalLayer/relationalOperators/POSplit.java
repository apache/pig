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
import java.util.LinkedList;
import java.util.List;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.util.IdentityHashSet;
import org.apache.pig.pen.util.ExampleTuple;
import org.apache.pig.pen.util.LineageTracer;

/**
 * The MapReduce Split operator.
 * <p>
 * The assumption here is that
 * the logical to physical translation
 * will create this dummy operator with
 * just the filename using which the input
 * branch will be stored and used for loading
 * Also the translation should make sure that
 * appropriate filter operators are configured
 * as outputs of this operator using the conditions
 * specified in the LOSplit. So LOSplit will be converted
 * into:
 * 
 *     |        |           |
 *  Filter1  Filter2 ... Filter3
 *     |        |    ...    |
 *     |        |    ...    |
 *     ---- POSplit -... ----
 * This is different than the existing implementation
 * where the POSplit writes to sidefiles after filtering
 * and then loads the appropriate file.
 * <p>
 * The approach followed here is as good as the old
 * approach if not better in many cases because
 * of the availability of attachinInputs. An optimization
 * that can ensue is if there are multiple loads that
 * load the same file, they can be merged into one and 
 * then the operators that take input from the load 
 * can be stored. This can be used when
 * the mapPlan executes to read the file only once and
 * attach the resulting tuple as inputs to all the 
 * operators that take input from this load.
 * 
 * In some cases where the conditions are exclusive and
 * some outputs are ignored, this approach can be worse.
 * But this leads to easier management of the Split and
 * also allows to reuse this data stored from the split
 * job whenever necessary.
 */
public class POSplit extends PhysicalOperator {

    private static final long serialVersionUID = 1L;
    
    /*
     * The filespec that is used to store and load the output of the split job
     * which is the job containing the split
     */
    private FileSpec splitStore;
       
    /*
     * The list of sub-plans the inner plan is composed of
     */
    private List<PhysicalPlan> myPlans = new ArrayList<PhysicalPlan>();
    
    private BitSet processedSet = new BitSet();
    
    private static Result empty = new Result(POStatus.STATUS_NULL, null);
    
    private boolean inpEOP = false;
    
    /**
     * Constructs an operator with the specified key
     * @param k the operator key
     */
    public POSplit(OperatorKey k) {
        this(k,-1,null);
    }

    /**
     * Constructs an operator with the specified key
     * and degree of parallelism 
     * @param k the operator key
     * @param rp the degree of parallelism requested
     */
    public POSplit(OperatorKey k, int rp) {
        this(k,rp,null);
    }

    /**
     * Constructs an operator with the specified key and inputs 
     * @param k the operator key
     * @param inp the inputs that this operator will read data from
     */
    public POSplit(OperatorKey k, List<PhysicalOperator> inp) {
        this(k,-1,inp);
    }

    /**
     * Constructs an operator with the specified key,
     * degree of parallelism and inputs
     * @param k the operator key
     * @param rp the degree of parallelism requested 
     * @param inp the inputs that this operator will read data from
     */
    public POSplit(OperatorKey k, int rp, List<PhysicalOperator> inp) {
        super(k, rp, inp);
    }

    @Override
    public void visit(PhyPlanVisitor v) throws VisitorException {
        v.visitSplit(this);
    }

    @Override
    public String name() {
        return getAliasString() + "Split - " + mKey.toString();
    }

    @Override
    public boolean supportsMultipleInputs() {
        return false;
    }

    @Override
    public boolean supportsMultipleOutputs() {
        return true;
    }

    /**
     * Returns the name of the file associated with this operator
     * @return the FileSpec associated with this operator
     */
    public FileSpec getSplitStore() {
        return splitStore;
    }

    /**
     * Sets the name of the file associated with this operator
     * @param splitStore the FileSpec used to store the data
     */
    public void setSplitStore(FileSpec splitStore) {
        this.splitStore = splitStore;
    }

    /**
     * Returns the list of nested plans. 
     * @return the list of the nested plans
     * @see org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PlanPrinter
     */
    public List<PhysicalPlan> getPlans() {
        return myPlans;
    }
    
    /**
     * Appends the specified plan to the end of 
     * the nested input plan list
     * @param inPlan plan to be appended to the list
     */
    public void addPlan(PhysicalPlan inPlan) {        
        myPlans.add(inPlan);
        processedSet.set(myPlans.size()-1);
    }

    /**
     * Removes plan from
     * the nested input plan list
     * @param plan plan to be removed
     */
    public void removePlan(PhysicalPlan plan) {
        myPlans.remove(plan);
        processedSet.clear(myPlans.size());
    }
   
    @Override
    public Result getNextTuple() throws ExecException {

        if (this.parentPlan.endOfAllInput) {
            
            return getStreamCloseResult();         
        
        } 
        
        if (processedSet.cardinality() == myPlans.size()) {
            
            Result inp = processInput();
            
            if (inp.returnStatus == POStatus.STATUS_EOP
                || inp.returnStatus == POStatus.STATUS_ERR ) {
                return inp;
            }
         
            Tuple tuple = (Tuple)inp.result;
            for (PhysicalPlan pl : myPlans) {
                pl.attachInput(tuple);
            }
            
            processedSet.clear();
        }
        
        return processPlan();                                       
    }

    private Result processPlan() throws ExecException {
   
        int idx = processedSet.nextClearBit(0);
        PhysicalOperator leaf = myPlans.get(idx).getLeaves().get(0);
        
        Result res = runPipeline(leaf);
        
        if (res.returnStatus == POStatus.STATUS_EOP) {
            processedSet.set(idx++);        
            if (idx < myPlans.size()) {
                res = processPlan();
            }
        }
        
        return (res.returnStatus == POStatus.STATUS_OK) ? res : empty;
    }
    
    private Result runPipeline(PhysicalOperator leaf) throws ExecException {
       
        Result res = null;
        
        while (true) {
            
            res = leaf.getNextTuple();
            
            if (res.returnStatus == POStatus.STATUS_OK) {                
                break;
            } else if (res.returnStatus == POStatus.STATUS_NULL) {
                continue;
            } else if (res.returnStatus == POStatus.STATUS_EOP) {
                break;
            } else if (res.returnStatus == POStatus.STATUS_ERR) {
                break;
            }
        }   
        
        return res;
    }
    
    private Result getStreamCloseResult() throws ExecException {
        Result res = null;
        
        while (true) {
            
            if (processedSet.cardinality() == myPlans.size()) {
                Result inp = processInput();
                if (inp.returnStatus == POStatus.STATUS_OK) {                
                    Tuple tuple = (Tuple)inp.result;
                    for (PhysicalPlan pl : myPlans) {
                        pl.attachInput(tuple);
                    }
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
            if (inpEOP ) {
                myPlans.get(idx).endOfAllInput = true;
            }
            PhysicalOperator leaf = myPlans.get(idx).getLeaves().get(0);
            
            res = leaf.getNextTuple();
           
            if (res.returnStatus == POStatus.STATUS_EOP)  {
                processedSet.set(idx++);        
                if (idx < myPlans.size()) {
                    continue;
                }
            } else {
                break;
            }
            
            if (!inpEOP && res.returnStatus == POStatus.STATUS_EOP) {                   
                continue;
            } else {
                break;
            }
        }
        
        return res;
                
    }
    
    @Override
    public Tuple illustratorMarkup(Object in, Object out, int eqClassIndex) {
      // no op  
      return null;
    }
        
}

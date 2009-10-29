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

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.FuncSpec;
import org.apache.pig.IndexableLoadFunc;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigMapReduce;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.BufferedPositionedInputStream;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.plan.NodeIdGenerator;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.util.MultiMap;
import org.apache.pig.impl.util.ObjectSerializer;

/** This operator implements merge join algorithm to do map side joins. 
 *  Currently, only two-way joins are supported. One input of join is identified as left
 *  and other is identified as right. Left input tuples are the input records in map.
 *  Right tuples are read from HDFS by opening right stream.
 *  
 *    This join doesn't support outer join.
 *    Data is assumed to be sorted in ascending order. It will fail if data is sorted in descending order.
 */

public class POMergeJoin extends PhysicalOperator {

    private static final long serialVersionUID = 1L;

    private final transient Log log = LogFactory.getLog(getClass());

    // flag to indicate when getNext() is called first.
    private boolean firstTime = true;

    //The Local Rearrange operators modeling the join key
    private POLocalRearrange[] LRs;

    private transient IndexableLoadFunc rightLoader;
    private OperatorKey opKey;

    private Object prevLeftKey;

    private Result prevLeftInp;

    private Object prevRightKey = null;

    private Result prevRightInp;

    private transient TupleFactory mTupleFactory;

    //boolean denoting whether we are generating joined tuples in this getNext() call or do we need to read in more data.
    private boolean doingJoin;

    private FuncSpec rightLoaderFuncSpec;

    private String rightInputFileName;

    // Buffer to hold accumulated left tuples.
    private List<Tuple> leftTuples;

    private MultiMap<PhysicalOperator, PhysicalPlan> inpPlans;

    private PhysicalOperator rightPipelineLeaf;

    private PhysicalOperator rightPipelineRoot;

    private boolean noInnerPlanOnRightSide;

    private PigContext pc;

    private Object curJoinKey;

    private Tuple curJoiningRightTup;

    private int counter; // # of tuples on left side with same key.

    private int leftTupSize = -1;

    private int rightTupSize = -1;

    private int arrayListSize = 1024;
    

    /**
     * @param k
     * @param rp
     * @param inp
     * @param inpPlans there can only be 2 inputs each being a List<PhysicalPlan>
     * Ex. join A by ($0,$1), B by ($1,$2);
     */
    public POMergeJoin(OperatorKey k, int rp, List<PhysicalOperator> inp, MultiMap<PhysicalOperator, PhysicalPlan> inpPlans,
            List<List<Byte>> keyTypes) throws ExecException{

        super(k, rp, inp);
        this.opKey = k;
        this.doingJoin = false;
        this.inpPlans = inpPlans;
        LRs = new POLocalRearrange[2];
        mTupleFactory = TupleFactory.getInstance();
        leftTuples = new ArrayList<Tuple>(arrayListSize);
        this.createJoinPlans(inpPlans,keyTypes);
    }

    /**
     * Configures the Local Rearrange operators to get keys out of tuple.
     * @throws ExecException 
     */
    private void createJoinPlans(MultiMap<PhysicalOperator, PhysicalPlan> inpPlans, List<List<Byte>> keyTypes) throws ExecException{

        int i=-1;
        for (PhysicalOperator inpPhyOp : inpPlans.keySet()) {
            ++i;
            POLocalRearrange lr = new POLocalRearrange(genKey());
            lr.setIndex(i);
            lr.setResultType(DataType.TUPLE);
            lr.setKeyType(keyTypes.get(i).size() > 1 ? DataType.TUPLE : keyTypes.get(i).get(0));
            try {
                lr.setPlans((List<PhysicalPlan>)inpPlans.get(inpPhyOp));
            } catch (PlanException pe) {
                int errCode = 2071;
                String msg = "Problem with setting up local rearrange's plans.";
                throw new ExecException(msg, errCode, PigException.BUG, pe);
            }
            LRs[i]= lr;
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public Result getNext(Tuple t) throws ExecException {

        Object curLeftKey;
        Result curLeftInp;

        if(firstTime){
            // Do initial setup.
            curLeftInp = processInput();
            if(curLeftInp.returnStatus != POStatus.STATUS_OK)
                return curLeftInp;       // Return because we want to fetch next left tuple.

            curLeftKey = extractKeysFromTuple(curLeftInp, 0);
            if(null == curLeftKey) // We drop the tuples which have null keys.
                return new Result(POStatus.STATUS_EOP, null);
            
            try {
                seekInRightStream(curLeftKey);
            } catch (IOException e) {
                throwProcessingException(true, e);
            } catch (ClassCastException e) {
                throwProcessingException(true, e);;
            }
            leftTuples.add((Tuple)curLeftInp.result);
            firstTime = false;
            prevLeftKey = curLeftKey;
            return new Result(POStatus.STATUS_EOP, null);
        }

        if(doingJoin){
            // We matched on keys. Time to do the join.

            if(counter > 0){    // We have left tuples to join with current right tuple.
                Tuple joiningLeftTup = leftTuples.get(--counter);
                leftTupSize = joiningLeftTup.size();
                Tuple joinedTup = mTupleFactory.newTuple(leftTupSize+rightTupSize);

                for(int i=0; i<leftTupSize; i++)
                    joinedTup.set(i, joiningLeftTup.get(i));

                for(int i=0; i < rightTupSize; i++)
                    joinedTup.set(i+leftTupSize, curJoiningRightTup.get(i));

                return new Result(POStatus.STATUS_OK, joinedTup);
            }
            // Join with current right input has ended. But bag of left tuples
            // may still join with next right tuple.

            doingJoin = false;

            while(true){
                Result rightInp = getNextRightInp();
                if(rightInp.returnStatus != POStatus.STATUS_OK){
                    prevRightInp = null;
                    return rightInp;
                }
                else{
                    Object rightKey = extractKeysFromTuple(rightInp, 1);
                    if(null == rightKey) // If we see tuple having null keys in stream, we drop them 
                        continue;       // and fetch next tuple.

                    int cmpval = ((Comparable)rightKey).compareTo(curJoinKey);
                    if (cmpval == 0){
                        // Matched the very next right tuple.
                        curJoiningRightTup = (Tuple)rightInp.result;
                        rightTupSize = curJoiningRightTup.size();
                        counter = leftTuples.size();
                        doingJoin = true;
                        return this.getNext(dummyTuple);

                    }
                    else if(cmpval > 0){    // We got ahead on right side. Store currently read right tuple.
                        if(!this.parentPlan.endOfAllInput){
                            prevRightKey = rightKey;
                            prevRightInp = rightInp;
                            // There cant be any more join on this key.
                            leftTuples = new ArrayList<Tuple>(arrayListSize);
                            leftTuples.add((Tuple)prevLeftInp.result);
                        }

                        else{           // This is end of all input and this is last join output.
                            // Right loader in this case wouldn't get a chance to close input stream. So, we close it ourself.
                            try {
                                rightLoader.close();
                            } catch (IOException e) {
                                // Non-fatal error. We can continue.
                                log.error("Received exception while trying to close right side file: " + e.getMessage());
                            }
                        }
                        return new Result(POStatus.STATUS_EOP, null);
                    }
                    else{   // At this point right side can't be behind.
                        int errCode = 1102;
                        String errMsg = "Data is not sorted on right side. Last two tuples encountered were: \n"+
                        curJoiningRightTup+ "\n" + (Tuple)rightInp.result ;
                        throw new ExecException(errMsg,errCode);
                    }    
                }
            }
        }

        curLeftInp = processInput();
        switch(curLeftInp.returnStatus){

        case POStatus.STATUS_OK:
            curLeftKey = extractKeysFromTuple(curLeftInp, 0);
            if(null == curLeftKey) // We drop the tuples which have null keys.
                return new Result(POStatus.STATUS_EOP, null);
            
            int cmpVal = ((Comparable)curLeftKey).compareTo(prevLeftKey);
            if(cmpVal == 0){
                // Keep on accumulating.
                leftTuples.add((Tuple)curLeftInp.result);
                return new Result(POStatus.STATUS_EOP, null);
            }
            else if(cmpVal > 0){ // Filled with left bag. Move on.
                curJoinKey = prevLeftKey;
                break;   
            }
            else{   // Current key < Prev Key
                int errCode = 1102;
                String errMsg = "Data is not sorted on left side. Last two keys encountered were: \n"+
                prevLeftKey+ "\n" + curLeftKey ;
                throw new ExecException(errMsg,errCode);
            }
 
        case POStatus.STATUS_EOP:
            if(this.parentPlan.endOfAllInput){
                // We hit the end on left input. 
                // Tuples in bag may still possibly join with right side.
                curJoinKey = prevLeftKey;
                curLeftKey = null;
                break;                
            }
            else    // Fetch next left input.
                return curLeftInp;

        default:    // If encountered with ERR / NULL on left side, we send it down.
            return curLeftInp;
        }

        if((null != prevRightKey) && !this.parentPlan.endOfAllInput && ((Comparable)prevRightKey).compareTo(curLeftKey) >= 0){

            // This will happen when we accumulated inputs on left side and moved on, but are still behind the right side
            // In that case, throw away the tuples accumulated till now and add the one we read in this function call.
            leftTuples = new ArrayList<Tuple>(arrayListSize);
            leftTuples.add((Tuple)curLeftInp.result);
            prevLeftInp = curLeftInp;
            prevLeftKey = curLeftKey;
            return new Result(POStatus.STATUS_EOP, null);
        }

        // Accumulated tuples with same key on left side.
        // But since we are reading ahead we still haven't checked the read ahead right tuple.
        // Accumulated left tuples may potentially join with that. So, lets check that first.
        
        if((null != prevRightKey) && prevRightKey.equals(prevLeftKey)){

            curJoiningRightTup = (Tuple)prevRightInp.result;
            counter = leftTuples.size();
            rightTupSize = curJoiningRightTup.size();
            doingJoin = true;
            prevLeftInp = curLeftInp;
            prevLeftKey = curLeftKey;
            return this.getNext(dummyTuple);
        }

        // We will get here only when curLeftKey > prevRightKey
        while(true){
            // Start moving on right stream to find the tuple whose key is same as with current left bag key.
            Result rightInp = getNextRightInp();
            if(rightInp.returnStatus != POStatus.STATUS_OK)
                return rightInp;

            Object extractedRightKey = extractKeysFromTuple(rightInp, 1);
            
            if(null == extractedRightKey) // If we see tuple having null keys in stream, we drop them 
                continue;       // and fetch next tuple.
            
            Comparable rightKey = (Comparable)extractedRightKey;
            
            if( prevRightKey != null && rightKey.compareTo(prevRightKey) < 0){
                // Sanity check.
                int errCode = 1102;
                String errMsg = "Data is not sorted on right side. Last two keys encountered were: \n"+
                prevRightKey+ "\n" + rightKey ;
                throw new ExecException(errMsg,errCode);
            }

            int cmpval = rightKey.compareTo(prevLeftKey);
            if(cmpval < 0)     // still behind the left side, do nothing, fetch next right tuple.
                continue;

            else if (cmpval == 0){  // Found matching tuple. Time to do join.

                curJoiningRightTup = (Tuple)rightInp.result;
                counter = leftTuples.size();
                rightTupSize = curJoiningRightTup.size();
                doingJoin = true;
                prevLeftInp = curLeftInp;
                prevLeftKey = curLeftKey;
                return this.getNext(dummyTuple);
            }

            else{    // We got ahead on right side. Store currently read right tuple.
                prevRightKey = rightKey;
                prevRightInp = rightInp;
                // Since we didn't find any matching right tuple we throw away the buffered left tuples and add the one read in this function call. 
                leftTuples = new ArrayList<Tuple>(arrayListSize);
                leftTuples.add((Tuple)curLeftInp.result);
                prevLeftInp = curLeftInp;
                prevLeftKey = curLeftKey;
                if(this.parentPlan.endOfAllInput){  // This is end of all input and this is last time we will read right input.
                    // Right loader in this case wouldn't get a chance to close input stream. So, we close it ourself.
                    try {
                        rightLoader.close();
                    } catch (IOException e) {
                     // Non-fatal error. We can continue.
                        log.error("Received exception while trying to close right side file: " + e.getMessage());
                    }
                }
                return new Result(POStatus.STATUS_EOP, null);
            }
        }
    }
    
    @SuppressWarnings("unchecked")
    private void seekInRightStream(Object firstLeftKey) throws IOException{
        rightLoader = (IndexableLoadFunc)PigContext.instantiateFuncFromSpec(rightLoaderFuncSpec);
        pc = (PigContext)ObjectSerializer.deserialize(PigMapReduce.sJobConf.get("pig.pigContext"));
        pc.connect();
        InputStream is = FileLocalizer.open(rightInputFileName, pc);
        rightLoader.initialize(PigMapReduce.sJobConf);
        // the main purpose of this bindTo call is supply the input file name
        // to the right loader - in the case of Pig's DefaultIndexableLoader
        // this is really not used since the index has all information required
        rightLoader.bindTo(rightInputFileName, new BufferedPositionedInputStream(is), 0, Long.MAX_VALUE);
        rightLoader.seekNear(
                firstLeftKey instanceof Tuple ? (Tuple)firstLeftKey : mTupleFactory.newTuple(firstLeftKey));
    }


    private Result getNextRightInp() throws ExecException{

        try {
            if(noInnerPlanOnRightSide){
                Tuple t = rightLoader.getNext();
                if(t == null) { // no more data on right side
                    return new Result(POStatus.STATUS_EOP, null);
                } else {
                    return new Result(POStatus.STATUS_OK, t);
                }
            } else {
                Result res = rightPipelineLeaf.getNext(dummyTuple);
                switch(res.returnStatus){
                case POStatus.STATUS_OK:
                    return res;

                case POStatus.STATUS_EOP:
                    Tuple t = rightLoader.getNext();
                    if(t == null) { // no more data on right side
                        return new Result(POStatus.STATUS_EOP, null);
                    } else {
                        // run the tuple through the pipeline
                        rightPipelineRoot.attachInput(t);
                        return this.getNextRightInp();
                        
                    }
                    default: // We don't deal with ERR/NULL. just pass them down
                        throwProcessingException(false, null);
                        
                }
            }
        } catch (IOException e) {
            throwProcessingException(true, e);
        }
        // we should never get here!
        return new Result(POStatus.STATUS_ERR, null);
    }

    public void throwProcessingException (boolean withCauseException, Exception e) throws ExecException {
        int errCode = 2176;
        String errMsg = "Error processing right input during merge join";
        if(withCauseException) {
            throw new ExecException(errMsg, errCode, PigException.BUG, e);
        } else {
            throw new ExecException(errMsg, errCode, PigException.BUG);
        }
    }

    private Object extractKeysFromTuple(Result inp, int lrIdx) throws ExecException{

        //Separate Key & Value of input using corresponding LR operator
        POLocalRearrange lr = LRs[lrIdx];
        lr.attachInput((Tuple)inp.result);
        Result lrOut = lr.getNext(dummyTuple);
        if(lrOut.returnStatus!=POStatus.STATUS_OK){
            int errCode = 2167;
            String errMsg = "LocalRearrange used to extract keys from tuple isn't configured correctly";
            throw new ExecException(errMsg,errCode,PigException.BUG);
        } 
          
        return ((Tuple) lrOut.result).get(1);
    }

    public void setupRightPipeline(PhysicalPlan rightPipeline) throws FrontendException{

        if(rightPipeline != null){
            if(rightPipeline.getLeaves().size() != 1 || rightPipeline.getRoots().size() != 1){
                int errCode = 2168;
                String errMsg = "Expected physical plan with exactly one root and one leaf.";
                throw new FrontendException(errMsg,errCode,PigException.BUG);
            }

            noInnerPlanOnRightSide = false;
            this.rightPipelineLeaf = rightPipeline.getLeaves().get(0);
            this.rightPipelineRoot = rightPipeline.getRoots().get(0);
            this.rightPipelineRoot.setInputs(null);            
        }
        else
            noInnerPlanOnRightSide = true;
    }

    private void readObject(ObjectInputStream is) throws IOException, ClassNotFoundException, ExecException{

        is.defaultReadObject();
        mTupleFactory = TupleFactory.getInstance();
    }


    private OperatorKey genKey(){
        return new OperatorKey(opKey.scope,NodeIdGenerator.getGenerator().getNextNodeId(opKey.scope));
    }

    public void setRightLoaderFuncSpec(FuncSpec rightLoaderFuncSpec) {
        this.rightLoaderFuncSpec = rightLoaderFuncSpec;
    }

    public List<PhysicalPlan> getInnerPlansOf(int index) {
        return (List<PhysicalPlan>)inpPlans.get(inputs.get(index));
    }

    /* (non-Javadoc)
     * @see org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator#visit(org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor)
     */
    @Override
    public void visit(PhyPlanVisitor v) throws VisitorException {
        v.visitMergeJoin(this);
    }

    /* (non-Javadoc)
     * @see org.apache.pig.impl.plan.Operator#name()
     */
    @Override
    public String name() {
        return "MergeJoin[" + DataType.findTypeName(resultType) + "]" +" - " + mKey.toString();
    }

    /* (non-Javadoc)
     * @see org.apache.pig.impl.plan.Operator#supportsMultipleInputs()
     */
    @Override
    public boolean supportsMultipleInputs() {
        return true;
    }

    /* (non-Javadoc)
     * @see org.apache.pig.impl.plan.Operator#supportsMultipleOutputs()
     */
    @Override
    public boolean supportsMultipleOutputs() {
        return false;
    }
    
    /**
     * @param rightInputFileName the rightInputFileName to set
     */
    public void setRightInputFileName(String rightInputFileName) {
        this.rightInputFileName = rightInputFileName;
    }
}

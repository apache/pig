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
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.FuncSpec;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigMapReduce;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POCast;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POProject;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.plan.NodeIdGenerator;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.util.MultiMap;
import org.apache.pig.impl.util.ObjectSerializer;

public class POMergeJoin extends PhysicalOperator {

    /** This operator implements merge join algorithm to do map side joins. 
     *  Currently, only two-way joins are supported. One input of join is identified as left
     *  and other is identified as right. Left input tuples are the input records in map.
     *  Right tuples are read from HDFS by opening right stream.
     *  
     *    This join doesn't support outer join.
     *    Data is assumed to be sorted in ascending order. It will fail if data is sorted in descending order.
     */
    private static final long serialVersionUID = 1L;

    private final transient Log log = LogFactory.getLog(getClass());

    // flag to indicate when getNext() is called first.
    private boolean firstTime = true;

    //The Local Rearrange operators modeling the join key
    private POLocalRearrange[] LRs;

    // FileSpec of index file which will be read from HDFS.
    private FileSpec indexFile;

    private POLoad rightLoader;

    private OperatorKey opKey;

    private Object prevLeftKey;

    private Result prevLeftInp;

    private Object prevRightKey = null;

    private Result prevRightInp;

    private transient TupleFactory mTupleFactory;

    //boolean denoting whether we are generating joined tuples in this getNext() call or do we need to read in more data.
    private boolean doingJoin;

    // Index is modeled as FIFO queue and LinkedList implements java Queue interface.  
    private LinkedList<Tuple> index;

    private FuncSpec rightLoaderFuncSpec;

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

    private List<POCast> casters;

    private List<POProject> projectors;

    /**
     * @param k
     * @param rp
     * @param inp
     * @param inpPlans there can only be 2 inputs each being a List<PhysicalPlan>
     * Ex. join A by ($0,$1), B by ($1,$2);
     */
    public POMergeJoin(OperatorKey k, int rp, List<PhysicalOperator> inp, MultiMap<PhysicalOperator, PhysicalPlan> inpPlans,List<List<Byte>> keyTypes) throws ExecException{

        super(k, rp, inp);
        this.opKey = k;
        this.doingJoin = false;
        this.inpPlans = inpPlans;
        LRs = new POLocalRearrange[2];
        mTupleFactory = TupleFactory.getInstance();
        leftTuples = new ArrayList<Tuple>(arrayListSize);
        this.createJoinPlans(inpPlans,keyTypes);
        setUpTypeCastingForIdxTup(keyTypes.get(0));
    }

    /** This function setups casting for key tuples which we read out of index file.
     * We set the type of key as DataByteArray(DBA) and then cast it into the type specified in schema.
     * If type is not specified in schema, then we will cast from DBA to DBA.
     */
    
    private void setUpTypeCastingForIdxTup(List<Byte> keyTypes){
         /*   
         * Cant reuse one POCast operator for all keys since POCast maintains some state
         * and hence its not safe to use one POCast. Thus we use one POCast for each key.
         */
        casters = new ArrayList<POCast>(keyTypes.size());
        projectors = new ArrayList<POProject>(keyTypes.size());

        for(Byte keytype : keyTypes){
            POCast caster = new POCast(genKey());
            List<PhysicalOperator> pp = new ArrayList<PhysicalOperator>(1);
            POProject projector = new POProject(genKey());
            projector.setResultType(DataType.BYTEARRAY);
            projector.setColumn(0);
            pp.add(projector);
            caster.setInputs(pp);
            caster.setResultType(keytype);
            projectors.add(projector);
            casters.add(caster);
        }
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
            
            seekInRightStream(curLeftKey);
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
                                rightLoader.tearDown();
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
                        rightLoader.tearDown();
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
    private void seekInRightStream(Object firstLeftKey) throws ExecException{

        /* Currently whole of index is read into memory. Typically, index is small. Usually 
           few KBs in size. So, this should not be an issue.
           However, reading whole index at startup time is not required. So, this can be improved upon.
           Assumption: Index being read is sorted on keys followed by filename, followed by offset.
         */

        // Index is modeled as FIFO Queue, that frees us from keeping track of which index entry should be read next.
        POLoad ld = new POLoad(genKey(), this.indexFile, false);
        try {
            pc = (PigContext)ObjectSerializer.deserialize(PigMapReduce.sJobConf.get("pig.pigContext"));
        } catch (IOException e) {
            int errCode = 2094;
            String msg = "Unable to deserialize pig context.";
            throw new ExecException(msg,errCode,e);
        }
        pc.connect();
        ld.setPc(pc);
        index = new LinkedList<Tuple>();
        for(Result res=ld.getNext(dummyTuple);res.returnStatus!=POStatus.STATUS_EOP;res=ld.getNext(dummyTuple))
            index.offer((Tuple) res.result);   

        Tuple prevIdxEntry = null;
        Tuple matchedEntry;
     
        // When the first call is made, we need to seek into right input at correct offset.
        while(true){
            // Keep looping till we find first entry in index >= left key
            // then return the prev idx entry.

            Tuple curIdxEntry = index.poll();
            if(null == curIdxEntry){
                // Its possible that we hit end of index and still doesn't encounter
                // idx entry >= left key, in that case return last index entry.
                matchedEntry = prevIdxEntry;
                break;
            }
            Object extractedKey = extractKeysFromIdxTuple(curIdxEntry);
            if(extractedKey == null){
                prevIdxEntry = curIdxEntry;
                continue;
            }
            
            if(((Comparable)extractedKey).compareTo(firstLeftKey) >= 0){
                if(null == prevIdxEntry)   // very first entry in index.
                    matchedEntry = curIdxEntry;
                else{
                    matchedEntry = prevIdxEntry;
                    index.addFirst(curIdxEntry);  // We need to add back the current index Entry because we are reading ahead.
                }
                break;
            }
            else
                prevIdxEntry = curIdxEntry;
        }

        if(matchedEntry == null){
            
            int errCode = 2165;
            String errMsg = "Problem in index construction.";
            throw new ExecException(errMsg,errCode,PigException.BUG);
        }
        
        Object extractedKey = extractKeysFromIdxTuple(matchedEntry);
        
        if(extractedKey != null){
            Class idxKeyClass = extractedKey.getClass();
            if( ! firstLeftKey.getClass().equals(idxKeyClass)){

                // This check should indeed be done on compile time. But to be on safe side, we do it on runtime also.
                int errCode = 2166;
                String errMsg = "Key type mismatch. Found key of type "+firstLeftKey.getClass().getCanonicalName()+" on left side. But, found key of type "+ idxKeyClass.getCanonicalName()+" in index built for right side.";
                throw new ExecException(errMsg,errCode,PigException.BUG);
            }
        }
        initRightLoader(matchedEntry);
    }

    /**innerKeyTypes
     * @param indexFile the indexFile to set
     */
    public void setIndexFile(FileSpec indexFile) {
        this.indexFile = indexFile;
    }

    private Object extractKeysFromIdxTuple(Tuple idxTuple) throws ExecException{

        int idxTupSize = idxTuple.size();
        List<Object> list = new ArrayList<Object>(idxTupSize-2);

        for(int i=0; i<idxTupSize-2; i++){

            projectors.get(i).attachInput(mTupleFactory.newTuple(idxTuple.get(i)));

            switch (casters.get(i).getResultType()) {

            case DataType.BYTEARRAY:    // POCast doesn't handle DBA. But we are saved, because in this case we don't need cast anyway.
                list.add(idxTuple.get(i));
                break;

            case DataType.CHARARRAY:
                list.add(casters.get(i).getNext(dummyString).result);
                break;

            case DataType.INTEGER:
                list.add(casters.get(i).getNext(dummyInt).result);
                break;

            case DataType.FLOAT:
                list.add(casters.get(i).getNext(dummyFloat).result);
                break;

            case DataType.DOUBLE:
                list.add(casters.get(i).getNext(dummyDouble).result);
                break;

            case DataType.LONG:
                list.add(casters.get(i).getNext(dummyLong).result);
                break;

            case DataType.TUPLE:
                list.add(casters.get(i).getNext(dummyTuple).result);
                break;

            default:
                int errCode = 2036;
                String errMsg = "Unhandled key type : "+casters.get(i).getResultType();
                throw new ExecException(errMsg,errCode,PigException.BUG);
            }
        }
        // If there is only one key, we don't want to wrap it into Tuple.
        return list.size() == 1 ? list.get(0) : mTupleFactory.newTuple(list);
    }

    private Result getNextRightInp() throws ExecException{

        if(noInnerPlanOnRightSide){
            Result res = rightLoader.getNext(dummyTuple);
            switch(res.returnStatus) {
            case POStatus.STATUS_OK:
                return res;

            case POStatus.STATUS_EOP:           // Current file has ended. Need to open next file by reading next index entry.
                String prevFile = rightLoader.getLFile().getFileName();
                while(true){                        // But next file may be same as previous one, because index may contain multiple entries for same file.
                    Tuple idxEntry = index.poll();
                    if(null == idxEntry)            // Index is finished too. Right stream is finished. No more tuples.
                        return res;
                    else{                           
                        if(prevFile.equals((String)idxEntry.get(idxEntry.size()-2)))
                            continue;
                        else{
                            initRightLoader(idxEntry);      // bind loader to file and get tuple from it.
                            return this.getNextRightInp();    
                        }
                    }
                }
            default:    // We pass down ERR/NULL.
                return res;
            }
        }

        else {
            Result res = rightPipelineLeaf.getNext(dummyTuple);
            switch(res.returnStatus){
            case POStatus.STATUS_OK:
                return res;

            case POStatus.STATUS_EOP:
                res = rightLoader.getNext(dummyTuple);

                switch(res.returnStatus) {
                case POStatus.STATUS_OK:
                    rightPipelineRoot.attachInput((Tuple)res.result);
                    return this.getNextRightInp();

                case POStatus.STATUS_EOP:          // Current file has ended. Need to open next file by reading next index entry.
                    String prevFile = rightLoader.getLFile().getFileName();
                    while(true){                        // But next file may be same as previous one, because index may contain multiple entries for same file.
                        Tuple idxEntry = index.poll();
                        if(null == idxEntry)          // Index is finished too. Right stream is finished. No more tuples.
                            return res;
                        else{
                            if(prevFile.equals((String)idxEntry.get(idxEntry.size()-2)))
                                continue;
                            else{
                                initRightLoader(idxEntry);
                                res = rightLoader.getNext(dummyTuple);
                                rightPipelineRoot.attachInput((Tuple)res.result);
                                return this.getNextRightInp();
                            }
                        }
                    }
                default:    // We don't deal with ERR/NULL. just pass them down
                    return res;
                }

            default:    // We don't deal with ERR/NULL. just pass them down
                return res;
            }            
        }
    }

    private void initRightLoader(Tuple idxEntry) throws ExecException{

        // bind loader to file pointed by this index Entry.
        int keysCnt = idxEntry.size();
        rightLoader = new POLoad(genKey(), new FileSpec((String)idxEntry.get(keysCnt-2),this.rightLoaderFuncSpec),(Long)idxEntry.get(keysCnt-1), false);
        rightLoader.setPc(pc);
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
        for(POCast caster : casters)
            caster.setLoadFSpec(rightLoaderFuncSpec);            
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
}

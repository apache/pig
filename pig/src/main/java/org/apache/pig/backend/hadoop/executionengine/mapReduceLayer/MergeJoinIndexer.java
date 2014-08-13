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
package org.apache.pig.backend.hadoop.executionengine.mapReduceLayer;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.pig.LoadCaster;
import org.apache.pig.LoadFunc;
import org.apache.pig.OrderedLoadFunc;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLocalRearrange;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.plan.NodeIdGenerator;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.util.ObjectSerializer;

/** Merge Join indexer is used to generate on the fly index for doing Merge Join efficiently.
 *  It samples first record from every block of right side input. 
 *  and returns tuple in the following format : 
 *  (key0, key1,...,position,splitIndex)
 *  These tuples are then sorted before being written out to index file on HDFS.
 */
public class MergeJoinIndexer  extends LoadFunc{

    private boolean firstRec = true;
    private transient TupleFactory mTupleFactory;
    private POLocalRearrange lr;
    private PhysicalPlan precedingPhyPlan;
    private int keysCnt;
    private PhysicalOperator rightPipelineLeaf;
    private PhysicalOperator rightPipelineRoot;
    private LoadFunc loader;
    private PigSplit pigSplit = null;
    private boolean ignoreNullKeys;
    
    /** @param funcSpec : Loader specification.
     *  @param innerPlan : This is serialized version of LR plan. We 
     *  want to keep only keys in our index file and not the whole tuple. So, we need LR and thus its plan
     *  to get keys out of the sampled tuple.  
     * @param serializedPhyPlan Serialized physical plan on right side.
     * @throws ExecException 
     */
    @SuppressWarnings("unchecked")
    public MergeJoinIndexer(String funcSpec, String innerPlan, String serializedPhyPlan, 
            String udfCntxtSignature, String scope, String ignoreNulls) throws ExecException{
        
        loader = (LoadFunc)PigContext.instantiateFuncFromSpec(funcSpec);
        loader.setUDFContextSignature(udfCntxtSignature);
        this.ignoreNullKeys = Boolean.parseBoolean(ignoreNulls);
        
        try {
            List<PhysicalPlan> innerPlans = (List<PhysicalPlan>)ObjectSerializer.deserialize(innerPlan);
            lr = new POLocalRearrange(new OperatorKey(scope,NodeIdGenerator.getGenerator().getNextNodeId(scope)));
            lr.setPlans(innerPlans);
            keysCnt = innerPlans.size();
            precedingPhyPlan = (PhysicalPlan)ObjectSerializer.deserialize(serializedPhyPlan);
            if(precedingPhyPlan != null){
                    if(precedingPhyPlan.getLeaves().size() != 1 || precedingPhyPlan.getRoots().size() != 1){
                        int errCode = 2168;
                        String errMsg = "Expected physical plan with exactly one root and one leaf.";
                        throw new ExecException(errMsg,errCode,PigException.BUG);
                    }
                this.rightPipelineLeaf = precedingPhyPlan.getLeaves().get(0);
                this.rightPipelineRoot = precedingPhyPlan.getRoots().get(0);
                this.rightPipelineRoot.setInputs(null);                            
            }
        }
        catch (IOException e) {
            int errCode = 2094;
            String msg = "Unable to deserialize plans in Indexer.";
            throw new ExecException(msg,errCode,e);
        }
        mTupleFactory = TupleFactory.getInstance();
    }

    @Override
    public Tuple getNext() throws IOException {
        
        if(!firstRec)   // We sample only one record per block.
            return null;
        WritableComparable<?> position = ((OrderedLoadFunc)loader).getSplitComparable(pigSplit.getWrappedSplit());
        Object key = null;
        Tuple wrapperTuple = mTupleFactory.newTuple(keysCnt+2);
        
        while(true){
            Tuple readTuple = loader.getNext();

            if(null == readTuple){    // We hit the end.

                for(int i =0; i < keysCnt; i++)
                    wrapperTuple.set(i, null);
                wrapperTuple.set(keysCnt, position);
                firstRec = false;
                return wrapperTuple;
            }

            if (null == precedingPhyPlan){

                lr.attachInput(readTuple);
                key = ((Tuple)lr.getNextTuple().result).get(1);
                lr.detachInput();
                if ( null == key && ignoreNullKeys) // Tuple with null key. Drop it.
                    continue;
                break;      
            }

            // There is a physical plan. 

            rightPipelineRoot.attachInput(readTuple);
            boolean fetchNewTup;

            while(true){

                Result res = rightPipelineLeaf.getNextTuple();
                switch(res.returnStatus){

                case POStatus.STATUS_OK:

                    lr.attachInput((Tuple)res.result);
                    key = ((Tuple)lr.getNextTuple().result).get(1);
                    lr.detachInput();
                    if ( null == key && ignoreNullKeys) // Tuple with null key. Drop it.
                        continue;
                     fetchNewTup = false;
                    break;

                case POStatus.STATUS_EOP:
                    fetchNewTup = true;
                    break;

                default:
                    int errCode = 2164;
                    String errMsg = "Expected EOP/OK as return status. Found: "+res.returnStatus;
                    throw new ExecException(errMsg,errCode);
                }            
                break;
            }
            if (!fetchNewTup)
                break;
        }

        if(key instanceof Tuple){
            Tuple tupKey = (Tuple)key;
            for(int i =0; i < tupKey.size(); i++)
                wrapperTuple.set(i, tupKey.get(i));
        }

        else
            wrapperTuple.set(0, key);

        wrapperTuple.set(keysCnt, position);
        wrapperTuple.set(keysCnt+1, pigSplit.getSplitIndex());
        firstRec = false;
        return wrapperTuple;
    }
    
    /* (non-Javadoc)
     * @see org.apache.pig.LoadFunc#getInputFormat()
     */
    @Override
    public InputFormat getInputFormat() throws IOException {
        return loader.getInputFormat();
    }

    /* (non-Javadoc)
     * @see org.apache.pig.LoadFunc#getLoadCaster()
     */
    @Override
    public LoadCaster getLoadCaster() throws IOException {
        return loader.getLoadCaster();
    }

    /* (non-Javadoc)
     * @see org.apache.pig.LoadFunc#prepareToRead(org.apache.hadoop.mapreduce.RecordReader, org.apache.hadoop.mapreduce.InputSplit)
     */
    @Override
    public void prepareToRead(RecordReader reader, PigSplit split) throws IOException {
        loader.prepareToRead(reader, split);
        pigSplit = split;
    }

    /* (non-Javadoc)
     * @see org.apache.pig.LoadFunc#setLocation(java.lang.String, org.apache.hadoop.mapreduce.Job)
     */
    @Override
    public void setLocation(String location, Job job) throws IOException {
        loader.setLocation(location, job);
    }
}

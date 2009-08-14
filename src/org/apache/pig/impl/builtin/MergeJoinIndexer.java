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
package org.apache.pig.impl.builtin;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.List;

import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLocalRearrange;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.io.BufferedPositionedInputStream;
import org.apache.pig.impl.plan.NodeIdGenerator;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.util.ObjectSerializer;

public class MergeJoinIndexer extends RandomSampleLoader {

    /** Merge Join indexer is used to generate on the fly index for doing Merge Join efficiently.
     *  It samples first record from every block of right side input (which is later opened as side file in merge join)
     *  and returns tuple in the following format : 
     *  (key0, key1,...,fileName, offset)
     *  These tuples are then sorted before being written out to index file on HDFS.
     */
    
    private boolean firstRec = true;
    private transient TupleFactory mTupleFactory;
    private String fileName;
    private POLocalRearrange lr;
    private int keysCnt;
    
    /** @param funcSpec : Loader specification.
     *  @param serializedPlans : This is serialized version of LR plan. We 
     *  want to keep only keys in our index file and not the whole tuple. So, we need LR and thus its plan
     *  to get keys out of the sampled tuple.  
     */
    @SuppressWarnings("unchecked")
    public MergeJoinIndexer(String funcSpec, String serializedPlans) throws ExecException{
        super(funcSpec,"1");

        try {
            List<PhysicalPlan> innerPlans = (List<PhysicalPlan>)ObjectSerializer.deserialize(serializedPlans);
            lr = new POLocalRearrange(new OperatorKey("MergeJoin Indexer",NodeIdGenerator.getGenerator().getNextNodeId("MergeJoin Indexer")));
            lr.setPlans(innerPlans);
            keysCnt = innerPlans.size();
            mTupleFactory = TupleFactory.getInstance();
        }
        catch (PlanException pe) {
            int errCode = 2071;
            String msg = "Problem with setting up local rearrange's plans.";
            throw new ExecException(msg, errCode, PigException.BUG, pe);
        }
        catch (IOException e) {
            int errCode = 2094;
            String msg = "Unable to deserialize inner plans in Indexer.";
            throw new ExecException(msg,errCode,e);
        }
    }

    @Override
    public void bindTo(String fileName, BufferedPositionedInputStream is,long offset, long end) throws IOException {
        this.fileName = fileName;
        super.bindTo(fileName, is, offset, end);
    }

    @Override
    public Tuple getNext() throws IOException {

        if(!firstRec)   // We sample only record per block.
            return null;

        while(true){
            long initialPos = loader.getPosition();
            Tuple t = loader.getSampledTuple();
            
            if(null == t){          // We hit the end of block because all keys are null. 
            
                Tuple wrapperTuple = mTupleFactory.newTuple(keysCnt+2);
                for(int i =0; i < keysCnt; i++)
                    wrapperTuple.set(i, null);
                wrapperTuple.set(keysCnt, fileName);
                wrapperTuple.set(keysCnt+1, initialPos);
                firstRec = false;
                return wrapperTuple;
            }
                
            Tuple dummyTuple = null;
            lr.attachInput(t);
            Object key = ((Tuple)lr.getNext(dummyTuple).result).get(1);
            if(null == key)     // Tuple with null key. Drop it. Get next.
                continue;
            
            Tuple wrapperTuple = mTupleFactory.newTuple(keysCnt+2);        
            if(key instanceof Tuple){
                Tuple tupKey = (Tuple)key;
                for(int i =0; i < tupKey.size(); i++)
                    wrapperTuple.set(i, tupKey.get(i));
            }

            else
                wrapperTuple.set(0, key);

            lr.detachInput();
            wrapperTuple.set(keysCnt, fileName);
            wrapperTuple.set(keysCnt+1, initialPos);

            firstRec = false;
            return wrapperTuple;
        }
    }

    private void readObject(ObjectInputStream is) throws IOException, ClassNotFoundException, ExecException{
        is.defaultReadObject();
        mTupleFactory = TupleFactory.getInstance();
    }
}

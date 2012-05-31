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

import java.util.List;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigMapReduce;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.InternalCachedBag;
import org.apache.pig.data.NonSpillableDataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.io.NullableTuple;
import org.apache.pig.impl.plan.NodeIdGenerator;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;

public class POJoinPackage extends POPackage {

    private static final long serialVersionUID = 1L;
    
    private POOptimizedForEach forEach;
    private boolean newKey = true;
    private Tuple res = null;
    private boolean lastInputTuple = false;
    private static final Tuple t1 = null;
    private static final Result eopResult = new Result(POStatus.STATUS_EOP, null);
    private boolean firstTime = true;
    private boolean useDefaultBag = false;

    public static final String DEFAULT_CHUNK_SIZE = "1000";

    private long chunkSize = Long.parseLong(DEFAULT_CHUNK_SIZE);
    private Result forEachResult;
    private DataBag[] dbs = null;

    private int lastBagIndex;
    
    public POJoinPackage(OperatorKey k, int rp, POPackage p, POForEach f) {
        super(k, rp);
        String scope = getOperatorKey().getScope();
        NodeIdGenerator nig = NodeIdGenerator.getGenerator();
        forEach = new POOptimizedForEach(new OperatorKey(scope,nig.getNextNodeId(scope)));
        if (p!=null)
        {
            setKeyType(p.getKeyType());
            setNumInps(p.getNumInps());
            lastBagIndex = numInputs - 1;
            setInner(p.getInner());
            setKeyInfo(p.getKeyInfo());
            this.isKeyTuple = p.isKeyTuple;
            this.isKeyCompound = p.isKeyCompound;
        }
        if (f!=null)
        {
            setInputPlans(f.getInputPlans());
            setToBeFlattened(f.getToBeFlattened());
        }
    }

    @Override
    public void visit(PhyPlanVisitor v) throws VisitorException {
        v.visitJoinPackage(this);
    }

    @Override
    public String name() {
        String fString = forEach.getFlatStr();
        return "POJoinPackage" + "(" + fString + ")" + "[" + DataType.findTypeName(resultType) + "]" +" - " + mKey.toString();
    }

    /**
     * Calls getNext to get next ForEach result. The input for POJoinPackage is 
     * a (key, NullableTuple) pair. We will materialize n-1 inputs into bags, feed input#n 
     * one tuple a time to the delegated ForEach operator, the input for ForEach is
     * 
     *     (input#1, input#2, input#3....input#n[i]), i=(1..k), suppose input#n consists
     *     
     * of k tuples.
     * For every ForEach input, pull all the results from ForEach.
     * getNext will be called multiple times for a particular input, 
     * it returns one output tuple from ForEach every time we call getNext, 
     * so we need to maintain internal status to keep tracking of where we are.
     */
    @Override
    public Result getNext(Tuple t) throws ExecException {
        
        if(firstTime){
            firstTime = false;
            if (PigMapReduce.sJobConfInternal.get() != null) {
                String bagType = PigMapReduce.sJobConfInternal.get().get("pig.cachedbag.type");
                if (bagType != null && bagType.equalsIgnoreCase("default")) {
                    useDefaultBag = true;
                }
            }
        }
        // if a previous call to foreach.getNext()
        // has still not returned all output, process it
        if (forEach.processingPlan)
        {
            forEachResult = forEach.getNext(t1);
            switch (forEachResult.returnStatus)
            {
            case POStatus.STATUS_OK:
            case POStatus.STATUS_NULL:
            case POStatus.STATUS_ERR:
                return forEachResult;
            case POStatus.STATUS_EOP:
                break;
            }
        }
        
        NullableTuple it = null;
        
        // If we see a new NullableTupleIterator, materialize n-1 inputs, construct ForEach input
        // tuple res = (key, input#1, input#2....input#n), the only missing value is input#n,
        // we will get input#n one tuple a time, fill in res, feed to ForEach.
        // After this block, we have the first tuple of input#n in hand (kept in variable it)
        if (newKey)
        {
            lastInputTuple = false;
            //Put n-1 inputs into bags
            dbs = new DataBag[numInputs];
            for (int i = 0; i < numInputs - 1; i++) {
                dbs[i] = useDefaultBag ? BagFactory.getInstance().newDefaultBag() 
                // In a very rare case if there is a POStream after this 
                // POJoinPackage in the pipeline and is also blocking the pipeline;
                // constructor argument should be 2 * numInputs. But for one obscure
                // case we don't want to pay the penalty all the time.        
                        : new InternalCachedBag(numInputs-1);                    
            }
            // For last bag, we always use NonSpillableBag.
            dbs[lastBagIndex] = new NonSpillableDataBag((int)chunkSize);
            
            //For each Nullable tuple in the input, put it
            //into the corresponding bag based on the index,
            // except for the last input, which we will stream
            // The tuples will arrive in the order of the index,
            // starting from index 0 and such that all tuples for
            // a given index arrive before a tuple for the next
            // index does.
            while (tupIter.hasNext()) {
                it = tupIter.next();
                int itIndex = it.getIndex();
                if (itIndex!= numInputs - 1)
                {
                    dbs[itIndex].add(getValueTuple(it, itIndex));
                }
                else
                {
                    lastInputTuple = true;
                    break;
                }
                if(reporter!=null) reporter.progress();
            }
            // If we don't have any tuple for input#n
            // we do not need any further process, return EOP
            if (!lastInputTuple)
            {
                // we will return at this point because we ought
                // to be having a flatten on this last input
                // and we have an empty bag which should result
                // in this key being taken out of the output
                newKey = true;
                return eopResult;
            }
            
            res = mTupleFactory.newTuple(numInputs+1);
            for (int i = 0; i < dbs.length; i++)
                res.set(i+1,dbs[i]);

            res.set(0,key);
            // if we have an inner anywhere and the corresponding
            // bag is empty, we can just return
            for (int i = 0; i < dbs.length - 1; i++) {
                if(inner[i]&&dbs[i].size()==0){
                    detachInput();
                    return eopResult;
                }
            }
            newKey = false;
            
			// set up the bag with last input to contain
			// a chunk of CHUNKSIZE values OR the entire bag if
			// it has less than CHUNKSIZE values - the idea is in most
			// cases the values are > CHUNKSIZE in number and in 
			// those cases we will be sending the last bag
			// as a set of smaller chunked bags thus holding lesser
			// in memory
			
			// the first tuple can be directly retrieved from "it"
			dbs[lastBagIndex].add(getValueTuple(it, it.getIndex()));
			for(int i = 0; i < chunkSize -1 && tupIter.hasNext(); i++) {
			    it = tupIter.next();
			    dbs[lastBagIndex].add(getValueTuple(it, it.getIndex()));
			}

			// Attach the input to forEach
            forEach.attachInput(res);
            
            // pull output tuple from ForEach
            Result forEachResult = forEach.getNext(t1);
            {
                switch (forEachResult.returnStatus)
                {
                case POStatus.STATUS_OK:
                case POStatus.STATUS_NULL:
                case POStatus.STATUS_ERR:
                    return forEachResult;
                case POStatus.STATUS_EOP:
                    break;
                }
            }
        }
        
        // Keep attaching input tuple to ForEach, until:
        // 1. We can initialize ForEach.getNext();
        // 2. There is no more input#n
        while (true)
        {
            if (tupIter.hasNext()) {
                // try setting up a bag of CHUNKSIZE OR
                // the remainder of the bag of last input
                // (if < CHUNKSIZE) to foreach
                dbs[lastBagIndex].clear(); // clear last chunk
                for(int i = 0; i < chunkSize && tupIter.hasNext(); i++) {
                    it = tupIter.next();
                    dbs[lastBagIndex].add(getValueTuple(it, it.getIndex()));
                }
            }
            else
            // if we do not have any more tuples for input#n, return EOP
            {
                detachInput();
                newKey = true;
                return eopResult;
            }
            // Attach the input to forEach
            forEach.attachInput(res);
            
            // pull output tuple from ForEach
            Result forEachResult = forEach.getNext(t1);
            {
                switch (forEachResult.returnStatus)
                {
                case POStatus.STATUS_OK:
                case POStatus.STATUS_NULL:
                case POStatus.STATUS_ERR:
                    return forEachResult;
                case POStatus.STATUS_EOP:
                    break;
                }
            }
        }
    }
    
    public List<PhysicalPlan> getInputPlans() {
        return forEach.getInputPlans();
    }

    public void setInputPlans(List<PhysicalPlan> plans) {
        forEach.setInputPlans(plans);
    }

    public void setToBeFlattened(List<Boolean> flattens) {
        forEach.setToBeFlattened(flattens);
    }

    /**
     * @return the forEach
     */
    public POOptimizedForEach getForEach() {
        return forEach;
    }

    /**
     * @param chunkSize - the chunk size for the biggest input
     */
    public void setChunkSize(long chunkSize) {
        this.chunkSize = chunkSize;
    }
}

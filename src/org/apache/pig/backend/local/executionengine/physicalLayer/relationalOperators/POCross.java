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
package org.apache.pig.backend.local.executionengine.physicalLayer.relationalOperators;

import java.util.Iterator;
import java.util.List;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.pen.util.ExampleTuple;

/**
 * This is a local implementation of the cross. Its a blocking operator.
 * It accumulates inputs into databags and then applies logic similar to 
 * foreach flatten(*) to get the output tuples
 * 
 *
 */
public class POCross extends PhysicalOperator {
    
    DataBag [] inputBags;
    Tuple [] data;
    Iterator [] its;

    public POCross(OperatorKey k) {
        super(k);
        // TODO Auto-generated constructor stub
    }

    public POCross(OperatorKey k, int rp) {
        super(k, rp);
        // TODO Auto-generated constructor stub
    }

    public POCross(OperatorKey k, List<PhysicalOperator> inp) {
        super(k, inp);
        // TODO Auto-generated constructor stub
    }

    public POCross(OperatorKey k, int rp, List<PhysicalOperator> inp) {
        super(k, rp, inp);
        // TODO Auto-generated constructor stub
    }

    @Override
    public void visit(PhyPlanVisitor v) throws VisitorException {
        // TODO Auto-generated method stub
        v.visitCross(this);

    }
    
    @Override
    public Result getNext(Tuple t) throws ExecException {
        Result res = new Result();
        int noItems = inputs.size();
        if(inputBags == null) {
            accumulateData();
        }
        
        if(its != null) {
            //we check if we are done with processing
            //we do that by checking if all the iterators are used up
            boolean finished = true;
            for(int i = 0; i < its.length; i++) {
                finished &= !its[i].hasNext();
            }
            if(finished) {
                res.returnStatus = POStatus.STATUS_EOP;
                return res;
            }
            
        }
        
        if(data == null) {
            //getNext being called for the first time or starting on new input data
            //we instantiate the template array and start populating it with data
            data = new Tuple[noItems];
            for(int i = 0; i < noItems; ++i) {
                data[i] = (Tuple) its[i].next();

            }
            res.result = CreateTuple(data);
            res.returnStatus = POStatus.STATUS_OK;
            return res;
        } else {
            for(int index = noItems - 1; index >= 0; --index) {
                if(its[index].hasNext()) {
                    data[index] =  (Tuple) its[index].next();
                    res.result = CreateTuple(data);
                    res.returnStatus = POStatus.STATUS_OK;
                    return res;
                }
                else{
                    // reset this index's iterator so cross product can be achieved
                    // we would be resetting this way only for the indexes from the end
                    // when the first index which needs to be flattened has reached the
                    // last element in its iterator, we won't come here - instead, we reset
                    // all iterators at the beginning of this method.
                    its[index] = (inputBags[index]).iterator();
                    data[index] = (Tuple) its[index].next();
                }

            }
        }
        
        return null;
    }
    
    private void accumulateData() throws ExecException {
        int count = 0;
        inputBags = new DataBag[inputs.size()];
        
        its = new Iterator[inputs.size()];
        for(PhysicalOperator op : inputs) {
            DataBag bag = BagFactory.getInstance().newDefaultBag();
            inputBags[count] = bag;
            for(Result res = op.getNext(dummyTuple); res.returnStatus != POStatus.STATUS_EOP; res = op.getNext(dummyTuple)) {
                if(res.returnStatus == POStatus.STATUS_NULL)
                    continue;
                if(res.returnStatus == POStatus.STATUS_ERR)
                    throw new ExecException("Error accumulating data in the local Cross operator");
                if(res.returnStatus == POStatus.STATUS_OK)
                    bag.add((Tuple) res.result);
            }
            its[count++] = bag.iterator();
        }
    }
    
    private Tuple CreateTuple(Tuple[] data) throws ExecException {
        Tuple out =  TupleFactory.getInstance().newTuple();
        
        for(int i = 0; i < data.length; ++i) {
            Tuple t = data[i];
            int size = t.size();
            for(int j = 0; j < size; ++j) {
                out.append(t.get(j));
            }

        }
        
        if(lineageTracer != null) {
            ExampleTuple tOut = new ExampleTuple();
            tOut.reference(out);
            lineageTracer.insert(tOut);
            for(int i = 0; i < data.length; i++) {
                lineageTracer.union(tOut, data[i]);
            }
            return tOut;
        }
        return out;
    }

    @Override
    public String name() {
        // TODO Auto-generated method stub
        return "POCrossLocal" + " - " + mKey.toString();
    }

    @Override
    public boolean supportsMultipleInputs() {
        // TODO Auto-generated method stub
        return true;
    }

    @Override
    public boolean supportsMultipleOutputs() {
        // TODO Auto-generated method stub
        return false;
    }

}

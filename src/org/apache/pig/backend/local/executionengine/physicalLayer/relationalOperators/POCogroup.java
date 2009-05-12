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

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.SortedDataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.pen.util.ExampleTuple;

/** This is a local implementation of Cogroup.
 * The inputs need to be connected to LocalRearranges possibly by the
 * logical to physical translator.
 * 
 * This is a blocking operator. The outputs of LRs are put into
 * SortedDataBags. They are sorted on the keys. We then start pulling
 * tuple out of these bags and start constructing output.
 * 
 *
 */
public class POCogroup extends PhysicalOperator {
    
    Tuple[] data = null;
    Iterator<Tuple>[] its = null;
    boolean[] inner;

    public POCogroup(OperatorKey k) {
	super(k);
	// TODO Auto-generated constructor stub
    }

    public POCogroup(OperatorKey k, int rp) {
	super(k, rp);
	// TODO Auto-generated constructor stub
    }

    public POCogroup(OperatorKey k, List<PhysicalOperator> inp) {
	super(k, inp);
    }

    public POCogroup(OperatorKey k, int rp, List<PhysicalOperator> inp) {
	super(k, rp, inp);
    }
    
    public void setInner(boolean[] inner) {
        this.inner = inner;
    }

    @Override
    public void visit(PhyPlanVisitor v) throws VisitorException {
	// TODO Auto-generated method stub
	v.visitCogroup(this);

    }

    @Override
    public String name() {
	// TODO Auto-generated method stub
	return "POCogroup" + "[" + DataType.findTypeName(resultType) + "]" +" - " + mKey.toString();
    }
    
    @Override
    public Result getNext(Tuple t) throws ExecException{
	if(its == null) {
	    accumulateData();
	}
	
	boolean done = true;
	Result res = new Result();
	for(int i = 0; i < data.length; i++) {
	    done &= (data[i] == null);
	}
	if(done) {
	    res.returnStatus = POStatus.STATUS_EOP;
	    its = null;
	    return res;
	}
	
	Tuple smallestTuple = getSmallest(data);
	Comparator<Tuple> comp = new groupComparator();
	
	int size = data.length;
	
	Tuple output = TupleFactory.getInstance().newTuple(size + 1);
	
	output.set(0, smallestTuple.get(1));
	for(int i = 1; i < size + 1; i++) {
	    output.set(i, BagFactory.getInstance().newDefaultBag());
	}
	ExampleTuple tOut = null;
	if(lineageTracer != null) {
	    tOut = new ExampleTuple(output);
	    lineageTracer.insert(tOut);
	}
	
	boolean loop = true;
	
	while(loop) {
	    loop = false;
	    for(int i = 0; i < size; i++) {
		if(data[i] != null && comp.compare(data[i], smallestTuple) == 0) {
		    loop = true;
		    DataBag bag = (DataBag) output.get(i + 1);
		    //update lineage if it exists
		    //Tuple temp = ((IndexedTuple) data[i].get(1)).toTuple();
		    Tuple temp = (Tuple) data[i].get(2);
		    if(lineageTracer != null) {
			if(((ExampleTuple)temp).synthetic) tOut.synthetic = true;
			lineageTracer.union(temp, tOut);
		    }
		    //bag.add(((IndexedTuple) data[i].get(1)).toTuple());
		    bag.add(temp);
		    if(its[i].hasNext()) 
			data[i] = its[i].next();
		    else
			data[i] = null;
			
		    
		}
	    }
	}
	if(lineageTracer != null)
	    res.result = tOut;
	else
	    res.result = output;
	
	res.returnStatus = POStatus.STATUS_OK;
//    System.out.println(output);
	for(int i = 0; i < size; i++) {
	    if(inner != null && inner[i] && ((DataBag)output.get(i+1)).size() == 0) {
	        res.returnStatus = POStatus.STATUS_NULL;
	        break;
	    }
	}
	
	
	return res;
    }
    
    private void accumulateData() throws ExecException {
	int size = inputs.size();
	its = new Iterator[size];
	data = new Tuple[size];
	for(int i = 0; i < size; i++) {
	    DataBag bag = new SortedDataBag(new groupComparator());
	    for(Result input = inputs.get(i).getNext(dummyTuple); input.returnStatus != POStatus.STATUS_EOP; input = inputs.get(i).getNext(dummyTuple)) {
	        if(input.returnStatus == POStatus.STATUS_ERR) {
	            throw new ExecException("Error accumulating output at local Cogroup operator");
	        }
	        if(input.returnStatus == POStatus.STATUS_NULL)
	            continue;
	        bag.add((Tuple) input.result);
	    }
	    
	    its[i] = bag.iterator();
	    data[i] = its[i].next();
	}

    }
    
//    private Tuple getSmallest(Tuple[] data) {
//	Tuple t = (Tuple) data[0];
//	Comparator<Tuple> comp = new groupComparator();
//	for(int i = 1; i < data.length; i++) {
//	    if(comp.compare(t, (Tuple) data[i]) < 0) 
//		t = data[i];
//	}
//	return t;
//    }
    
    private Tuple getSmallest(Tuple[] data) {
	Tuple t = null;
	Comparator<Tuple> comp = new groupComparator();
	
	for(int i = 0; i < data.length; i++) {
	    if(data[i] == null) continue;
	    if(t == null) {
		t = data[i];
		continue; //since the previous data was probably null so we dont really need a comparison
	    }
	    if(comp.compare(t, (Tuple) data[i]) > 0) 
		t = data[i];
	}
	return t;
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
    
    private class groupComparator implements Comparator<Tuple> {

	public int compare(Tuple o1, Tuple o2) {
	    //We want to make it as efficient as possible by only comparing the keys
	    Object t1 = null;
	    Object t2 = null;
	    try {
		t1 = o1.get(1);
		t2 = o2.get(1);
	    
	    } catch (ExecException e) {
		// TODO Auto-generated catch block
		throw new RuntimeException("Error comparing tuples");
	    }
	    
	    return DataType.compare(t1, t2);
	}
	
	public boolean equals(Object obj) {
	    return this.equals(obj);
	}
	
    }

}

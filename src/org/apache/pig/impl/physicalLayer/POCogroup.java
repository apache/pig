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
package org.apache.pig.impl.physicalLayer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.pig.data.AmendableTuple;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Datum;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.eval.EvalSpec;
import org.apache.pig.impl.eval.collector.DataCollector;
import org.apache.pig.impl.logicalLayer.LOCogroup;
import org.apache.pig.impl.logicalLayer.LogicalOperator;


// n-ary, blocking operator. Output has schema: < group_label, { <1>, <2>, ... }, { <a>, <b>, ... } >
class POCogroup extends PhysicalOperator {
    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	List<Datum[]>[] sortedInputs;
    List<EvalSpec>       specs;
        
    public POCogroup(List<EvalSpec> specs, int outputType) {
    	super(outputType);
        this.inputs = new PhysicalOperator[specs.size()];
        this.specs = specs;
    }

    // drain all inputs, and sort each by group (remember, this is a blocking operator!)
    @Override
	@SuppressWarnings("unchecked")
    public boolean open(boolean continueFromLast) throws IOException {
        if (!super.open(continueFromLast))
            return false;

        sortedInputs = new ArrayList[inputs.length];

        for (int i = 0; i < inputs.length; i++) {
        	
        	final int finalI = i;
            sortedInputs[i] = new ArrayList<Datum[]>();
            
            DataCollector outputFromSpec = new DataCollector(null){
            	@Override
            	public void add(Datum d) {
            		sortedInputs[finalI].add(LOCogroup.getGroupAndTuple(d));
            	}
            };
            
            DataCollector inputToSpec = specs.get(i).setupPipe(outputFromSpec);

            Tuple t;            
            while ((t = (Tuple) inputs[i].getNext()) != null) {
                inputToSpec.add(t);
            }
            inputToSpec.finishPipe();

            Collections.sort(sortedInputs[i], new Comparator<Datum[]>() {
                public int compare(Datum[] a, Datum[] b) {
                    return a[0].compareTo(b[0]);
                }
            });
        }

        return true;
    }

    @Override
	public Tuple getNext() throws IOException {
        
        while (true) { // loop until we find a tuple we're allowed to output (or we hit the end)

            // find the smallest group among all inputs (this is the group we should make a tuple
            // out of)
            Datum smallestGroup = null;
            for (int i = 0; i < inputs.length; i++) {
                if (sortedInputs[i].size() > 0) {
                    Datum g = (sortedInputs[i].get(0))[0];
                    if (smallestGroup == null || g.compareTo(smallestGroup)<0)
                        smallestGroup = g;
                }
            }

            if (smallestGroup == null)
                return null; // we hit the end of the groups, so we're done

            // find all tuples in each input pertaining to the group of interest, and combine the
            // data into a single tuple
            
            Tuple output;
            if (outputType == LogicalOperator.AMENDABLE) output = new AmendableTuple(1 + inputs.length, smallestGroup);
            else output = new Tuple(1 + inputs.length);

            // set first field to the group tuple
            output.setField(0, smallestGroup);
            
            if (lineageTracer != null) lineageTracer.insert(output);

            boolean done = true;
            for (int i = 0; i < inputs.length; i++) {
                DataBag b = BagFactory.getInstance().getNewBag();

                while (sortedInputs[i].size() > 0) {
                    Datum g = sortedInputs[i].get(0)[0];

                    Tuple t = (Tuple) sortedInputs[i].get(0)[1];

                    if (g.compareTo(smallestGroup) < 0) {
                        sortedInputs[i].remove(0); // discard this tuple
                    } else if (g.equals(smallestGroup)) {
                        b.add(t);
                        if (lineageTracer != null) lineageTracer.union(t, output);   // update lineage
                        sortedInputs[i].remove(0);
                    } else {
                        break;
                    }
                }

                if (specs.get(i).isInner() && b.isEmpty())
                    done = false; // this input uses "inner" semantics, and it has no tuples for
                                    // this group, so suppress the tuple we're currently building

                output.setField(1 + i, b);
            }

            if (done)
                return output;
        }

    }

    public void visit(POVisitor v) {
        v.visitCogroup(this);
    }

}

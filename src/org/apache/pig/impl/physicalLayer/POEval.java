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

import org.apache.pig.data.Tuple;
import org.apache.pig.impl.eval.EvalSpec;
import org.apache.pig.impl.eval.collector.DataCollector;
import org.apache.pig.impl.util.DataBuffer;


// unary, non-blocking operator.
class POEval extends PhysicalOperator {
    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	EvalSpec spec;
    private DataBuffer buf = null;
    private DataCollector evalPipeline = null;
    private boolean inputDrained;
    Tuple lastAdded = null;   // for lineage bookkeeping

    public POEval(PhysicalOperator input, EvalSpec specIn, int outputType) {
    	super(outputType);
    	inputs = new PhysicalOperator[1];
        inputs[0] = input;

        spec = specIn;
    }

    public POEval(EvalSpec specIn, int outputType) {
    	super(outputType);
        inputs = new PhysicalOperator[1];
        inputs[0] = null;

        spec = specIn;
    }

    @Override
	public boolean open(boolean continueFromLast) throws IOException {
        if (!super.open(continueFromLast)) return false;
    
        if (buf==null)
        	buf = new DataBuffer();
        if (evalPipeline == null)
        	evalPipeline = spec.setupPipe(buf);
            
        inputDrained = false;
        
        return true;
    }

    @Override
	public Tuple getNext() throws IOException {
    	while (true) {        	
        	if (buf.isEmpty()){
                // no pending outputs, so look to input to provide more food
                
                if (inputDrained){
                    return null;
                }
                	                
                Tuple nextTuple = inputs[0].getNext();
                
                if (nextTuple == null){
                    inputDrained = true;
                    evalPipeline.finishPipe();
                }else{
                	evalPipeline.add(nextTuple);
                	lastAdded = nextTuple;   // for lineage bookkeeping
                }
        	}else{
        		Tuple output = (Tuple)buf.removeFirst();
        		if (lineageTracer != null) {
        		    lineageTracer.insert(output);
        		    if (lastAdded != null) lineageTracer.union(lastAdded, output);   // update lineage (assumes one-to-many relationship between tuples added to pipeline and output!!)
        		}
        		return output;
        	}            
        }
    }
       
}

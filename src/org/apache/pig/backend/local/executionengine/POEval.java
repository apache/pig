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
package org.apache.pig.backend.local.executionengine;

import java.io.IOException;
import java.io.PrintStream;
import java.util.List;
import java.util.Map;

import org.apache.pig.backend.executionengine.ExecPhysicalOperator;
import org.apache.pig.data.ExampleTuple;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.eval.EvalSpec;
import org.apache.pig.impl.eval.collector.DataCollector;
import org.apache.pig.impl.physicalLayer.PhysicalOperator;
import org.apache.pig.impl.physicalLayer.POVisitor;
import org.apache.pig.impl.util.DataBuffer;
import org.apache.pig.impl.logicalLayer.OperatorKey;

// unary, non-blocking operator.
public class POEval extends PhysicalOperator {
    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    EvalSpec spec;
    private DataBuffer buf = null;
    private DataCollector evalPipeline = null;
    private boolean inputDrained;
    Tuple lastAdded = null;   // for lineage bookkeeping

    public POEval(String scope, 
                  long id, 
                  Map<OperatorKey, ExecPhysicalOperator> opTable,
                  OperatorKey input, 
                  EvalSpec specIn, 
                  int outputType) {
        super(scope, id, opTable, outputType);
        inputs = new OperatorKey[1];
        inputs[0] = input;

        spec = specIn;
    }

    public POEval(String scope, 
                  long id, 
                  Map<OperatorKey, ExecPhysicalOperator> opTable,
                  EvalSpec specIn, 
                  int outputType) {
        super(scope, id, opTable, outputType);
        inputs = new OperatorKey[1];
        inputs[0] = null;

        spec = specIn;
    }

    @Override
    public boolean open() throws IOException {
        if (!super.open()) return false;
    
        if (buf==null)
            buf = new DataBuffer();
        if (evalPipeline == null)
            evalPipeline = spec.setupPipe(null, buf);
            
        inputDrained = false;
        if(lineageTracer != null) spec.setLineageTracer(lineageTracer);
        
        return true;
    }
    
    /*@Override
    public void close() throws IOException {
    	// call close() on all inputs
        if (inputs != null)
            for (int i = 0; i < inputs.length; i++)
                ((PhysicalOperator)opTable.get(inputs[i])).close();
        buf = null;
        evalPipeline = null;
    }*/

    @Override
    public Tuple getNext() throws IOException {
        while (true) {            
            if (buf.isEmpty()){
                // no pending outputs, so look to input to provide more food
                
                if (inputDrained){
                    return null;
                }
                                    
                Tuple nextTuple = ((PhysicalOperator)opTable.get(inputs[0])).getNext();
                
                if (nextTuple == null){
                    inputDrained = true;
                    evalPipeline.finishPipe();
                }else{
                    evalPipeline.add(nextTuple);
                    lastAdded = nextTuple;   // for lineage bookkeeping
                }
            }else{
                Tuple output = (Tuple)buf.removeFirst();
                ExampleTuple tOut = new ExampleTuple();
                tOut.copyFrom(output);
                if (lineageTracer != null) {
        			List<Tuple> children = lineageTracer.getFlattenChildren(output);
        			if(children != null) {
        				//the output tuple we get is not a example tuple. so we take it out and put in the converted exampletuple
        				lineageTracer.removeFlattenMap(output);
        				lineageTracer.addFlattenMap(tOut, children);
        			}
        		    lineageTracer.insert(tOut);
        		    if (lastAdded != null) {
        		    	if(((ExampleTuple)lastAdded).isSynthetic()) tOut.makeSynthetic();
        		    	lineageTracer.union(lastAdded, tOut);   // update lineage (assumes one-to-many relationship between tuples added to pipeline and output!!)
        		    	//lineageTracer.union(tOut, lastAdded);
        		    }
        		}
                /*if (lineageTracer != null) {
                    lineageTracer.insert(output);
                    if (lastAdded != null) lineageTracer.union(lastAdded, output);   // update lineage (assumes one-to-many relationship between tuples added to pipeline and output!!)
                }*/
                //return output;
                if(lineageTracer != null)
                	return tOut;
                else
                	return output;
            }            
        }
    }
       
    public void visit(POVisitor v) {
        v.visitEval(this);
    }

}

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
import java.io.Serializable;

import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.LogicalOperator;
import org.apache.pig.impl.util.LineageTracer;


abstract public class PhysicalOperator implements Serializable {

    public PhysicalOperator[] inputs = null;
    int outputType;
    protected LineageTracer lineageTracer = null;
    
    /**
     * 
     * This constructor should go away when we eventually implement CQ even for the mapreduce exec type 
     */
    public PhysicalOperator(){
    	outputType = LogicalOperator.FIXED;
    }
   
    
    public PhysicalOperator(int outputType){
    	this.outputType = outputType;
    }
    
    public boolean open(boolean continueFromLast) throws IOException {
        // call open() on all inputs
        if (inputs != null) {
            for (int i = 0; i < inputs.length; i++) {
                if (!inputs[i].open(continueFromLast))
                    return false;
            }
        }
        return true;
    }

    abstract public Tuple getNext() throws IOException;

    public void close() throws IOException {
        // call close() on all inputs
        if (inputs != null)
            for (int i = 0; i < inputs.length; i++)
                inputs[i].close();
    }
    
    public void setLineageTracer(LineageTracer lineageTracer) {
        this.lineageTracer = lineageTracer;
    }

    public int getOutputType(){
    	return outputType;
    }

    public abstract void visit(POVisitor v);
}

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


class POUnion extends PhysicalOperator {
    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	int currentInput;

    public POUnion(PhysicalOperator[] inputsIn, int outputType) {
    	super(outputType);
        inputs = inputsIn;
        currentInput = 0;
    }

    public POUnion(int numInputs, int outputType) {
    	super(outputType);
        inputs = new PhysicalOperator[numInputs];
        for (int i = 0; i < inputs.length; i++)
            inputs[i] = null;
    }
    
    @Override
	public boolean open(boolean continueFromLast) throws IOException{
    	if (!super.open(continueFromLast)){
    		return false;
    	}
    	currentInput = 0;
    	return true;
    }

    @Override
	public Tuple getNext() throws IOException {
        while (currentInput < inputs.length) {
            Tuple t = inputs[currentInput].getNext();

            if (t == null) {
                currentInput++;
                continue;
            } else {
                Tuple output = t;
                if (lineageTracer != null) {
                    lineageTracer.insert(output);     // update lineage (this line is needed, to generate the correct counts)
                    lineageTracer.union(t, output);   // (this line amounts to a no-op)
                }
                return output;
            }
        }

        return null;
    }

    public void visit(POVisitor v) {
        v.visitUnion(this);
    }

}

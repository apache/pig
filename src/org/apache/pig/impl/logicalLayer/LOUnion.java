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
package org.apache.pig.impl.logicalLayer;

import java.util.List;

import org.apache.pig.impl.logicalLayer.schema.TupleSchema;



public class LOUnion extends LogicalOperator {
	private static final long serialVersionUID = 1L;
	   
    public LOUnion(List<LogicalOperator> inputsIn) {
    	super(inputsIn);        
    }

    @Override
	public String name() {
        return "Union";
    }

    @Override
	public TupleSchema outputSchema() {
    	if (schema == null){
    		TupleSchema longest = getInputs().get(0).outputSchema();
	        int current = 0;
	        for (LogicalOperator lo : getInputs()) {
	            if (lo != null && lo.outputSchema() != null && lo.outputSchema().numFields() > current) {
	                longest = lo.outputSchema();
	                current = longest.numFields();
	            }
	        }
	        schema = longest.copy();
    	}
    	
    	schema.setAlias(alias);
        return schema;
    }
	
	@Override
	public int getOutputType(){
	 	int outputType = FIXED;
    	for (int i=0; i<getInputs().size(); i++){
    		switch (getInputs().get(i).getOutputType()){
    			case FIXED: 
    				continue;
    			case MONOTONE: 
    				if (outputType == FIXED)
    					outputType = MONOTONE;
    				continue;
    			case AMENDABLE:
    				outputType = AMENDABLE;
    			default:	
    				throw new RuntimeException("Invalid input type to the UNION operator");
    		} 
    	}
    	return outputType;
	}
}

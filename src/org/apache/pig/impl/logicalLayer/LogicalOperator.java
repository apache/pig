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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.pig.impl.logicalLayer.schema.TupleSchema;



abstract public class LogicalOperator implements Serializable {
    public String            alias                = null;
    
    public static final int  FIXED                = 1;
    public static final int  MONOTONE             = 2;
    public static final int  UPDATABLE            = 3;   // Reserved for future use
    public static final int  AMENDABLE            = 4;

	protected int            requestedParallelism = -1;
	protected TupleSchema    schema               = null;
	protected List<LogicalOperator> inputs;
	
	protected LogicalOperator(){
		this.inputs = new ArrayList<LogicalOperator>();
	}
	
	protected LogicalOperator(List<LogicalOperator> inputs) {
		this.inputs = inputs;
	}

	protected LogicalOperator(LogicalOperator input) {
		this.inputs = new ArrayList<LogicalOperator>();
		inputs.add(input);
	}

	public String getAlias() {
		return alias;
	}

	public void setAlias(String newAlias) {
		alias = newAlias;
	}

	public int getRequestedParallelism() {
		return requestedParallelism;
	}

	public void setRequestedParallelism(int newRequestedParallelism) {
		requestedParallelism = newRequestedParallelism;
	}

	@Override
	public String toString() {
		StringBuffer result = new StringBuffer(super.toString());
		result.append(" (alias: ");
		result.append(alias);
		result.append(", requestedParallelism: ");
		result.append(requestedParallelism);
		result.append(')');
		return result.toString();
	}

	public abstract TupleSchema outputSchema();

    public String name() {
        return "ROOT";
    }

    public List<LogicalOperator> getInputs() {
        return inputs;
    }

    public String arguments() {
        return "";
    }

    public List<String> getFuncs() {
        List<String> funcs = new LinkedList<String>();
        for (int i = 0; i < inputs.size(); i++) {
            funcs.addAll(inputs.get(i).getFuncs());
        }
        return funcs;
    }
    
    public abstract int getOutputType();

	public void setSchema(TupleSchema schema) {
		this.schema = schema;
	}
}

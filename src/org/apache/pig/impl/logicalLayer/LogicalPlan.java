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
import java.util.LinkedList;
import java.util.List;

import org.apache.pig.impl.PigContext;




public class LogicalPlan implements Serializable{
	private static final long serialVersionUID = 1L;

	protected LogicalOperator root;
	protected PigContext pigContext = null;
	
	protected String alias = null;

	public LogicalOperator getRoot() {
		return root;
	}

	public LogicalPlan(LogicalOperator rootIn, PigContext pigContext) {
		this.pigContext = pigContext;
		root = rootIn;
        alias = root.alias;
	}
	
    public void setRoot(LogicalOperator newRoot) {
        root = newRoot;
    }
	
	public PigContext getPigContext() {
		return pigContext;
	}

	public String getAlias() {
		return alias;
	}

	public void setAlias(String newAlias) {
		alias = newAlias;
	}


	public List<String> getFuncs() {
        if (root == null) return new LinkedList<String>();
        else return root.getFuncs();
    }
	
	// indentation for root is 0
	@Override
	public String toString() {		
		StringBuffer sb = new StringBuffer();
		sb.append(root.name() +"(" + root.arguments() +")\n");
		sb.append(appendChildren(root, 1));
		return sb.toString();
	}
	public String appendChildren(LogicalOperator parent, int indentation) {
		StringBuffer sb = new StringBuffer();
		List<LogicalOperator> children = parent.getInputs();
		for(int i=0; i != children.size(); i++) {
			for(int j=0; j != indentation; j++) {
				sb.append("\t");
			}
			sb.append(children.get(i).name() + "(" + children.get(i).arguments()+ ")\n");
			sb.append(appendChildren(children.get(i), indentation+1));
		}
		return sb.toString();
	}
	
	public int getOutputType(){
		return root.getOutputType();
	}
	
}

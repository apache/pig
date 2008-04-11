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
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.Map;

import org.apache.pig.impl.PigContext;

import org.apache.pig.backend.executionengine.ExecLogicalPlan;

public class LogicalPlan implements Serializable, ExecLogicalPlan{
    private static final long serialVersionUID = 1L;

    protected OperatorKey root;
    protected Map<OperatorKey, LogicalOperator> opTable;
    protected PigContext pigContext = null;
    
    protected String alias = null;
    
    public OperatorKey getRoot() {
        return root;
    }

    public LogicalPlan(OperatorKey rootIn,
                       Map<OperatorKey, LogicalOperator> opTable ,
                       PigContext pigContext) {
        this.pigContext = pigContext;
        this.root = rootIn;
        this.opTable = opTable;
        alias = opTable.get(root).alias;
    }
    
    public Map<OperatorKey, LogicalOperator> getOpTable() {
        return this.opTable;
    }
    
    public LogicalOperator getRootOperator() {
    	return opTable.get(root);
    }
    
    public void setRoot(OperatorKey newRoot) {
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
        else return opTable.get(root).getFuncs();
    }
    
    // indentation for root is 0
    @Override
    public String toString() {        
        StringBuilder sb = new StringBuilder();
        sb.append(opTable.get(root).name() +"(" + opTable.get(root).arguments() +")\n");
        sb.append(appendChildren(root, 1));
        return sb.toString();
    }
    public String appendChildren(OperatorKey parent, int indentation) {
        StringBuilder sb = new StringBuilder();
        List<OperatorKey> children = opTable.get(parent).getInputs();
        for(int i=0; i != children.size(); i++) {
            for(int j=0; j != indentation; j++) {
                sb.append("\t");
            }
            
            sb.append(opTable.get(children.get(i)).name() + 
                      "(" + opTable.get(children.get(i)).arguments()+ ")\n");
            sb.append(appendChildren(children.get(i), indentation+1));
        }
        return sb.toString();
    }
    
    public int getOutputType(){
        return opTable.get(root).getOutputType();
    }
    
    public void explain(OutputStream out) {
    	LOVisitor lprinter =  new LOTreePrinter(new PrintStream(out));   	
        
        opTable.get(root).visit(lprinter);
    }
}

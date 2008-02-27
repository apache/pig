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
import java.util.Map;

import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.LogicalOperator;
import org.apache.pig.impl.logicalLayer.OperatorKey;
import org.apache.pig.impl.util.LineageTracer;
import org.apache.pig.backend.executionengine.ExecPhysicalOperator;
import org.apache.pig.impl.physicalLayer.POVisitor;

public abstract class PhysicalOperator implements Serializable, ExecPhysicalOperator {

    public Map<OperatorKey, ExecPhysicalOperator> opTable;
    public OperatorKey[] inputs = null;
    public int outputType;
    protected LineageTracer lineageTracer = null;
    protected String scope;
    protected long id;
    
    public PhysicalOperator(String scope, 
                            long id,
                            Map<OperatorKey, ExecPhysicalOperator> opTable, 
                            int outputType){
        this.scope = scope;
        this.id = id;
        this.opTable = opTable;
        this.outputType = outputType;
        
        opTable.put(getOperatorKey(), this);
    }
    
    public OperatorKey getOperatorKey() {
        return new OperatorKey(scope, id);
    }
    
    public String getScope() {
        return this.scope;
    }
    
    public long getId() {
        return this.id;
    }
    
    public boolean open() throws IOException {
        // call open() on all inputs
        if (inputs != null) {
            for (int i = 0; i < inputs.length; i++) {
                if (! ((PhysicalOperator)opTable.get(inputs[i])).open())
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
                ((PhysicalOperator)opTable.get(inputs[i])).close();
    }
    
    public void setLineageTracer(LineageTracer lineageTracer) {
        this.lineageTracer = lineageTracer;
    }

    public int getOutputType(){
        return outputType;
    }

    public abstract void visit(POVisitor v);
}

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
import java.util.ArrayList;
import java.util.Map;

import org.apache.pig.backend.executionengine.ExecPhysicalOperator;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.eval.cond.Cond;
import org.apache.pig.impl.physicalLayer.PhysicalOperator;
import org.apache.pig.impl.physicalLayer.POVisitor;
import org.apache.pig.impl.logicalLayer.OperatorKey;

public class POSplit extends PhysicalOperator {
    static final long serialVersionUID = 1L; 
    protected ArrayList<Cond> conditions;
    protected int readFromCond;

    public POSplit(String scope, 
                   long id, 
                   Map<OperatorKey, ExecPhysicalOperator> opTable, 
                   OperatorKey input, 
                   ArrayList<Cond> conditions,
                   int readFromCond,
                   int outputType){
        super(scope, id, opTable, outputType);
        
        this.inputs = new OperatorKey[1];
        this.inputs[0] = input;
        
        this.conditions = conditions;
        this.readFromCond = readFromCond;
    }
    
    @Override
    public boolean open() throws IOException{
        if (!super.open()){
            return false;
        }

        return ((PhysicalOperator)opTable.get(this.inputs[0])).open();
    }

    @Override
    public Tuple getNext() throws IOException {
        Tuple nextTuple = null;
        
        while ((nextTuple = ((PhysicalOperator)opTable.get(this.inputs[ 0 ])).getNext()) != null) {
            boolean emitTuple = this.conditions.get(this.readFromCond).eval(nextTuple);
            
            if (emitTuple) {
                break;
            }
        }

        return nextTuple;
    }

    public void visit(POVisitor v) {
        v.visitSplit(this);
    }
}

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
import java.util.Iterator;
import java.util.Map;

import org.apache.pig.backend.executionengine.ExecPhysicalOperator;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.eval.EvalSpec;
import org.apache.pig.impl.physicalLayer.PhysicalOperator;
import org.apache.pig.impl.physicalLayer.POVisitor;
import org.apache.pig.impl.logicalLayer.OperatorKey;


public class POSort extends PhysicalOperator {
    static final long serialVersionUID = 1L; 
    EvalSpec sortSpec;
    transient Iterator<Tuple> iter;
    
    
    public POSort(String scope, 
                  long id, 
                  Map<OperatorKey, ExecPhysicalOperator> opTable, 
                  EvalSpec sortSpec, 
                  int outputType) {
        super(scope, id, opTable, outputType);
        this.sortSpec = sortSpec;
        this.inputs = new OperatorKey[1];
    }

    @Override
    public boolean open() throws IOException {
        if (!super.open())
            return false;
        DataBag bag = BagFactory.getInstance().newSortedBag(sortSpec);
        
        Tuple t;
        while((t = ((PhysicalOperator)opTable.get(inputs[0])).getNext())!=null){
            bag.add(t);
        }
        iter = bag.iterator();
        return true;
    }
    
    @Override
    public Tuple getNext() throws IOException {
        if (iter.hasNext())
            return iter.next();
        else
            return null;
    }

    @Override
    public void visit(POVisitor v) {
        v.visitSort(this);
    }

}

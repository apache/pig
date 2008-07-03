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

import java.io.OutputStream;
import java.io.PrintStream;
import java.util.Properties;
import java.util.Map;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.executionengine.ExecPhysicalOperator;
import org.apache.pig.backend.executionengine.ExecPhysicalPlan;
import org.apache.pig.impl.physicalLayer.POTreePrinter;
import org.apache.pig.impl.physicalLayer.PhysicalOperator;
import org.apache.pig.impl.physicalLayer.POVisitor;
import org.apache.pig.impl.physicalLayer.POPrinter;
import org.apache.pig.impl.logicalLayer.OperatorKey;

public class LocalPhysicalPlan implements ExecPhysicalPlan {
    private static final long serialVersionUID = 1;
    
    protected OperatorKey root;
    protected Map<OperatorKey, ExecPhysicalOperator> opTable;
    
    LocalPhysicalPlan(OperatorKey root,
                      Map<OperatorKey, ExecPhysicalOperator> opTable) {
        this.root = root;
        this.opTable = opTable;
    }
    
    public Properties getConfiguration() {
        throw new UnsupportedOperationException();
    }

    public void updateConfiguration(Properties configuration)
        throws ExecException {
        throw new UnsupportedOperationException();
    }
             
    public void explain(OutputStream out) {
    	POVisitor lprinter = new POTreePrinter(opTable, new PrintStream(out));    	
        ((PhysicalOperator)opTable.get(root)).visit(lprinter);
    }
    
    public Map<OperatorKey, ExecPhysicalOperator> getOpTable() {
        return opTable;
    }
    
    public OperatorKey getRoot() {
        return root;
    }
}

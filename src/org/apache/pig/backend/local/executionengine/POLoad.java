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
import java.util.Map;

import org.apache.pig.LoadFunc;
import org.apache.pig.backend.executionengine.ExecPhysicalOperator;
import org.apache.pig.data.ExampleTuple;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.BufferedPositionedInputStream;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.physicalLayer.PhysicalOperator;
import org.apache.pig.impl.physicalLayer.POVisitor;
import org.apache.pig.impl.logicalLayer.OperatorKey;


public class POLoad extends PhysicalOperator {
    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    String   filename;
    LoadFunc lf;
    boolean bound = false;
    PigContext pigContext;

    public POLoad(String scope, 
                  long id, 
                  Map<OperatorKey, ExecPhysicalOperator> opTable,
                  PigContext pigContext, 
                  FileSpec fileSpec, 
                  int outputType) {
        super(scope, id, opTable, outputType);
        inputs = new OperatorKey[0];

        filename = fileSpec.getFileName();
        try{
            lf = (LoadFunc) PigContext.instantiateFuncFromSpec(fileSpec.getFuncSpec());
        }catch(Exception e){
            throw new RuntimeException(e);
        }

        this.pigContext = pigContext;
    }

    @Override
    public boolean open() throws IOException {
        if (!bound){
            lf.bindTo(filename, new BufferedPositionedInputStream(FileLocalizer.open(filename, pigContext)), 0, Long.MAX_VALUE);
            bound = true;
        }
        return true;
    }

    @Override
    public void close() throws IOException {
        super.close();
        bound = false;
    }
    
    @Override
    public Tuple getNext() throws IOException {
    	Tuple t = lf.getNext();
    	if(lineageTracer != null) {
    		ExampleTuple tOut = new ExampleTuple();
    		if(t != null) {
    			tOut.copyFrom(t);
    			return tOut;
    		}
    		return null;
    	}
    	return t;
        //return lf.getNext();
    }

    @Override
    public void visit(POVisitor v) {
        v.visitLoad(this);
   }

}

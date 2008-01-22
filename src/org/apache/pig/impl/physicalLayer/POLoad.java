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

import org.apache.pig.LoadFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.BufferedPositionedInputStream;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.io.FileSpec;


public class POLoad extends PhysicalOperator {
    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	String   filename;
    LoadFunc lf;
    boolean bound = false;
    PigContext pigContext;

    public POLoad(PigContext pigContext, FileSpec fileSpec, int outputType) {
    	super(outputType);
        inputs = new PhysicalOperator[0];

        filename = fileSpec.getFileName();
        try{
        	lf = (LoadFunc) PigContext.instantiateFuncFromSpec(fileSpec.getFuncSpec());
        }catch(Exception e){
        	throw new RuntimeException(e);
        }

        this.pigContext = pigContext;
    }

    @Override
	public boolean open(boolean continueFromLast) throws IOException {
    	if (!bound){
    		lf.bindTo(filename, new BufferedPositionedInputStream(FileLocalizer.open(filename, pigContext)), 0, Long.MAX_VALUE);
    		bound = true;
    	}
        return true;
    }

    @Override
	public Tuple getNext() throws IOException {
        return lf.getNext();
    }

    @Override
    public void visit(POVisitor v) {
        v.visitLoad(this);
    }

}

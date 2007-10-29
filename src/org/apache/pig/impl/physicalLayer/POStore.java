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

import org.apache.pig.StoreFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.io.PigFile;
import org.apache.pig.impl.logicalLayer.LogicalOperator;


public class POStore extends PhysicalOperator {
    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private PigFile f;
    private String funcSpec;
    boolean append = false;
    PigContext pigContext;

    public POStore(PhysicalOperator input, FileSpec outputFileSpec, boolean append, PigContext pigContext) {
    	super(LogicalOperator.FIXED);
    	funcSpec = outputFileSpec.getFuncSpec();
        inputs = new PhysicalOperator[1];
        inputs[0] = input;
        System.out.println("Creating " + outputFileSpec.getFileName());
        f = new PigFile(outputFileSpec.getFileName(), append);
        this.append = append;
        this.pigContext = pigContext;
    }

    public POStore(FileSpec outputFileSpec, boolean append, PigContext pigContext) {
    	super(LogicalOperator.FIXED);
        inputs = new PhysicalOperator[1];
        inputs[0] = null;
        funcSpec = outputFileSpec.getFuncSpec();
        f = new PigFile(outputFileSpec.getFileName(), append);
        this.append = append;
        this.pigContext = pigContext;
    }

    @Override
	public Tuple getNext() throws IOException {
        // get all tuples from input, and store them.
        DataBag b = new DataBag();
        Tuple t;
        while ((t = (Tuple) inputs[0].getNext()) != null) {
            b.add(t);
        }
        try {
        	StoreFunc func = (StoreFunc) PigContext.instantiateFuncFromSpec(funcSpec);
        	f.store(b, func, pigContext);
        } catch(IOException e) {
        	throw e;
        } catch(Exception e) {
        	IOException ne = new IOException(e.getClass().getName() + ": " + e.getMessage());
        	ne.setStackTrace(e.getStackTrace());
        	throw ne;
        }

        return null;
    }
    
    @Override
	public int getOutputType(){
    	System.err.println("No one should be asking my output type");
    	new RuntimeException().printStackTrace();
    	System.exit(1);
    	return -1;
    }

}

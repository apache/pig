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
package org.apache.pig.impl.builtin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.pig.ExecType;
import org.apache.pig.LoadFunc;
import org.apache.pig.SamplableLoader;
import org.apache.pig.backend.datastorage.DataStorage;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.BufferedPositionedInputStream;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.Pair;

/**
 * Abstract class that specifies the interface for sample loaders
 *
 */
//XXX : FIXME - make this work with new load-store redesign
public abstract class SampleLoader implements LoadFunc {

	protected int numSamples;
	protected long skipInterval;
    protected LoadFunc loader;
	private TupleFactory factory;
	private boolean initialized = false;

    
    public SampleLoader(String funcSpec) {
    	loader = (LoadFunc)PigContext.instantiateFuncFromSpec(funcSpec);
    }
    
    public void setNumSamples(int n) {
    	numSamples = n;
    }
    
    public int getNumSamples() {
    	return numSamples;
    }
    
    /* (non-Javadoc)
     * @see org.apache.pig.LoadFunc#getInputFormat()
     */
    @Override
    public InputFormat getInputFormat() {
        return loader.getInputFormat();
    }

    /* (non-Javadoc)
	 * @see org.apache.pig.LoadFunc#getNext()
	 */
	public Tuple getNext() throws IOException {
	   // estimate how many tuples there are in the map
	   // based on the 
	    return null;   
	}

	public void computeSamples(ArrayList<Pair<FileSpec, Boolean>> inputs, PigContext pc) throws ExecException {
		// TODO Auto-generated method stub
		
	}

}

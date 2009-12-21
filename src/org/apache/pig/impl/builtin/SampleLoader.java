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
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.Pair;
import org.mortbay.log.Log;

/**
 * Abstract class that specifies the interface for sample loaders
 *
 */
public abstract class SampleLoader implements LoadFunc {

	protected int numSamples;
	protected long skipInterval;
    protected SamplableLoader loader;
	private TupleFactory factory;

    
    public SampleLoader(String funcSpec) {
        funcSpec = funcSpec.replaceAll("\\\\'", "'");
        loader = (SamplableLoader)PigContext.instantiateFuncFromSpec(funcSpec);
    }
    
    public void setNumSamples(int n) {
    	numSamples = n;
    }
    
    public int getNumSamples() {
    	return numSamples;
    }
    
	/* (non-Javadoc)
	 * @see org.apache.pig.LoadFunc#bindTo(java.lang.String, org.apache.pig.impl.io.BufferedPositionedInputStream, long, long)
	 */
	public void bindTo(String fileName, BufferedPositionedInputStream is,
			long offset, long end) throws IOException {
        skipInterval = (end - offset)/numSamples;
        loader.bindTo(fileName, is, offset, end);
	}

	/* (non-Javadoc)
	 * @see org.apache.pig.LoadFunc#bytesToBag(byte[])
	 */
	public DataBag bytesToBag(byte[] b) throws IOException {
        return loader.bytesToBag(b);
	}

	/* (non-Javadoc)
	 * @see org.apache.pig.LoadFunc#bytesToCharArray(byte[])
	 */
	public String bytesToCharArray(byte[] b) throws IOException {
        return loader.bytesToCharArray(b);
	}

	/* (non-Javadoc)
	 * @see org.apache.pig.LoadFunc#bytesToDouble(byte[])
	 */
	public Double bytesToDouble(byte[] b) throws IOException {
        return loader.bytesToDouble(b);
	}

	/* (non-Javadoc)
	 * @see org.apache.pig.LoadFunc#bytesToFloat(byte[])
	 */
	public Float bytesToFloat(byte[] b) throws IOException {
        return loader.bytesToFloat(b);
	}

	/* (non-Javadoc)
	 * @see org.apache.pig.LoadFunc#bytesToInteger(byte[])
	 */
	public Integer bytesToInteger(byte[] b) throws IOException {
		return loader.bytesToInteger(b);
	}

	/* (non-Javadoc)
	 * @see org.apache.pig.LoadFunc#bytesToLong(byte[])
	 */
	public Long bytesToLong(byte[] b) throws IOException {
        return loader.bytesToLong(b);
	}

	/* (non-Javadoc)
	 * @see org.apache.pig.LoadFunc#bytesToMap(byte[])
	 */
	public Map<String, Object> bytesToMap(byte[] b) throws IOException {
        return loader.bytesToMap(b);
	}

	/* (non-Javadoc)
	 * @see org.apache.pig.LoadFunc#bytesToTuple(byte[])
	 */
	public Tuple bytesToTuple(byte[] b) throws IOException {
        return loader.bytesToTuple(b);
	}

	/* (non-Javadoc)
	 * @see org.apache.pig.LoadFunc#determineSchema(java.lang.String, org.apache.pig.ExecType, org.apache.pig.backend.datastorage.DataStorage)
	 */
	public Schema determineSchema(String fileName, ExecType execType,
			DataStorage storage) throws IOException {
        return loader.determineSchema(fileName, execType, storage);
	}

	/* (non-Javadoc)
	 * @see org.apache.pig.LoadFunc#fieldsToRead(org.apache.pig.impl.logicalLayer.schema.Schema)
	 */
	@Override
	public LoadFunc.RequiredFieldResponse fieldsToRead(RequiredFieldList requiredFields) throws FrontendException {
	    return loader.fieldsToRead(requiredFields);
	}

	/* (non-Javadoc)
	 * @see org.apache.pig.LoadFunc#getNext()
	 */
	public Tuple getNext() throws IOException {
       long initialPos = loader.getPosition();
        
        // we move to next boundry
        Tuple t = loader.getSampledTuple();        
        long finalPos = loader.getPosition();

        long toSkip = skipInterval - (finalPos - initialPos);
        if (toSkip > 0) {
            long rc = loader.skip(toSkip);
            
            // if we did not skip enough
            // in the first attempt, call
            // in.skip() repeatedly till we
            // skip enough
            long remainingSkip = toSkip - rc;
            while(remainingSkip > 0) {
                rc = loader.skip(remainingSkip);
                if(rc == 0) {
                    // underlying stream saw EOF
                    break;
                }
                remainingSkip -= rc;
            }
        }       
        
        if (t == null) {
        	return null;
        }
        
		if (factory == null) {
        	factory = TupleFactory.getInstance();
        }

        // copy existing field 
        Tuple m = factory.newTuple(t.size()+1);
        for(int i=0; i<t.size(); i++) {
        	m.set(i, t.get(i));
        }
        
        // add size of the tuple at the end
        m.set(t.size(), (finalPos-initialPos) + 1); // offset 1 for null
        
        return m;		
	}

	public void computeSamples(ArrayList<Pair<FileSpec, Boolean>> inputs, PigContext pc) throws ExecException {
		// TODO Auto-generated method stub
		
	}

}

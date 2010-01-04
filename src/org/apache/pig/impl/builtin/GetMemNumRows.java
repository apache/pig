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
import java.lang.reflect.Type;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

/**
 * UDF to get memory size of a tuple and extracts number of rows value from
 * special tuple created by PoissonSampleLoader
 * It is used by skewed join.
 * 
 */

public class GetMemNumRows extends EvalFunc<Tuple>{          

    private TupleFactory factory;
	
    public GetMemNumRows() {
    	factory = TupleFactory.getInstance();
    }      

    /**
     * @param  in - input tuple
     * @return - tuple having size in memory of this tuple and numRows if this
     * is specially marked tuple having number of rows field 
     */    
    public Tuple exec(Tuple in) throws IOException {
    	if (in == null) {
    	    return null;
    	}
    	long memSize = in.getMemorySize();
    	long numRows = 0;

    	
    	//  if this is specially marked tuple, get the number of rows
        int tSize = in.size();
    	if(tSize >=2 && 
    	    PoissonSampleLoader.NUMROWS_TUPLE_MARKER.equals(in.get(tSize-2)) ){
    	    numRows = (Long)in.get(tSize-1);
    	}
    	
    	//create tuple to be returned
    	Tuple t = factory.newTuple(2);
    	t.set(0, memSize);
    	t.set(1, numRows);
    	return t;
    }
    
    public Type getReturnType(){
        return Tuple.class;
    }       
}

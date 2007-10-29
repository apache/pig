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
package org.apache.pig.impl.eval.window;

import java.util.*;

import org.apache.pig.data.Datum;
import org.apache.pig.data.TimestampedTuple;
import org.apache.pig.impl.eval.collector.DataCollector;

public class TupleWindowSpec extends WindowSpec {
	private static final long serialVersionUID = 1L;
	   
	int numTuples;
    transient List<Datum> window;
	
	public TupleWindowSpec(windowType type, int numTuples){
		super(type);
		this.numTuples = numTuples;
        window = new LinkedList<Datum>();
	}
	
	@Override
	protected DataCollector setupDefaultPipe(DataCollector endOfPipe) {
	    return new DataCollector(endOfPipe) {

	        @Override
			public void add(Datum d) {
	        	if (d!=null){
	        	
		        	if (d instanceof TimestampedTuple){
		        		if (((TimestampedTuple) d).isHeartBeat()) return;
		        	}
		        	
	                window.add(d);
	                while (window.size() > numTuples) {
	                    window.remove(0);
	                }
	        	}
	        	
	        	// emit entire window content to output collector
                for (Iterator<Datum> it = window.iterator(); it.hasNext(); ) {
                    addToSuccessor(it.next());
                }
	        }
	        
	       
	    };
    }
	
    @Override
    public String toString() {
    	StringBuilder sb = new StringBuilder();
    	sb.append("[WINDOW ");
    	sb.append(type);
    	sb.append(" TUPLES ");
    	sb.append(numTuples);
    	sb.append("]");
    	return sb.toString();
    }

}

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
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;

import org.apache.pig.data.Tuple;


/*
 * 	Right now putting in a very dumb implementation which replicates the queue
	every slave
 */
class POSplitMaster extends POSplit {
    
	
    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	Map<POSplit, LinkedList<Tuple>> lists;

    public POSplitMaster(PhysicalOperator input, int outputType) {
    	super(outputType);
        inputs = new PhysicalOperator[1];
        inputs[0] = input;

        lists = new HashMap<POSplit, LinkedList<Tuple>>();
        registerSlave(this); // register self as one consumer
    }

    @Override
	public Tuple getNext() throws IOException {
        return slaveGetNext(this);
    }

    // should only be called by POSplitSlave or POSplitMaster
    public void registerSlave(POSplit slave) {
        lists.put(slave, new LinkedList<Tuple>());
    }

    // should only be called by POSplitSlave or POSplitMaster
    public Tuple slaveGetNext(POSplit slave) throws IOException {
        
    	LinkedList<Tuple> list = lists.get(slave);
    	
    	if (list.isEmpty()){
    		//Get more input
    		Tuple t = inputs[0].getNext();
    		Iterator<LinkedList<Tuple>> iter = lists.values().iterator();
    		
    		while(iter.hasNext()){
    			iter.next().add(t);
    		}
       	}
    	
    	return list.removeFirst();
    }

    /*
    private Tuple ReadFromBuffer(int pos) throws IOException {
        while (buffer.size() <= pos)
            buffer.add(inputs[0].getNext());
        return buffer.get(pos);
    }
    
    
    private void ShrinkBuffer() {
        Set<POSplit> slaves = positions.keySet();

        // determine how many buffer positions we can safely remove
        int n = buffer.size(); // begin optimistically
        for (Iterator<POSplit> it = slaves.iterator(); it.hasNext();) {
            int pos = positions.get(it.next()).intValue();
            if (pos < n)
                n = pos; // if our current "n" would screw up this slave, reduce it
        }

        // now, perform the removal
        for (int i = 0; i < n; i++)
            buffer.removeFirst();

        // update the positions based on the removal
        if (n > 0) {
            for (Iterator<POSplit> it = slaves.iterator(); it.hasNext();) {
                POSplit slave = it.next();
                int pos = positions.get(slave).intValue();
                positions.put(slave, new Integer(pos - n));
            }
        }
    }
	*/
}

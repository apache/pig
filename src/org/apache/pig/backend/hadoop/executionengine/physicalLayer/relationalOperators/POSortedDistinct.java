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
package org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators;

import java.util.List;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.plan.OperatorKey;

/**
 * This operator is a variation of PODistinct, the input to this operator
 * must be sorted already. 
 *
 */
public class POSortedDistinct extends PODistinct {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private transient Tuple lastTuple;

	public POSortedDistinct(OperatorKey k, int rp, List<PhysicalOperator> inp) {
        super(k, rp, inp);
    }

    public POSortedDistinct(OperatorKey k, int rp) {
        super(k, rp);
    }

    public POSortedDistinct(OperatorKey k, List<PhysicalOperator> inp) {
        super(k, inp);
    }

    public POSortedDistinct(OperatorKey k) {
        super(k);
    }
    

    public Result getNext(Tuple t) throws ExecException {
    	while(true) {
    		Result in = processInput();
    		if (in.returnStatus == POStatus.STATUS_NULL) {
    			continue;
    		}
    		
    		if (in.returnStatus == POStatus.STATUS_OK) {
    			if (lastTuple == null || !lastTuple.equals(in.result)) {
    				lastTuple = (Tuple)in.result;
    				return in;
    			} else {
    				continue;
    			}
    		}
    		
    		if (in.returnStatus == POStatus.STATUS_EOP) {
    			if (!isAccumulative() || !isAccumStarted()) {
    				lastTuple = null;
    			}
    			return in;
    		}
    		
    		// if there is an error, just return
    		return in;
    	}
    }
    
    @Override
    public String name() {
        return getAliasString() + "POSortedDistinct" + "["
                + DataType.findTypeName(resultType) + "]" + " - "
                + mKey.toString();
    }
}

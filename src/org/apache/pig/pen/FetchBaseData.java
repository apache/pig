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
package org.apache.pig.pen;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.pig.backend.executionengine.ExecPhysicalOperator;
import org.apache.pig.backend.local.executionengine.POLoad;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.logicalLayer.LOLoad;
import org.apache.pig.impl.logicalLayer.LogicalOperator;
import org.apache.pig.impl.logicalLayer.OperatorKey;
import org.apache.pig.impl.logicalLayer.parser.NodeIdGenerator;
import org.apache.pig.impl.util.LineageTracer;

public class FetchBaseData {
	static void ReadBaseData(LogicalOperator op, Map<LOLoad, DataBag> baseData, int sampleSize, PigContext pigContext) throws IOException {
	    
    	if (baseData == null) {
    		throw new IOException("BaseData is null!");
    	} 

    	DataBag test = baseData.get(op);
    	if(test != null) {
    		//The data exists locally and need not be fetched again
    		return;
    	}
    	
        if (op instanceof LOLoad) {
        	FileSpec fileSpec = ((LOLoad)op).getInputFileSpec(); 
        	
        	if(op.outputSchema().fields.isEmpty()) {
        		throw new IOException("Illustrate command needs a user defined schema to function. Please specify a schema while loading the data.");
        	}

        	DataBag opBaseData = BagFactory.getInstance().newDefaultBag();
            //POLoad poLoad = new POLoad(pigContext, ((LOLoad) op).getInputFileSpec(), op.getOutputType());
        	POLoad poLoad = new POLoad(op.getScope(), 
        								NodeIdGenerator.getGenerator().getNextNodeId(op.getOperatorKey().getScope()), 
        								new HashMap<OperatorKey, ExecPhysicalOperator> (),
        								pigContext,
        								fileSpec,
        								LogicalOperator.FIXED
        								);
        	poLoad.setLineageTracer(new LineageTracer());
            poLoad.open();
            for (int i = 0; i < sampleSize; i++) {
                Tuple t = poLoad.getNext();

                if (t == null) break;
                opBaseData.add(t);
            }
            poLoad.close();
            
            baseData.put((LOLoad) op, opBaseData);
        } else {
            /*for (Iterator<LogicalOperator> it = op.getInputs().iterator(); it.hasNext(); ) {
                ReadBaseData(it.next(), baseData, sampleSize, pigContext);
            }*/
        	for(OperatorKey opKey : op.getInputs()) {
        		ReadBaseData(op.getOpTable().get(opKey), baseData, sampleSize, pigContext);
        	}
        }
    }
}

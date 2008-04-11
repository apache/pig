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

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.pig.data.DataBag;
import org.apache.pig.data.ExampleTuple;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.eval.FilterSpec;
import org.apache.pig.impl.logicalLayer.LOEval;
import org.apache.pig.impl.logicalLayer.LogicalOperator;
import org.apache.pig.impl.util.IdentityHashSet;

public class Metrics {
	static float getRealness(LogicalOperator op, Map<LogicalOperator, DataBag> exampleData, boolean overallRealness) {
    	//StringBuffer str = new StringBuffer();
    	int noTuples = 0;
    	int noSynthetic = 0;
    	for(Map.Entry<LogicalOperator, DataBag> e : exampleData.entrySet()) {
    		//if(e.getKey() instanceof LORead) continue;
    		DataBag bag;
    		if(overallRealness) {
    			bag = exampleData.get(e.getKey());
    		} else {
    			bag = exampleData.get(op);
    		}
    		noTuples += bag.cardinality();
    		for(Iterator<Tuple> it = bag.iterator(); it.hasNext();) {
    			if(((ExampleTuple)it.next()).isSynthetic()) noSynthetic++;
    		}
    		if(!overallRealness) break;

		}
    	
    	if(overallRealness) {
    		return 100*(1 - ((float)noSynthetic / (float)noTuples));
    	} else {
    		return 100*(1 - ((float)noSynthetic / (float)noTuples));
    	}
    	
    }

    static float getConciseness(LogicalOperator op, Map<LogicalOperator, DataBag> exampleData, Map<LogicalOperator, Collection<IdentityHashSet<Tuple>>> OperatorToEqClasses, boolean overallConciseness) {
    	DataBag bag = exampleData.get(op);

    	int noEqCl = OperatorToEqClasses.get(op).size();
    	int noTuples = bag.cardinality();
    	
    	
    	float conciseness = 100*((float)noEqCl / (float)noTuples);
    	if(!overallConciseness) {
    	
    		return ((conciseness > 100.0) ? 100.0f : conciseness);
    	} else {

    	
    		noEqCl = 0;
    		noTuples = 0;
    		conciseness = 0;
    		int noOperators = 0;

    		for(Map.Entry<LogicalOperator, Collection<IdentityHashSet<Tuple>>> e : OperatorToEqClasses.entrySet()) {
    			//if(e.getKey() instanceof LORead) continue;
    			noOperators++; //we need to keep a track of these and not use OperatorToEqClasses.size() as LORead shouldn't be considered a operator
    			bag = exampleData.get(e.getKey());
    	
    			noTuples = bag.cardinality();
    			noEqCl = e.getValue().size();
    			float concise = 100*((float)noEqCl / (float)noTuples);
    			concise = (concise > 100) ? 100 : concise;
    			conciseness += concise;
    		}
    		conciseness /= (float)noOperators;
    		
    		return conciseness;
    	}
    
    }
    
    
    static float getCompleteness(LogicalOperator op, Map<LogicalOperator, DataBag> exampleData, Map<LogicalOperator, Collection<IdentityHashSet<Tuple>>> OperatorToEqClasses, boolean overallCompleteness) {
    
    	int noClasses = 0;
    	int noCoveredClasses = 0;
    	int noOperators = 0;
    	Map<Integer, Boolean> coveredClasses;
    	float completeness = 0;
    	if(!overallCompleteness) {
    		Collection<IdentityHashSet<Tuple>> eqClasses = OperatorToEqClasses.get(op);
    		DataBag bag;
    		if(op instanceof LOEval && ((LOEval)op).getSpec() instanceof FilterSpec) {
    			//bag = exampleData.get(op.getInputs().get(0));
    			bag = exampleData.get(op.getOpTable().get(op.getInputs().get(0)));
    		} else {
    			bag = exampleData.get(op);
    		}
    		coveredClasses = getCompletenessLogic(bag, eqClasses);
    		noClasses = eqClasses.size();
    		for(Map.Entry<Integer, Boolean> e : coveredClasses.entrySet() ) {
    			if(e.getValue()) {
    				noCoveredClasses ++;
    			}
    		}
    		
    
    		return 100*((float)noCoveredClasses)/(float)noClasses;
    	} else {
    		for(Map.Entry<LogicalOperator, Collection<IdentityHashSet<Tuple>>> e : OperatorToEqClasses.entrySet()) {
    			noCoveredClasses = 0;
    			noClasses = 0;
    			
    			//if(e.getKey() instanceof LORead) continue; //We don't consider LORead a operator.
    			
    			noOperators++;
    			Collection<IdentityHashSet<Tuple>> eqClasses = e.getValue();
    			LogicalOperator lop = e.getKey();
    			DataBag bag;
    			if(lop instanceof LOEval && ((LOEval)lop).getSpec() instanceof FilterSpec) {
        			//bag = exampleData.get(lop.getInputs().get(0));
    				bag = exampleData.get(lop.getOpTable().get(lop.getInputs().get(0)));
        		} else {
        			bag = exampleData.get(lop);
        		}
    			coveredClasses = getCompletenessLogic(bag, eqClasses);
        		noClasses += eqClasses.size();
        		for(Map.Entry<Integer, Boolean> e_result : coveredClasses.entrySet() ) {
        			if(e_result.getValue()) {
        				noCoveredClasses ++;
        			}
        		}
        		completeness += 100*((float)noCoveredClasses/(float)noClasses);
    		}
    		completeness /= (float)noOperators;
    
    		return completeness;
    	}
    	
    
    }
    
    static Map<Integer, Boolean> getCompletenessLogic(DataBag bag, Collection<IdentityHashSet<Tuple>> eqClasses) {
    	Map<Integer, Boolean> coveredClasses = new HashMap<Integer, Boolean> ();
    	
		for(Iterator<Tuple> it = bag.iterator(); it.hasNext(); ) {
			Tuple t = it.next();
			int classId = 0;
			for(IdentityHashSet<Tuple> eqClass : eqClasses) {

				if(eqClass.contains(t)) {
					coveredClasses.put(classId, true);
				}
				classId ++;
			}
		}

    	
		return coveredClasses;
    	
    }
}

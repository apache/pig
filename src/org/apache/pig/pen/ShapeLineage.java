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
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.pig.backend.executionengine.ExecPhysicalOperator;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.eval.EvalSpec;
import org.apache.pig.impl.eval.FilterSpec;
import org.apache.pig.impl.logicalLayer.LOCogroup;
import org.apache.pig.impl.logicalLayer.LOEval;
import org.apache.pig.impl.logicalLayer.LOLoad;
import org.apache.pig.impl.logicalLayer.LogicalOperator;
import org.apache.pig.impl.logicalLayer.OperatorKey;
import org.apache.pig.impl.util.IdentityHashSet;
import org.apache.pig.impl.util.LineageTracer;


public class ShapeLineage {
	static Map<LOLoad, DataBag> TrimLineages(LogicalOperator root, Map<LOLoad, DataBag> baseData, Map<LogicalOperator, DataBag> derivedData, LineageTracer lineage, Map<LogicalOperator, Collection<IdentityHashSet<Tuple>>> OperatorToEqClasses, Map<OperatorKey, OperatorKey> logicalToPhysicalKeys, Map<OperatorKey, ExecPhysicalOperator> physicalOpTable) {
    	Map <LOLoad, DataBag> modifiedBaseData = new HashMap<LOLoad, DataBag>();
    	//modifiedBaseData.putAll(baseData);
    	
    	List<IdentityHashSet<Tuple>> affinityGroups = new LinkedList<IdentityHashSet<Tuple>>();
    	IdentityHashSet<Tuple> a1 = new IdentityHashSet<Tuple>();
    	affinityGroups.add(a1);
    	IdentityHashSet<Tuple> a2 = new IdentityHashSet<Tuple>();
    	affinityGroups.add(a2);
    	TrimLineages(root, root, modifiedBaseData, derivedData, affinityGroups, lineage, OperatorToEqClasses, 0.0, logicalToPhysicalKeys, physicalOpTable);
    	
    	return modifiedBaseData;
    	
    }
    
    static void TrimLineages(LogicalOperator root, LogicalOperator currentOp, Map<LOLoad, DataBag> baseData, Map<LogicalOperator, DataBag> derivedData, List<IdentityHashSet<Tuple>> affinityGroups, LineageTracer lineage, Map<LogicalOperator, Collection<IdentityHashSet<Tuple>>> OperatorToEqClasses, double completeness, Map<OperatorKey, OperatorKey> logicalToPhysicalKeys, Map<OperatorKey, ExecPhysicalOperator> physicalOpTable) {
    	//With lineage added in the lineageTracer to track the parent-child relationship, only filter needs a consideration since we need a record that doesn't satisfy the filter condition
    	IdentityHashSet<Tuple> affinityGroup = affinityGroups.get(0);
    	if(affinityGroup.size() == 0) {
    		//first operator/root in the logical plan
    		DataBag bag = derivedData.get(currentOp);
    		for(Iterator<Tuple> it = bag.iterator(); it.hasNext();) {
    			Tuple t = it.next();
    			if(lineage.flattenLineageContains(t)) {
    				affinityGroup.add(t);
    				break;
    			}
    			affinityGroup.add(t);
    		}
    	}
    	
    	
    	//if any member of the affinity group is present in the flattenLineage, that needs to be handled specially
    	IdentityHashSet<Tuple> flattenElements = affinityGroups.get(1);
    	
    	IdentityHashSet<Tuple> newAffinityGroup = new IdentityHashSet<Tuple>();
    	if(currentOp instanceof LOEval) {
    		EvalSpec spec = ((LOEval)currentOp).getSpec();
    		if(spec instanceof FilterSpec) {
    			//here we need to put a Tuple that doesn't pass the filter into the affinity groups.
    			LogicalOperator input = currentOp.getOpTable().get(currentOp.getInputs().get(0)); //since its a filter, we know there is only 1 input.
    			DataBag inputData = derivedData.get(input);
    			for(Iterator<Tuple> it = inputData.iterator(); it.hasNext();) {
    				Tuple t = it.next();
    				if(!((FilterSpec)spec).cond.eval(t)) {
    					newAffinityGroup.add(t);
    					break;
    				}
    			}
    		}
    		for(Tuple t : affinityGroup) {
    			if(lineage.flattenLineageContains(t)) {
    				flattenElements.addAll(lineage.getFlattenChildren(t));
    			}
    			newAffinityGroup.addAll(lineage.getChildren(t));
    		}
    		//Affinity group for eval ready, calling recursion
    		affinityGroup.clear();
    		affinityGroup.addAll(newAffinityGroup);
    		TrimLineages(root, currentOp.getOpTable().get(currentOp.getInputs().get(0)), baseData, derivedData, affinityGroups, lineage, OperatorToEqClasses, completeness, logicalToPhysicalKeys, physicalOpTable);
    	}
    	
    	if(currentOp instanceof LOCogroup) {
    		
    		List<OperatorKey> inputs = currentOp.getInputs();
    		int numInputs = inputs.size();
    		IdentityHashSet<Tuple> flattenAffinityGroup = affinityGroups.get(1);
    		
    		
    		if(numInputs == 1) {
    			//this is a group case
    			double score = 0;
    			double bestScore = -1;
    			int best = 0;
    			double nextBestScore = -1;
    			int nextBest = 0;
    			for(Tuple t : affinityGroup) {
    				int index = 0;
    				List<Tuple> children = lineage.getChildren(t);
    				if(children.size() == 1) {
    					Tuple test = children.get(0);
    					if(!newAffinityGroup.contains(test))
    						newAffinityGroup.add(test);
    				} else {
    					if(flattenAffinityGroup.size() > 0) {
    						newAffinityGroup.addAll(flattenAffinityGroup);
    						flattenAffinityGroup.clear();
    					} else {
    						for(Tuple child : children) {
    							score = 1 / lineage.getWeightedCounts(child, 2, 1);
    							if(score > bestScore) {
    								nextBest = best;
    								best = index;
    								nextBestScore = bestScore;
    								bestScore = score;
    							} else if(score > nextBestScore) {
    								nextBest = index;
    								nextBestScore = score;
    							}
    							index++;
    						}

    						newAffinityGroup.add(children.get(best));
    						newAffinityGroup.add(children.get(nextBest));
    					}
    				}
    			}
    			affinityGroup.clear();
    			affinityGroup.addAll(newAffinityGroup);
    			TrimLineages(root, currentOp.getOpTable().get(inputs.get(0)), baseData, derivedData, affinityGroups, lineage, OperatorToEqClasses, completeness, logicalToPhysicalKeys, physicalOpTable);
    		} else {
    			//This is a cogroup case
    			newAffinityGroup.addAll(affinityGroup);
    			for(int i = 0; i < numInputs; i++) {
    				IdentityHashSet<Tuple> cogroupAffinityGroup = new IdentityHashSet<Tuple>();
    				for(Tuple t : newAffinityGroup) {
    					DataBag data = t.getBagField(i+1);
						//Ideally we should have no field with null values because of synthetic data generation. 
						if(data.size() == 0)
							continue;
						
						if(flattenAffinityGroup.size() > 0) {
							for(Iterator<Tuple> it = data.iterator(); it.hasNext(); ) {
								Tuple child = it.next();
								if(flattenAffinityGroup.contains(child)) {
									cogroupAffinityGroup.add(child);
									flattenAffinityGroup.remove(child);
								}
							}
						} else {
							//The children are all the tuples present in data
							Tuple best = null;
							double bestScore = -1;
							for(Iterator<Tuple> it = data.iterator(); it.hasNext(); ) {
								Tuple child = it.next();

								double score = 1 / lineage.getWeightedCounts(child, 2, 1);
								if(score > bestScore) {
									best = child;
									bestScore = score;
								}
							}
							cogroupAffinityGroup.add(best);
						}
    				}
    				affinityGroup.clear();
    				affinityGroup.addAll(cogroupAffinityGroup);
    				TrimLineages(root, currentOp.getOpTable().get(inputs.get(i)), baseData, derivedData, affinityGroups, lineage, OperatorToEqClasses, completeness, logicalToPhysicalKeys, physicalOpTable);
    			}
    		}
    	}
    	
    	
    	if(currentOp instanceof LOLoad) {
    		
    		getBaseData(currentOp, affinityGroup, lineage, baseData, derivedData);
    	}
	    	
    	return;
    	
    }
    
       

    static void getBaseData(LogicalOperator lOp, IdentityHashSet<Tuple> affinityGroup, LineageTracer lineage, Map<LOLoad, DataBag> baseData, Map<LogicalOperator, DataBag> derivedData) {
    	DataBag bag = baseData.get(lOp);
    	IdentityHashSet<Tuple> temp = new IdentityHashSet<Tuple>();
    	if(bag == null) {
    		bag = BagFactory.getInstance().newDefaultBag();
    		baseData.put((LOLoad) lOp, bag);
    	} else {
    		//Now we try to ensure that the same tuple is not added twice
    		//In effect we are trying to get the union of multiple updates happening to the baseData
    		for(Iterator<Tuple> it = bag.iterator(); it.hasNext(); ) {
    			temp.add(it.next());
    		}
    		bag.clear();
    	}
    	
    	for(Tuple t : affinityGroup) {
    		temp.add(t);
    	}
    	for(Tuple t : temp) {
    		bag.add(t);
    	}
    	return;
    }
    
    static Map<LOLoad, DataBag> PruneBaseData(Map<LOLoad, DataBag> baseData, DataBag rootOutput, IdentityHashSet<Tuple> syntheticTuples, LineageTracer lineage, Collection<IdentityHashSet<Tuple>> equivalenceClasses) {
        
        IdentityHashMap<Tuple, Collection<Tuple>> membershipMap = lineage.getMembershipMap();
        IdentityHashMap<Tuple, Double> lineageGroupWeights = lineage.getWeightedCounts(2f, 1);
        
        // compute a mapping from lineage group to the set of equivalence classes covered by it
        IdentityHashMap<Tuple, Set<Integer>> lineageGroupToEquivClasses = new IdentityHashMap<Tuple, Set<Integer>>();
        int equivClassId = 0;
        for (IdentityHashSet<Tuple> equivClass : equivalenceClasses) {
            for (Tuple t : equivClass) {
                Tuple lineageGroup = lineage.getRepresentative(t);
                Set<Integer> entry = lineageGroupToEquivClasses.get(lineageGroup);
                if (entry == null) {
                    entry = new HashSet<Integer>();
                    lineageGroupToEquivClasses.put(lineageGroup, entry);
                }
                entry.add(equivClassId);
            }
            
            equivClassId++;
        }
        
        // select lineage groups such that we cover all equivalence classes
        IdentityHashSet<Tuple> selectedLineageGroups = new IdentityHashSet<Tuple>();
        while (!lineageGroupToEquivClasses.isEmpty()) {
            // greedily find the lineage group with the best "score", where score = # equiv classes covered / group weight
            double bestScore = -1;
            Tuple bestLineageGroup = null;
            Set<Integer> bestEquivClassesCovered = null;
            for (Tuple lineageGroup : lineageGroupToEquivClasses.keySet()) {
                double weight = lineageGroupWeights.get(lineageGroup);

                Set<Integer> equivClassesCovered = lineageGroupToEquivClasses.get(lineageGroup);
                int numEquivClassesCovered = equivClassesCovered.size();
                double score = ((double) numEquivClassesCovered) / ((double)weight);
                
                if (score > bestScore) {
                	
                    bestScore = score;
                    bestLineageGroup = lineageGroup;
                    bestEquivClassesCovered = equivClassesCovered;
                }
            }
            // add the best-scoring lineage group to the set of ones we plan to retain
            selectedLineageGroups.add(bestLineageGroup);

            // make copy of bestEquivClassesCovered (or else the code that follows won't work correctly, because removing from the reference set)
            Set<Integer> toCopy = bestEquivClassesCovered;
            bestEquivClassesCovered = new HashSet<Integer>();
            bestEquivClassesCovered.addAll(toCopy);
            
            // remove the classes we've now covered
            Collection<Tuple> toRemove = new LinkedList<Tuple>();
            for (Tuple lineageGroup : lineageGroupToEquivClasses.keySet()) {
                Set<Integer> equivClasses = lineageGroupToEquivClasses.get(lineageGroup);
                equivClasses.removeAll(bestEquivClassesCovered);
                if (equivClasses.size() == 0) toRemove.add(lineageGroup);
            }
            for (Tuple removeMe : toRemove) lineageGroupToEquivClasses.remove(removeMe);
        }
        
        // revise baseData to only contain the tuples that are part of selectedLineageGroups
        IdentityHashSet<Tuple> tuplesToRetain = new IdentityHashSet<Tuple>();
        for (Tuple lineageGroup : selectedLineageGroups) {
            Collection<Tuple> members = membershipMap.get(lineageGroup);
            for (Tuple t : members) tuplesToRetain.add(t);
        }
        Map<LOLoad, DataBag> newBaseData = new HashMap<LOLoad, DataBag>();
        for (LOLoad loadOp : baseData.keySet()) {
            DataBag data = baseData.get(loadOp);
            //DataBag newData = new DataBag();
            DataBag newData = BagFactory.getInstance().newDefaultBag();
            for (Iterator<Tuple> it = data.iterator(); it.hasNext(); ) {
                Tuple t = it.next();
                if (tuplesToRetain.contains(t)) newData.add(t);
            }
            newBaseData.put(loadOp, newData);
        }
        return newBaseData;
    }

}

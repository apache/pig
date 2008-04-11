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
package org.apache.pig.impl.util;

import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.pig.data.ExampleTuple;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.LogicalOperator;

public class LineageTracer {
    
    // Use textbook Union-Find data structure, with counts associated with items
    
    // note: we test for equality by comparing tuple references, not by calling the "equals()" method
    //       the "IdentityHashMap" data structure is based on reference equality
    IdentityHashMap<Tuple, Tuple> parents = new IdentityHashMap<Tuple, Tuple>();
    IdentityHashMap<Tuple, Integer> counts = new IdentityHashMap<Tuple, Integer>();   // has one entry per unique tuple being tracked
    IdentityHashMap<Tuple, Integer> ranks = new IdentityHashMap<Tuple, Integer>();
    
    //This is to maintain a proper child-parent relationship for lineage-trimming
    //Flatten acts like a corner case
    //Key - Output of an operator
    //Value - List of input tuples that generate that output
    IdentityHashMap<Tuple, List<Tuple>> lineage = new IdentityHashMap<Tuple, List<Tuple>>();
    
    IdentityHashMap<Tuple, List<Tuple>> flattenLineage = new IdentityHashMap<Tuple, List<Tuple>>();
    
    // insert a new tuple (if a tuple is inserted multiple times, it gets a count > 1)
    public void insert(Tuple t) {
        if (parents.containsKey(t)) {
            counts.put(t, counts.get(t)+1);
        } else {
            parents.put(t, t);
            counts.put(t, 1);
            ranks.put(t, 0);
        }
        
        lineage.put(t, null);
    }
    
    public void unionFlatten(Tuple in, Tuple out) {
    	List<Tuple> inputs = flattenLineage.get(out);
    	if(inputs == null) {
    		inputs = new LinkedList<Tuple>();
    		flattenLineage.put(out, inputs);
    	}
    	inputs.add(in);
    }
    
    public void addFlattenMap(Tuple out, List<Tuple> children) {
    	flattenLineage.put(out, children);
    }
    
    // union two tuple sets
    public void union(Tuple t1, Tuple t2) {
        link(getRepresentative(t1), getRepresentative(t2));
        
        //t1 - input; t2 - output
        Tuple input = t1;
        Tuple output = t2;
        List<Tuple> inputs = lineage.get(output);
		if(inputs == null) {
			inputs = new LinkedList<Tuple>();
			lineage.put(output, inputs);
		}
		inputs.add(input);
    }
    
    //return the children of a Tuple
    public List<Tuple> getChildren(Tuple t) {
    	return lineage.get(t);
    }
    
    //return the weight of a part of the lineage given a starting point
    public double getWeightedCounts(Tuple parent, double synthetic, double omittable) {
		Collection<Tuple> children = lineage.get(parent);
		double weight = 0;
		if(children == null) {
			//its a leaf
			weight = 1.0;
			if(((ExampleTuple)parent).omittable == true) weight = weight * synthetic;
			if(((ExampleTuple)parent).synthetic == true) weight = weight * omittable;
			return weight;
		}
		
		for(Tuple t : children) {
			weight += getWeightedCounts(t, synthetic, omittable);
		}
		return weight;
	}
    
    // find the set representative of a given tuple
    public Tuple getRepresentative(Tuple t) {
        Tuple tParent = parents.get(t);
        //New addition --shubhamc
        if(tParent == null) {
        	this.insert(t);
        	tParent = parents.get(t);
        }
        
        if (tParent != t) {
            tParent = getRepresentative(tParent);
            
            parents.put(t, tParent);
        }
        return tParent;
    }
    
    private void link(Tuple t1, Tuple t2) {
        int t1Rank = ranks.get(t1);
        int t2Rank = ranks.get(t2);
        if (t1Rank > t2Rank) {
            parents.put(t2, t1);
        } else {
            parents.put(t1, t2);
            if (t1Rank == t2Rank) ranks.put(t2, t2Rank + 1);
        }
    }
    
    // get the cardinality of each tuple set (identified by a representative tuple)
    public IdentityHashMap<Tuple, Double> getCounts() {
        return getWeightedCounts(1, 1);
    }
    

    // --changed - shubhamc
    public IdentityHashMap<Tuple, Double> getWeightedCounts(float syntheticMultipler, float omittableMultiplier) {
        IdentityHashMap<Tuple, Double> repCounts = new IdentityHashMap<Tuple, Double>();
        IdentityHashSet<Tuple> specialSets = new IdentityHashSet<Tuple>();
        
        for (IdentityHashMap.Entry<Tuple, Integer> e : counts.entrySet()) {
            Tuple t = e.getKey();

            float newCount = counts.get(t);
            if(((ExampleTuple)t).isSynthetic()) newCount = newCount * syntheticMultipler;
            if(((ExampleTuple)t).isOmittable()) newCount = newCount * omittableMultiplier;
            
            Tuple rep = getRepresentative(t);
            double oldCount = (repCounts.containsKey(rep))? repCounts.get(rep) : 0;
            repCounts.put(rep, oldCount + newCount);
//            if (specialTuples.contains(t)) specialSets.add(rep);
        }
        /*
        for (IdentityHashMap.Entry<Tuple, Integer> e : repCounts.entrySet()) {
            if (specialSets.contains(e.getKey())) e.setValue(e.getValue() * multiplier);
        }
		*/
        return repCounts;
    }
    
    
    // get all members of the set containing t
    public Collection<Tuple> getMembers(Tuple t) {
        Tuple representative = getRepresentative(t);
        
        Collection<Tuple> members = new LinkedList<Tuple>();
        for (IdentityHashMap.Entry<Tuple, Integer> e : counts.entrySet()) {
            Tuple t1 = e.getKey();
            if (getRepresentative(t1) == representative) members.add(t1);
        }
        return members;
    }
    
    // get a mapping from set representatives to members
    public IdentityHashMap<Tuple, Collection<Tuple>> getMembershipMap() {
        IdentityHashMap<Tuple, Collection<Tuple>> map = new IdentityHashMap<Tuple, Collection<Tuple>>();
        for (IdentityHashMap.Entry<Tuple, Integer> e : counts.entrySet()) {
            Tuple t = e.getKey();

            Tuple representative = getRepresentative(t);
            Collection<Tuple> members = map.get(representative);
            if (members == null) {
                members = new LinkedList<Tuple>();
                map.put(representative, members);
            }
            members.add(t);
        }
        return map;
    }
    
    public boolean flattenLineageContains(Tuple t) {
    	return flattenLineage.containsKey(t);
    }
    
    public List<Tuple> getFlattenChildren(Tuple t) {
    	return flattenLineage.get(t);
    }
    
    public void removeFlattenMap(Tuple t) {
    	flattenLineage.remove(t);
    }
        

}

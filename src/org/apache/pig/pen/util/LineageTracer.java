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
package org.apache.pig.pen.util;

import java.util.*;

import org.apache.pig.data.Tuple;

public class LineageTracer {

    // Use textbook Union-Find data structure, with counts associated with items

    // note: we test for equality by comparing tuple references, not by calling
    // the "equals()" method
    // the "IdentityHashMap" data structure is based on reference equality
    IdentityHashMap<Tuple, Tuple> parents = new IdentityHashMap<Tuple, Tuple>();
    IdentityHashMap<Tuple, Integer> counts = new IdentityHashMap<Tuple, Integer>(); // has
    // one
    // entry
    // per
    // unique
    // tuple
    // being
    // tracked
    IdentityHashMap<Tuple, Integer> ranks = new IdentityHashMap<Tuple, Integer>();

    // insert a new tuple (if a tuple is inserted multiple times, it gets a
    // count > 1)
    public void insert(Tuple t) {
        if (parents.containsKey(t)) {
            counts.put(t, counts.get(t) + 1);
        } else {
            parents.put(t, t);
            counts.put(t, 1);
            ranks.put(t, 0);
        }
    }

    // union two tuple sets
    public void union(Tuple t1, Tuple t2) {
        link(getRepresentative(t1), getRepresentative(t2));
    }

    // find the set representative of a given tuple
    public Tuple getRepresentative(Tuple t) {
        Tuple tParent = parents.get(t);
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
            if (t1Rank == t2Rank)
                ranks.put(t2, t2Rank + 1);
        }
    }

    // get the cardinality of each tuple set (identified by a representative
    // tuple)
    public IdentityHashMap<Tuple, Double> getCounts() {
        return getWeightedCounts(2f, 1f);
    }

    // get the cardinality of each tuple set, weighted in a special way
    // weighting works like this: if a tuple set contains one or more tuples
    // from the "specialTuples" set, we multiply its value by "multiplier"
    // public IdentityHashMap<Tuple, Integer>
    // getWeightedCounts(IdentityHashSet<Tuple> specialTuples, int multiplier) {
    // IdentityHashMap<Tuple, Integer> repCounts = new IdentityHashMap<Tuple,
    // Integer>();
    // IdentityHashSet<Tuple> specialSets = new IdentityHashSet<Tuple>();
    //        
    // for (IdentityHashMap.Entry<Tuple, Integer> e : counts.entrySet()) {
    // Tuple t = e.getKey();
    //
    // int newCount = counts.get(t);
    // Tuple rep = getRepresentative(t);
    // int oldCount = (repCounts.containsKey(rep))? repCounts.get(rep) : 0;
    // repCounts.put(rep, oldCount + newCount);
    // if (specialTuples.contains(t)) specialSets.add(rep);
    // }
    //        
    // for (IdentityHashMap.Entry<Tuple, Integer> e : repCounts.entrySet()) {
    // if (specialSets.contains(e.getKey())) e.setValue(e.getValue() *
    // multiplier);
    // }
    //
    // return repCounts;
    // }

    public IdentityHashMap<Tuple, Double> getWeightedCounts(
            float syntheticMultipler, float omittableMultiplier) {
        IdentityHashMap<Tuple, Double> repCounts = new IdentityHashMap<Tuple, Double>();

        for (IdentityHashMap.Entry<Tuple, Integer> e : counts.entrySet()) {
            Tuple t = e.getKey();

            float newCount = counts.get(t);
            if (((ExampleTuple) t).synthetic)
                newCount = newCount * syntheticMultipler;
            if (((ExampleTuple) t).omittable)
                newCount = newCount * omittableMultiplier;

            Tuple rep = getRepresentative(t);
            double oldCount = (repCounts.containsKey(rep)) ? repCounts.get(rep)
                    : 0;
            repCounts.put(rep, oldCount + newCount);
            // if (specialTuples.contains(t)) specialSets.add(rep);
        }
        /*
         * for (IdentityHashMap.Entry<Tuple, Integer> e : repCounts.entrySet())
         * { if (specialSets.contains(e.getKey())) e.setValue(e.getValue()
         * multiplier); }
         */
        return repCounts;
    }

    // get all members of the set containing t
    public Collection<Tuple> getMembers(Tuple t) {
        Tuple representative = getRepresentative(t);

        Collection<Tuple> members = new LinkedList<Tuple>();
        for (IdentityHashMap.Entry<Tuple, Integer> e : counts.entrySet()) {
            Tuple t1 = e.getKey();
            if (getRepresentative(t1) == representative)
                members.add(t1);
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
}

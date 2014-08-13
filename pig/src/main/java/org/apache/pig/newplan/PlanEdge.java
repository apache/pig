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

package org.apache.pig.newplan;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;

import org.apache.pig.impl.util.MultiMap;
import org.apache.pig.impl.util.Pair;

public class PlanEdge extends MultiMap<Operator, Operator> {
    
    private static final long serialVersionUID = 1L;

    public PlanEdge() {
        super();
    }

    /**
     * @param size Initial size of the map
     */
    public PlanEdge(int size) {
        super(size);
    }

    /**
     * Add an element to the map.
     * @param key The key to store the value under.  If the key already
     * exists the value will be added to the collection for that key, it
     * will not replace the existing value (as in a standard map).
     * @param value value to store.
     * @param pos position in the arraylist to store the new value at.
     * Positions are zero based.
     */
    public void put(Operator key, Operator value, int pos) {
        ArrayList<Operator> list = mMap.get(key);
        if (list == null) {
            list = new ArrayList<Operator>();
            if (pos != 0) {
                throw new IndexOutOfBoundsException(
                    "First edge cannot have position greater than 1");
            }
            list.add(value);
            mMap.put(key, list);
        } else {
            list.add(pos, value);
        }
    }

    /**
     * Remove one value from an existing key and return which position in
     * the arraylist the value was at..  If that is the last value
     * for the key, then remove the key too.
     * @param key Key to remove the value from.
     * @param value Value to remove.
     * @return A pair containing the value being removed and an integer
     * indicating the position, or null if the key or value does
     * not exist.  Positions are zero based.
     */
    public Pair<Operator, Integer> removeWithPosition(Operator key,
                                                      Operator value) {
        ArrayList<Operator> list = mMap.get(key);
        if (list == null) return null;

        int index = -1;
        Iterator<Operator> i = list.iterator();
        Operator keeper = null;
        for (int j = 0; i.hasNext(); j++) {
            keeper = i.next();
            //if (keeper.equals(value)) {
            if (keeper == value) {
                i.remove();
                index = j;
                break;
            }
        }
        
        if (index == -1) return null;

        if (list.size() == 0) {
            mMap.remove(key);
        }

        return new Pair<Operator, Integer>(keeper, index);
    }

    public PlanEdge shallowClone() {
        // shallow clone: elements not cloned
        PlanEdge result = new PlanEdge();
        for (Map.Entry<Operator, ArrayList<Operator>> entry : mMap.entrySet()) {
            ArrayList<Operator> list = new ArrayList<Operator>();
            list.addAll(entry.getValue());
            result.put(entry.getKey(), list);
        }
        return result;
    }
}

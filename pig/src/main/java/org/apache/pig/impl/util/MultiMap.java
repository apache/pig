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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * An implementation of multi-map.  We can't use Apache commons
 * MultiValueMap because it isn't serializable.  And we don't want to use
 * MultiHashMap, as it is marked deprecated.
 * 
 * This class can't extend Map, because it needs to change the semantics of
 * put, so that you give it one key and one value, and it either creates a
 * new entry with the key and a new collection of value (if the is not yet
 * in the map) or adds the values to the existing collection for the key
 * (if the key is already in the map).
 */
public class MultiMap<K, V> implements Serializable {

	// Change this if you modify the class.
	static final long serialVersionUID = 2L;

    protected Map<K, ArrayList<V>> mMap = null;

    public MultiMap() {
        mMap = new HashMap<K, ArrayList<V>>();
    }

    /**
     * @param size Initial size of the map
     */
    public MultiMap(int size) {
        mMap = new HashMap<K, ArrayList<V>>(size);
    }

    /**
     * Add an element to the map.
     * @param key The key to store the value under.  If the key already
     * exists the value will be added to the collection for that key, it
     * will not replace the existing value (as in a standard map).
     * @param value value to store.
     */
    public void put(K key, V value) {
        ArrayList<V> list = mMap.get(key);
        if (list == null) {
            list = new ArrayList<V>();
            list.add(value);
            mMap.put(key, list);
        } else {
            list.add(value);
        }
    }

    /**
     * Add a key to the map with a collection of elements.
     * @param key The key to store the value under.  If the key already
     * exists the value will be added to the collection for that key, it
     * will not replace the existing value (as in a standard map).
     * @param values collection of values to store.
     */
    public void put(K key, Collection<V> values) {
        ArrayList<V> list = mMap.get(key);
        if (list == null) {
            list = new ArrayList<V>(values);
            mMap.put(key, list);
        } else {
            list.addAll(values);
        }
    }

    /**
     * Get the collection of values associated with a given key.
     * @param key Key to fetch values for.
     * @return list of values, or null if the key is not in the map.
     */
    public List<V> get(K key) {
        return mMap.get(key);
    }

    /**
     * Remove one value from an existing key.  If that is the last value
     * for the key, then remove the key too.
     * @param key Key to remove the value from.
     * @param value Value to remove.
     * @return The value being removed, or null if the key or value does
     * not exist.
     */
    public V remove(K key, V value) {
        ArrayList<V> list = mMap.get(key);
        if (list == null) return null;

        Iterator<V> i = list.iterator();
        V keeper = null;
        while (i.hasNext()) {
            keeper = i.next();
            if (keeper.equals(value)) {
                i.remove();
                break;
            }
        }

        if (list.size() == 0) {
            mMap.remove(key);
        }

        return keeper;
    }

    /**
     * Remove all the values associated with the given key
     * @param key the key to be removed
     * @return list of all value being removed
     */
    public Collection<V> removeKey(K key) {
        return mMap.remove(key) ;
    }

    /**
     * Get a set of all the keys in this map.
     * @return Set of keys.
     */
    public Set<K> keySet() {
        return mMap.keySet();
    }

    /**
     * Get a single collection of all the values in the map.  All of the
     * values in the map will be conglomerated into one collection.  There
     * will not be any duplicate removal.
     * @return collection of values.
     */
    public Collection<V> values() {
        Set<K> keys = mMap.keySet();
        int size = 0;
        for (K k : keys) {
            size += mMap.get(k).size();
        }
        Collection<V> values = new ArrayList<V>(size);
        for (K k : keys) {
            values.addAll(mMap.get(k));
        }
        return values;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        Set<K> keys = mMap.keySet();
        boolean hasNext = false;
        sb.append("{");
        for (K k : keys) {
            if(hasNext) {
                sb.append(",");
            } else {
                hasNext = true;
            }
            sb.append(k.toString() + "=");
            sb.append(mMap.get(k));
        }
        sb.append("}");
        return sb.toString();
    }
    
    /**
     * Get the number of keys in the map.
     * @return number of keys.
     */
    public int size() {
        return mMap.size();
    }

    public boolean isEmpty() {
        return mMap.isEmpty();
    }

    public void clear() {
        mMap.clear();
    }

    public boolean containsKey(K key) {
        return mMap.containsKey(key);
    }

    public boolean containsValue(V val) {
        return mMap.containsValue(val);
    }
}

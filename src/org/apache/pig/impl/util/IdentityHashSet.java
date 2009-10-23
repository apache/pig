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

import java.util.*;
import java.util.Map.Entry;

public class IdentityHashSet<E> implements Set<E> {
    
    IdentityHashMap<E, Object> map = new IdentityHashMap<E, Object>();

    public boolean add(E element) {
        if (map.containsKey(element)) {
            return false;
        } else {
            map.put(element, null);
            return true;
        }
    }

    public boolean addAll(Collection<? extends E> elements) {
        boolean anyChanges = false;
        for (E element : elements) {
            if (!map.containsKey(element)) {
                anyChanges = true;
                map.put(element, null);
            }
        }
        return anyChanges;
    }

    public void clear() {
        map.clear();
    }

    public boolean contains(Object element) {
        return map.containsKey(element);
    }

    public boolean containsAll(Collection<?> elements) {
        for (Object element : elements) {
            if (!map.containsKey(element)) return false;
        }
        return true;
    }

    public boolean isEmpty() {
        return map.isEmpty();
    }

    public Iterator<E> iterator() {
        
        return new Iterator<E>() {
            Iterator<Map.Entry<E, Object>> it = map.entrySet().iterator();
            
            public boolean hasNext() {
                return it.hasNext();
            }

            public E next() {
                return it.next().getKey();
            }

            public void remove() {
                it.remove();
            }
        };
    }

    public boolean remove(Object element) {
        if (map.containsKey(element)) {
            map.remove(element);
            return true;
        } else {
            return false;
        }
    }

    public boolean removeAll(Collection<?> elements) {
        for (Object element : elements) map.remove(element);
        return true;
    }

    @SuppressWarnings("unchecked")
    public boolean retainAll(Collection<?> elements) {
        IdentityHashMap<E, Object> newMap = new IdentityHashMap<E, Object>();

        for (Object element : elements) {
            if (map.containsKey(element)) newMap.put((E) element, null);
        }
        
        boolean anyChanges = newMap.size() != map.size();
        
        map = newMap;
        return anyChanges;
    }

    public int size() {
        return map.size();
    }

    public Object[] toArray() {
        throw new UnsupportedOperationException("Unsupported operation on IdentityHashSet.");
    }

    public <T> T[] toArray(T[] dummy) {
        throw new UnsupportedOperationException("Unsupported operation on IdentityHashSet.");
    }

    public String toString() {
	StringBuffer buf = new StringBuffer();
	buf.append("{");

	Iterator<Entry<E, Object>> i = map.entrySet().iterator();
	boolean hasNext = i.hasNext();
	while (hasNext) {
	    Entry<E, Object> e = i.next();
	    E key = e.getKey();
	    buf.append(key);
	    hasNext = i.hasNext();
	    if (hasNext)
		buf.append(", ");
	}

	buf.append("}");
	return buf.toString();
    }
    
}

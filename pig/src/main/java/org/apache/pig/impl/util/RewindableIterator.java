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

import java.io.IOException;
import java.util.*;

public class RewindableIterator<E> {
    private Iterator<E> it;
    private ArrayList<E> buf;
    int pos = 0;
    boolean noRewind = false;
    
    public RewindableIterator(Iterator<E> it) {
        this.it = it;
        buf = new ArrayList<E>();
    }

    public boolean hasNext() {
        return (buf.size() > pos || it.hasNext());
    }
    
    public boolean hasNext(int k) {
        int need = k - (buf.size() - pos);
        
        while (need > 0) {
            if (!it.hasNext()) {
                return false;
            } else {
                buf.add(it.next());
                need--;
            }
        }
        
        return true;
    }
    
    public void rewind() throws IOException {
        if (noRewind) throw new IOException("Internal error: attempt to rewind RewindableIterator after rewind has been disabled.");
        pos = 0;
    }
    
    public void noRewind() {
        noRewind = true;
        
        // clear out part of buffer that has been read
        while (pos > 0) {
            buf.remove(0);
            pos--;
        }
    }

    public E next() {
        if (noRewind) {
            if (buf.size() <= pos) return it.next();
            else return buf.remove(pos);
        } else {           
            if (buf.size() <= pos) buf.add(it.next());
        
            return buf.get(pos++);
        }
    }

}

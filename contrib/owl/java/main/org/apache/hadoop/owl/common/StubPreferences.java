
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

package org.apache.hadoop.owl.common;

import java.util.HashMap;
import java.util.Map;
import java.util.prefs.AbstractPreferences;
import java.util.prefs.BackingStoreException;

public class StubPreferences extends AbstractPreferences {

    Map<String,String> contents = null;
    Map<String,StubPreferences> subNodes = null;

    private boolean isNodeRemoved;

    @Override
    public boolean isRemoved(){
        return isNodeRemoved;
    }

    protected StubPreferences(AbstractPreferences parent, String name) {
        super(parent, name);
        contents = new HashMap<String,String>();
        subNodes = new HashMap<String,StubPreferences>();
        isNodeRemoved = false;
    }

    @Override
    protected AbstractPreferences childSpi(String name) {
        if (!subNodes.containsKey(name)){
            subNodes.put(name, new StubPreferences(this,name));
        }else{
            StubPreferences child = subNodes.get(name);
            if ((child == null)||(child.isRemoved())){
                subNodes.put(name, new StubPreferences(this,name));
            }
        }
        return subNodes.get(name);
    }

    @Override
    protected String[] childrenNamesSpi() throws BackingStoreException {
        return subNodes.keySet().toArray(new String[subNodes.keySet().size()]);
    }

    @Override
    protected void flushSpi() throws BackingStoreException {
        // Dummy implementation, does not map to a backing store anyway, all in memory.
    }

    @Override
    protected String getSpi(String key) {
        if (contents.containsKey(key)){
            return contents.get(key);
        }
        return null;
    }

    @Override
    protected String[] keysSpi() throws BackingStoreException {
        return contents.keySet().toArray(new String[contents.keySet().size()]);
    }

    @Override
    protected void putSpi(String key, String value) {
        contents.put(key, value);
    }

    @Override
    protected void removeNodeSpi() throws BackingStoreException {
        contents.clear();
        subNodes.clear();
        isNodeRemoved = true;
    }

    @Override
    protected void removeSpi(String key) {
        contents.remove(key);
    }

    @Override
    protected void syncSpi() throws BackingStoreException {
        // Dummy implementation, does not map to a backing store anyway, all in memory.
    }

}

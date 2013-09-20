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

package org.apache.pig.newplan.logical.relational;

import java.util.ArrayList;
import java.util.List;

import org.apache.pig.newplan.Operator;

/**
 * 
 * This class provides information regarding the LogicalPlan. Make sure to
 * avoid exposing LogicalPlan itself. Only data regarding the logical plan
 * could be exposed but none of Pig internals (plans, operators etc) should
 * be.
 *
 */
public class LogicalPlanData {

    // Never expose LogicalPlan
    private final LogicalPlan lp;

    public LogicalPlanData(LogicalPlan lp) {
        if(lp == null) {
            throw new RuntimeException("LogicalPlan is null.");
        }
        this.lp = lp; 
    }

    /**
     * 
     * @return This method return the list of source paths defined 
     *         in the script/query.
     */
    public List<String> getSources() {
        List<LOLoad> sources = getLOLoads();
        if (sources == null) {
            return null;
        }

        List<String> result = new ArrayList<String>();
        for (LOLoad load : sources) {
            result.add(load.getFileSpec().getFileName());
        }

        return result;
    }

    /**
     * 
     * @return This method returns the list of store paths in the script/query.
     */
    public List<String> getSinks() {
        List<LOStore> sinks = getLOStores();
        if (sinks == null) {
            return null;
        }
        List<String> result = new ArrayList<String>();
        for (LOStore sink : sinks) {
            result.add(sink.getFileSpec().getFileName());
        }

        return result;
    }

    /**
     * 
     * @return This method returns the list of LoadFunc(s) used.
     */
    public List<String> getLoadFuncs() {
        List<LOLoad> sources = getLOLoads();
        if (sources == null) {
            return null;
        }
        List<String> result = new ArrayList<String>();
        for (LOLoad load : sources) {
            result.add(load.getFileSpec().getFuncName());
        }

        return result;
    }

    /**
     * 
     * @return This method returns the list of StoreFunc(s) used.
     */
    public List<String> getStoreFuncs() {
        List<LOStore> sinks = getLOStores();
        if (sinks == null) {
            return null;
        }
        List<String> storeFuncs = new ArrayList<String>();
        for (LOStore sink : sinks) {
            storeFuncs.add(sink.getFileSpec().getFuncName());
        }

        return storeFuncs;
    }

    /**
     * Internal to Pig. Do not expose this method
     * @return
     */
    private List<LOLoad> getLOLoads() {
        List<Operator> sources = lp.getSources();
        if (sources == null) {
            return null;
        }
        List<LOLoad> result = new ArrayList<LOLoad>();
        for (Operator source : sources) {
            if (source instanceof LOLoad) {
                LOLoad load = (LOLoad) source;
                result.add(load);
            }
        }

        return result;
    }
    
    /**
     * Internal to Pig. Do not expose this method
     * @return
     */
    private List<LOStore> getLOStores() {
        List<Operator> sinks = lp.getSinks();
        if (sinks == null) {
            return null;
        }
        List<LOStore> result = new ArrayList<LOStore>();
        for (Operator sink : sinks) {
            if (sink instanceof LOStore) {
                LOStore store = (LOStore) sink;
                result.add(store);
            }
        }

        return result;
    }

}

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

import java.util.Iterator;
import java.util.List;

import org.apache.pig.classification.InterfaceAudience;
import org.apache.pig.classification.InterfaceStability;
import org.apache.pig.newplan.Operator;

import com.google.common.collect.Lists;

/**
 * 
 * This class provides information regarding the LogicalPlan. Make sure to avoid
 * exposing LogicalPlan itself. Only data regarding the logical plan could be
 * exposed but none of Pig internals (plans, operators etc) should be.
 * 
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class LogicalPlanData {

    // Never expose LogicalPlan
    private final LogicalPlan lp;
    private int numLogicalRelationalOperators;
    // Sources and Sinks here refer to Load and Store operators
    private final List<LOLoad> sources;
    private final List<LOStore> sinks;

    public LogicalPlanData(LogicalPlan lp) {
        if (lp == null) {
            throw new RuntimeException("LogicalPlan is null.");
        }
        this.lp = lp;
        this.numLogicalRelationalOperators = 0;
        this.sources = Lists.newArrayList();
        this.sinks = Lists.newArrayList();
        init();
    }

    private void init() {
        Iterator<Operator> ops = this.lp.getOperators();

        while (ops.hasNext()) {
            Operator op = ops.next();
            if (op instanceof LogicalRelationalOperator) {
                this.numLogicalRelationalOperators++;
                if (op instanceof LOLoad) {
                    sources.add((LOLoad) op);
                } else if (op instanceof LOStore) {
                    sinks.add((LOStore) op);
                }
            }
        }
    }

    /**
     * Returns the number of {@link LogicalRelationalOperator}s present in the
     * pig script.
     * 
     * @return number of logical relational operators (Load, Join, Store etc)
     * 
     */
    public int getNumLogicalRelationOperators() {
        return this.numLogicalRelationalOperators;
    }

    /**
     * 
     * @return number of Load statements in the script
     */
    public int getNumSources() {
        return this.sources.size();
    }

    /**
     * 
     * @return number of Store statements in the script
     */
    public int getNumSinks() {
        return this.sinks.size();
    }

    /**
     * 
     * @return This method return the list of Load paths defined in the
     *         script/query.
     */
    public List<String> getSources() {
        List<String> result = Lists.newArrayList();
        for (LOLoad load : this.sources) {
            result.add(load.getFileSpec().getFileName());
        }

        return result;
    }

    /**
     * 
     * @return This method returns the list of store paths in the script/query.
     */
    public List<String> getSinks() {
        List<String> result = Lists.newArrayList();
        for (LOStore sink : this.sinks) {
            result.add(sink.getFileSpec().getFileName());
        }

        return result;
    }

    /**
     * 
     * @return This method returns the list of LoadFunc(s) used.
     */
    public List<String> getLoadFuncs() {
        List<String> result = Lists.newArrayList();
        for (LOLoad load : this.sources) {
            result.add(load.getFileSpec().getFuncName());
        }

        return result;
    }

    /**
     * 
     * @return This method returns the list of StoreFunc(s) used.
     */
    public List<String> getStoreFuncs() {
        List<String> storeFuncs = Lists.newArrayList();
        for (LOStore sink : this.sinks) {
            storeFuncs.add(sink.getFileSpec().getFuncName());
        }

        return storeFuncs;
    }
}

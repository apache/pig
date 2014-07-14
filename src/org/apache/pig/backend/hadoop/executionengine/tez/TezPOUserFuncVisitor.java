/**
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
package org.apache.pig.backend.hadoop.executionengine.tez;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POUserFunc;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.impl.plan.DepthFirstWalker;
import org.apache.pig.impl.plan.VisitorException;

public class TezPOUserFuncVisitor extends TezOpPlanVisitor {
    private Set<String> cacheFiles = new HashSet<String>();

    public TezPOUserFuncVisitor(TezOperPlan plan) {
        super(plan, new DepthFirstWalker<TezOperator, TezOperPlan>(plan));
    }

    @Override
    public void visitTezOp(TezOperator tezOp) throws VisitorException {
        if(!tezOp.plan.isEmpty()) {
            UdfCacheFileVisitor udfCacheFileVisitor = new UdfCacheFileVisitor(tezOp.plan);
            udfCacheFileVisitor.visit();
        }
    }

    class UdfCacheFileVisitor extends PhyPlanVisitor {

        public UdfCacheFileVisitor(PhysicalPlan plan) {
            super(plan, new DepthFirstWalker<PhysicalOperator, PhysicalPlan>(plan));
        }

        public void visitUserFunc(POUserFunc udf) throws VisitorException {
            String[] files = udf.getCacheFiles();
            if (files != null) {
                cacheFiles.addAll(Arrays.asList(files));
            }
        }
    }

    public Set<String> getCacheFiles() {
        return cacheFiles;
    }
}

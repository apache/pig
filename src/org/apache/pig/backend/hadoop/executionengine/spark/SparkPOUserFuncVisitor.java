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
package org.apache.pig.backend.hadoop.executionengine.spark;

import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.UdfCacheShipFilesVisitor;
import org.apache.pig.backend.hadoop.executionengine.spark.plan.SparkOpPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.spark.plan.SparkOperPlan;
import org.apache.pig.backend.hadoop.executionengine.spark.plan.SparkOperator;
import org.apache.pig.impl.plan.DepthFirstWalker;
import org.apache.pig.impl.plan.VisitorException;

import java.util.HashSet;
import java.util.Set;

public class SparkPOUserFuncVisitor extends SparkOpPlanVisitor {
    private Set<String> cacheFiles = new HashSet<>();
    private Set<String> shipFiles = new HashSet<>();

    public SparkPOUserFuncVisitor(SparkOperPlan plan) {
        super(plan, new DepthFirstWalker<>(plan));
    }

    @Override
    public void visitSparkOp(SparkOperator sparkOperator) throws VisitorException {
        if(!sparkOperator.physicalPlan.isEmpty()) {
            UdfCacheShipFilesVisitor udfCacheFileVisitor = new UdfCacheShipFilesVisitor(sparkOperator.physicalPlan);
            udfCacheFileVisitor.visit();
            cacheFiles.addAll(udfCacheFileVisitor.getCacheFiles());
            shipFiles.addAll(udfCacheFileVisitor.getShipFiles());
        }
    }

    public Set<String> getCacheFiles() {
        return cacheFiles;
    }

    public Set<String> getShipFiles() {
        return shipFiles;
    }
}

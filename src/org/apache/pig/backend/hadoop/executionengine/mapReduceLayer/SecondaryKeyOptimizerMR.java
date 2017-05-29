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
package org.apache.pig.backend.hadoop.executionengine.mapReduceLayer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROpPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROperPlan;
import org.apache.pig.backend.hadoop.executionengine.optimizer.SecondaryKeyOptimizer;
import org.apache.pig.backend.hadoop.executionengine.util.SecondaryKeyOptimizerUtil;
import org.apache.pig.backend.hadoop.executionengine.util.SecondaryKeyOptimizerUtil.SecondaryKeyOptimizerInfo;
import org.apache.pig.classification.InterfaceAudience;
import org.apache.pig.impl.plan.DepthFirstWalker;
import org.apache.pig.impl.plan.VisitorException;

@InterfaceAudience.Private
public class SecondaryKeyOptimizerMR extends MROpPlanVisitor implements SecondaryKeyOptimizer {
    private static Log log = LogFactory.getLog(SecondaryKeyOptimizerMR.class);
    private SecondaryKeyOptimizerInfo info;

    /**
     * @param plan
     *            The MROperPlan to visit to discover keyType
     */
    public SecondaryKeyOptimizerMR(MROperPlan plan) {
        super(plan, new DepthFirstWalker<MapReduceOper, MROperPlan>(plan));
    }


    @Override
    public void visitMROp(MapReduceOper mr) throws VisitorException {
        // Only optimize for Cogroup case
        if (mr.isGlobalSort())
            return;

        // Don't optimize when we already have a custom partitioner
        if (mr.getCustomPartitioner()!=null)
            return;

        SecondaryKeyOptimizerUtil secondaryKeyOptUtil = new SecondaryKeyOptimizerUtil();
        info = secondaryKeyOptUtil.applySecondaryKeySort(mr.mapPlan, mr.reducePlan);
        if (info != null && info.isUseSecondaryKey()) {
            mr.setUseSecondaryKey(true);
            mr.setSecondarySortOrder(info.getSecondarySortOrder());
            log.info("Using Secondary Key Optimization for MapReduce node " + mr.getOperatorKey());
        }
    }


    @Override
    public int getNumSortRemoved() {
        return (info == null) ? 0 : info.getNumSortRemoved();
    }

    @Override
    public int getNumDistinctChanged() {
        return (info == null) ? 0 : info.getNumDistinctChanged();
    }

    @Override
    public int getNumUseSecondaryKey() {
        return (info == null) ? 0 : info.getNumUseSecondaryKey();
    }

}

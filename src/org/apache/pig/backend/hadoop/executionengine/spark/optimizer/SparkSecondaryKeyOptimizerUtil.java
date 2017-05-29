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
package org.apache.pig.backend.hadoop.executionengine.spark.optimizer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POGlobalRearrange;
import org.apache.pig.backend.hadoop.executionengine.util.SecondaryKeyOptimizerUtil;

import java.util.List;

public class SparkSecondaryKeyOptimizerUtil extends SecondaryKeyOptimizerUtil{
    private static Log log = LogFactory.getLog(SparkSecondaryKeyOptimizerUtil.class);

    @Override
    protected PhysicalOperator getCurrentNode(PhysicalOperator root, PhysicalPlan reducePlan) {
        PhysicalOperator currentNode = null;

        if (!(root instanceof POGlobalRearrange)) {
            log.debug("Expected reduce root to be a POGlobalRearrange, skip secondary key optimizing");
            currentNode = null;
        } else {
            List<PhysicalOperator> globalRearrangeSuccs = reducePlan
                    .getSuccessors(root);
            if (globalRearrangeSuccs.size() == 1) {
                currentNode = globalRearrangeSuccs.get(0);
            } else {
                log.debug("Expected successor of a POGlobalRearrange is POPackage, skip secondary key optimizing");
                currentNode = null;
            }
        }

        return currentNode;
    }
}

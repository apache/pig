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
package org.apache.pig.tez;

import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.tez.TezLauncher;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.TezPlanContainer;
import org.apache.pig.backend.hadoop.executionengine.util.MapRedUtil;
import org.apache.pig.impl.PigContext;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.test.Util;

public class TezUtil {
    public static TezPlanContainer buildTezPlanContainer(String query, PigContext pc) throws Exception {
        LogicalPlan lp = Util.parse(query, pc);
        Util.optimizeNewLP(lp);
        PhysicalPlan pp = Util.buildPhysicalPlanFromNewLP(lp, pc);
        TezPlanContainer tezPlanContainer = buildTezPlanWithOptimizer(pp, pc);
        return tezPlanContainer;
    }

    public static TezPlanContainer buildTezPlanWithOptimizer(PhysicalPlan pp, PigContext pc) throws Exception {
        MapRedUtil.checkLeafIsStore(pp, pc);
        TezLauncher launcher = new TezLauncher();
        return launcher.compile(pp, pc);
    }
}

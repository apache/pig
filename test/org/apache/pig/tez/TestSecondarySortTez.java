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

import org.apache.pig.PigConfiguration;
import org.apache.pig.backend.hadoop.executionengine.optimizer.SecondaryKeyOptimizer;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.TezCompiler;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.TezOperPlan;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.optimizer.CombinerOptimizer;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.optimizer.SecondaryKeyOptimizerTez;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.test.MiniGenericCluster;
import org.apache.pig.test.TestSecondarySort;
import org.apache.pig.test.Util;

public class TestSecondarySortTez extends TestSecondarySort {

    public TestSecondarySortTez() {
        super();
    }

    @Override
    public MiniGenericCluster getCluster() {
        return MiniGenericCluster.buildCluster(MiniGenericCluster.EXECTYPE_TEZ);
    }

    @Override
    public SecondaryKeyOptimizer visitSecondaryKeyOptimizer(String query)
            throws Exception, VisitorException {
        PhysicalPlan pp = Util.buildPp(pigServer, query);
        TezCompiler comp = new TezCompiler(pp, pc);
        TezOperPlan tezPlan = comp.compile();
        boolean nocombiner = Boolean.parseBoolean(pc.getProperties().getProperty(
                PigConfiguration.PIG_EXEC_NO_COMBINER, "false"));

        // Run CombinerOptimizer on Tez plan
        if (!nocombiner) {
            boolean doMapAgg = Boolean.parseBoolean(pc.getProperties()
                    .getProperty(PigConfiguration.PIG_EXEC_MAP_PARTAGG,
                            "false"));
            CombinerOptimizer co = new CombinerOptimizer(tezPlan, doMapAgg);
            co.visit();
        }

        SecondaryKeyOptimizerTez skOptimizer = new SecondaryKeyOptimizerTez(
                tezPlan);
        skOptimizer.visit();
        return skOptimizer;
    }

}


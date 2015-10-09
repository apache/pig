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

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.pig.backend.executionengine.ExecJob;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.tez.TezLauncher;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.TezOperPlan;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.TezOperator;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.TezPlanContainer;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.TezPlanContainerNode;
import org.apache.pig.impl.PigContext;
import org.apache.pig.test.TestPigStats;
import org.apache.pig.tools.pigstats.PigStats.JobGraph;
import org.apache.pig.tools.pigstats.tez.TezPigScriptStats;
import org.apache.pig.tools.pigstats.tez.TezScriptState;
import org.apache.pig.tools.pigstats.tez.TezScriptState.TezDAGScriptInfo;

public class TestPigStatsTez extends TestPigStats {
    @Override
    public void addSettingsToConf(Configuration conf, String scriptFileName) throws IOException {
        TezScriptState ss = TezScriptState.get();
        ss.setScript(new File(scriptFileName));
        ss.addDAGSettingsToConf(conf);
    }

    @Override
    public void checkPigStats(ExecJob job) {
        JobGraph jobGraph = job.getStatistics().getJobGraph();
        assertEquals(1, jobGraph.getJobList().size());
    }

    @Override
    public void checkPigStatsAlias(PhysicalPlan pp, PigContext pc) throws Exception {
        TezPlanContainer planContainer = new TezLauncher().compile(pp, pc);
        assertEquals(planContainer.size(), 1);
        TezPlanContainerNode containerNode = planContainer.iterator().next();
        TezOperPlan tezPlan = containerNode.getTezOperPlan();
        assertEquals(tezPlan.size(), 6);

        TezPigScriptStats tezStats = new TezPigScriptStats(pc);
        tezStats.initialize(planContainer);

        TezScriptState ss = TezScriptState.get();
        TezDAGScriptInfo scriptInfo = ss.getDAGScriptInfo(containerNode.getOperatorKey().toString());

        TezOperator tezOper = tezPlan.getRoots().get(0);
        assertEquals(getAlias(scriptInfo, tezOper), "A,B,C");
        tezOper = tezPlan.getSuccessors(tezOper).get(0);
        assertEquals(getAlias(scriptInfo, tezOper), "C,D");
        TezOperator partitionerOper = null;
        for (TezOperator succ : tezPlan.getSuccessors(tezOper)) {
            if (succ.isSampleAggregation()) {
                assertEquals(getAlias(scriptInfo, succ), "");
            } else {
                assertEquals(getAlias(scriptInfo, succ), "D");
                partitionerOper = succ;
            }
        }
        tezOper = partitionerOper;
        assertEquals(getAlias(scriptInfo, tezOper), "D");
        tezOper = tezPlan.getSuccessors(tezOper).get(0);
        assertEquals(getAlias(scriptInfo, tezOper), "");
        tezOper = tezPlan.getSuccessors(tezOper).get(0);
        assertEquals(getAlias(scriptInfo, tezOper), "");
    }

    public static String getAlias(TezDAGScriptInfo scriptInfo, TezOperator oper) throws Exception {
        String alias = scriptInfo.getAlias(oper);
        return alias;
    }
}

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
import static org.junit.Assert.assertTrue;

import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.pig.backend.hadoop.executionengine.JobCreationException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.tez.TezDagBuilder;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.TezCompiler;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.TezOperPlan;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.TezPlanContainerNode;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.optimizer.LoaderProcessor;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.optimizer.ParallelismSetter;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.test.TestJobSubmission;
import org.apache.pig.test.Util;
import org.apache.pig.tools.pigstats.ScriptState;
import org.apache.pig.tools.pigstats.tez.TezScriptState;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.Vertex;
import org.junit.Assume;
import org.junit.BeforeClass;

public class TestJobSubmissionTez extends TestJobSubmission {

    @BeforeClass
    public static void oneTimeSetup() throws Exception{
        String execType = System.getProperty("test.exec.type");
        Assume.assumeTrue("This test suite should only run in tez mode", execType.equalsIgnoreCase("tez"));
        TestJobSubmission.oneTimeSetUp();
    }

    @Override
    public void checkJobControlCompilerErrResult(PhysicalPlan pp, PigContext pc) throws Exception {
        TezOperPlan tezPlan = buildTezPlan(pp, pc);

        LoaderProcessor loaderStorer = new LoaderProcessor(tezPlan, pc);
        loaderStorer.visit();

        ParallelismSetter parallelismSetter = new ParallelismSetter(tezPlan, pc);
        parallelismSetter.visit();

        DAG tezDag = getTezDAG(tezPlan, pc);
        TezDagBuilder dagBuilder = new TezDagBuilder(pc, tezPlan, tezDag, new HashMap<String, LocalResource>());
        try {
            dagBuilder.visit();
        } catch (VisitorException jce) {
            assertTrue(((JobCreationException)jce.getCause()).getErrorCode() == 1068);
        }
    }

    @Override
    public void checkDefaultParallelResult(PhysicalPlan pp, PigContext pc) throws Exception {
        TezOperPlan tezPlan = buildTezPlan(pp, pc);

        LoaderProcessor loaderStorer = new LoaderProcessor(tezPlan, pc);
        loaderStorer.visit();

        ParallelismSetter parallelismSetter = new ParallelismSetter(tezPlan, pc);
        parallelismSetter.visit();

        DAG tezDag = getTezDAG(tezPlan, pc);
        TezDagBuilder dagBuilder = new TezDagBuilder(pc, tezPlan, tezDag, new HashMap<String, LocalResource>());
        dagBuilder.visit();
        for (Vertex v : tezDag.getVertices()) {
            if (!v.getInputVertices().isEmpty()) {
                Configuration conf = TezUtils.createConfFromUserPayload(v.getProcessorDescriptor().getUserPayload());
                int parallel = v.getParallelism();
                assertEquals(parallel, 100);
                Util.assertConfLong(conf, "pig.info.reducers.default.parallel", 100);
                Util.assertConfLong(conf, "pig.info.reducers.requested.parallel", -1);
                Util.assertConfLong(conf, "pig.info.reducers.estimated.parallel", -1);
            }
        }
    }

    private TezOperPlan buildTezPlan(PhysicalPlan pp, PigContext pc) throws Exception{
        TezCompiler comp = new TezCompiler(pp, pc);
        comp.compile();
        return comp.getTezPlan();
    }

    private DAG getTezDAG(TezOperPlan tezPlan, PigContext pc) {
        TezPlanContainerNode tezPlanNode = new TezPlanContainerNode(OperatorKey.genOpKey("DAGName"), tezPlan);
        TezScriptState scriptState = new TezScriptState("test");
        ScriptState.start(scriptState);
        scriptState.setDAGScriptInfo(tezPlanNode);
        DAG tezDag = DAG.create(tezPlanNode.getOperatorKey().toString());
        return tezDag;
    }
}

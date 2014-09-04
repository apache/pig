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

import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.tez.TezCompiler;
import org.apache.pig.backend.hadoop.executionengine.tez.TezDagBuilder;
import org.apache.pig.backend.hadoop.executionengine.tez.TezOperPlan;
import org.apache.pig.backend.hadoop.executionengine.tez.optimizers.LoaderProcessor;
import org.apache.pig.backend.hadoop.executionengine.tez.optimizers.ParallelismSetter;
import org.apache.pig.impl.PigContext;
import org.apache.pig.test.TestGroupConstParallel;
import org.apache.pig.tools.pigstats.PigStats.JobGraph;
import org.apache.pig.tools.pigstats.tez.TezTaskStats;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.Vertex;
import org.junit.Assume;
import org.junit.BeforeClass;

public class TestGroupConstParallelTez extends TestGroupConstParallel {

    @BeforeClass
    public static void oneTimeSetup() throws Exception{
        String execType = System.getProperty("test.exec.type");
        Assume.assumeTrue("This test suite should only run in tez mode", execType.equalsIgnoreCase("tez"));
        TestGroupConstParallel.oneTimeSetup();
    }

    @Override
    public void checkGroupAllWithParallelGraphResult(JobGraph jGraph) {
        TezTaskStats ts = (TezTaskStats)jGraph.getSinks().get(0);
        assertEquals(ts.getParallelism(), 1);
    }

    @Override
    public void checkGroupConstWithParallelResult(PhysicalPlan pp, PigContext pc) throws Exception {
        TezOperPlan tezPlan = buildTezPlan(pp, pc);

        LoaderProcessor loaderStorer = new LoaderProcessor(tezPlan, pc);
        loaderStorer.visit();

        ParallelismSetter parallelismSetter = new ParallelismSetter(tezPlan, pc);
        parallelismSetter.visit();

        DAG tezDag = DAG.create("test");
        TezDagBuilder dagBuilder = new TezDagBuilder(pc, tezPlan, tezDag, null);
        dagBuilder.visit();
        for (Vertex v : tezDag.getVertices()) {
            if (!v.getInputVertices().isEmpty()) {
                assertEquals(v.getParallelism(), 1);
            }
        }
    }

    @Override
    public void checkGroupNonConstWithParallelResult(PhysicalPlan pp, PigContext pc) throws Exception {
        TezOperPlan tezPlan = buildTezPlan(pp, pc);

        LoaderProcessor loaderStorer = new LoaderProcessor(tezPlan, pc);
        loaderStorer.visit();

        ParallelismSetter parallelismSetter = new ParallelismSetter(tezPlan, pc);
        parallelismSetter.visit();

        DAG tezDag = DAG.create("test");
        TezDagBuilder dagBuilder = new TezDagBuilder(pc, tezPlan, tezDag, null);
        dagBuilder.visit();
        for (Vertex v : tezDag.getVertices()) {
            if (!v.getInputVertices().isEmpty()) {
                assertEquals(v.getParallelism(), 100);
            }
        }
    }

    private TezOperPlan buildTezPlan(PhysicalPlan pp, PigContext pc) throws Exception{
        TezCompiler comp = new TezCompiler(pp, pc);
        comp.compile();
        return comp.getTezPlan();
    }
}

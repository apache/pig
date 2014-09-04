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

import org.apache.hadoop.conf.Configuration;
import org.apache.pig.backend.hadoop.executionengine.JobCreationException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.tez.TezCompiler;
import org.apache.pig.backend.hadoop.executionengine.tez.TezDagBuilder;
import org.apache.pig.backend.hadoop.executionengine.tez.TezOperPlan;
import org.apache.pig.backend.hadoop.executionengine.tez.optimizers.LoaderProcessor;
import org.apache.pig.backend.hadoop.executionengine.tez.optimizers.ParallelismSetter;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.test.TestJobSubmission;
import org.apache.pig.test.Util;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.Vertex;

public class TestJobSubmissionTez extends TestJobSubmission {
    @Override
    public void checkJobControlCompilerErrResult(PhysicalPlan pp, PigContext pc) throws Exception {
        TezOperPlan tezPlan = buildTezPlan(pp, pc);

        LoaderProcessor loaderStorer = new LoaderProcessor(tezPlan, pc);
        loaderStorer.visit();

        ParallelismSetter parallelismSetter = new ParallelismSetter(tezPlan, pc);
        parallelismSetter.visit();

        DAG tezDag = DAG.create("test");
        TezDagBuilder dagBuilder = new TezDagBuilder(pc, tezPlan, tezDag, null);
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

        DAG tezDag = DAG.create("test");
        TezDagBuilder dagBuilder = new TezDagBuilder(pc, tezPlan, tezDag, null);
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
}

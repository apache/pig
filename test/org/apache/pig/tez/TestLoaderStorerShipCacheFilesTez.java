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

import java.util.ArrayList;
import java.util.List;

import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.tez.TezLauncher;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.TezOperPlan;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.TezPOUserFuncVisitor;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.TezPlanContainer;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.test.TestLoaderStorerShipCacheFiles;
import org.junit.Assert;

public class TestLoaderStorerShipCacheFilesTez extends TestLoaderStorerShipCacheFiles {

    @Override
    protected void checkPlan(PhysicalPlan pp, String[] expectedFiles, int size, PigContext pigContext) throws Exception {
        TezLauncher launcher = new TezLauncher();
        TezPlanContainer tezPlanContainer = launcher.compile(pp, pigContext);
        assertPlanContains(tezPlanContainer.getRoots().get(0).getNode(), expectedFiles, size);
    }

    private void assertPlanContains(TezOperPlan plan, String[] expectedFiles, int size) throws VisitorException {
        TezPOUserFuncVisitor udfVisitor = new TezPOUserFuncVisitor(plan);
        udfVisitor.visit();

        List<String> shipFiles = new ArrayList<String>();
        shipFiles.addAll(udfVisitor.getShipFiles());

        Assert.assertEquals(shipFiles.size(), size);
        assertContains(shipFiles, expectedFiles);
    }


}

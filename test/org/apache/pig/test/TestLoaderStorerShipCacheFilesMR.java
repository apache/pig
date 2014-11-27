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
package org.apache.pig.test;

import java.util.ArrayList;
import java.util.List;

import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MapReduceOper;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROperPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.UdfCacheShipFilesVisitor;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.plan.VisitorException;
import org.junit.Assert;

public class TestLoaderStorerShipCacheFilesMR extends TestLoaderStorerShipCacheFiles {

    @Override
    protected void checkPlan(PhysicalPlan pp, String[] expectedFiles, int size, PigContext pigContext) throws Exception {
        MROperPlan mrPlan = Util.buildMRPlan(pp, pigContext);
        assertPlanContains(mrPlan.getLeaves().get(0), expectedFiles, size);
    }
    
    private void assertPlanContains(MapReduceOper mro, String[] expectedFiles, int size) throws VisitorException {
        List<String> cacheFiles = new ArrayList<String>();
        List<String> shipFiles = new ArrayList<String>();
        UdfCacheShipFilesVisitor mapUdfCacheFileVisitor = new UdfCacheShipFilesVisitor(mro.mapPlan);
        mapUdfCacheFileVisitor.visit();
        cacheFiles.addAll(mapUdfCacheFileVisitor.getCacheFiles());
        shipFiles.addAll(mapUdfCacheFileVisitor.getShipFiles());

        UdfCacheShipFilesVisitor reduceUdfCacheFileVisitor = new UdfCacheShipFilesVisitor(mro.reducePlan);
        reduceUdfCacheFileVisitor.visit();
        cacheFiles.addAll(reduceUdfCacheFileVisitor.getCacheFiles());
        shipFiles.addAll(reduceUdfCacheFileVisitor.getShipFiles());

        Assert.assertEquals(shipFiles.size(), size);
        assertContains(shipFiles, expectedFiles);
    }
}

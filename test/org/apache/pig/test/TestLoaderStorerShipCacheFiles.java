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

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MapReduceOper;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROperPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.UdfCacheShipFilesVisitor;
import org.apache.pig.backend.hadoop.executionengine.tez.TezLauncher;
import org.apache.pig.backend.hadoop.executionengine.tez.TezOperPlan;
import org.apache.pig.backend.hadoop.executionengine.tez.TezPOUserFuncVisitor;
import org.apache.pig.backend.hadoop.executionengine.tez.TezPlanContainer;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.util.Utils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestLoaderStorerShipCacheFiles {
    private static PigServer pigServer;

    @Before
    public void setUp() throws Exception {
        pigServer = new PigServer(ExecType.LOCAL);
        pigServer.getPigContext().inExplain = true;
    }

    @Test
    public void testShipOrcLoader() throws Exception {
        String query = "a = load 'test/org/apache/pig/builtin/orc/orc-file-11-format.orc' using OrcStorage();" +
                "store a into 'ooo';";
        PhysicalPlan pp = Util.buildPp(pigServer, query);

        String hadoopVersion = "20S";
        if (Utils.isHadoop23() || Utils.isHadoop2()) {
            hadoopVersion = "23";
        }
        String[] expectedJars = new String[] {"hive-common", "hive-exec", "hive-serde", 
                "hive-shims-0." + hadoopVersion, "hive-shims-common-0", "hive-shims-common-secure"};

        MROperPlan mrPlan = Util.buildMRPlan(pp, pigServer.getPigContext());
        assertMRPlanContains(mrPlan.getRoots().get(0), expectedJars, 6);

        TezLauncher launcher = new TezLauncher();
        TezPlanContainer tezPlanContainer = launcher.compile(pp, pigServer.getPigContext());
        assertTezPlanContains(tezPlanContainer.getRoots().get(0).getNode(), expectedJars, 6);
    }

    @Test
    public void testShipOrcStorer() throws Exception {
        String query = "a = load '1.txt' as (name:chararray, age:int, gpa:double);" +
                "store a into 'ooo' using OrcStorage;";
        PhysicalPlan pp = Util.buildPp(pigServer, query);

        String hadoopVersion = "20S";
        if (Utils.isHadoop23() || Utils.isHadoop2()) {
            hadoopVersion = "23";
        }
        String[] expectedJars = new String[] {"hive-common", "hive-exec", "hive-serde", 
                "hive-shims-0." + hadoopVersion, "hive-shims-common-0", "hive-shims-common-secure"};

        MROperPlan mrPlan = Util.buildMRPlan(pp, pigServer.getPigContext());
        assertMRPlanContains(mrPlan.getRoots().get(0), expectedJars, 6);

        TezLauncher launcher = new TezLauncher();
        TezPlanContainer tezPlanContainer = launcher.compile(pp, pigServer.getPigContext());
        assertTezPlanContains(tezPlanContainer.getRoots().get(0).getNode(), expectedJars, 6);
    }

    @Test
    public void testShipAvroLoader() throws Exception {
        String query = "a = load '1.txt' as (name:chararray, age:int, gpa:double);" +
                "store a into 'ooo' using AvroStorage();";
        PhysicalPlan pp = Util.buildPp(pigServer, query);

        String[] expectedJars = new String[] {"avro-1", "avro-mapred-"};

        MROperPlan mrPlan = Util.buildMRPlan(pp, pigServer.getPigContext());
        assertMRPlanContains(mrPlan.getRoots().get(0), expectedJars, 2);

        TezLauncher launcher = new TezLauncher();
        TezPlanContainer tezPlanContainer = launcher.compile(pp, pigServer.getPigContext());
        assertTezPlanContains(tezPlanContainer.getRoots().get(0).getNode(), expectedJars, 2);
    }

    @Test
    public void testShipJsonLoader() throws Exception {
        String query = "a = load '1.txt' as (name:chararray, age:int, gpa:double);" +
                "b = order a by name;" +
                "store b into 'ooo' using JsonStorage();";
        PhysicalPlan pp = Util.buildPp(pigServer, query);

        String[] expectedJars = new String[] {"jackson-core-"};

        MROperPlan mrPlan = Util.buildMRPlan(pp, pigServer.getPigContext());
        assertMRPlanContains(mrPlan.getLeaves().get(0), expectedJars, 1);

        TezLauncher launcher = new TezLauncher();
        TezPlanContainer tezPlanContainer = launcher.compile(pp, pigServer.getPigContext());
        assertTezPlanContains(tezPlanContainer.getRoots().get(0).getNode(), expectedJars, 1);
    }

    private void assertMRPlanContains(MapReduceOper mro, String[] expectedFiles, int size) throws VisitorException {
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

    private void assertTezPlanContains(TezOperPlan plan, String[] expectedFiles, int size) throws VisitorException {
        TezPOUserFuncVisitor udfVisitor = new TezPOUserFuncVisitor(plan);
        udfVisitor.visit();

        List<String> shipFiles = new ArrayList<String>();
        shipFiles.addAll(udfVisitor.getShipFiles());

        Assert.assertEquals(shipFiles.size(), size);
        assertContains(shipFiles, expectedFiles);
    }

    private void assertContains(List<String> collectedFiles, String[] expectedFiles) {
        for (String expectedFile : expectedFiles) {
            boolean found = false;
            for (String collectedFile : collectedFiles) {
                if (collectedFile.contains(expectedFile)) {
                    found = true;
                    break;
                }
            }
            Assert.assertTrue(found);
        }
    }
}

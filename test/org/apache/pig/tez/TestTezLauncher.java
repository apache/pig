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

import static org.junit.Assert.assertTrue;

import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.tez.TezExecType;
import org.apache.pig.backend.hadoop.executionengine.tez.TezLauncher;
import org.apache.pig.impl.PigContext;
import org.apache.pig.test.MiniCluster;
import org.apache.pig.test.Util;
import org.apache.pig.tools.pigstats.PigStats;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test cases to test the TezLauncher.
 */
public class TestTezLauncher {
    private static PigContext pc;
    private static PigServer pigServer;
    private static MiniCluster cluster;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        cluster = MiniCluster.buildCluster();
        pc = new PigContext(new TezExecType(), cluster.getProperties());
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        cluster.shutDown();
    }

    @Before
    public void setUp() throws ExecException {
        pigServer = new PigServer(pc);
    }

    @Test
    public void testRun1() throws Exception {
        String query =
                "a = load '/tmp/input' as (x:int, y:int);" +
                "b = filter a by x > 0;" +
                "c = foreach b generate y;" +
                "store c into '/tmp/output';";

        PhysicalPlan pp = Util.buildPp(pigServer, query);
        TezLauncher launcher = new TezLauncher();
        PigStats pigStats = launcher.launchPig(pp, "testRun1", pc);
        // TODO: The assert is commented out now because the current YARN mini
        // cluster cannot run Tez jobs. Re-enable it when Tez mini cluster is
        // available.
        // assertTrue(pigStats.isSuccessful());
    }
}


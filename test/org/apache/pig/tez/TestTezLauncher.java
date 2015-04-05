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
import org.apache.pig.PigServer;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.tez.TezExecType;
import org.apache.pig.backend.hadoop.executionengine.tez.TezLauncher;
import org.apache.pig.backend.hadoop.executionengine.tez.util.MRToTezHelper;
import org.apache.pig.impl.PigContext;
import org.apache.pig.test.MiniGenericCluster;
import org.apache.pig.test.Util;
import org.apache.pig.tools.pigstats.PigStats;
import org.junit.After;
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
    private static MiniGenericCluster cluster;

    private static final String INPUT_FILE = "TestTezLauncherInput";
    private static final String[] INPUT_RECORDS = {
        "100\tapple",
        "200\torange",
        "300\tstrawberry",
        "300\tpear",
        "100\tapple",
        "300\tpear",
        "400\tapple",
    };

    private static final String OUTPUT_FILE = "TestTezLauncherOutput";
    private static final String[] OUTPUT_RECORDS = {
        "all\t{(apple),(pear),(pear),(strawberry),(orange)}"
    };

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        cluster = MiniGenericCluster.buildCluster(MiniGenericCluster.EXECTYPE_TEZ);
        pc = new PigContext(new TezExecType(), cluster.getProperties());
        Util.createInputFile(cluster, INPUT_FILE, INPUT_RECORDS);
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        cluster.shutDown();
    }

    @Before
    public void setUp() throws Exception {
        pigServer = new PigServer(pc);
    }

    @After
    public void tearDown() throws Exception {
        Util.deleteFile(cluster, OUTPUT_FILE);
    }

    @Test
    public void testRun1() throws Exception {
        String query =
                "a = load '" + INPUT_FILE + "' as (x:int, y:chararray);" +
                "b = filter a by x > 100;" +
                "c = foreach b generate y;" +
                "d = group c all;" +
                "store d into '" + OUTPUT_FILE + "';";

        PhysicalPlan pp = Util.buildPp(pigServer, query);
        TezLauncher launcher = new TezLauncher();
        PigStats pigStats = launcher.launchPig(pp, "testRun1", pc);
        assertTrue(pigStats.isSuccessful());

        String[] output = Util.readOutput(cluster.getFileSystem(), OUTPUT_FILE);
        for (int i = 0; i < output.length; i++) {
            assertEquals(OUTPUT_RECORDS[i], output[i]);
        }

        assertEquals(1, pigStats.getInputStats().size());
        assertEquals(INPUT_FILE, pigStats.getInputStats().get(0).getName());

        assertEquals(1, pigStats.getOutputStats().size());
        assertEquals(OUTPUT_FILE, pigStats.getOutputStats().get(0).getName());
    }

    @Test
    public void testQueueName() throws Exception {
        Configuration conf = new Configuration();
        conf.set("tez.queue.name", "special");
        conf = MRToTezHelper.getDAGAMConfFromMRConf(conf);
        assertEquals(conf.get("tez.queue.name"), "special");
        
    }
}


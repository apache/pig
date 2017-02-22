/**
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

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.pig.PigConfiguration;
import org.apache.pig.PigRunner;
import org.apache.pig.PigServer;
import org.apache.pig.impl.plan.OperatorPlan;
import org.apache.pig.test.Util;
import org.apache.pig.tools.pigstats.JobStats;
import org.apache.pig.tools.pigstats.OutputStats;
import org.apache.pig.tools.pigstats.PigProgressNotificationListener;
import org.apache.pig.tools.pigstats.PigStats;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test class for tez specific behaviour tests
 */
public class TestTezJobExecution {

    private static final String TEST_DIR = Util.getTestDirectory(TestTezJobExecution.class);

    private static final String INPUT_FILE = TEST_DIR + Path.SEPARATOR + "input";
    private PigServer pigServer;

    @BeforeClass
    public static void oneTimeSetUp() throws Exception {
        Util.deleteDirectory(new File(TEST_DIR));
        new File(TEST_DIR).mkdirs();
        Util.createLocalInputFile(INPUT_FILE, new String[] {
            "1", "1", "1", "2", "2", "2"
        });
    }

    @AfterClass
    public static void oneTimeTearDown() throws Exception {
        Util.deleteDirectory(new File(TEST_DIR));
    }

    @Before
    public void setUp() throws Exception {
        pigServer = new PigServer("tez_local");
    }

    @Test
    public void testUnionParallelHashValuePartition() throws IOException {
        String output = TEST_DIR + Path.SEPARATOR + "output1";
        String query = "A = LOAD '" + INPUT_FILE + "';"
                + "B = LOAD '" + INPUT_FILE + "';"
                + "C = UNION A, B PARALLEL 2;"
                + "STORE C into '" + output + "';";
        pigServer.registerQuery(query);
        String part0 = FileUtils.readFileToString(new File(output + Path.SEPARATOR + "part-v002-o000-r-00000"));
        String part1 = FileUtils.readFileToString(new File(output + Path.SEPARATOR + "part-v002-o000-r-00001"));
        assertEquals("2\n2\n2\n2\n2\n2\n", part0);
        assertEquals("1\n1\n1\n1\n1\n1\n", part1);
    }

    @Test
    public void testDAGDiscoveryDisabled() throws IOException {
        String output1 = TEST_DIR + Path.SEPARATOR + "output-parallel";
        String output2 = TEST_DIR + Path.SEPARATOR + "output-autoparallel";
        String scriptFile = TEST_DIR + Path.SEPARATOR + "testDAGRecoveryDisable.pig";
        String query = "A = LOAD '" + INPUT_FILE + "';"
                + "B = GROUP A BY $0 PARALLEL 1;"
                + "STORE B into '" + output1 + "';"
                + "exec;"
                + "C = LOAD '" + INPUT_FILE + "';"
                + "D = GROUP C BY $0;"
                + "STORE D into '" + output2 + "';";

        Util.createLocalInputFile(scriptFile, new String[] {query});

        String[] args = { "-x", "tez_local", scriptFile };

        TestNotificationListener listener = new TestNotificationListener();
        // Recovery is not disabled when there is auto parallelism. Should reuse AM application session
        PigStats stats = PigRunner.run(args, listener);
        assertTrue(stats.isSuccessful());
        assertEquals(1, listener.getJobsStarted().size());

        Util.deleteFile(pigServer.getPigContext(), output1);
        Util.deleteFile(pigServer.getPigContext(), output2);

        // Recovery is disabled when there is auto parallelism. Should use two different AMs
        listener.reset();
        args = new String[] {
                "-D" + PigConfiguration.PIG_TEZ_AUTO_PARALLELISM_DISABLE_DAG_RECOVERY + "=true",
                "-x",
                "tez_local",
                scriptFile };
        stats = PigRunner.run(args, listener);
        assertTrue(stats.isSuccessful());
        assertEquals(2, listener.getJobsStarted().size());
    }


    private static class TestNotificationListener implements PigProgressNotificationListener {

        private Set<String> jobsStarted = new HashSet<String>();

        public void reset() {
            this.jobsStarted.clear();
        }

        public Set<String> getJobsStarted() {
            return jobsStarted;
        }

        @Override
        public void initialPlanNotification(String scriptId,
                OperatorPlan<?> plan) {
        }

        @Override
        public void launchStartedNotification(String scriptId,
                int numJobsToLaunch) {
        }

        @Override
        public void jobsSubmittedNotification(String scriptId,
                int numJobsSubmitted) {
        }

        @Override
        public void jobStartedNotification(String scriptId, String assignedJobId) {
            jobsStarted.add(assignedJobId);
        }

        @Override
        public void jobFinishedNotification(String scriptId, JobStats jobStats) {
        }

        @Override
        public void jobFailedNotification(String scriptId, JobStats jobStats) {
        }

        @Override
        public void outputCompletedNotification(String scriptId,
                OutputStats outputStats) {
        }

        @Override
        public void progressUpdatedNotification(String scriptId, int progress) {

        }

        @Override
        public void launchCompletedNotification(String scriptId,
                int numJobsSucceeded) {
        }

    }

}

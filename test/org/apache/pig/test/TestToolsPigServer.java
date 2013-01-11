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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.Iterator;
import java.util.List;

import org.apache.pig.ExecType;
import org.apache.pig.backend.executionengine.ExecJob;
import org.apache.pig.backend.executionengine.ExecJob.JOB_STATUS;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.data.Tuple;
import org.apache.pig.tools.ToolsPigServer;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

public class TestToolsPigServer {
    private ToolsPigServer pig = null;
    static MiniCluster cluster = MiniCluster.buildCluster();
    private File stdOutRedirectedFile;

    @Before
    public void setUp() throws Exception{
        pig = new ToolsPigServer(ExecType.MAPREDUCE, cluster.getProperties());
        stdOutRedirectedFile = new File("stdout.redirected");
        // Create file if it does not exist
        if(!stdOutRedirectedFile.createNewFile())
            fail("Unable to create input files");
    }

    @After
    public void tearDown() throws Exception{
        pig = null;
        stdOutRedirectedFile.delete();
    }

    @AfterClass
    public static void oneTimeTearDown() throws Exception {
        cluster.shutDown();
    }

    @Test
    public void testToolsPigServer() throws Exception {
        Util.createInputFile(cluster, "input", new String[] {
                "abc\t123",
                "def\t456",
                "ghi\t789"});

        File scriptFile = Util.createFile(new String[]{
                "a = load 'input' using " + PigStorage.class.getName() + "('\t');",
                "b = foreach a generate $0;",
                "store b into '/bla';"
                });
        pig.registerNoRun(scriptFile.getAbsolutePath(), null, null);

        ToolsPigServer.PigPlans plans = pig.getPlans();

        assertNotNull(plans);
        assertNotNull(plans.lp);

        List<ExecJob> jobs = pig.runPlan(plans.lp, "testToolsPigServer");

        assertEquals(1, jobs.size());
        ExecJob job = jobs.get(0);
        assertEquals(JOB_STATUS.COMPLETED, job.getStatus());
        Iterator<Tuple> results = job.getResults();
        assertTrue(results.hasNext()); // should have one record
        Tuple t = results.next();
        assertEquals("abc", t.get(0).toString());
        assertTrue(results.hasNext());
    }

    @Test
    public void testToolsPigServerRegister() throws Exception {
        Util.createLocalInputFile("testRegisterScripts.jar",
            new String[]{"mary had a little lamb"});

        Util.createInputFile(cluster, "input1", new String[] {
                "abc:123",
                "def:456",
                "ghi:789"});

        File scriptFile = Util.createFile(new String[]{
                "register 'testRegisterScripts.jar';",
                "a = load 'input1' using " + PigStorage.class.getName() + "(':');",

                "b = foreach a generate $0;",
                "store b into '/bla1';"
                });
        pig.registerNoRun(scriptFile.getAbsolutePath(), null, null);

        ToolsPigServer.PigPlans plans = pig.getPlans();

        assertNotNull(plans);
        assertNotNull(plans.lp);

        List<ExecJob> jobs = pig.runPlan(plans.lp, "testToolsPigServer");

        assertEquals(1, jobs.size());
        ExecJob job = jobs.get(0);
        assertEquals(JOB_STATUS.COMPLETED, job.getStatus());
        Iterator<Tuple> results = job.getResults();
        assertTrue(results.hasNext()); // should have one record
        Tuple t = results.next();
        assertEquals("abc", t.get(0).toString());
        assertTrue(results.hasNext());
    }
}
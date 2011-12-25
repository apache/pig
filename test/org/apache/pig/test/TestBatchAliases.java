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

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import junit.framework.Assert;
import junit.framework.TestCase;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecJob;
import org.apache.pig.impl.io.FileLocalizer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
@RunWith(JUnit4.class)
public class TestBatchAliases extends TestCase {

    private PigServer myPig;

    @Override
    @Before
    public void setUp() throws Exception {
        System.setProperty("opt.multiquery", ""+true);
        myPig = new PigServer(ExecType.LOCAL, new Properties());
        deleteOutputFiles();
    }
    
    @Override
    @After
    public void tearDown() throws Exception {
        deleteOutputFiles();
    }

    @Test
    public void testBatchAliases() throws IOException {

        // test case: key ('group') isn't part of foreach output
        // and keys have the same type.
        try {
            myPig.setBatchOn();

            myPig.registerQuery("a = load 'test/org/apache/pig/test/data/passwd' " +
                                "using PigStorage(':') as (uname:chararray, passwd:chararray, uid:int, gid:int);");
            myPig.registerQuery("b = group a by uid;");
            myPig.registerQuery("c = group a by gid;");
            myPig.registerQuery("d = foreach b generate SUM(a.gid);");
            myPig.registerQuery("e = foreach c generate group, COUNT(a);");
            myPig.registerQuery("store d into '/tmp/output1';");
            myPig.registerQuery("store e into '/tmp/output2';");

            List<ExecJob> jobs = myPig.executeBatch();
            boolean foundD=false;
            boolean foundE=false;
            for (ExecJob job : jobs) {
                assertTrue(job.getStatus() == ExecJob.JOB_STATUS.COMPLETED);
                foundD = foundD || "d".equals(job.getAlias());
                foundE = foundE || "e".equals(job.getAlias());
            }
            assertTrue(foundD);
            assertTrue(foundE);

        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    private void deleteOutputFiles() {
        try {
            FileLocalizer.delete("/tmp/output1", myPig.getPigContext());
            FileLocalizer.delete("/tmp/output2", myPig.getPigContext());
            FileLocalizer.delete("/tmp/output3", myPig.getPigContext());
            FileLocalizer.delete("/tmp/output4", myPig.getPigContext());
            FileLocalizer.delete("/tmp/output5", myPig.getPigContext());
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }
}

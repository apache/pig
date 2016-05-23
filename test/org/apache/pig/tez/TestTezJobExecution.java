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

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.pig.PigServer;
import org.apache.pig.builtin.RoundRobinPartitioner;
import org.apache.pig.test.Util;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test class for tez specific behaviour tests
 */
public class TestTezJobExecution {

    private static final String TEST_DIR = Util.getTestDirectory(TestTezJobExecution.class);

    private PigServer pigServer;

    @BeforeClass
    public static void oneTimeSetUp() throws Exception {
        Util.deleteDirectory(new File(TEST_DIR));
        new File(TEST_DIR).mkdirs();
    }

    @AfterClass
    public static void oneTimeTearDown() throws Exception {
        Util.deleteDirectory(new File(TEST_DIR));
    }

    @Before
    public void setUp() throws Exception {
        pigServer = new PigServer(Util.getLocalTestMode());
    }

    @Test
    public void testUnionParallelRoundRobinBatchSize() throws IOException {
        String input = TEST_DIR + Path.SEPARATOR + "input1";
        String output = TEST_DIR + Path.SEPARATOR + "output1";
        Util.createInputFile(pigServer.getPigContext(), input, new String[] {
            "1", "1", "1", "2", "2", "2"
        });
        String query = "A = LOAD '" + input + "';"
                + "B = LOAD '" + input + "';"
                + "C = UNION A, B PARALLEL 2;"
                + "STORE C into '" + output + "';";
        pigServer.getPigContext().getProperties().setProperty(
                RoundRobinPartitioner.PIG_ROUND_ROBIN_PARTITIONER_BATCH_SIZE, "3");
        pigServer.registerQuery(query);
        String part0 = FileUtils.readFileToString(new File(output + Path.SEPARATOR + "part-v002-o000-r-00000"));
        String part1 = FileUtils.readFileToString(new File(output + Path.SEPARATOR + "part-v002-o000-r-00001"));
        assertEquals("1\n1\n1\n1\n1\n1\n", part0);
        assertEquals("2\n2\n2\n2\n2\n2\n", part1);
    }

}

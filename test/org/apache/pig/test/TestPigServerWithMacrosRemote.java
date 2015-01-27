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

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.Iterator;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.pig.PigServer;
import org.apache.pig.data.Tuple;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

public class TestPigServerWithMacrosRemote {
    private static MiniGenericCluster cluster = MiniGenericCluster.buildCluster();

    @Before
    public void setup() throws Exception {
        Util.resetStateForExecModeSwitch();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        cluster.shutDown();
    }

    @Test
    public void testRegisterRemoteMacro() throws Throwable {
        PigServer pig = new PigServer(cluster.getExecType(), cluster.getProperties());

        String macroName = "util.pig";
        File macroFile = File.createTempFile("tmp", "");
        PrintWriter pw = new PrintWriter(new FileWriter(macroFile));
        pw.println("DEFINE row_count(X) RETURNS Z { Y = group $X all; $Z = foreach Y generate COUNT($X); };");
        pw.close();

        FileSystem fs = cluster.getFileSystem();
        fs.copyFromLocalFile(new Path(macroFile.getAbsolutePath()), new Path(macroName));

        // find the absolute path for the directory so that it does not
        // depend on configuration
        String absPath = fs.getFileStatus(new Path(macroName)).getPath().toString();

        Util.createInputFile(cluster, "testRegisterRemoteMacro_input", new String[]{"1", "2"});

        pig.registerQuery("import '" + absPath + "';");
        pig.registerQuery("a = load 'testRegisterRemoteMacro_input';");
        pig.registerQuery("b = row_count(a);");
        Iterator<Tuple> iter = pig.openIterator("b");

        assertEquals(2L, ((Long)iter.next().get(0)).longValue());

        pig.shutdown();
    }
}

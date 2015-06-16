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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import org.apache.pig.PigServer;
import org.apache.pig.data.Tuple;
import org.junit.AfterClass;
import org.junit.Test;

public class TestScalarAliases  {
    static MiniGenericCluster cluster = MiniGenericCluster.buildCluster();
    private PigServer pigServer;

    @AfterClass
    public static void oneTimeTearDown() throws Exception {
        cluster.shutDown();
    }

    public static void deleteDirectory(File file) {
        if (file.exists()) {
            Util.deleteDirectory(file);
        }
    }

    public static File createLocalInputFile(String filename, String[] inputData)
            throws IOException {
        new File(filename).getParentFile().mkdirs();
        return Util.createLocalInputFile(filename, inputData);
    }

    // See PIG-1434
    @Test
    public void testScalarAliasesSplitClause() throws Exception{
        pigServer = new PigServer(cluster.getExecType(), cluster.getProperties());
        String[] input = {
                "1\t5",
                "2\t10",
                "3\t20"
        };

        // Test the use of scalars in expressions
        String inputPath = "table_testScalarAliasesSplitClause";
        String output = "table_testScalarAliasesSplitClauseDir";
        Util.createInputFile(cluster, inputPath, input);
        // Test in script mode
        pigServer.setBatchOn();
        pigServer.registerQuery("A = LOAD '"+inputPath+"' as (a0: long, a1: double);");
        pigServer.registerQuery("B = group A all;");
        pigServer.registerQuery("C = foreach B generate COUNT(A) as count;");
        pigServer.registerQuery("split A into Y if (2 * C.count) < a1, X if a1 == 5;");
        pigServer.registerQuery("Store Y into '"+output+"';");
        pigServer.executeBatch();
        // Check output
        pigServer.registerQuery("Z = LOAD '"+output+"' as (a0: int, a1: double);");

        Iterator<Tuple> iter = pigServer.openIterator("Z");

        // Y gets only last 2 elements
        Tuple t = iter.next();
        assertTrue(t.toString().equals("(2,10.0)"));

        t = iter.next();
        assertTrue(t.toString().equals("(3,20.0)"));

        assertFalse(iter.hasNext());
        Util.deleteFile(cluster, output);

    }

    @Test
    public void testScalarErrMultipleRowsInInput() throws Exception{
        Util.resetStateForExecModeSwitch();
        pigServer = new PigServer(cluster.getExecType(), cluster.getProperties());
        String[] input = {
                "1\t5",
                "2\t10",
                "3\t20"
        };
        String INPUT_FILE = "table_testScalarAliasesMulRows";
        Util.createInputFile(cluster, INPUT_FILE, input);
        pigServer.registerQuery("A = LOAD '" + INPUT_FILE +  "' as (a0: long, a1: double);");
        pigServer.registerQuery("B = LOAD '" + INPUT_FILE +  "' as (b0: long, b1: double);");
        pigServer.registerQuery("C = foreach A generate $0, B.$0;");
        try {
            pigServer.openIterator("C");
            fail("exception expected - scalar input has multiple rows");
        } catch (IOException pe){
            Util.checkStrContainsSubStr(pe.getCause().getMessage(),
                    "Scalar has more than one row in the output"
            );
        }
    }
}

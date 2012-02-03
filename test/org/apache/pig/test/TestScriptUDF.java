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

import java.io.File;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

import junit.framework.Assert;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.io.FileLocalizer;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

public class TestScriptUDF{
    static MiniCluster cluster = MiniCluster.buildCluster();
    private PigServer pigServer;

    TupleFactory mTf = TupleFactory.getInstance();
    BagFactory mBf = BagFactory.getInstance();
    
    @Before
    public void setUp() throws Exception{
        FileLocalizer.setR(new Random());
        pigServer = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
    }
    
    @AfterClass
    public static void oneTimeTearDown() throws Exception {
        cluster.shutDown();
    }
    
    // See PIG-928
    @Test
    public void testJavascriptExampleScript() throws Exception{
        String[] script = {
                "helloworld.outputSchema = \"word:chararray\";",
                "function helloworld() {",
                "return 'Hello, World';",
                "}",
                "complex.outputSchema = \"word:chararray,num:long\";",
                "function complex(word) {",
                "return {word:word, num:word.length};",
                "}",
        };
        String[] input = {
                "one\t1",
                "two\t2",
                "three\t3"
        };

        Util.createInputFile(cluster, "table_testJavascriptExampleScript", input);
        Util.createLocalInputFile( "testJavascriptExampleScript.js", script);

        // Test the namespace
        pigServer.registerCode("testJavascriptExampleScript.js", "javascript", "myfuncs");
        pigServer.registerQuery("A = LOAD 'table_testJavascriptExampleScript' as (a0:chararray, a1:long);");
        pigServer.registerQuery("B = foreach A generate myfuncs.helloworld(), myfuncs.complex($0);");

        Iterator<Tuple> iter = pigServer.openIterator("B");
        Assert.assertTrue(iter.hasNext());
        Tuple t = iter.next();

        Assert.assertEquals(((Tuple)t.get(1)).get(1), 3);

        Assert.assertTrue(iter.hasNext());
        t = iter.next();

        Assert.assertEquals(((Tuple)t.get(1)).get(1), 3);

        Assert.assertTrue(iter.hasNext());
        t = iter.next();

        Assert.assertEquals(((Tuple)t.get(1)).get(1), 5);

    }

    // See Pig-1653 -- left here because we can't force absolute paths in e2e harness
    @Test
    public void testPythonAbsolutePath() throws Exception{
        String[] script = {
                "#!/usr/bin/python",
                "@outputSchema(\"x:{t:(num:long)}\")",
                "def square(number):" ,
                "\treturn (number * number)"
        };
        String[] input = {
                "1\t3",
                "2\t4",
                "3\t5"
        };

        Util.createInputFile(cluster, "table_testPythonAbsolutePath", input);
        File scriptFile = Util.createLocalInputFile( "testPythonAbsolutePath.py", script);

        // Test the namespace
        pigServer.registerCode(scriptFile.getAbsolutePath(), "jython", "pig");
        pigServer.registerQuery("A = LOAD 'table_testPythonAbsolutePath' as (a0:long, a1:long);");
        pigServer.registerQuery("B = foreach A generate pig.square(a0);");

        Iterator<Tuple> iter = pigServer.openIterator("B");
        Assert.assertTrue(iter.hasNext());
        Tuple t = iter.next();

        Assert.assertTrue(t.toString().equals("(1)"));

        Assert.assertTrue(iter.hasNext());
        t = iter.next();

        Assert.assertTrue(t.toString().equals("(4)"));

        Assert.assertTrue(iter.hasNext());
        t = iter.next();

        Assert.assertTrue(t.toString().equals("(9)"));
        
        Assert.assertFalse(iter.hasNext());
    }

    /** See Pig-1824
     * test importing a second module/file from the local fs from within
     * the first module.
     *
     * NOTE: this unit test also covers the "import re" test case.
     * not all users have a jython install, so there is no explicit unit test
     * for "import re".
     * to use a jython install, the Lib dir must be in the jython search path
     * via env variable JYTHON_HOME=jy_home or JYTHON_PATH=jy_home/Lib:...
     * 
     * Left in for now as we don't have paths to include other scripts in a
     * script in the e2e harness.
     *
     * @throws Exception
     */
    @Test
    public void testPythonNestedImport() throws Exception {
        // Skip for hadoop 23 until PIG-2433 fixed
        if (Util.isHadoop23())
            return;
        
        String[] scriptA = {
                "#!/usr/bin/python",
                "def square(number):" ,
                " return (number * number)"
        };
        String[] scriptB = {
                "#!/usr/bin/python",
                "import scriptA",
                "@outputSchema(\"x:{t:(num:double)}\")",
                "def sqrt(number):" ,
                " return (number ** .5)",
                "@outputSchema(\"x:{t:(num:long)}\")",
                "def square(number):" ,
                " return long(scriptA.square(number))"
        };
        String[] input = {
                "1\t3",
                "2\t4",
                "3\t5"
        };

        Util.createInputFile(cluster, "table_testPythonNestedImport", input);
        Util.createLocalInputFile("scriptA.py", scriptA);
        File scriptFileB = Util.createLocalInputFile("scriptB.py", scriptB);

        // Test the namespace: import B, which, in turn, imports A
        pigServer.registerCode(scriptFileB.getAbsolutePath(), "jython", "pig");
        pigServer.registerQuery("A = LOAD 'table_testPythonNestedImport' as (a0:long, a1:long);");
        pigServer.registerQuery("B = foreach A generate pig.square(a0);");

        Iterator<Tuple> iter = pigServer.openIterator("B");
        Assert.assertTrue(iter.hasNext());
        Tuple t = iter.next();

        Assert.assertTrue(t.toString().equals("(1)"));

        Assert.assertTrue(iter.hasNext());
        t = iter.next();

        Assert.assertTrue(t.toString().equals("(4)"));

        Assert.assertTrue(iter.hasNext());
        t = iter.next();

        Assert.assertTrue(t.toString().equals("(9)"));
        
        Assert.assertFalse(iter.hasNext());
    }
}

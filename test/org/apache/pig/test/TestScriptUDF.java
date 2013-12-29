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

import junit.framework.Assert;

import org.apache.hadoop.util.Shell;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
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
                "complex.outputSchema = \"(word:chararray,num:long)\";",
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

    /** See Pig-1824, PIG-2433
     * test importing a second module/file from the local fs from within
     * the first module.
     *
     * to use a jython install, the Lib dir must be in the jython search path
     * via env variable JYTHON_HOME=jy_home or JYTHONPATH=jy_home/Lib:... or
     * jython-standalone.jar should be in the classpath
     * 
     * Left in for now as we don't have paths to include other scripts in a
     * script in the e2e harness.
     *
     * @throws Exception
     */
    @Test
    public void testPythonNestedImportCwdInClassPath() throws Exception {
        testPythonNestedImport(".", "scriptA.py", "scriptB.py");
    }

    @Test
    public void testPythonNestedImportClassPath() throws Exception {
        // Use different names for the script as PythonInterpreter is static in JythonScriptEngine 
        testPythonNestedImport("build/classes", "scriptC.py", "scriptD.py");
    }

    public void testPythonNestedImport(String importScriptLocation, String script1Name,
            String script2Name) throws Exception {
        String[] script1 = {
                "#!/usr/bin/python",
                "def square(number):" ,
                " return (number * number)"
        };
        String script1ModuleName = script1Name.replace(".py", "");
        String[] script2 = {
                "#!/usr/bin/python",
                "import " + script1ModuleName,
                "@outputSchema(\"x:{t:(num:double)}\")",
                "def sqrt(number):" ,
                " return (number ** .5)",
                "@outputSchema(\"x:{t:(num:long)}\")",
                "def square(number):" ,
                " return long(" + script1ModuleName + ".square(number))"
        };
        String[] input = {
                "1\t3",
                "2\t4",
                "3\t5"
        };
        Util.deleteFile(cluster, "table_testPythonNestedImport");
        Util.createInputFile(cluster, "table_testPythonNestedImport", input);
        Util.createLocalInputFile(importScriptLocation + "/" + script1Name, script1);
        String script2FilePath = Util.createLocalInputFile(script2Name, script2).getAbsolutePath();

        // Test the namespace: import file2, which, in turn, imports file1
        pigServer.registerCode(script2FilePath, "jython", "pig");
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

    @Test
    public void testPythonBuiltinModuleImport1() throws Exception {
        String[] script = {
                "#!/usr/bin/python",
                "import os",
                "@outputSchema(\"env:chararray\")",
                "def getEnv(envkey):" ,
                " return os.getenv(envkey);"
        };
        String userenv = Shell.WINDOWS?"USERNAME":"USER";
        String[] input = {
                userenv,
                "PATH"
        };

        Util.createInputFile(cluster, "testPythonBuiltinModuleImport1", input);
        File scriptFile = Util.createLocalInputFile("importos.py", script);

        pigServer.registerCode(scriptFile.getAbsolutePath(), "jython", "pig");
        pigServer.registerQuery("A = LOAD 'testPythonBuiltinModuleImport1' as (a0:chararray);");
        pigServer.registerQuery("B = foreach A generate pig.getEnv(a0);");

        Iterator<Tuple> iter = pigServer.openIterator("B");
        Assert.assertTrue(iter.hasNext());
        Tuple t = iter.next();
        Assert.assertTrue(t.get(0).toString().equals(System.getenv(input[0])));
        Assert.assertTrue(iter.hasNext());
        t = iter.next();
        Assert.assertTrue(t.get(0).toString().equals(System.getenv(input[1])));
        Assert.assertFalse(iter.hasNext());
    }

    @Test
    public void testPythonBuiltinModuleImport2() throws Exception {
        String[] script = {
                "#!/usr/bin/python",
                "import re",
                "@outputSchema(\"word:chararray\")",
                "def resplit(content,regex,index):" ,
                " return re.compile(regex).split(content)[index];"
        };
        String[] input = {
                "Hello world",
                "The quick brown fox jumps over the lazy dog"
        };

        Util.createInputFile(cluster, "testPythonBuiltinModuleImport2", input);
        File scriptFile = Util.createLocalInputFile("importre.py", script);

        String[] pigScript = {
                "register '" + scriptFile.getAbsolutePath() + "' using jython as pig;",
                "A = LOAD 'testPythonBuiltinModuleImport2' as (a0:chararray);",
                "B = foreach A generate pig.resplit(a0, '\\\\s+', 0);"
        };

        String pigScriptFile = Util.createLocalInputFile("importre.pig", pigScript).getPath();
        pigServer.registerScript(pigScriptFile);

        Iterator<Tuple> iter = pigServer.openIterator("B");
        Assert.assertTrue(iter.hasNext());
        Tuple t = iter.next();
        Assert.assertTrue(t.get(0).toString().equals(input[0].split("\\s+")[0]));
        Assert.assertTrue(iter.hasNext());
        t = iter.next();
        Assert.assertTrue(t.get(0).toString().equals(input[1].split("\\s+")[0]));
        Assert.assertFalse(iter.hasNext());
    }
}

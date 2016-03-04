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

import static org.apache.pig.builtin.mock.Storage.resetData;
import static org.apache.pig.builtin.mock.Storage.tuple;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.InvalidInputException;
import org.apache.pig.PigServer;
import org.apache.pig.ResourceSchema;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.builtin.mock.Storage;
import org.apache.pig.builtin.mock.Storage.Data;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.util.PropertiesUtil;
import org.apache.pig.scripting.ScriptEngine;
import org.apache.pig.tools.grunt.Grunt;
import org.apache.pig.tools.grunt.GruntParser;
import org.apache.pig.tools.pigstats.PigStats;
import org.junit.Before;
import org.junit.Test;

import com.google.common.io.Files;

public class TestPigServerLocal {
    private File tempDir;

    @Before
    public void setUp() throws Exception{
        tempDir = Files.createTempDir();
        tempDir.deleteOnExit();
        registerNewResource(tempDir.getAbsolutePath());
    }

    // dynamically add more resources to the system class loader
    private static void registerNewResource(String file) throws Exception {
        URL urlToAdd = new File(file).toURI().toURL();
        URLClassLoader sysLoader = (URLClassLoader) ClassLoader.getSystemClassLoader();
        Method addMethod = URLClassLoader.class.
                getDeclaredMethod("addURL", new Class[]{URL.class});
        addMethod.setAccessible(true);
        addMethod.invoke(sysLoader, new Object[]{urlToAdd});
    }

    @Test
    public void testParamSubstitution() throws Exception{
        // using params map
        PigServer pig=new PigServer(Util.getLocalTestMode());
        Map<String,String> params=new HashMap<String, String>();
        params.put("input", "test/org/apache/pig/test/data/passwd");
        File scriptFile=Util.createFile(new String[]{"a = load '$input' using PigStorage(':');"});
        pig.registerScript(scriptFile.getAbsolutePath(),params);
        Iterator<Tuple> iter=pig.openIterator("a");
        int index=0;
        List<Tuple> expectedTuples=Util.readFile2TupleList("test/org/apache/pig/test/data/passwd", ":");
        while(iter.hasNext()){
            Tuple tuple=iter.next();
            assertEquals(tuple.get(0).toString(), expectedTuples.get(index).get(0).toString());
            index++;
        }

        // using param file
        pig=new PigServer(Util.getLocalTestMode());
        List<String> paramFile=new ArrayList<String>();
        paramFile.add(Util.createFile(new String[]{"input=test/org/apache/pig/test/data/passwd2"}).getAbsolutePath());
        pig.registerScript(scriptFile.getAbsolutePath(),paramFile);
        iter=pig.openIterator("a");
        index=0;
        expectedTuples=Util.readFile2TupleList("test/org/apache/pig/test/data/passwd2", ":");
        while(iter.hasNext()){
            Tuple tuple=iter.next();
            assertEquals(tuple.get(0).toString(), expectedTuples.get(index).get(0).toString());
            index++;
        }

        // using both param value and param file, param value should override param file
        pig=new PigServer(Util.getLocalTestMode());
        pig.registerScript(scriptFile.getAbsolutePath(),params,paramFile);
        iter=pig.openIterator("a");
        index=0;
        expectedTuples=Util.readFile2TupleList("test/org/apache/pig/test/data/passwd", ":");
        while(iter.hasNext()){
            Tuple tuple=iter.next();
            assertEquals(tuple.get(0).toString(), expectedTuples.get(index).get(0).toString());
            index++;
        }
    }

    // PIG-3469
    @Test
    public void testNonExistingSecondDirectoryInSkewJoin() throws Exception {
        String script =
          "exists = LOAD 'test/org/apache/pig/test/data/InputFiles/jsTst1.txt' AS (x:chararray, a:long);" +
          "missing = LOAD '/non/existing/directory' AS (a:long);" +
          "missing = FOREACH ( GROUP missing BY a ) GENERATE $0 AS a, COUNT_STAR($1);" +
          "joined = JOIN exists BY a, missing BY a USING 'skewed';" +
          "STORE joined INTO '/tmp/test_out.tsv';";

        PigServer pig = new PigServer(Util.getLocalTestMode());
        if (Util.getLocalTestMode().toString().startsWith("TEZ")) {
            try {
                pig.registerScript(new ByteArrayInputStream(script.getBytes("UTF-8")));
                fail("Expect front exception");
            } catch(Exception ex) {
                Throwable excp = ex;
                assertTrue(excp instanceof FrontendException);
                excp = excp.getCause();
                assertTrue(excp instanceof VisitorException);
                excp = excp.getCause();
                assertTrue(excp instanceof ExecException);
                excp = excp.getCause();
                assertTrue(excp instanceof InvalidInputException);
            }
        } else {
            // Execution of the script should fail, but without throwing any exceptions (such as NPE)
            try {
                pig.registerScript(new ByteArrayInputStream(script.getBytes("UTF-8")));
            } catch(Exception ex) {
                fail("Unexpected exception: " + ex);
            }
        }
    }

    // build the pig script from in-memory, and wrap it as ByteArrayInputStream
    @Test
    public void testRegisterScriptFromStream() throws Exception{
        // using params map
        PigServer pig=new PigServer(Util.getLocalTestMode());
        Map<String,String> params=new HashMap<String, String>();
        params.put("input", "test/org/apache/pig/test/data/passwd");
        String script="a = load '$input' using PigStorage(':');";
        pig.registerScript(new ByteArrayInputStream(script.getBytes("UTF-8")),params);
        Iterator<Tuple> iter=pig.openIterator("a");
        int index=0;
        List<Tuple> expectedTuples=Util.readFile2TupleList("test/org/apache/pig/test/data/passwd", ":");
        while(iter.hasNext()){
            Tuple tuple=iter.next();
            assertEquals(tuple.get(0).toString(), expectedTuples.get(index).get(0).toString());
            index++;
        }

        // using param file
        pig=new PigServer(Util.getLocalTestMode());
        List<String> paramFile=new ArrayList<String>();
        paramFile.add(Util.createFile(new String[]{"input=test/org/apache/pig/test/data/passwd2"}).getAbsolutePath());
        pig.registerScript(new ByteArrayInputStream(script.getBytes("UTF-8")),paramFile);
        iter=pig.openIterator("a");
        index=0;
        expectedTuples=Util.readFile2TupleList("test/org/apache/pig/test/data/passwd2", ":");
        while(iter.hasNext()){
            Tuple tuple=iter.next();
            assertEquals(tuple.get(0).toString(), expectedTuples.get(index).get(0).toString());
            index++;
        }

        // using both param value and param file, param value should override param file
        pig=new PigServer(Util.getLocalTestMode());
        pig.registerScript(new ByteArrayInputStream(script.getBytes("UTF-8")),params,paramFile);
        iter=pig.openIterator("a");
        index=0;
        expectedTuples=Util.readFile2TupleList("test/org/apache/pig/test/data/passwd", ":");
        while(iter.hasNext()){
            Tuple tuple=iter.next();
            assertEquals(tuple.get(0).toString(), expectedTuples.get(index).get(0).toString());
            index++;
        }
    }

    @Test
    public void testSecondarySort() throws Exception {
        PigServer pigServer = new PigServer(Util.getLocalTestMode());
        Data data = resetData(pigServer);

        data.set("foo",
            tuple("a", 1, "b"),
            tuple("b", 2, "c"),
            tuple("c", 3, "d")
            );

        pigServer.registerQuery("A = LOAD 'foo' USING mock.Storage() AS (f1:chararray,f2:int,f3:chararray);");
        pigServer.registerQuery("B = order A by f1,f2,f3 DESC;");
        pigServer.registerQuery("STORE B INTO 'bar' USING mock.Storage();");

        List<Tuple> out = data.get("bar");
        assertEquals(tuple("a", 1, "b"), out.get(0));
        assertEquals(tuple("b", 2, "c"), out.get(1));
        assertEquals(tuple("c", 3, "d"), out.get(2));
    }

    @Test(expected = RuntimeException.class)
    public void testLocationStrictCheck() throws Exception {
        Properties properties = PropertiesUtil.loadDefaultProperties();
        properties.setProperty("pig.location.check.strict", "true");
        PigServer pigServer = new PigServer(Util.getLocalTestMode(), properties);
        Data data = resetData(pigServer);

        data.set("foo",
                tuple("a", 1, "b"),
                tuple("b", 2, "c"),
                tuple("c", 3, "d"));

        pigServer.registerQuery("A = LOAD 'foo' USING mock.Storage() AS (f1:chararray,f2:int,f3:chararray);");
        pigServer.registerQuery("B = order A by f1,f2,f3 DESC;");
        pigServer.registerQuery("C = order A by f1,f2,f3;");
        // Storing to same location 'bar' should throw a RuntimeException
        pigServer.registerQuery("STORE B INTO 'bar' USING mock.Storage();");
        pigServer.registerQuery("STORE C INTO 'bar' USING mock.Storage();");

        List<Tuple> out = data.get("bar");
        assertEquals(tuple("a", 1, "b"), out.get(0));
        assertEquals(tuple("b", 2, "c"), out.get(1));
        assertEquals(tuple("c", 3, "d"), out.get(2));
    }

    @Test
    public void testSkipParseInRegisterForBatch() throws Throwable {
        if (Util.getLocalTestMode().toString().startsWith("TEZ")) {
            _testSkipParseInRegisterForBatch(false, 8, 4);
            _testSkipParseInRegisterForBatch(true, 5, 1);
            _testParseBatchWithScripting(5, 1);
        } else if (Util.getLocalTestMode().toString().startsWith("SPARK")) {
            // 6 = 4 (Once per registerQuery) + 2 (SortConverter , PigRecordReader)
            // 4 (Once per registerQuery)
            _testSkipParseInRegisterForBatch(false, 6, 4);

            // 3 = 1 (registerQuery) + 2 (SortConverter, PigRecordReader)
            // 1 (registerQuery)
            _testSkipParseInRegisterForBatch(true, 3, 1);
        } else {
            // numTimesInitiated = 10. 4 (once per registerQuery) + 6 (launchPlan->RandomSampleLoader,
            // InputSizeReducerEstimator, getSplits->RandomSampleLoader,
            // createRecordReader->RandomSampleLoader, getSplits, createRecordReader)
            // numTimesSchemaCalled = 4 (once per registerQuery)
            _testSkipParseInRegisterForBatch(false, 10, 4);
            // numTimesInitiated = 7 (parseAndBuild, launchPlan->RandomSampleLoader,
            // InputSizeReducerEstimator, getSplits->RandomSampleLoader,
            // createRecordReader->RandomSampleLoader, getSplits, createRecordReader)
            // numTimesSchemaCalled = 1 (parseAndBuild)
            _testSkipParseInRegisterForBatch(true, 7, 1);
            _testParseBatchWithScripting(7, 1);
        }
    }

    @Test
    // See PIG-3967
    public void testGruntValidation() throws Exception {
        PigServer pigServer = new PigServer(Util.getLocalTestMode());
        Data data = resetData(pigServer);

        data.set("foo",
                tuple("a", 1, "b"),
                tuple("b", 2, "c"),
                tuple("c", 3, "d"));

        pigServer.setValidateEachStatement(true);
        pigServer.registerQuery("A = LOAD 'foo' USING mock.Storage() AS (f1:chararray,f2:int,f3:chararray);");
        pigServer.registerQuery("store A into '" + Util.generateURI(tempDir.toString(), pigServer.getPigContext()) + "/testGruntValidation1';");
        pigServer.registerQuery("B = LOAD 'foo' USING mock.Storage() AS (f1:chararray,f2:int,f3:chararray);");
        pigServer.registerQuery("store B into '" + Util.generateURI(tempDir.toString(), pigServer.getPigContext()) + "/testGruntValidation2';"); // This should pass
        boolean validationExceptionCaptured = false;
        try {
            // This should fail due to output validation
            pigServer.registerQuery("store A into '" + Util.generateURI(tempDir.toString(),pigServer.getPigContext()) + "/testGruntValidation1';");
        } catch (FrontendException e) {
            validationExceptionCaptured = true;
        }

        assertTrue(validationExceptionCaptured);
    }

    private void _testSkipParseInRegisterForBatch(boolean skipParseInRegisterForBatch,
            int numTimesInitiated, int numTimesSchemaCalled) throws Throwable {
        MockTrackingStorage.numTimesInitiated = 0;
        MockTrackingStorage.numTimesSchemaCalled = 0;
        String query = "A = LOAD 'foo' USING " + MockTrackingStorage.class.getName() + "();\n" +
                "B = order A by $0,$1,$2;\n" +
                "C = LIMIT B 2;\n" +
                "STORE C INTO 'bar' USING mock.Storage();\n";
        BufferedReader in = new BufferedReader(new StringReader(query));
        Properties properties = new Properties();
        properties.setProperty("io.sort.mb", "2");
        PigContext pigContext = new PigContext(Util.getLocalTestMode(), properties);
        Data data;
        if (skipParseInRegisterForBatch) {
            data = resetData(pigContext);
            data.set("foo", tuple("a", 1, "b"), tuple("b", 2, "c"), tuple("c", 3, "d"));
            Grunt grunt = new Grunt(in, pigContext);
            grunt.exec(); // Calls grunt.parseStopOnError(); which executes as batch
        }
        else {
            PigServer pigServer = new PigServer(pigContext);
            data = resetData(pigServer);
            data.set("foo", tuple("a", 1, "b"), tuple("b", 2, "c"), tuple("c", 3, "d"));
            GruntParser grunt = new GruntParser(in, pigServer);
            grunt.setInteractive(false);
            grunt.parseStopOnError(true); //not batch
        }

        assertEquals(numTimesInitiated, MockTrackingStorage.numTimesInitiated);
        assertEquals(numTimesSchemaCalled, MockTrackingStorage.numTimesSchemaCalled);
        List<Tuple> out = data.get("bar");
        assertEquals(2, out.size());
        assertEquals(tuple("a", 1, "b"), out.get(0));
        assertEquals(tuple("b", 2, "c"), out.get(1));
    }

    private void _testParseBatchWithScripting(int numTimesInitiated, int numTimesSchemaCalled) throws Throwable {
        MockTrackingStorage.numTimesInitiated = 0;
        MockTrackingStorage.numTimesSchemaCalled = 0;

        String[] script = {
                "#!/usr/bin/python",
                "from org.apache.pig.scripting import *",
                "P = Pig.compile(\"\"\"" +
                        "A = load 'foo' USING org.apache.pig.test.TestPigServerLocal\\$MockTrackingStorage();" +
                        "B = order A by $0,$1,$2;" +
                        "C = LIMIT B 2;" +
                        "store C into 'bar' USING mock.Storage();" +
                        "\"\"\")",
                "Q = P.bind()",
                "stats = Q.runSingle()",
                "if stats.isSuccessful():",
                "\tprint 'success!'",
                "else:",
                "\traise 'failed'"
        };

        Properties properties = new Properties();
        properties.setProperty("io.sort.mb", "2");
        PigContext pigContext = new PigContext(Util.getLocalTestMode(), properties);
        PigServer pigServer = new PigServer(pigContext);
        Data data = resetData(pigContext);
        data.set("foo", tuple("a", 1, "b"), tuple("b", 2, "c"), tuple("c", 3, "d"));

        String scriptFile = tempDir + File.separator + "_testParseBatchWithScripting.py";
        Util.createLocalInputFile(scriptFile , script);
        ScriptEngine scriptEngine = ScriptEngine.getInstance("jython");
        Map<String, List<PigStats>> statsMap = scriptEngine.run(pigServer.getPigContext(), scriptFile);

        for (List<PigStats> stats : statsMap.values()) {
            for (PigStats s : stats) {
                assertTrue(s.isSuccessful());
            }
        }

        assertEquals(numTimesInitiated, MockTrackingStorage.numTimesInitiated);
        assertEquals(numTimesSchemaCalled, MockTrackingStorage.numTimesSchemaCalled);
        List<Tuple> out = data.get("bar");
        assertEquals(2, out.size());
        assertEquals(tuple("a", 1, "b"), out.get(0));
        assertEquals(tuple("b", 2, "c"), out.get(1));
    }

    public static class MockTrackingStorage extends Storage {

        public static int numTimesInitiated = 0;
        public static int numTimesSchemaCalled = 0;

        public MockTrackingStorage() {
            super();
            numTimesInitiated++;
        }

        @Override
        public ResourceSchema getSchema(String location, Job job) throws IOException {
            numTimesSchemaCalled++;
            return super.getSchema(location, job);
        }
    }
}

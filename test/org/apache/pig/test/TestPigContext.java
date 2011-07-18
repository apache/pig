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


import static org.junit.Assert.*;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import junit.framework.TestCase;

import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileLocalizer;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

public class TestPigContext {

    private static final String TMP_DIR_PROP = "/tmp/hadoop-hadoop";
    private static final String FS_NAME = "file:///";
    private static final String JOB_TRACKER = "local";

    private File input;
    private PigContext pigContext;
    static MiniCluster cluster = null;
    
    @BeforeClass 
    public static void oneTimeSetup(){
        cluster = MiniCluster.buildCluster();
    }
    
    @Before
    public void setUp() throws Exception {
        pigContext = new PigContext(ExecType.LOCAL, getProperties());
        input = File.createTempFile("PigContextTest-", ".txt");
    }
    
    /**
     * Passing an already configured pigContext in PigServer constructor. 
     */
    @Test
    public void testSetProperties_way_num01() throws Exception {
        PigServer pigServer = new PigServer(pigContext);
        registerAndStore(pigServer);
        
        check_asserts(pigServer);
    }

    /**
     * Setting properties through PigServer constructor directly. 
     */
    @Test
    public void testSetProperties_way_num02() throws Exception {
        PigServer pigServer = new PigServer(ExecType.LOCAL, getProperties());
        registerAndStore(pigServer);
        
        check_asserts(pigServer);
    }

    /**
     * using connect() method. 
     */
    @Test
    public void testSetProperties_way_num03() throws Exception {
        pigContext.connect();
        PigServer pigServer = new PigServer(pigContext);
        registerAndStore(pigServer);
        
        check_asserts(pigServer);
    }
    
    @Test
    public void testHadoopExceptionCreation() throws Exception {
    	Object object = PigContext.instantiateFuncFromSpec("org.apache.hadoop.mapred.FileAlreadyExistsException");
    	assertTrue(object instanceof FileAlreadyExistsException);
    }
    
    @Test
    // See PIG-832
    public void testImportList() throws Exception {
        
        String FILE_SEPARATOR = System.getProperty("file.separator");
        File tmpDir = File.createTempFile("test", "");
        tmpDir.delete();
        tmpDir.mkdir();
        
        File udf1Dir = new File(tmpDir.getAbsolutePath()+FILE_SEPARATOR+"com"+FILE_SEPARATOR+"xxx"+FILE_SEPARATOR+"udf1");
        udf1Dir.mkdirs();
        File udf2Dir = new File(tmpDir.getAbsolutePath()+FILE_SEPARATOR+"com"+FILE_SEPARATOR+"xxx"+FILE_SEPARATOR+"udf2");
        udf2Dir.mkdirs();
        File udf1JavaSrc = new File(udf1Dir.getAbsolutePath()+FILE_SEPARATOR+"TestUDF1.java");
        File udf2JavaSrc = new File(udf2Dir.getAbsolutePath()+FILE_SEPARATOR+"TestUDF2.java");
        
        String udf1Src = new String("package com.xxx.udf1;\n"+
                "import java.io.IOException;\n"+
                "import org.apache.pig.EvalFunc;\n"+
                "import org.apache.pig.data.Tuple;\n"+
                "public class TestUDF1 extends EvalFunc<Integer>{\n"+
                "public Integer exec(Tuple input) throws IOException {\n"+
                "return 1;}\n"+
                "}");
        
        String udf2Src = new String("package com.xxx.udf2;\n"+
                "import org.apache.pig.builtin.PigStorage;\n" +
                "public class TestUDF2 extends PigStorage { }\n");

        // generate java file
        FileOutputStream outStream1 = 
            new FileOutputStream(udf1JavaSrc);
        OutputStreamWriter outWriter1 = new OutputStreamWriter(outStream1);
        outWriter1.write(udf1Src);
        outWriter1.close();
        
        FileOutputStream outStream2 = 
            new FileOutputStream(udf2JavaSrc);
        OutputStreamWriter outWriter2 = new OutputStreamWriter(outStream2);
        outWriter2.write(udf2Src);
        outWriter2.close();
        
        // compile
        int status;
        status = Util.executeJavaCommand("javac -cp "+System.getProperty("java.class.path") + " " + udf1JavaSrc);
        status = Util.executeJavaCommand("javac -cp "+System.getProperty("java.class.path") + " " + udf2JavaSrc);
                
        // generate jar file
        String jarName = "TestUDFJar.jar";
        String jarFile = tmpDir.getAbsolutePath() + FILE_SEPARATOR + jarName;
        status = Util.executeJavaCommand("jar -cf " + tmpDir.getAbsolutePath() + FILE_SEPARATOR + jarName + 
                              " -C " + tmpDir.getAbsolutePath() + " " + "com");
        assertTrue(status==0);
        Properties properties = cluster.getProperties();
        PigContext localPigContext = new PigContext(ExecType.MAPREDUCE, properties);
        
        //register jar using properties
        localPigContext.getProperties().setProperty("pig.additional.jars", jarFile);
        PigServer pigServer = new PigServer(localPigContext);

        PigContext.initializeImportList("com.xxx.udf1:com.xxx.udf2.");
        ArrayList<String> importList = PigContext.getPackageImportList();
        assertTrue(importList.size()==5);
        assertTrue(importList.get(0).equals("com.xxx.udf1."));
        assertTrue(importList.get(1).equals("com.xxx.udf2."));
        assertTrue(importList.get(2).equals(""));
        assertTrue(importList.get(3).equals("org.apache.pig.builtin."));
        assertTrue(importList.get(4).equals("org.apache.pig.impl.builtin."));
        
        Object udf = PigContext.instantiateFuncFromSpec("TestUDF1");
        assertTrue(udf.getClass().toString().endsWith("com.xxx.udf1.TestUDF1"));

        int LOOP_COUNT = 40;
        File tmpFile = File.createTempFile("test", "txt");
        tmpFile.deleteOnExit();
        String localInput[] = new String[LOOP_COUNT];
        Random r = new Random(1);
        int rand;
        for(int i = 0; i < LOOP_COUNT; i++) {
            rand = r.nextInt(100);
            localInput[i] = Integer.toString(rand);
        }
        Util.createInputFile(cluster, tmpFile.getCanonicalPath(), localInput);        
        FileLocalizer.deleteTempFiles();
        pigServer.registerQuery("A = LOAD '" + tmpFile.getCanonicalPath() + "' using TestUDF2() AS (num:chararray);");
        pigServer.registerQuery("B = foreach A generate TestUDF1(num);");
        Iterator<Tuple> iter = pigServer.openIterator("B");
        if(!iter.hasNext()) fail("No output found");
        while(iter.hasNext()){
            Tuple t = iter.next();
            assertTrue(t.get(0) instanceof Integer);
            assertTrue((Integer)t.get(0) == 1);
        }
        Util.deleteFile(cluster, tmpFile.getCanonicalPath());
        Util.deleteDirectory(tmpDir);
    }

    // See PIG-1824
    @SuppressWarnings("deprecation")
    @Test
    public void testScriptFiles() throws Exception {
        PigContext pc = new PigContext(ExecType.LOCAL, getProperties());
        final int n = pc.scriptFiles.size();
        pc.addScriptFile("test/path-1824");
        assertEquals("test/path-1824", pc.getScriptFiles().get("test/path-1824").toString());
        assertEquals("script files should not be populated", n, pc.scriptFiles.size());

        pc.addScriptFile("path-1824", "test/path-1824");
        assertEquals("test/path-1824", pc.getScriptFiles().get("path-1824").toString());
        assertEquals("script files should not be populated", n, pc.scriptFiles.size());
        
        // last add wins when using an alias
        pc.addScriptFile("path-1824", "test/some/other/path-1824");
        assertEquals("test/some/other/path-1824", pc.getScriptFiles().get("path-1824").toString());
        assertEquals("script files should not be populated", n, pc.scriptFiles.size());

        // clean up
        pc.getScriptFiles().remove("path-1824");
        pc.getScriptFiles().remove("test/path-1824");
    }

    @After
    public void tearDown() throws Exception {
        input.delete();
    }
    
    @AfterClass
    public static void oneTimeTearDown() throws Exception {
        cluster.shutDown();
    }
    
    
    private static Properties getProperties() {
        Properties props = new Properties();
        props.put("mapred.job.tracker", JOB_TRACKER);
        props.put("fs.default.name", FS_NAME);
        props.put("hadoop.tmp.dir", TMP_DIR_PROP);
        return props;
    }

    private List<String> getCommands() {
        List<String> commands = new ArrayList<String>();
        commands.add("my_input = LOAD '" + Util.encodeEscape(input.getAbsolutePath()) + "' USING PigStorage();");
        commands.add("words = FOREACH my_input GENERATE FLATTEN(TOKENIZE($0));");
        commands.add("grouped = GROUP words BY $0;");
        commands.add("counts = FOREACH grouped GENERATE group, COUNT(words);");
        return commands;
    }

    private void registerAndStore(PigServer pigServer) throws IOException {
        // pigServer.debugOn();
        List<String> commands = getCommands();
        for (final String command : commands) {
            pigServer.registerQuery(command);
        }
        String outFile = input.getAbsolutePath() + ".out";
        pigServer.store("counts", outFile);
        Util.deleteFile(cluster, outFile);
    }

    private void check_asserts(PigServer pigServer) {
        assertEquals(JOB_TRACKER, pigServer.getPigContext().getProperties().getProperty("mapred.job.tracker"));
        assertEquals(FS_NAME, pigServer.getPigContext().getProperties().getProperty("fs.default.name"));
        assertEquals(TMP_DIR_PROP, pigServer.getPigContext().getProperties().getProperty("hadoop.tmp.dir"));
    }
}

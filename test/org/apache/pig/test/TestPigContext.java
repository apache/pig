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
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import junit.framework.TestCase;

import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.pig.EvalFunc;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileLocalizer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestPigContext extends TestCase {

    private static final String TMP_DIR_PROP = "/tmp/hadoop-hadoop";
    private static final String FS_NAME = "machine:9000";
    private static final String JOB_TRACKER = "machine:9001";

    private File input;
    private PigContext pigContext;
    MiniCluster cluster = MiniCluster.buildCluster();
    
    @Before
    @Override
    protected void setUp() throws Exception {
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
        
        check_asserts();
    }

    /**
     * Setting properties through PigServer constructor directly. 
     */
    @Test
    public void testSetProperties_way_num02() throws Exception {
        PigServer pigServer = new PigServer(ExecType.LOCAL, getProperties());
        registerAndStore(pigServer);
        
        check_asserts();
    }

    /**
     * using connect() method. 
     */
    @Test
    public void testSetProperties_way_num03() throws Exception {
        pigContext.connect();
        PigServer pigServer = new PigServer(pigContext);
        registerAndStore(pigServer);
        
        check_asserts();
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
        
        File tempDir = new File(tmpDir.getAbsolutePath());
        Util.deleteDirectory(tempDir);
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
        status = Util.executeShellCommand("javac -cp "+System.getProperty("java.class.path") + " " + udf1JavaSrc);
        status = Util.executeShellCommand("javac -cp "+System.getProperty("java.class.path") + " " + udf2JavaSrc);
                
        // generate jar file
        String jarName = "TestUDFJar.jar";
        status = Util.executeShellCommand("jar -cf " + tmpDir.getAbsolutePath() + FILE_SEPARATOR + jarName + 
                              " -C " + tmpDir.getAbsolutePath() + " " + "com");
        assertTrue(status==0);
        
        PigServer pig = new PigServer(pigContext);
        pig.registerJar(tmpDir.getAbsolutePath() + FILE_SEPARATOR + jarName);

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
        PrintStream ps = new PrintStream(new FileOutputStream(tmpFile));
        Random r = new Random(1);
        int rand;
        for(int i = 0; i < LOOP_COUNT; i++) {
            rand = r.nextInt(100);
            ps.println(rand);
        }
        ps.close();
        
        FileLocalizer.deleteTempFiles();
        PigServer pigServer = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        pigServer.registerQuery("A = LOAD '" + Util.generateURI(tmpFile.toString()) + "' using TestUDF2() AS (num:chararray);");
        pigServer.registerQuery("B = foreach A generate TestUDF1(num);");
        Iterator<Tuple> iter = pigServer.openIterator("B");
        if(!iter.hasNext()) fail("No output found");
        while(iter.hasNext()){
            Tuple t = iter.next();
            assertTrue(t.get(0) instanceof Integer);
            assertTrue((Integer)t.get(0) == 1);
        }
        
        Util.deleteDirectory(tempDir);
    }

    @After
    @Override
    protected void tearDown() throws Exception {
        input.delete();
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
        pigServer.debugOn();
        List<String> commands = getCommands();
        for (final String command : commands) {
            pigServer.registerQuery(command);
        }
        pigServer.store("counts", input.getAbsolutePath() + ".out");
    }

    private void check_asserts() {
        assertEquals(JOB_TRACKER, pigContext.getProperties().getProperty("mapred.job.tracker"));
        assertEquals(FS_NAME, pigContext.getProperties().getProperty("fs.default.name"));
        assertEquals(TMP_DIR_PROP, pigContext.getProperties().getProperty("hadoop.tmp.dir"));
    }
}

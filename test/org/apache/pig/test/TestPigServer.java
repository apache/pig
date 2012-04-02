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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.pig.impl.PigContext;


import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.LogUtils;
import org.apache.pig.impl.util.PropertiesUtil;
import org.apache.pig.impl.util.Utils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TestPigServer {
    private PigServer pig = null;
    static MiniCluster cluster = MiniCluster.buildCluster();
    private File stdOutRedirectedFile;

    @Before
    public void setUp() throws Exception{
        FileLocalizer.setInitialized(false);
        pig = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        stdOutRedirectedFile = new File("stdout.redirected");
        // Create file if it does not exist
        try {
            if(!stdOutRedirectedFile.createNewFile())
                Assert.fail("Unable to create input files");
        } catch (IOException e) {
            Assert.fail("UAssert.assertTruee to create input files:" + e.getMessage());
        }
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
    
    private final static String FILE_SEPARATOR = System.getProperty("file.separator");
    
    // make sure that name is included or not (depending on flag "included") 
    // in the given list of stings
    private static void verifyStringContained(List<URL> list, String name, boolean included) {
        Iterator<URL> iter = list.iterator();
        boolean nameIsSubstring = false;
        int count = 0;
        
        while (iter.hasNext()) {
            if (iter.next().toString().contains(name)) {
                nameIsSubstring = true;
                ++count;
            }
        }
        
        if (included) {
            Assert.assertTrue(nameIsSubstring);
            Assert.assertTrue(count == 1);
        }
        else {
            Assert.assertFalse(nameIsSubstring);
        }
    }
    
    // creates an empty jar file
    private static void createFakeJarFile(String location, String name) 
                                          throws IOException {
        createFakeJarFile(location, name,
                FileSystem.getLocal(cluster.getConfiguration()).getRaw());
    }

    // creates an empty jar file
    private static void createFakeJarFile(String location, String name, FileSystem fs)
                                          throws IOException {
        System.err. println("Location: " + location + " name: " + name);
        Path dir = new Path(location);
        fs.mkdirs(dir);

        Assert.assertTrue(fs.createNewFile(new Path(dir, name)));
    }
    
    // dynamically add more resources to the system class loader
    private static void registerNewResource(String file) throws Exception {
        URL urlToAdd = new File(file).toURI().toURL();
        URLClassLoader sysLoader = (URLClassLoader) ClassLoader.getSystemClassLoader();
        Method addMethod = URLClassLoader.class.
                            getDeclaredMethod("addURL",
                                              new Class[]{URL.class});
        addMethod.setAccessible(true);
        addMethod.invoke(sysLoader, new Object[]{urlToAdd});
    }
    
    /**
     * The jar file to register is not present
     */
    @Test
    public void testRegisterJarFileNotPresent() throws Throwable {
        // resister a jar file that does not exist
        
        String jarName = "BadFileNameTestJarNotPresent.jar";
        
        // jar name is not present to start with
        verifyStringContained(pig.getPigContext().extraJars, jarName, false);

        boolean exceptionRaised = false;
        try {
            pig.registerJar(jarName);
        }
        catch (IOException e) {
            exceptionRaised = true;
        }        
        Assert.assertTrue(exceptionRaised);
        verifyStringContained(pig.getPigContext().extraJars, jarName, false);
    }

    /**
     * Jar file to register is not present in the system resources
     * in this case name of jar file is relative to current working dir
     */
    @Test
    public void testRegisterJarLocalDir() throws Throwable {
        String dir1 = "test1_register_jar_local";
        String dir2 = "test2_register_jar_local";
        String jarLocation = dir1 + FILE_SEPARATOR +
                              dir2 + FILE_SEPARATOR;
        String jarName = "TestRegisterJarLocal.jar";
        
        
        createFakeJarFile(jarLocation, jarName);
        
        verifyStringContained(pig.getPigContext().extraJars, jarName, false);
        
        boolean exceptionRaised = false;
        try {
            pig.registerJar(jarLocation + jarName);
        }
        catch (IOException e) {
            exceptionRaised = true;
        }        
        Assert.assertFalse(exceptionRaised);
        verifyStringContained(pig.getPigContext().extraJars, jarName, true);

        // clean-up
        Assert.assertTrue((new File(jarLocation + jarName)).delete());
        (new File(dir1 + FILE_SEPARATOR + dir2)).delete();
        (new File(dir1)).delete();
    }

    /**
     * Jar file is located via system resources
     * Test verifies that even with multiple resources matching,
     * only one of them is registered.
     */
    @Test
    public void testRegisterJarFromResources () throws Throwable {
        String dir = "test_register_jar_res_dir";
        String subDir1 = "test_register_jar_res_sub_dir1";
        String subDir2 = "test_register_jar_res_sub_dir2";
        String jarName = "TestRegisterJarFromRes.jar";
        String jarLocation1 = dir + FILE_SEPARATOR + subDir1 + FILE_SEPARATOR;
        String jarLocation2 = dir + FILE_SEPARATOR + subDir2 + FILE_SEPARATOR;
        
        
        createFakeJarFile(jarLocation1, jarName);
        createFakeJarFile(jarLocation2, jarName);
        
        verifyStringContained(pig.getPigContext().extraJars, jarName, false);
        
        registerNewResource(jarLocation1);
        registerNewResource(jarLocation2);
        
        boolean exceptionRaised = false;
        try {
            pig.registerJar(jarName);
        }
        catch (IOException e) {
            exceptionRaised = true;
        }
        Assert.assertFalse(exceptionRaised);
        verifyStringContained(pig.getPigContext().extraJars, jarName, true);

        // clean-up
        Assert.assertTrue((new File(jarLocation1 + jarName)).delete());
        Assert.assertTrue((new File(jarLocation2 + jarName)).delete());
        (new File(jarLocation1)).delete();
        (new File(jarLocation2)).delete();
        (new File(dir)).delete();
    }

    /**
     * Use a resource inside a jar file.
     * Verify that the containing jar file is registered correctly.
     * @throws Exception
     */
    @Test
    public void testRegisterJarResourceInJar() throws Throwable {
        String dir = "test_register_jar_res_in_jar";
        String subDir = "sub_dir";
        String jarName = "TestRegisterJarNonEmpty.jar";
        String className = "TestRegisterJar";
        String javaSrc = "package " + subDir + "; class " + className + " { }";

        
        // create dirs
        (new File(dir + FILE_SEPARATOR + subDir)).mkdirs();

        // generate java file
        FileOutputStream outStream = 
            new FileOutputStream(new File(dir + FILE_SEPARATOR + subDir +
                                    FILE_SEPARATOR + className + ".java"));
        
        OutputStreamWriter outWriter = new OutputStreamWriter(outStream);
        outWriter.write(javaSrc);
        outWriter.close();
        
        // compile
        int status;
        status = Util.executeJavaCommand("javac " + dir + FILE_SEPARATOR + subDir +
                               FILE_SEPARATOR + className + ".java");
        Assert.assertTrue(status==0);

        // remove src file
        (new File(dir + FILE_SEPARATOR + subDir +
                  FILE_SEPARATOR + className + ".java")).delete();

        // generate jar file
        status = Util.executeJavaCommand("jar -cf " + dir + FILE_SEPARATOR + jarName + " " +
                              "-C " + dir + " " + subDir);
        Assert.assertTrue(status==0);
        
        // remove class file and sub_dir
        (new File(dir + FILE_SEPARATOR + subDir +
                  FILE_SEPARATOR + className + ".class")).delete();
        (new File(dir + FILE_SEPARATOR + subDir)).delete();
        
        // register resource
        registerNewResource(dir + FILE_SEPARATOR + jarName);
        
        // load the specific resource
        boolean exceptionRaised = false;
        try {
            pig.registerJar("sub_dir/TestRegisterJar.class");
        }
        catch (IOException e) {
            exceptionRaised = true;
        }
        
        // verify proper jar file is located
        Assert.assertFalse(exceptionRaised);
        verifyStringContained(pig.getPigContext().extraJars, jarName, true);

        // clean up Jar file and test dir
        (new File(dir + FILE_SEPARATOR + jarName)).delete();
        (new File(dir)).delete();
    }
    
    @Test
    public void testRegisterJarGlobbingRelative() throws Throwable {
        String dir = "test1_register_jar_globbing_relative";
        String jarLocation = dir + FILE_SEPARATOR;
        String jar1Name = "TestRegisterJarGlobbing1.jar";
        String jar2Name = "TestRegisterJarGlobbing2.jar";
        
        createFakeJarFile(jarLocation, jar1Name);
        createFakeJarFile(jarLocation, jar2Name);
        
        boolean exceptionRaised = false;
        try {
            pig.registerJar(jarLocation + "TestRegisterJarGlobbing*.jar");
        }
        catch (IOException e) {
            exceptionRaised = true;
        }        
        Assert.assertFalse(exceptionRaised);
        verifyStringContained(pig.getPigContext().extraJars, jar1Name, true);
        verifyStringContained(pig.getPigContext().extraJars, jar2Name, true);

        // clean-up
        Assert.assertTrue((new File(jarLocation + jar1Name)).delete());
        Assert.assertTrue((new File(jarLocation + jar2Name)).delete());
        (new File(dir)).delete();
    }
    
    @Test
    public void testRegisterJarGlobbingAbsolute() throws Throwable {
        String dir = "test1_register_jar_globbing_absolute";
        String jarLocation = dir + FILE_SEPARATOR;
        String jar1Name = "TestRegisterJarGlobbing1.jar";
        String jar2Name = "TestRegisterJarGlobbing2.jar";
        
        createFakeJarFile(jarLocation, jar1Name);
        createFakeJarFile(jarLocation, jar2Name);
        
        boolean exceptionRaised = false;
        String currentDir = System.getProperty("user.dir");
        try {
            pig.registerJar(new File(currentDir, dir) + FILE_SEPARATOR + "TestRegisterJarGlobbing*.jar");
        }
        catch (IOException e) {
            exceptionRaised = true;
        }        
        Assert.assertFalse(exceptionRaised);
        verifyStringContained(pig.getPigContext().extraJars, jar1Name, true);
        verifyStringContained(pig.getPigContext().extraJars, jar2Name, true);

        // clean-up
        Assert.assertTrue((new File(jarLocation + jar1Name)).delete());
        Assert.assertTrue((new File(jarLocation + jar2Name)).delete());
        (new File(dir)).delete();
    }

    @Test
    public void testRegisterRemoteGlobbingJar() throws Throwable {
        String dir = "test1_register_remote_jar_globbing";
        String jarLocation = dir + FILE_SEPARATOR;
        String jar1Name = "TestRegisterRemoteJarGlobbing1.jar";
        String jar2Name = "TestRegisterRemoteJarGlobbing2.jar";

        FileSystem fs = cluster.getFileSystem();
        createFakeJarFile(jarLocation, jar1Name, fs);
        createFakeJarFile(jarLocation, jar2Name, fs);

        // find the absolute path for the directory so that it does not
        // depend on configuration
        String absPath = fs.getFileStatus(new Path(jarLocation)).getPath().toString();

        boolean exceptionRaised = false;
        try {
            pig.registerJar(absPath + FILE_SEPARATOR + "TestRegister{Remote}Jar*.jar");
        }
        catch (IOException e) {
            e.printStackTrace();
            exceptionRaised = true;
        }
        Assert.assertFalse(exceptionRaised);
        verifyStringContained(pig.getPigContext().extraJars, jar1Name, true);
        verifyStringContained(pig.getPigContext().extraJars, jar2Name, true);

        // clean-up
        Assert.assertTrue(fs.delete(new Path(jarLocation), true));
    }
    
    @Test
    public void testRegisterRemoteMacro() throws Throwable {
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
        
        Assert.assertTrue(((Long)iter.next().get(0))==2);
    }
    
    @Test
    public void testRegisterRemoteScript() throws Throwable {
        String scriptName = "script.py";
        File scriptFile = File.createTempFile("tmp", "");
        PrintWriter pw = new PrintWriter(new FileWriter(scriptFile));
        pw.println("@outputSchema(\"word:chararray\")\ndef helloworld():\n    return 'Hello, World'");
        pw.close();
        
        FileSystem fs = cluster.getFileSystem();
        fs.copyFromLocalFile(new Path(scriptFile.getAbsolutePath()), new Path(scriptName));
        
        // find the absolute path for the directory so that it does not
        // depend on configuration
        String absPath = fs.getFileStatus(new Path(scriptName)).getPath().toString();
        
        Util.createInputFile(cluster, "testRegisterRemoteScript_input", new String[]{"1", "2"});
        pig.registerCode(absPath, "jython", "pig");
        pig.registerQuery("a = load 'testRegisterRemoteScript_input';");
        pig.registerQuery("b = foreach a generate pig.helloworld($0);");
        Iterator<Tuple> iter = pig.openIterator("b");
        
        Assert.assertTrue(iter.next().get(0).equals("Hello, World"));
        Assert.assertTrue(iter.next().get(0).equals("Hello, World"));
        Assert.assertFalse(iter.hasNext());
    }

    @Test
    public void testDescribeLoad() throws Throwable {
        PrintStream console = System.out;
        PrintStream out = new PrintStream(new BufferedOutputStream(new FileOutputStream(stdOutRedirectedFile)));

        pig.registerQuery("a = load 'a' as (field1: int, field2: float, field3: chararray );") ;
        System.setOut(out);
        pig.dumpSchema("a") ;
        out.close(); // Remember this!
        System.setOut(console);

        String s;
        InputStream fileWithStdOutContents = new DataInputStream( new BufferedInputStream( new FileInputStream(stdOutRedirectedFile)));
        BufferedReader reader = new BufferedReader(new InputStreamReader(fileWithStdOutContents));
        while ((s = reader.readLine()) != null) {
            Assert.assertTrue(s.equals("a: {field1: int,field2: float,field3: chararray}") == true);
        }
        reader.close();
    }

    @Test
    public void testDescribeFilter() throws Throwable {
        PrintStream console = System.out;
        PrintStream out = new PrintStream(new BufferedOutputStream(new FileOutputStream(stdOutRedirectedFile)));

        pig.registerQuery("a = load 'a' as (field1: int, field2: float, field3: chararray );") ;
        pig.registerQuery("b = filter a by field1 > 10;") ;
        System.setOut(out);
        pig.dumpSchema("b") ;
        out.close(); // Remember this!
        System.setOut(console);

        String s;
        InputStream fileWithStdOutContents = new DataInputStream( new BufferedInputStream( new FileInputStream(stdOutRedirectedFile)));
        BufferedReader reader = new BufferedReader(new InputStreamReader(fileWithStdOutContents));
        while ((s = reader.readLine()) != null) {
            Assert.assertTrue(s.equals("b: {field1: int,field2: float,field3: chararray}") == true);
        }
        fileWithStdOutContents.close();
    }

    @Test
    public void testDescribeDistinct() throws Throwable {
        PrintStream console = System.out;
        PrintStream out = new PrintStream(new BufferedOutputStream(new FileOutputStream(stdOutRedirectedFile)));

        pig.registerQuery("a = load 'a' as (field1: int, field2: float, field3: chararray );") ;
        pig.registerQuery("b = distinct a ;") ;
        System.setOut(out);
        pig.dumpSchema("b") ;
        out.close(); // Remember this!
        System.setOut(console);

        String s;
        InputStream fileWithStdOutContents = new DataInputStream( new BufferedInputStream( new FileInputStream(stdOutRedirectedFile)));
        BufferedReader reader = new BufferedReader(new InputStreamReader(fileWithStdOutContents));
        while ((s = reader.readLine()) != null) {
            Assert.assertTrue(s.equals("b: {field1: int,field2: float,field3: chararray}") == true);
        }
        fileWithStdOutContents.close();
    }

    @Test
    public void testDescribeSort() throws Throwable {
        PrintStream console = System.out;
        PrintStream out = new PrintStream(new BufferedOutputStream(new FileOutputStream(stdOutRedirectedFile)));

        pig.registerQuery("a = load 'a' as (field1: int, field2: float, field3: chararray );") ;
        pig.registerQuery("b = order a by * desc;") ;
        System.setOut(out);
        pig.dumpSchema("b") ;
        out.close(); // Remember this!
        System.setOut(console);

        String s;
        InputStream fileWithStdOutContents = new DataInputStream( new BufferedInputStream( new FileInputStream(stdOutRedirectedFile)));
        BufferedReader reader = new BufferedReader(new InputStreamReader(fileWithStdOutContents));
        while ((s = reader.readLine()) != null) {
            Assert.assertTrue(s.equals("b: {field1: int,field2: float,field3: chararray}") == true);
        }
        fileWithStdOutContents.close();
    }

    @Test
    public void testDescribeLimit() throws Throwable {
        PrintStream console = System.out;
        PrintStream out = new PrintStream(new BufferedOutputStream(new FileOutputStream(stdOutRedirectedFile)));

        pig.registerQuery("a = load 'a' as (field1: int, field2: float, field3: chararray );") ;
        pig.registerQuery("b = limit a 10;") ;
        System.setOut(out);
        pig.dumpSchema("b") ;
        out.close(); // Remember this!
        System.setOut(console);

        String s;
        InputStream fileWithStdOutContents = new DataInputStream( new BufferedInputStream( new FileInputStream(stdOutRedirectedFile)));
        BufferedReader reader = new BufferedReader(new InputStreamReader(fileWithStdOutContents));
        while ((s = reader.readLine()) != null) {
            Assert.assertTrue(s.equals("b: {field1: int,field2: float,field3: chararray}") == true);
        }
        fileWithStdOutContents.close();
    }

    @Test
    public void testDescribeForeach() throws Throwable {
        PrintStream console = System.out;
        PrintStream out = new PrintStream(new BufferedOutputStream(new FileOutputStream(stdOutRedirectedFile)));

        pig.registerQuery("a = load 'a' as (field1: int, field2: float, field3: chararray );") ;
        pig.registerQuery("b = foreach a generate field1 + 10;") ;
        System.setOut(out);
        pig.dumpSchema("b") ;
        out.close(); 
        System.setOut(console);

        String s;
        InputStream fileWithStdOutContents = new DataInputStream( new BufferedInputStream( new FileInputStream(stdOutRedirectedFile)));
        BufferedReader reader = new BufferedReader(new InputStreamReader(fileWithStdOutContents));
        while ((s = reader.readLine()) != null) {
            Assert.assertTrue(s.equals("b: {int}") == true);
        }
        fileWithStdOutContents.close();
    }

    @Test
    public void testDescribeForeachFail() throws Throwable {

        pig.registerQuery("a = load 'a' as (field1: int, field2: float, field3: chararray );") ;
        pig.registerQuery("b = foreach a generate field1 + 10;") ;
        try {
            pig.dumpSchema("c") ;
            Assert.fail("Error expected");
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("Unable to describe schema for alias c"));
        }
    }

    @Test
    public void testDescribeForeachNoSchema() throws Throwable {
        PrintStream console = System.out;
        PrintStream out = new PrintStream(new BufferedOutputStream(new FileOutputStream(stdOutRedirectedFile)));

        pig.registerQuery("a = load 'a' ;") ;
        pig.registerQuery("b = foreach a generate *;") ;
        System.setOut(out);
        pig.dumpSchema("b") ;
        out.close(); 
        System.setOut(console);

        String s;
        InputStream fileWithStdOutContents = new DataInputStream( new BufferedInputStream( new FileInputStream(stdOutRedirectedFile)));
        BufferedReader reader = new BufferedReader(new InputStreamReader(fileWithStdOutContents));
        while ((s = reader.readLine()) != null) {
            Assert.assertTrue(s.equals("Schema for b unknown."));
        }
        fileWithStdOutContents.close();
    }

    @Test
    public void testDescribeCogroup() throws Throwable {
        PrintStream console = System.out;
        PrintStream out = new PrintStream(new BufferedOutputStream(new FileOutputStream(stdOutRedirectedFile)));

        pig.registerQuery("a = load 'a' as (field1: int, field2: float, field3: chararray );") ;
        pig.registerQuery("b = load 'b' as (field4, field5: double, field6: chararray );") ;
        pig.registerQuery("c = cogroup a by field1, b by field4;") ;
        System.setOut(out);
        pig.dumpSchema("c") ;
        out.close(); 
        System.setOut(console);

        String s;
        InputStream fileWithStdOutContents = new DataInputStream( new BufferedInputStream( new FileInputStream(stdOutRedirectedFile)));
        BufferedReader reader = new BufferedReader(new InputStreamReader(fileWithStdOutContents));
        while ((s = reader.readLine()) != null) {
            Assert.assertTrue(s.equals("c: {group: int,a: {(field1: int,field2: float,field3: chararray)},b: {(field4: bytearray,field5: double,field6: chararray)}}") == true);
        }
        fileWithStdOutContents.close();
    }

    @Test
    public void testDescribeCross() throws Throwable {
        PrintStream console = System.out;
        PrintStream out = new PrintStream(new BufferedOutputStream(new FileOutputStream(stdOutRedirectedFile)));

        pig.registerQuery("a = load 'a' as (field1: int, field2: float, field3: chararray );") ;
        pig.registerQuery("b = load 'b' as (field4, field5: double, field6: chararray );") ;
        pig.registerQuery("c = cross a, b;") ;
        System.setOut(out);
        pig.dumpSchema("c") ;
        out.close(); 
        System.setOut(console);

        String s;
        InputStream fileWithStdOutContents = new DataInputStream( new BufferedInputStream( new FileInputStream(stdOutRedirectedFile)));
        BufferedReader reader = new BufferedReader(new InputStreamReader(fileWithStdOutContents));
        while ((s = reader.readLine()) != null) {
            Assert.assertTrue(s.equals("c: {a::field1: int,a::field2: float,a::field3: chararray,b::field4: bytearray,b::field5: double,b::field6: chararray}") == true);
        }
        fileWithStdOutContents.close();
    }

    @Test
    public void testDescribeJoin() throws Throwable {
        PrintStream console = System.out;
        PrintStream out = new PrintStream(new BufferedOutputStream(new FileOutputStream(stdOutRedirectedFile)));

        pig.registerQuery("a = load 'a' as (field1: int, field2: float, field3: chararray );") ;
        pig.registerQuery("b = load 'b' as (field4, field5: double, field6: chararray );") ;
        pig.registerQuery("c = join a by field1, b by field4;") ;
        System.setOut(out);
        pig.dumpSchema("c") ;
        out.close(); 
        System.setOut(console);

        String s;
        InputStream fileWithStdOutContents = new DataInputStream( new BufferedInputStream( new FileInputStream(stdOutRedirectedFile)));
        BufferedReader reader = new BufferedReader(new InputStreamReader(fileWithStdOutContents));
        while ((s = reader.readLine()) != null) {
            Assert.assertEquals("c: {a::field1: int,a::field2: float,a::field3: chararray,b::field4: bytearray,b::field5: double,b::field6: chararray}", s );
        }
        fileWithStdOutContents.close();
    }

    @Test
    public void testDescribeUnion() throws Throwable {
        PrintStream console = System.out;
        PrintStream out = new PrintStream(new BufferedOutputStream(new FileOutputStream(stdOutRedirectedFile)));

        pig.registerQuery("a = load 'a' as (field1: int, field2: float, field3: chararray );") ;
        pig.registerQuery("b = load 'b' as (field4, field5: double, field6: chararray );") ;
        pig.registerQuery("c = union a, b;") ;
        System.setOut(out);
        pig.dumpSchema("c") ;
        out.close(); 
        System.setOut(console);

        String s;
        InputStream fileWithStdOutContents = new DataInputStream( new BufferedInputStream( new FileInputStream(stdOutRedirectedFile)));
        BufferedReader reader = new BufferedReader(new InputStreamReader(fileWithStdOutContents));
        while ((s = reader.readLine()) != null) {
            Assert.assertTrue(s.equals("c: {field1: int,field2: double,field3: chararray}") == true);
        }
        fileWithStdOutContents.close();
    }

    @Test
    public void testDescribeComplex() throws Throwable {
        PrintStream console = System.out;
        PrintStream out = new PrintStream(new BufferedOutputStream(new FileOutputStream(stdOutRedirectedFile)));

        pig.registerQuery("a = load 'a' as (site: chararray, count: int, itemCounts: bag { itemCountsTuple: tuple (type: chararray, typeCount: int, f: float, m: map[]) } ) ;") ;
        pig.registerQuery("b = foreach a generate site, count, FLATTEN(itemCounts);") ;
        System.setOut(out);
        pig.dumpSchema("b") ;
        out.close();
        System.setOut(console);

        String s;
        InputStream fileWithStdOutContents = new DataInputStream( new BufferedInputStream( new FileInputStream(stdOutRedirectedFile)));
        BufferedReader reader = new BufferedReader(new InputStreamReader(fileWithStdOutContents));
        while ((s = reader.readLine()) != null) {
            // strip away the initial schema alias and the
            // curlies surrounding the schema to construct
            // the schema object from the schema string
            s = s.replaceAll("^.*\\{", "");
            s = s.replaceAll("\\}$", "");
            Schema actual = Utils.getSchemaFromString( s);
            Schema expected = Utils.getSchemaFromString(
                    "site: chararray,count: int," +
                    "itemCounts::type: chararray,itemCounts::typeCount: int," +
                    "itemCounts::f: float,itemCounts::m: map[ ]");
            Assert.assertEquals(expected, actual);
        }
        fileWithStdOutContents.close();
    }
    
    @Test
    public void testParamSubstitution() throws Exception{
        // using params map
        PigServer pig=new PigServer(ExecType.LOCAL);
        Map<String,String> params=new HashMap<String, String>();
        params.put("input", "test/org/apache/pig/test/data/passwd");
        File scriptFile=Util.createFile(new String[]{"a = load '$input' using PigStorage(':');"});
        pig.registerScript(scriptFile.getAbsolutePath(),params);
        Iterator<Tuple> iter=pig.openIterator("a");
        int index=0;
        List<Tuple> expectedTuples=Util.readFile2TupleList("test/org/apache/pig/test/data/passwd", ":");
        while(iter.hasNext()){
            Tuple tuple=iter.next();
            Assert.assertEquals(tuple.get(0).toString(), expectedTuples.get(index).get(0).toString());
            index++;
        }
        
        // using param file
        pig=new PigServer(ExecType.LOCAL);
        List<String> paramFile=new ArrayList<String>();
        paramFile.add(Util.createFile(new String[]{"input=test/org/apache/pig/test/data/passwd2"}).getAbsolutePath());
        pig.registerScript(scriptFile.getAbsolutePath(),paramFile);
        iter=pig.openIterator("a");
        index=0;
        expectedTuples=Util.readFile2TupleList("test/org/apache/pig/test/data/passwd2", ":");
        while(iter.hasNext()){
            Tuple tuple=iter.next();
            Assert.assertEquals(tuple.get(0).toString(), expectedTuples.get(index).get(0).toString());
            index++;
        }
        
        // using both param value and param file, param value should override param file
        pig=new PigServer(ExecType.LOCAL);
        pig.registerScript(scriptFile.getAbsolutePath(),params,paramFile);
        iter=pig.openIterator("a");
        index=0;
        expectedTuples=Util.readFile2TupleList("test/org/apache/pig/test/data/passwd", ":");
        while(iter.hasNext()){
            Tuple tuple=iter.next();
            Assert.assertEquals(tuple.get(0).toString(), expectedTuples.get(index).get(0).toString());
            index++;
        }
    }
    
    // build the pig script from in-memory, and wrap it as ByteArrayInputStream
    @Test
    public void testRegisterScriptFromStream() throws Exception{
        // using params map
        PigServer pig=new PigServer(ExecType.LOCAL);
        Map<String,String> params=new HashMap<String, String>();
        params.put("input", "test/org/apache/pig/test/data/passwd");
        String script="a = load '$input' using PigStorage(':');";
        pig.registerScript(new ByteArrayInputStream(script.getBytes("UTF-8")),params);
        Iterator<Tuple> iter=pig.openIterator("a");
        int index=0;
        List<Tuple> expectedTuples=Util.readFile2TupleList("test/org/apache/pig/test/data/passwd", ":");
        while(iter.hasNext()){
            Tuple tuple=iter.next();
            Assert.assertEquals(tuple.get(0).toString(), expectedTuples.get(index).get(0).toString());
            index++;
        }
        
        // using param file
        pig=new PigServer(ExecType.LOCAL);
        List<String> paramFile=new ArrayList<String>();
        paramFile.add(Util.createFile(new String[]{"input=test/org/apache/pig/test/data/passwd2"}).getAbsolutePath());
        pig.registerScript(new ByteArrayInputStream(script.getBytes("UTF-8")),paramFile);
        iter=pig.openIterator("a");
        index=0;
        expectedTuples=Util.readFile2TupleList("test/org/apache/pig/test/data/passwd2", ":");
        while(iter.hasNext()){
            Tuple tuple=iter.next();
            Assert.assertEquals(tuple.get(0).toString(), expectedTuples.get(index).get(0).toString());
            index++;
        }
        
        // using both param value and param file, param value should override param file
        pig=new PigServer(ExecType.LOCAL);
        pig.registerScript(new ByteArrayInputStream(script.getBytes("UTF-8")),params,paramFile);
        iter=pig.openIterator("a");
        index=0;
        expectedTuples=Util.readFile2TupleList("test/org/apache/pig/test/data/passwd", ":");
        while(iter.hasNext()){
            Tuple tuple=iter.next();
            Assert.assertEquals(tuple.get(0).toString(), expectedTuples.get(index).get(0).toString());
            index++;
        }
    }
    
    @Test
    public void testPigProperties() throws Throwable {
        File propertyFile = new File("pig.properties");
        File cliPropertyFile = new File("commandLine_pig.properties");
        
        Properties properties = PropertiesUtil.loadDefaultProperties();
        Assert.assertTrue(properties.getProperty("pig.spill.gc.activation.size").equals("40000000"));
        Assert.assertTrue(properties.getProperty("test123")==null);

        PrintWriter out = new PrintWriter(new FileWriter(propertyFile));
        out.println("test123=properties");
        out.close();

        properties = PropertiesUtil.loadDefaultProperties();
        Assert.assertTrue(properties.getProperty("test123").equals("properties"));
        
        out = new PrintWriter(new FileWriter(cliPropertyFile));
        out.println("test123=cli_properties");
        out.close();

        properties = PropertiesUtil.loadDefaultProperties();
        PropertiesUtil.loadPropertiesFromFile(properties,
                "commandLine_pig.properties");
        Assert.assertTrue(properties.getProperty("test123").equals("cli_properties"));
        
        propertyFile.delete();
        cliPropertyFile.delete();
    }

    @Test
    public void testPigTempDir() throws Throwable {
        File propertyFile = new File("pig.properties");
        PrintWriter out = new PrintWriter(new FileWriter(propertyFile));
        out.println("pig.temp.dir=/opt/temp");
        out.close();
        Properties properties = PropertiesUtil.loadDefaultProperties();
        PigContext pigContext=new PigContext(ExecType.LOCAL, properties);
        pigContext.connect();
        FileLocalizer.setInitialized(false);
        String tempPath= FileLocalizer.getTemporaryPath(pigContext).toString();
        Assert.assertTrue(tempPath.startsWith("file:/opt/temp"));
        propertyFile.delete();
        FileLocalizer.setInitialized(false);
    }
    
    @Test
    public void testDescribeForEachFlatten() throws Throwable {
        pig.registerQuery("a = load 'a';") ;
        pig.registerQuery("b = group a by $0;") ;
        pig.registerQuery("c = foreach b generate flatten(a);") ;
        Schema s = pig.dumpSchema("c") ;
        Assert.assertTrue(s==null);
    }

    @Test // PIG-2059
    public void test1() throws Throwable {
    	pig.setValidateEachStatement(true);
        pig.registerQuery("A = load 'x' as (u, v);") ;
        try {
            pig.registerQuery("B = foreach A generate $2;") ;
        } catch(FrontendException ex) {
            String msg = "Out of bound access. " +
            "Trying to access non-existent column: 2";
            Util.checkMessageInException(ex, msg);
            return;
        }
        Assert.fail( "Query is supposed to fail." );
    }

    @Test
	public void testDefaultPigProperties() throws Throwable {
    	//Test with PigServer
    	PigServer pigServer = new PigServer(ExecType.MAPREDUCE);
    	Properties properties = pigServer.getPigContext().getProperties();
    	
    	Assert
		.assertTrue(properties.getProperty(
				"pig.exec.reducers.max").equals("999"));
		Assert.assertTrue(properties.getProperty("aggregate.warning").equals("true"));
		Assert.assertTrue(properties.getProperty("opt.multiquery").equals("true"));
		Assert.assertTrue(properties.getProperty("stop.on.failure").equals("false"));
    	
		//Test with properties file
		File propertyFile = new File("pig.properties");

		properties = PropertiesUtil.loadDefaultProperties();
		
		Assert
		.assertTrue(properties.getProperty(
				"pig.exec.reducers.max").equals("999"));
		Assert.assertTrue(properties.getProperty("aggregate.warning").equals("true"));
		Assert.assertTrue(properties.getProperty("opt.multiquery").equals("true"));
		Assert.assertTrue(properties.getProperty("stop.on.failure").equals("false"));
		
		PrintWriter out = new PrintWriter(new FileWriter(propertyFile));
		out.println("aggregate.warning=false");
		out.println("opt.multiquery=false");
		out.println("stop.on.failure=true");
		
		out.close();

		properties = PropertiesUtil.loadDefaultProperties();
		Assert.assertTrue(properties.getProperty("aggregate.warning")
				.equals("false"));
		Assert.assertTrue(properties.getProperty("opt.multiquery")
				.equals("false"));
		Assert.assertTrue(properties.getProperty("stop.on.failure")
				.equals("true"));

		propertyFile.delete();
	}
}

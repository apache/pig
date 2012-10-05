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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
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
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.PropertiesUtil;
import org.apache.pig.impl.util.Utils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import com.google.common.io.Files;

public class TestPigServer {
    private PigServer pig = null;
    static MiniCluster cluster = MiniCluster.buildCluster();
    private File tempDir;

    @Before
    public void setUp() throws Exception{
        FileLocalizer.setInitialized(false);
        pig = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());

        tempDir = Files.createTempDir();
        tempDir.deleteOnExit();
        registerNewResource(tempDir.getAbsolutePath());
    }
    
    @After
    public void tearDown() throws Exception{
        pig = null;
        tempDir.delete();
    }
    
    @AfterClass
    public static void oneTimeTearDown() throws Exception {
        cluster.shutDown();
    }
    
    private final static String FILE_SEPARATOR = System.getProperty("file.separator");
    
    // make sure that name is included or not (depending on flag "included") 
    // in the given list of stings
    private static void verifyStringContained(List<URL> list, String name, boolean included) {
        int count = 0;
        
        for (URL url : list) {
            if (url.toString().contains(name)) {
                if (!included) {
                    fail("Included is false, but url ["+url+"] contains name ["+name+"]");
            }
                assertEquals("Too many urls contain name: " + name, 1, ++count);
        }
        }
        if (included) {
            assertEquals("Number of urls that contain name [" + name + "] != 1", 1, count);
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

        assertTrue(fs.createNewFile(new Path(dir, name)));
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
        boolean raisedException = false;
        try {
            pig.registerJar(jarName);
        } catch (IOException e) {
            raisedException = true;
        }
        assertTrue("registerJar on jarName ["+jarName+"] should have raised an exception", raisedException);
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
        
            pig.registerJar(jarLocation + jarName);
        verifyStringContained(pig.getPigContext().extraJars, jarName, true);

        // clean-up
        assertTrue((new File(jarLocation + jarName)).delete());
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
        
            pig.registerJar(jarName);
        verifyStringContained(pig.getPigContext().extraJars, jarName, true);

        // clean-up
        assertTrue((new File(jarLocation1 + jarName)).delete());
        assertTrue((new File(jarLocation2 + jarName)).delete());
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
        assertEquals(0, status);

        // remove src file
        (new File(dir + FILE_SEPARATOR + subDir +
                  FILE_SEPARATOR + className + ".java")).delete();

        // generate jar file
        status = Util.executeJavaCommand("jar -cf " + dir + FILE_SEPARATOR + jarName + " " +
                              "-C " + dir + " " + subDir);
        assertEquals(0, status);
        
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
        assertFalse(exceptionRaised);
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
        
            pig.registerJar(jarLocation + "TestRegisterJarGlobbing*.jar");
        verifyStringContained(pig.getPigContext().extraJars, jar1Name, true);
        verifyStringContained(pig.getPigContext().extraJars, jar2Name, true);

        // clean-up
        assertTrue((new File(jarLocation + jar1Name)).delete());
        assertTrue((new File(jarLocation + jar2Name)).delete());
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
        
        String currentDir = System.getProperty("user.dir");
            pig.registerJar(new File(currentDir, dir) + FILE_SEPARATOR + "TestRegisterJarGlobbing*.jar");
        verifyStringContained(pig.getPigContext().extraJars, jar1Name, true);
        verifyStringContained(pig.getPigContext().extraJars, jar2Name, true);

        // clean-up
        assertTrue((new File(jarLocation + jar1Name)).delete());
        assertTrue((new File(jarLocation + jar2Name)).delete());
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

            pig.registerJar(absPath + FILE_SEPARATOR + "TestRegister{Remote}Jar*.jar");

        verifyStringContained(pig.getPigContext().extraJars, jar1Name, true);
        verifyStringContained(pig.getPigContext().extraJars, jar2Name, true);

        // clean-up
        assertTrue(fs.delete(new Path(jarLocation), true));
    }

    public void testDescribeLoad() throws Throwable {
        pig.registerQuery("a = load 'a' as (field1: int, field2: float, field3: chararray );") ;
        Schema dumpedSchema = pig.dumpSchema("a") ;
        Schema expectedSchema = Utils.getSchemaFromString("field1: int,field2: float,field3: chararray");
        assertEquals(expectedSchema, dumpedSchema);
    }

    @Test
    public void testDescribeFilter() throws Throwable {
        pig.registerQuery("a = load 'a' as (field1: int, field2: float, field3: chararray );") ;
        pig.registerQuery("b = filter a by field1 > 10;") ;
        Schema dumpedSchema = pig.dumpSchema("b") ;
        Schema expectedSchema = Utils.getSchemaFromString("field1: int,field2: float,field3: chararray");
        assertEquals(expectedSchema, dumpedSchema);
    }

    @Test
    public void testDescribeDistinct() throws Throwable {
        pig.registerQuery("a = load 'a' as (field1: int, field2: float, field3: chararray );") ;
        pig.registerQuery("b = distinct a ;") ;
        Schema dumpedSchema = pig.dumpSchema("b") ;
        Schema expectedSchema = Utils.getSchemaFromString("field1: int,field2: float,field3: chararray");
        assertEquals(expectedSchema, dumpedSchema);
    }

    @Test
    public void testDescribeSort() throws Throwable {
        pig.registerQuery("a = load 'a' as (field1: int, field2: float, field3: chararray );") ;
        pig.registerQuery("b = order a by * desc;") ;
        Schema dumpedSchema = pig.dumpSchema("b") ;
        Schema expectedSchema = Utils.getSchemaFromString("field1: int,field2: float,field3: chararray");
        assertEquals(expectedSchema, dumpedSchema);
    }

    @Test
    public void testDescribeLimit() throws Throwable {
        pig.registerQuery("a = load 'a' as (field1: int, field2: float, field3: chararray );") ;
        pig.registerQuery("b = limit a 10;") ;
        Schema dumpedSchema = pig.dumpSchema("b") ;
        Schema expectedSchema = Utils.getSchemaFromString("field1: int,field2: float,field3: chararray");
        assertEquals(expectedSchema, dumpedSchema);
    }

    @Test
    public void testDescribeForeach() throws Throwable {
        pig.registerQuery("a = load 'a' as (field1: int, field2: float, field3: chararray );") ;
        pig.registerQuery("b = foreach a generate field1 + 10;") ;
        Schema dumpedSchema = pig.dumpSchema("b") ;
        Schema expectedSchema = new Schema(new Schema.FieldSchema(null, DataType.INTEGER));
        assertEquals(expectedSchema, dumpedSchema);
    }

    @Test
    public void testDescribeForeachFail() throws Throwable {

        pig.registerQuery("a = load 'a' as (field1: int, field2: float, field3: chararray );") ;
        pig.registerQuery("b = foreach a generate field1 + 10;") ;
        try {
            pig.dumpSchema("c") ;
            fail("Error expected");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("Unable to describe schema for alias c"));
        }
    }

    @Test
    public void testDescribeForeachNoSchema() throws Throwable {
        pig.registerQuery("a = load 'a' ;") ;
        pig.registerQuery("b = foreach a generate *;") ;
        Schema dumpedSchema = pig.dumpSchema("b") ;
        assertNull(dumpedSchema);
    }

    @Test
    public void testDescribeCogroup() throws Throwable {
        pig.registerQuery("a = load 'a' as (field1: int, field2: float, field3: chararray );") ;
        pig.registerQuery("b = load 'b' as (field4, field5: double, field6: chararray );") ;
        pig.registerQuery("c = cogroup a by field1, b by field4;") ;
        Schema dumpedSchema = pig.dumpSchema("c") ;
        Schema expectedSchema = Utils.getSchemaFromString("group:int,a:{(field1:int,field2:float,field3:chararray)},b:{(field4:bytearray,field5:double,field6:chararray)}");
        assertEquals(expectedSchema, dumpedSchema);
    }

    @Test
    public void testDescribeCross() throws Throwable {
        pig.registerQuery("a = load 'a' as (field1: int, field2: float, field3: chararray );") ;
        pig.registerQuery("b = load 'b' as (field4, field5: double, field6: chararray );") ;
        pig.registerQuery("c = cross a, b;") ;
        Schema dumpedSchema = pig.dumpSchema("c") ;
        Schema expectedSchema = Utils.getSchemaFromString("a::field1: int,a::field2: float,a::field3: chararray,b::field4: bytearray,b::field5: double,b::field6: chararray");
        assertEquals(expectedSchema, dumpedSchema);
    }

    @Test
    public void testDescribeJoin() throws Throwable {
        pig.registerQuery("a = load 'a' as (field1: int, field2: float, field3: chararray );") ;
        pig.registerQuery("b = load 'b' as (field4, field5: double, field6: chararray );") ;
        pig.registerQuery("c = join a by field1, b by field4;") ;
        Schema dumpedSchema = pig.dumpSchema("c");
        Schema expectedSchema = Utils.getSchemaFromString("a::field1: int,a::field2: float,a::field3: chararray,b::field4: bytearray,b::field5: double,b::field6: chararray");
        assertEquals(expectedSchema, dumpedSchema);
    }

    @Test
    public void testDescribeUnion() throws Throwable {
        pig.registerQuery("a = load 'a' as (field1: int, field2: float, field3: chararray );") ;
        pig.registerQuery("b = load 'b' as (field4, field5: double, field6: chararray );") ;
        pig.registerQuery("c = union a, b;") ;
        Schema dumpedSchema = pig.dumpSchema("c") ;
        Schema expectedSchema = Utils.getSchemaFromString("field1: int,field2: double,field3: chararray");
        assertEquals(expectedSchema, dumpedSchema);
    }

    @Test
    public void testDescribeComplex() throws Throwable {
        pig.registerQuery("a = load 'a' as (site: chararray, count: int, itemCounts: bag { itemCountsTuple: tuple (type: chararray, typeCount: int, f: float, m: map[]) } ) ;") ;
        pig.registerQuery("b = foreach a generate site, count, FLATTEN(itemCounts);") ;
        Schema dumpedSchema = pig.dumpSchema("b") ;
        Schema expectedSchema = Utils.getSchemaFromString(
                    "site: chararray,count: int," +
                    "itemCounts::type: chararray,itemCounts::typeCount: int," +
                    "itemCounts::f: float,itemCounts::m: map[ ]");
        assertEquals(expectedSchema, dumpedSchema);
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
            assertEquals(tuple.get(0).toString(), expectedTuples.get(index).get(0).toString());
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
            assertEquals(tuple.get(0).toString(), expectedTuples.get(index).get(0).toString());
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
            assertEquals(tuple.get(0).toString(), expectedTuples.get(index).get(0).toString());
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
            assertEquals(tuple.get(0).toString(), expectedTuples.get(index).get(0).toString());
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
            assertEquals(tuple.get(0).toString(), expectedTuples.get(index).get(0).toString());
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
            assertEquals(tuple.get(0).toString(), expectedTuples.get(index).get(0).toString());
            index++;
        }
    }
    
    @Test
    public void testPigProperties() throws Throwable {
        File propertyFile = new File(tempDir, "pig.properties");
        propertyFile.deleteOnExit();
        TestPigServer.registerNewResource(propertyFile.getAbsolutePath());
        File cliPropertyFile = new File("commandLine_pig.properties");
        cliPropertyFile.deleteOnExit();
        TestPigServer.registerNewResource(cliPropertyFile.getAbsolutePath());
        
        Properties properties = PropertiesUtil.loadDefaultProperties();
        assertEquals("40000000", properties.getProperty("pig.spill.gc.activation.size"));
        assertNull(properties.getProperty("test123"));

        PrintWriter out = new PrintWriter(new FileWriter(propertyFile));
        out.println("test123=properties");
        out.close();
        properties = PropertiesUtil.loadDefaultProperties();

        assertEquals("properties", properties.getProperty("test123"));
        
        out = new PrintWriter(new FileWriter(cliPropertyFile));
        out.println("test123=cli_properties");
        out.close();

        properties = PropertiesUtil.loadDefaultProperties();
        PropertiesUtil.loadPropertiesFromFile(properties,
                "commandLine_pig.properties");
        assertEquals("cli_properties", properties.getProperty("test123"));
        
        propertyFile.delete();
        cliPropertyFile.delete();
    }

    @Test
    public void testPigTempDir() throws Throwable {
        File propertyFile = new File(tempDir, "pig.properties");
        propertyFile.deleteOnExit();
        PrintWriter out = new PrintWriter(new FileWriter(propertyFile));
        out.println("pig.temp.dir=/opt/temp");
        out.close();
        Properties properties = PropertiesUtil.loadDefaultProperties();
        PigContext pigContext=new PigContext(ExecType.LOCAL, properties);
        pigContext.connect();
        FileLocalizer.setInitialized(false);
        String tempPath= FileLocalizer.getTemporaryPath(pigContext).toString();
        assertTrue(tempPath.startsWith("file:/opt/temp"));
        propertyFile.delete();
        FileLocalizer.setInitialized(false);
    }
    
    @Test
    public void testDescribeForEachFlatten() throws Throwable {
        pig.registerQuery("a = load 'a';") ;
        pig.registerQuery("b = group a by $0;") ;
        pig.registerQuery("c = foreach b generate flatten(a);") ;
        Schema s = pig.dumpSchema("c") ;
        assertNull(s);
    }

    @Test // PIG-2059
    public void test1() throws Throwable {
    	pig.setValidateEachStatement(true);
        pig.registerQuery("A = load 'x' as (u, v);") ;
        try {
            pig.registerQuery("B = foreach A generate $2;") ;
            fail("Query is supposed to fail.");
        } catch(FrontendException ex) {
            String msg = "Out of bound access. " +
            "Trying to access non-existent column: 2";
            Util.checkMessageInException(ex, msg);
        }
    }

    @Test
	public void testDefaultPigProperties() throws Throwable {
    	//Test with PigServer
    	PigServer pigServer = new PigServer(ExecType.MAPREDUCE);
    	Properties properties = pigServer.getPigContext().getProperties();
    	
        assertEquals("999", properties.getProperty("pig.exec.reducers.max"));
        assertEquals("true", properties.getProperty("aggregate.warning"));
        assertEquals("true", properties.getProperty("opt.multiquery"));
        assertEquals("false", properties.getProperty("stop.on.failure"));
    	
		//Test with properties file
		File propertyFile = new File(tempDir, "pig.properties");

		properties = PropertiesUtil.loadDefaultProperties();
		
		assertEquals("999", properties.getProperty("pig.exec.reducers.max"));
        assertEquals("true", properties.getProperty("aggregate.warning"));
        assertEquals("true", properties.getProperty("opt.multiquery"));
        assertEquals("false", properties.getProperty("stop.on.failure"));
		
		PrintWriter out = new PrintWriter(new FileWriter(propertyFile));
		out.println("aggregate.warning=false");
		out.println("opt.multiquery=false");
		out.println("stop.on.failure=true");
		
		out.close();

		properties = PropertiesUtil.loadDefaultProperties();
		assertEquals("false", properties.getProperty("aggregate.warning"));
		assertEquals("false", properties.getProperty("opt.multiquery"));
		assertEquals("true", properties.getProperty("stop.on.failure"));

		propertyFile.delete();
	}
}

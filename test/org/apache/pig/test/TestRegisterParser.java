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
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.Writer;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;

import org.apache.pig.ExecType;
import org.apache.pig.impl.PigContext;
import org.apache.pig.PigServer;
import org.apache.pig.parser.ParserException;
import org.apache.pig.parser.RegisterResolver;
import org.apache.pig.tools.grunt.Grunt;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.apache.pig.impl.util.PropertiesUtil;
import org.apache.hadoop.fs.LocalFileSystem;


public class TestRegisterParser {
    private PigServer pigServer;
    private static final String TEST_JAR_DIR = "/tmp/";

    @Before
    public void setUp() throws Exception {
        Properties properties = PropertiesUtil.loadDefaultProperties();
        properties.setProperty("fs.s3.impl", LocalFileSystem.class.getName());
        properties.setProperty("fs.s3n.impl", LocalFileSystem.class.getName());
        properties.setProperty("fs.s3a.impl", LocalFileSystem.class.getName());

	pigServer = new PigServer(ExecType.LOCAL, properties);

	// Generate test jar files
	for (int i = 1; i <= 5; i++) {
	    Writer output = null;
	    String dataFile = TEST_JAR_DIR + "testjar-" + i + ".jar";
	    File file = new File(dataFile);
	    output = new BufferedWriter(new FileWriter(file));
	    output.write("sample");
	    output.close();
	}
    }

    // Test to check if registering a jar using a file resolver adds the right jar to the classpath.
    @Test
    public void testRegisterArtifactWithFileResolver() throws Throwable {
	PigContext context = pigServer.getPigContext();

	File confFile = new File("test/org/apache/pig/test/data/testivysettings.xml");
	System.setProperty("grape.config", confFile.toString());

	// 'changing=true' tells Grape to re-fetch the jar rather than picking it from the groovy cache.
	String strCmd = "register ivy://testgroup:testjar:1?changing=true\n";
	ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
	InputStreamReader reader = new InputStreamReader(cmd);

	Grunt grunt = new Grunt(new BufferedReader(reader), context);
	grunt.exec();

	assertEquals(context.extraJars + " of size 1", 1, context.extraJars.size());
	assertTrue(context.extraJars.get(0) + " ends with /testjar-1.jar",
		context.extraJars.get(0).toString().endsWith("/testjar-1.jar"));
    }

    // Test to check if all dependencies are successfully added to the classpath
    @Test
    public void testRegisterArtifact() throws URISyntaxException, IOException, ParserException {
	URI[] list = new URI[5];
	list[0] = new URI(TEST_JAR_DIR + "testjar-1.jar");
	list[1] = new URI(TEST_JAR_DIR + "testjar-2.jar");
	list[2] = new URI(TEST_JAR_DIR + "testjar-3.jar");
	list[3] = new URI(TEST_JAR_DIR + "testjar-4.jar");
	list[4] = new URI(TEST_JAR_DIR + "testjar-5.jar");

	// Make sure that the jars are not in the classpath
	for (URI dependency : list) {
	    Assert.assertFalse(pigServer.getPigContext().hasJar(dependency.toString()));
	}

	RegisterResolver registerResolver = Mockito.spy(new RegisterResolver(pigServer));
	Mockito.doReturn(list).when(registerResolver).resolve(new URI("ivy://testQuery"));
	registerResolver.parseRegister("ivy://testQuery", null, null);

	for (URI dependency : list) {
	    Assert.assertTrue(pigServer.getPigContext().hasJar(dependency.toString()));
	}
    }

    @Test
    public void testResolveForVariousFileSystemSchemes() throws URISyntaxException, IOException, ParserException {
        URI[] list = new URI[6];
        list[0] = new URI("file://test.jar");
        list[1] = new URI("hdfs://test.jar");
        list[2] = new URI("s3://test.jar");
        list[3] = new URI("s3n://test.jar");
        list[4] = new URI("s3a://test.jar");
        list[5] = new URI("test.jar");

        RegisterResolver registerResolver = new RegisterResolver(pigServer);
        for (URI uri : list) {
            URI[] resolvedUris = registerResolver.resolve(uri);
	    Assert.assertEquals(1, resolvedUris.length);
            Assert.assertEquals(uri, resolvedUris[0]);
        }
    }

    @Test(expected = ParserException.class)
    public void testResolveParseException() throws URISyntaxException, IOException, ParserException {
        new RegisterResolver(pigServer).resolve(new URI("abc://test.jar"));
    }

    @Test(expected = URISyntaxException.class)
    public void testResolveURISyntaxException() throws URISyntaxException, IOException, ParserException {
        new RegisterResolver(pigServer).resolve(new URI("123://test.jar"));
    }

    // Throw error when a scripting language and namespace is specified for a jar
    @Test(expected = ParserException.class)
    public void testRegisterJarException1() throws IOException, ParserException {
	new RegisterResolver(pigServer).parseRegister("test.jar", "jython", "myfunc");
    }

    // Throw error when a scripting language and namespace is specified for an ivy coordinate
    @Test(expected = ParserException.class)
    public void testRegisterJarException2() throws IOException, ParserException {
	new RegisterResolver(pigServer).parseRegister("ivy://org:mod:ver", "jython", "myfunc");
    }

    // Throw error when a scripting language is specified for a jar
    @Test(expected = ParserException.class)
    public void testRegisterJarException3() throws IOException, ParserException {
	new RegisterResolver(pigServer).parseRegister("test.jar", "jython", null);
    }

    // Throw error when an Illegal URI is passed
    @Test(expected = ParserException.class)
    public void testIllegalUriException() throws IOException, ParserException {
	new RegisterResolver(pigServer).parseRegister("ivy:||org:mod:ver", null, null);
    }

    @After
    public void close() {
	// delete sample jars
	for (int i = 1; i <= 5; i++) {
	    String dataFile = TEST_JAR_DIR + "testjar-" + i + ".jar";

	    File f = new File(dataFile);
	    if (!f.delete()) {
		throw new RuntimeException("Could not delete the data file");
	    }
	}
    }
}

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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.builtin.mock.Storage;
import org.apache.pig.data.Tuple;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.Iterator;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

import static org.apache.pig.builtin.mock.Storage.resetData;
import static org.apache.pig.builtin.mock.Storage.tuple;

public class TestPigServerWithMacros {
    private PigServer pig = null;
    
    @Before
    public void setUp() throws Exception{
        pig = new PigServer(ExecType.LOCAL);
    }

    @After
    public void tearDown() throws Exception{
        pig = null;
    }

    @Test
    public void testRegisterRemoteMacro() throws Throwable {
        String macroName = "util.pig";
        File macroFile = File.createTempFile("tmp", "");
        PrintWriter pw = new PrintWriter(new FileWriter(macroFile));
        pw.println("DEFINE row_count(X) RETURNS Z { Y = group $X all; $Z = foreach Y generate COUNT($X); };");
        pw.close();

        Path macroPath = new Path(macroName);
        FileSystem fs = macroPath.getFileSystem(new Configuration());

        fs.copyFromLocalFile(new Path(macroFile.getAbsolutePath()), macroPath);

        // find the absolute path for the directory so that it does not
        // depend on configuration
        String absPath = fs.getFileStatus(new Path(macroName)).getPath().toString();

        pig = new PigServer(ExecType.LOCAL);
        Storage.Data data = resetData(pig);
        data.set("some_path", "(l:chararray)", tuple("first row"), tuple("second row"));

        pig.registerQuery("import '" + absPath + "';");
        pig.registerQuery("a = load 'some_path' USING mock.Storage();");
        pig.registerQuery("b = row_count(a);");
        Iterator<Tuple> iter = pig.openIterator("b");

        Assert.assertEquals(2L, iter.next().get(0));
    }

    @Test
    public void testInlineMacro() throws Throwable {
        Storage.Data data = resetData(pig);
        data.set("some_path", "(l:chararray)", tuple("first row"), tuple("second row"));

        pig.registerQuery("DEFINE row_count(X) RETURNS Z { Y = group $X all; $Z = foreach Y generate COUNT($X); };");
        pig.registerQuery("a = load 'some_path' USING mock.Storage();");
        pig.registerQuery("b = row_count(a);");
        Iterator<Tuple> iter = pig.openIterator("b");

        Assert.assertEquals(2L, iter.next().get(0));
    }

    @Test
    public void testRegisterRemoteScript() throws Throwable {
        String scriptName = "script.py";
        File scriptFile = File.createTempFile("tmp", "");
        PrintWriter pw = new PrintWriter(new FileWriter(scriptFile));
        pw.println("@outputSchema(\"word:chararray\")\ndef helloworld():\n    return 'Hello, World'");
        pw.close();

        Path scriptPath = new Path(scriptName);
        FileSystem fs = scriptPath.getFileSystem(new Configuration());
        fs.copyFromLocalFile(new Path(scriptFile.getAbsolutePath()), scriptPath);

        // find the absolute path for the directory so that it does not
        // depend on configuration
        String absPath = fs.getFileStatus(scriptPath).getPath().toString();

        Storage.Data data = resetData(pig);
        data.set("some_path", "(l:chararray)", tuple(tuple("first row")), tuple(tuple("second row")));

        pig.registerCode(absPath, "jython", "pig");
        pig.registerQuery("a = load 'some_path' USING mock.Storage();");
        pig.registerQuery("b = foreach a generate pig.helloworld($0);");
        Iterator<Tuple> iter = pig.openIterator("b");

        Assert.assertTrue(iter.next().get(0).equals("Hello, World"));
        Assert.assertTrue(iter.next().get(0).equals("Hello, World"));
        Assert.assertFalse(iter.hasNext());
    }
    
    @Test
    public void testRegisterResourceMacro() throws Throwable {
        String macrosFile = "test/pig/macros.pig";
        File macrosJarFile = File.createTempFile("macros", ".jar");
        
        System.out.println("Creating macros jar " + macrosJarFile);
        
        Manifest manifest = new Manifest();
        manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
        
        JarOutputStream jarStream = new JarOutputStream(new FileOutputStream(macrosJarFile), manifest);
        
        JarEntry jarEntry = new JarEntry(macrosFile);
        jarEntry.setTime(System.currentTimeMillis());
        jarStream.putNextEntry(jarEntry);        
        
        PrintWriter pw = new PrintWriter(jarStream);
        pw.println("DEFINE row_count_in_jar(X) RETURNS Z { Y = group $X all; $Z = foreach Y generate COUNT($X); };");
        pw.close();        
        
        jarStream.close();
        
        Storage.Data data = resetData(pig);
        data.set("some_path", "(l:int)", tuple(tuple("1")), tuple(tuple("2")), tuple(tuple("3")), tuple(tuple("10")), tuple(tuple("11")));
                
        System.out.println("Registering macros jar " + macrosJarFile);
        pig.registerJar(macrosJarFile.toString());
        
        pig.registerQuery("import '" + macrosFile + "';");
        pig.registerQuery("a = load 'some_path' USING mock.Storage();");
        pig.registerQuery("b = row_count_in_jar(a);");
        Iterator<Tuple> iter = pig.openIterator("b");
        
        Assert.assertTrue(((Long)iter.next().get(0))==5);
    }
}

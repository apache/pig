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

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.Iterator;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.builtin.mock.Storage;
import org.apache.pig.data.Tuple;
import org.junit.AfterClass;
import org.junit.Test;

public class TestPigServerWithMacros {
    private static MiniCluster cluster = MiniCluster.buildCluster();

    @AfterClass
    public static void tearDown() throws Exception {
        cluster.shutDown();
    }

    @Test
    public void testRegisterRemoteMacro() throws Throwable {
        PigServer pig = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());

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

        assertEquals(2L, ((Long)iter.next().get(0)).longValue());

        pig.shutdown();
    }

    @Test
    public void testInlineMacro() throws Throwable {
        PigServer pig = new PigServer(ExecType.LOCAL);

        Storage.Data data = resetData(pig);
        data.set("some_path", "(l:chararray)", tuple("first row"), tuple("second row"));

        pig.registerQuery("DEFINE row_count(X) RETURNS Z { Y = group $X all; $Z = foreach Y generate COUNT($X); };");
        pig.registerQuery("a = load 'some_path' USING mock.Storage();");
        pig.registerQuery("b = row_count(a);");
        Iterator<Tuple> iter = pig.openIterator("b");

        assertEquals(2L, ((Long)iter.next().get(0)).longValue());

        pig.shutdown();
    }
    
    @Test
    public void testRegisterResourceMacro() throws Throwable {
        PigServer pig = new PigServer(ExecType.LOCAL);

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
        
        assertTrue(((Long)iter.next().get(0))==5);

        pig.shutdown();
    }
}

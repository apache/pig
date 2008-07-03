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
import java.io.IOException;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.util.List;
import java.util.Iterator;
import java.net.URL;
import java.net.URLClassLoader;
import java.lang.reflect.Method;

import org.apache.pig.PigServer;
import org.apache.pig.PigServer.ExecType;

import org.junit.Test;
import junit.framework.TestCase;


public class TestPigServer extends PigExecTestCase {
    
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
            assertTrue(nameIsSubstring);
            assertTrue(count == 1);
        }
        else {
            assertFalse(nameIsSubstring);
        }
    }
    
    // creates an empty jar file
    private static void createFakeJarFile(String location, String name) 
            throws IOException {
        assertFalse((new File(name)).canRead());
        
        assertTrue((new File(location)).mkdirs());
        
        assertTrue((new File(location + FILE_SEPARATOR + name)).
                    createNewFile());
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
    
    private static void executeShellCommand(String cmd) throws Exception {
        Process cmdProc = Runtime.getRuntime().exec(cmd);
        
        cmdProc.waitFor();
        
        assertTrue(cmdProc.exitValue() == 0);
    }
    
    /**
     * The jar file to register is not present
     */
    @Test
    public void testRegisterJarFileNotPresent() throws Throwable {
        // resister a jar file that does not exist
        
        String jarName = "BadFileNameTestJarNotPresent.jar";
        
        // jar name is not present to start with
        verifyStringContained(pigServer.getPigContext().extraJars, jarName, false);

        boolean exceptionRaised = false;
        try {
            pigServer.registerJar(jarName);
        }
        catch (IOException e) {
            exceptionRaised = true;
        }        
        assertTrue(exceptionRaised);
        verifyStringContained(pigServer.getPigContext().extraJars, jarName, false);
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
        
        verifyStringContained(pigServer.getPigContext().extraJars, jarName, false);
        
        boolean exceptionRaised = false;
        try {
            pigServer.registerJar(jarLocation + jarName);
        }
        catch (IOException e) {
            exceptionRaised = true;
        }        
        assertFalse(exceptionRaised);
        verifyStringContained(pigServer.getPigContext().extraJars, jarName, true);

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
        
        verifyStringContained(pigServer.getPigContext().extraJars, jarName, false);
        
        registerNewResource(jarLocation1);
        registerNewResource(jarLocation2);
        
        boolean exceptionRaised = false;
        try {
            pigServer.registerJar(jarName);
        }
        catch (IOException e) {
            exceptionRaised = true;
        }
        assertFalse(exceptionRaised);
        verifyStringContained(pigServer.getPigContext().extraJars, jarName, true);

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
        executeShellCommand("javac " + dir + FILE_SEPARATOR + subDir +
                               FILE_SEPARATOR + className + ".java");

        // remove src file
        (new File(dir + FILE_SEPARATOR + subDir +
                  FILE_SEPARATOR + className + ".java")).delete();

        // generate jar file
        executeShellCommand("jar -cf " + dir + FILE_SEPARATOR + jarName + " " +
                              "-C " + dir + " " + subDir);
        
        // remove class file and sub_dir
        (new File(dir + FILE_SEPARATOR + subDir +
                  FILE_SEPARATOR + className + ".class")).delete();
        (new File(dir + FILE_SEPARATOR + subDir)).delete();
        
        // register resource
        registerNewResource(dir + FILE_SEPARATOR + jarName);
        
        // load the specific resource
        boolean exceptionRaised = false;
        try {
            pigServer.registerJar("sub_dir/TestRegisterJar.class");
        }
        catch (IOException e) {
            exceptionRaised = true;
        }
        
        // verify proper jar file is located
        assertFalse(exceptionRaised);
        verifyStringContained(pigServer.getPigContext().extraJars, jarName, true);

        // clean up Jar file and test dir
        (new File(dir + FILE_SEPARATOR + jarName)).delete();
        (new File(dir)).delete();
    }
}
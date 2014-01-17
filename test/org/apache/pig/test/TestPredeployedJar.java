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
import java.util.HashSet;
import java.util.Properties;
import java.util.jar.JarFile;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.impl.util.JarManager;
import org.junit.Assert;
import org.junit.Test;

/**
 * Ensure that jars marked as predeployed are not included in the generated 
 * job jar. 
 */
public class TestPredeployedJar {
    @Test
    public void testPredeployedJar() throws IOException, ClassNotFoundException {
        // The default jar should contain jackson classes
        PigServer pigServer = new PigServer(ExecType.LOCAL, new Properties());
        File jobJarFile = Util.createTempFileDelOnExit("Job", ".jar");
        FileOutputStream fos = new FileOutputStream(jobJarFile);
        JarManager.createJar(fos, new HashSet<String>(), pigServer.getPigContext());
        JarFile jobJar = new JarFile(jobJarFile);
        Assert.assertNotNull(jobJar.getJarEntry("org/codehaus/jackson/JsonParser.class"));
        
        // Now let's mark the jackson jar as predeployed. 
        pigServer = new PigServer(ExecType.LOCAL, new Properties());
        pigServer.getPigContext().markJarAsPredeployed(JarManager.findContainingJar(
                org.codehaus.jackson.JsonParser.class));
        jobJarFile = Util.createTempFileDelOnExit("Job", ".jar");
        fos = new FileOutputStream(jobJarFile);
        JarManager.createJar(fos, new HashSet<String>(), pigServer.getPigContext());
        jobJar = new JarFile(jobJarFile);
        
        // And check that the predeployed jar was not added
        Assert.assertNull(jobJar.getJarEntry("org/codehaus/jackson/JsonParser.class"));
    }
    
    @Test
    public void testPredeployedJarsProperty() throws ExecException {
        Properties p = new Properties();
        p.setProperty("pig.predeployed.jars", "zzz");
        PigServer pigServer = new PigServer(ExecType.LOCAL, p);
        
        Assert.assertTrue(pigServer.getPigContext().predeployedJars.contains("zzz"));
        
        p = new Properties();
        p.setProperty("pig.predeployed.jars", "aaa" + File.pathSeparator + "bbb");
        pigServer = new PigServer(ExecType.LOCAL, p);
        
        Assert.assertTrue(pigServer.getPigContext().predeployedJars.contains("aaa"));
        Assert.assertTrue(pigServer.getPigContext().predeployedJars.contains("bbb"));
        
        Assert.assertFalse(pigServer.getPigContext().predeployedJars.contains("zzz"));
    }
}

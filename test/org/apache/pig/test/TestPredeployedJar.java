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
import java.util.Iterator;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.SimpleLayout;
import org.apache.pig.ExecType;
import org.apache.pig.PigConfiguration;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.JobControlCompiler;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.JarManager;
import org.junit.Assert;
import org.junit.Test;

/**
 * Ensure that jars marked as predeployed are not included in the generated 
 * job jar. 
 */
public class TestPredeployedJar {
    static MiniGenericCluster cluster = MiniGenericCluster.buildCluster();
    @Test
    public void testPredeployedJar() throws IOException, ClassNotFoundException {
        Logger logger = Logger.getLogger(JobControlCompiler.class);
        logger.removeAllAppenders();
        logger.setLevel(Level.INFO);
        SimpleLayout layout = new SimpleLayout();
        File logFile = File.createTempFile("log", "");
        FileAppender appender = new FileAppender(layout, logFile.toString(), false, false, 0);
        logger.addAppender(appender);
        
        PigServer pigServer = new PigServer(ExecType.MAPREDUCE, cluster.getConfiguration());
        pigServer.getPigContext().getProperties().put(PigConfiguration.PIG_OPT_FETCH, "false");
        String[] inputData = new String[] { "hello", "world" };
        Util.createInputFile(cluster, "a.txt", inputData);
        String guavaJar = JarManager.findContainingJar(com.google.common.collect.Multimaps.class);

        pigServer.registerQuery("a = load 'a.txt' as (line:chararray);");
        Iterator<Tuple> it = pigServer.openIterator("a");

        String content = FileUtils.readFileToString(logFile);
        Assert.assertTrue(content.contains(guavaJar));
        
        logFile = File.createTempFile("log", "");
        
        // Now let's mark the guava jar as predeployed.
        pigServer.getPigContext().markJarAsPredeployed(guavaJar);
        it = pigServer.openIterator("a");

        content = FileUtils.readFileToString(logFile);
        Assert.assertFalse(content.contains(guavaJar));
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

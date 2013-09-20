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
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import junit.framework.Assert;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecJob;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MRExecutionEngine;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MapReduceLauncher;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MapReduceOper;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROperPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.tools.pigstats.PigStats;
import org.apache.pig.tools.pigstats.PigStats.JobGraph;
import org.apache.pig.tools.pigstats.ScriptState;
import org.apache.pig.tools.pigstats.mapreduce.MRScriptState;
import org.junit.Test;

public class TestPigStats  {

    private static final Log LOG = LogFactory.getLog(TestPigStats.class);

    @Test
    public void testPigScriptInConf() throws Exception {
        PrintWriter w = new PrintWriter(new FileWriter("test.pig"));
        w.println("register /mydir/sath.jar");
        w.println("register /mydir/lib/hadoop-tools-0.20.201.0-SNAPSHOT.jar");
        w.println("register /mydir/lib/jackson-core-asl-1.4.2.jar");
        w.println("register /mydir/lib/jackson-mapper-asl-1.4.2.jar");
        w.close();
        
        MRScriptState ss = MRScriptState.get();
        ss.setScript(new File("test.pig"));
        Configuration conf = new Configuration();
        MapReduceOper mro = new MapReduceOper(new OperatorKey());
        ss.addSettingsToConf(mro, conf);
        
        String s = conf.get("pig.script");
        String script = new String(Base64.decodeBase64(s.getBytes()));
        
        String expected = 
            "register /mydir/sath.jar\n" +
            "register /mydir/lib/hadoop-tools-0.20.201.0-SNAPSHOT.jar\n" +
            "register /mydir/lib/jackson-core-asl-1.4.2.jar\n"  +
            "register /mydir/lib/jackson-mapper-asl-1.4.2.jar\n";
        
        Assert.assertEquals(expected, script);
    }
    
    @Test
    public void testJythonScriptInConf() throws Exception {
        String[] script = {
                "#!/usr/bin/python",
                "from org.apache.pig.scripting import *",
                "Pig.fs(\"rmr simple_out\")",
                "input = 'simple_table'",
                "output = 'simple_out'",
                "P = Pig.compile(\"\"\"a = load '$input';store a into '$output';\"\"\")",
                "Q = P.bind({'input':input, 'output':output})",
                "stats = Q.runSingle()",
                "if stats.isSuccessful():",
                "\tprint 'success!'",
                "else:",
                "\traise 'failed'"
        };
        
        Util.createLocalInputFile( "testScript.py", script);
        
        MRScriptState ss = MRScriptState.get();
        ss.setScript(new File("testScript.py"));
        Configuration conf = new Configuration();
        MapReduceOper mro = new MapReduceOper(new OperatorKey());
        ss.addSettingsToConf(mro, conf);
        
        String s = conf.get("pig.script");
        String actual = new String(Base64.decodeBase64(s.getBytes()));
        
        String expected = 
            "#!/usr/bin/python\n" +
            "from org.apache.pig.scripting import *\n" +
            "Pig.fs(\"rmr simple_out\")\n" +
            "input = 'simple_table'\n" +
            "output = 'simple_out'\n" +
            "P = Pig.compile(\"\"\"a = load '$input';store a into '$output';\"\"\")\n" +
            "Q = P.bind({'input':input, 'output':output})\n" +
            "stats = Q.runSingle()\n" +
            "if stats.isSuccessful():\n" +
            "\tprint 'success!'\n" +
            "else:\n" +
            "\traise 'failed'\n";
        
        Assert.assertEquals(expected, actual);
    }
    
    @Test
    public void testBytesWritten_JIRA_1027() {

        File outputFile = null;
        try {
            String fileName = this.getClass().getName() + "_" + "testBytesWritten_JIRA_1027";
            outputFile = File.createTempFile(fileName, ".out");
            String filePath = outputFile.getAbsolutePath();
            outputFile.delete();
            PigServer pig = new PigServer(ExecType.LOCAL);
            pig.registerQuery("A = load 'test/org/apache/pig/test/data/passwd';");
            ExecJob job = pig.store("A", filePath);
            PigStats stats = job.getStatistics();
            File dataFile = new File( outputFile.getAbsoluteFile() + File.separator + "part-00000" );
            assertEquals(dataFile.length(), stats.getBytesWritten());
        } catch (IOException e) {
            LOG.error("Error while generating file", e);
            fail("Encountered IOException");
        } finally {
            if (outputFile != null) {
                // Hadoop Local mode creates a directory
                // Hence we need to delete a directory recursively
                deleteDirectory(outputFile);
            }
        }
    }
    
    @Test
    public void testPigStatsAlias() throws Exception {
        try {
            PigServer pig = new PigServer(ExecType.LOCAL);
            pig.setBatchOn();
            pig.registerQuery("A = load 'input' as (name, age, gpa);");
            pig.registerQuery("B = group A by name;");
            pig.registerQuery("C = foreach B generate group, COUNT(A);");
            pig.registerQuery("D = order C by $1;");
            pig.registerQuery("E = limit D 10;");
            pig.registerQuery("store E into 'alias_output';");
            
            LogicalPlan lp = getLogicalPlan(pig);
            PhysicalPlan pp = ((MRExecutionEngine)pig.getPigContext().getExecutionEngine()).compile(lp,
                    null);
            MROperPlan mp = getMRPlan(pp, pig.getPigContext());
            assertEquals(4, mp.getKeys().size());
            
            MapReduceOper mro = mp.getRoots().get(0);
            assertEquals("A,B,C", getAlias(mro));
            
            mro = mp.getSuccessors(mro).get(0);
            assertEquals("D", getAlias(mro));
             
            mro = mp.getSuccessors(mro).get(0);
            assertEquals("D", getAlias(mro));
        } finally {
            File outputfile = new File("alias_output");
            if (outputfile.exists()) {
                // Hadoop Local mode creates a directory
                // Hence we need to delete a directory recursively
                deleteDirectory(outputfile);
            }
        }
    }
    
    @Test
    public void testPigStatsGetList() {
        File outputFile = null;
        try {
            String filename = this.getClass().getSimpleName() + "_" + "testPigStatsGetList";
            outputFile = File.createTempFile(filename, ".out");
            String filePath = outputFile.getAbsolutePath();
            outputFile.delete();
            PigServer pigServer = new PigServer(ExecType.LOCAL);
            pigServer.registerQuery("a = load 'test/org/apache/pig/test/data/passwd';");
            pigServer.registerQuery("b = group a by $0;");
            pigServer.registerQuery("c = foreach b generate group, COUNT(a) as cnt;");
            pigServer.registerQuery("d = group c by cnt;");
            pigServer.registerQuery("e = foreach d generate group;");
            ExecJob job = pigServer.store("e", filePath);
            JobGraph jobGraph = job.getStatistics().getJobGraph();
            assertEquals(2, jobGraph.getJobList().size());

        } catch (IOException e) {
            LOG.error("IOException while creating file ", e);
            fail("Encountered IOException");
        } finally {
            if (outputFile != null) {
                // delete the directory before returning
                deleteDirectory(outputFile);
            }
        }
    }
    
    private void deleteDirectory(File dir) {
        try {
            FileUtils.deleteDirectory(dir);
        } catch (IOException e) {
            LOG.error("Could not delete directory " + dir, e);
        }
    }

    public static LogicalPlan getLogicalPlan(PigServer pig) throws Exception {
        java.lang.reflect.Method buildLp = pig.getClass().getDeclaredMethod("buildLp");
        buildLp.setAccessible(true);
        return (LogicalPlan ) buildLp.invoke( pig );
    }
    
    public static MROperPlan getMRPlan(PhysicalPlan pp, PigContext ctx) throws Exception {
        MapReduceLauncher launcher = new MapReduceLauncher();
        java.lang.reflect.Method compile = launcher.getClass()
                .getDeclaredMethod("compile",
                        new Class[] { PhysicalPlan.class, PigContext.class });
        compile.setAccessible(true);
        return (MROperPlan) compile.invoke(launcher, new Object[] { pp, ctx });
    }
           
    public static String getAlias(MapReduceOper mro) throws Exception {
        ScriptState ss = ScriptState.get();
        java.lang.reflect.Method getAlias = ss.getClass()
                .getDeclaredMethod("getAlias",
                        new Class[] { MapReduceOper.class });
        getAlias.setAccessible(true);
        return (String)getAlias.invoke(ss, new Object[] { mro });
    }
         
}

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

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;

import junit.framework.Assert;

import org.apache.pig.ExecType;
import org.apache.pig.PigException;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecJob;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MapReduceLauncher;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MapReduceOper;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROperPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.util.MapRedUtil;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.logicalLayer.LogicalPlan;
import org.apache.pig.impl.util.LogUtils;
import org.apache.pig.tools.pigscript.parser.ParseException;
import org.apache.pig.tools.pigstats.PigStats;
import org.apache.pig.tools.pigstats.ScriptState;
import org.junit.Before;
import org.junit.Test;

public class TestPigStats  {

    @Test
    public void testBytesWritten_JIRA_1027() {

        File outputFile = null;
        try {
            outputFile = File.createTempFile("JIAR_1027", ".out");
            String filePath = outputFile.getAbsolutePath();
            outputFile.delete();
            PigServer pig = new PigServer(ExecType.LOCAL);
            pig.registerQuery("A = load 'test/org/apache/pig/test/data/passwd';");
            ExecJob job = pig.store("A", filePath);
            PigStats stats = job.getStatistics();
            File dataFile = new File( outputFile.getAbsoluteFile() + File.separator + "part-00000" );
            assertEquals(dataFile.length(), stats.getBytesWritten());
        } catch (IOException e) {
            e.printStackTrace();
            System.err.println( e.getMessage() );
            fail("IOException happened");
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
            pig.registerQuery("A = load 'input' as (name, age, gpa);");
            pig.registerQuery("B = group A by name;");
            pig.registerQuery("C = foreach B generate group, COUNT(A);");
            pig.registerQuery("D = order C by $1;");
            pig.registerQuery("E = limit D 10;");
            pig.registerQuery("store E into 'alias_output';");
            
            LogicalPlan lp = getLogicalPlan(pig);
            PhysicalPlan pp = pig.getPigContext().getExecutionEngine().compile(lp,
                    null);
            MROperPlan mp = getMRPlan(pp, pig.getPigContext());
            
            assertEquals(3, mp.getKeys().size());
            
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
    
    private void deleteDirectory( File dir ) {
        File[] files = dir.listFiles();
        for( File file : files ) {
            if( file.isDirectory() ) {
                deleteDirectory(file);
            } else {
                file.delete();
            }
        }
        dir.delete();
    }
    
    public static LogicalPlan getLogicalPlan(PigServer pig) throws Exception {
        java.lang.reflect.Method compileLp = pig.getClass()
                .getDeclaredMethod("compileLp",
                        new Class[] { String.class });
        compileLp.setAccessible(true);
        return (LogicalPlan) compileLp.invoke(pig, new Object[] { null });
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

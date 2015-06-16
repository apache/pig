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
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecJob;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MapReduceLauncher;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MapReduceOper;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROperPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.tools.pigstats.PigStats;
import org.apache.pig.tools.pigstats.PigStats.JobGraph;
import org.apache.pig.tools.pigstats.ScriptState;
import org.apache.pig.tools.pigstats.mapreduce.MRScriptState;
import org.junit.Test;

public class TestPigStatsMR extends TestPigStats {

    @Override
    @Test
    public void testBytesWritten_JIRA_1027() throws Exception {

        FileLocalizer.setInitialized(false);
        // This test cannot be run in MR local mode due to lack of counters
        MiniGenericCluster cluster = MiniGenericCluster.buildCluster();
        try {
            String filePath = "/tmp/" + this.getClass().getName() + "_"
                    + "testBytesWritten_JIRA_1027";

            PigServer pig = new PigServer(cluster.getExecType(), cluster.getProperties());
            String inputFile = "test/org/apache/pig/test/data/passwd";
            Util.copyFromLocalToCluster(cluster, inputFile, inputFile);
            pig.registerQuery("A = load '" + inputFile + "';");
            ExecJob job = pig.store("A", filePath);
            PigStats stats = job.getStatistics();
            Path dataFile = Util.getFirstPartFile(new Path(filePath));
            FileStatus fs = cluster.getFileSystem().getFileStatus(dataFile);
            assertEquals(fs.getLen(), stats.getBytesWritten());
        } catch (IOException e) {
            LOG.error("Error while generating file", e);
            fail("Encountered IOException");
        } finally {
            FileLocalizer.setInitialized(false);
            cluster.shutDown();
        }
    }

    @Override
    public void addSettingsToConf(Configuration conf, String scriptFileName) {
        MRScriptState ss = MRScriptState.get();
        ss.setScript(new File(scriptFileName));
        MapReduceOper mro = new MapReduceOper(new OperatorKey());
        ss.addSettingsToConf(mro, conf);
    }

    @Override
    public void checkPigStats(ExecJob job) {
        JobGraph jobGraph = job.getStatistics().getJobGraph();
        assertEquals(2, jobGraph.getJobList().size());
    }

    @Override
    public void checkPigStatsAlias(PhysicalPlan pp, PigContext pc) throws Exception {
        MROperPlan mp = getMRPlan(pp, pc);
        assertEquals(4, mp.getKeys().size());

        MapReduceOper mro = mp.getRoots().get(0);
        assertEquals("A,B,C", getAlias(mro));

        mro = mp.getSuccessors(mro).get(0);
        assertEquals("D", getAlias(mro));

        mro = mp.getSuccessors(mro).get(0);
        assertEquals("D", getAlias(mro));
    }

    private static MROperPlan getMRPlan(PhysicalPlan pp, PigContext ctx) throws Exception {
        MapReduceLauncher launcher = new MapReduceLauncher();
        java.lang.reflect.Method compile = launcher.getClass()
                .getDeclaredMethod("compile",
                        new Class[] { PhysicalPlan.class, PigContext.class });
        compile.setAccessible(true);
        return (MROperPlan) compile.invoke(launcher, new Object[] { pp, ctx });
    }

    private static String getAlias(MapReduceOper mro) throws Exception {
        ScriptState ss = ScriptState.get();
        java.lang.reflect.Method getAlias = ss.getClass()
                .getDeclaredMethod("getAlias",
                        new Class[] { MapReduceOper.class });
        getAlias.setAccessible(true);
        return (String)getAlias.invoke(ss, new Object[] { mro });
    }
}

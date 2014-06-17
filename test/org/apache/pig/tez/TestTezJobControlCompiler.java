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
package org.apache.pig.tez;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.pig.PigConfiguration;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.tez.MultiQueryOptimizerTez;
import org.apache.pig.backend.hadoop.executionengine.tez.TezCompiler;
import org.apache.pig.backend.hadoop.executionengine.tez.TezJobControlCompiler;
import org.apache.pig.backend.hadoop.executionengine.tez.TezLocalExecType;
import org.apache.pig.backend.hadoop.executionengine.tez.TezOperPlan;
import org.apache.pig.backend.hadoop.executionengine.tez.TezOperator;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.util.Pair;
import org.apache.pig.test.Util;
import org.apache.pig.test.junit.OrderedJUnit4Runner;
import org.apache.pig.test.junit.OrderedJUnit4Runner.TestOrder;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.Vertex;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * Test cases to test the TezJobControlCompiler.
 */
@RunWith(OrderedJUnit4Runner.class)
@TestOrder({
    "testRun1",
    "testRun2",
    "testRun3",
    "testTezParallelismEstimatorOrderBy",
    "testTezParallelismEstimatorFilterFlatten",
    "testTezParallelismEstimatorHashJoin",
    "testTezParallelismEstimatorSplitBranch",
    "testTezParallelismDefaultParallelism"
})
public class TestTezJobControlCompiler {
    private static PigContext pc;
    private static PigServer pigServer;
    private static URI input1;
    private static URI input2;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        input1 = Util.createTempFileDelOnExit("input1", "txt").toURI();
        input2 = Util.createTempFileDelOnExit("input2", "txt").toURI();
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
    }

    @Before
    public void setUp() throws ExecException {
        pc = new PigContext(new TezLocalExecType(), new Properties());
        pigServer = new PigServer(pc);
    }

    @Test
    public void testRun1() throws Exception {
        String query =
                "a = load '" + input1 +"' as (x:int, y:int);" +
                "b = filter a by x > 0;" +
                "c = foreach b generate y;" +
                "store c into 'file:///tmp/output';";

        Pair<TezOperPlan, DAG> compiledPlan = compile(query);

        // Make sure DAG has a single vertex.
        List<TezOperator> roots = compiledPlan.first.getRoots();
        assertEquals(1, roots.size());

        Vertex root = compiledPlan.second.getVertex(roots.get(0).getOperatorKey().toString());
        assertNotNull(root);
        assertEquals(0, root.getInputVertices().size());
        assertEquals(0, root.getOutputVertices().size());
    }

    @Test
    public void testRun2() throws Exception {
        String query =
                "a = load '" + input1 +"' as (x:int, y:int);" +
                "b = group a by x;" +
                "c = foreach b generate group, a;" +
                "store c into 'file:///tmp/output';";

        Pair<TezOperPlan, DAG> compiledPlan = compile(query);

        // Make sure DAG has two vertices, and the root vertex is the input of
        // the leaf vertex.
        List<TezOperator> roots = compiledPlan.first.getRoots();
        assertEquals(1, roots.size());
        List<TezOperator> leaves = compiledPlan.first.getLeaves();
        assertEquals(1, leaves.size());

        Vertex root = compiledPlan.second.getVertex(roots.get(0).getOperatorKey().toString());
        assertNotNull(root);
        assertEquals(0, root.getInputVertices().size());
        assertEquals(1, root.getOutputVertices().size());

        Vertex leaf = compiledPlan.second.getVertex(leaves.get(0).getOperatorKey().toString());
        assertNotNull(leaf);
        assertEquals(1, leaf.getInputVertices().size());
        assertEquals(0, leaf.getOutputVertices().size());

        assertEquals(root.getOutputVertices().get(0), leaf);
        assertEquals(root, leaf.getInputVertices().get(0));
    }

    @Test
    public void testRun3() throws Exception {
        String query =
                "a = load '" + input1 +"' as (x:int, y:int);" +
                "b = load '" + input2 +"' as (x:int, z:int);" +
                "c = join a by x, b by x;" +
                "d = foreach c generate a::x as x, y, z;" +
                "store d into 'file:///tmp/output';";

        Pair<TezOperPlan, DAG> compiledPlan = compile(query);

        // Make sure DAG has three vertices, and the two root vertices are the
        // input of the leaf vertex.
        List<TezOperator> roots = compiledPlan.first.getRoots();
        assertEquals(2, roots.size());
        List<TezOperator> leaves = compiledPlan.first.getLeaves();
        assertEquals(1, leaves.size());

        Vertex root0 = compiledPlan.second.getVertex(roots.get(0).getOperatorKey().toString());
        Vertex root1 = compiledPlan.second.getVertex(roots.get(1).getOperatorKey().toString());
        assertNotNull(root0);
        assertNotNull(root1);
        assertEquals(0, root0.getInputVertices().size());
        assertEquals(1, root1.getOutputVertices().size());

        Vertex leaf = compiledPlan.second.getVertex(leaves.get(0).getOperatorKey().toString());
        assertNotNull(leaf);
        assertEquals(2, leaf.getInputVertices().size());
        assertEquals(0, leaf.getOutputVertices().size());

        assertEquals(root0.getOutputVertices().get(0), leaf);
        assertEquals(root1.getOutputVertices().get(0), leaf);
        assertTrue(leaf.getInputVertices().contains(root0));
        assertTrue(leaf.getInputVertices().contains(root1));
    }

    static public class ArbitarySplitsInputformat extends TextInputFormat {
        @Override
        public List<InputSplit> getSplits(JobContext job) throws IOException {
            String inputDir = job.getConfiguration().get(INPUT_DIR, "");
            String numSplitString = inputDir.substring(inputDir.lastIndexOf(File.separator)+1);
            int numSplit = Integer.parseInt(numSplitString);
            List<InputSplit> splits = new ArrayList<InputSplit>();
            for (int i=0;i<numSplit;i++) {
                splits.add(new FileSplit(new Path("dummy"), 0, 0, null));
            }
            return splits;
        }
    }

    static public class ArbitarySplitsLoader extends PigStorage {
        public ArbitarySplitsLoader() {}

        @Override
        public InputFormat getInputFormat() {
            return new ArbitarySplitsInputformat();
        }
    }

    @Test
    public void testTezParallelismEstimatorOrderBy() throws Exception{
        pc.getProperties().setProperty(PigConfiguration.PIG_NO_SPLIT_COMBINATION, "true");
        String query = "a = load '2' using " + ArbitarySplitsLoader.class.getName()
                + "() as (name:chararray, age:int, gpa:double);"
                + "b = group a by name parallel 3;"
                + "c = foreach b generate group as name, AVG(a.age) as age;"
                + "d = order c by age;"
                + "store d into 'output';";
        Pair<TezOperPlan, DAG> compiledPlan = compile(query);
        TezOperator sortOper = compiledPlan.first.getLeaves().get(0);
        Vertex sortVertex = compiledPlan.second.getVertex(sortOper.getOperatorKey().toString());
        assertEquals(sortVertex.getParallelism(), -1);
    }

    @Test
    public void testTezParallelismEstimatorFilterFlatten() throws Exception{
        pc.getProperties().setProperty(PigConfiguration.PIG_NO_SPLIT_COMBINATION, "true");
        String query = "a = load '10' using " + ArbitarySplitsLoader.class.getName()
                + "() as (name:chararray, age:int, gpa:double);"
                + "b = filter a by age>20;"
                + "c = group b by name;"
                + "d = foreach c generate group, flatten(b.gpa);"
                + "e = group d by group;"
                + "store e into 'output';";
        Pair<TezOperPlan, DAG> compiledPlan = compile(query);
        TezOperator leafOper = compiledPlan.first.getLeaves().get(0);
        Vertex leafVertex = compiledPlan.second.getVertex(leafOper.getOperatorKey().toString());
        assertEquals(leafVertex.getParallelism(), 70);
    }

    @Test
    public void testTezParallelismEstimatorHashJoin() throws Exception{
        pc.getProperties().setProperty(PigConfiguration.PIG_NO_SPLIT_COMBINATION, "true");
        String query = "a = load '10' using " + ArbitarySplitsLoader.class.getName()
                + "() as (name:chararray, age:int, gpa:double);"
                + "b = load '5' using " + ArbitarySplitsLoader.class.getName()
                + "() as (name:chararray, course:chararray);"
                + "c = join a by name, b by name;"
                + "store c into 'output';";
        Pair<TezOperPlan, DAG> compiledPlan = compile(query);
        TezOperator leafOper = compiledPlan.first.getLeaves().get(0);
        Vertex leafVertex = compiledPlan.second.getVertex(leafOper.getOperatorKey().toString());
        assertEquals(leafVertex.getParallelism(), 15);
    }
    
    @Test
    public void testTezParallelismEstimatorSplitBranch() throws Exception{
        pc.getProperties().setProperty(PigConfiguration.PIG_NO_SPLIT_COMBINATION, "true");
        String query = "a = load '10' using " + ArbitarySplitsLoader.class.getName()
                + "() as (name:chararray, age:int, gpa:double);"
                + "b = filter a by age>20;"
                + "c = filter a by age>50;"
                + "store b into 'o1';"
                + "d = group c by name;"
                + "store d into 'o2';";
        Pair<TezOperPlan, DAG> compiledPlan = compile(query);
        TezOperator leafOper = compiledPlan.first.getLeaves().get(0);
        Vertex leafVertex = compiledPlan.second.getVertex(leafOper.getOperatorKey().toString());
        assertEquals(leafVertex.getParallelism(), 7);
    }
    
    @Test
    public void testTezParallelismDefaultParallelism() throws Exception{
        pc.getProperties().setProperty(PigConfiguration.PIG_NO_SPLIT_COMBINATION, "true");
        pc.defaultParallel = 5;
        String query = "a = load '10' using " + ArbitarySplitsLoader.class.getName()
                + "() as (name:chararray, age:int, gpa:double);"
                + "b = group a by name;"
                + "store b into 'output';";
        Pair<TezOperPlan, DAG> compiledPlan = compile(query);
        TezOperator leafOper = compiledPlan.first.getLeaves().get(0);
        Vertex leafVertex = compiledPlan.second.getVertex(leafOper.getOperatorKey().toString());
        assertEquals(leafVertex.getParallelism(), 5);
    }

    private Pair<TezOperPlan, DAG> compile(String query) throws Exception {
        PhysicalPlan pp = Util.buildPp(pigServer, query);
        TezCompiler comp = new TezCompiler(pp, pc);
        TezOperPlan tezPlan = comp.compile();
        TezJobControlCompiler jobComp = new TezJobControlCompiler(pc, new Configuration());
        MultiQueryOptimizerTez mqOptimizer = new MultiQueryOptimizerTez(tezPlan);
        mqOptimizer.visit();
        DAG dag = jobComp.buildDAG(tezPlan, new HashMap<String, LocalResource>());
        return new Pair<TezOperPlan, DAG>(tezPlan, dag);
    }
}


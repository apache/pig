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

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.Properties;

import org.apache.pig.PigConfiguration;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.tez.TezLauncher;
import org.apache.pig.backend.hadoop.executionengine.tez.TezLocalExecType;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.TezPlanContainer;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.TezPlanContainerPrinter;
import org.apache.pig.builtin.OrcStorage;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.plan.NodeIdGenerator;
import org.apache.pig.test.TestMultiQueryBasic.DummyStoreWithOutputFormat;
import org.apache.pig.test.Util;
import org.apache.pig.test.utils.TestHelper;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test cases to test the TezCompiler. VERY IMPORTANT NOTE: The tests here
 * compare results with a "golden" set of outputs. In each test case here, the
 * operators generated have a random operator key which uses Java's Random
 * class. So if there is a code change which changes the number of operators
 * created in a plan, then  the "golden" file for that test case
 * need to be changed.
 */

public class TestTezCompiler {
    private static PigContext pc;
    private static PigServer pigServer;
    private static final int MAX_SIZE = 100000;

    // If for some reason, the golden files need to be regenerated, set this to
    // true - THIS WILL OVERWRITE THE GOLDEN FILES - So use this with caution
    // and only for the test cases you need and are sure of.
    private boolean generate = false;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        pc = new PigContext(new TezLocalExecType(), new Properties());
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
    }

    @Before
    public void setUp() throws ExecException {
        resetScope();
        pc.getProperties().remove(PigConfiguration.PIG_OPT_MULTIQUERY);
        pc.getProperties().remove(PigConfiguration.PIG_TEZ_OPT_UNION);
        pc.getProperties().remove(PigConfiguration.PIG_EXEC_NO_SECONDARY_KEY);
        pigServer = new PigServer(pc);
    }

    private void resetScope() {
        NodeIdGenerator.reset();
        PigServer.resetScope();
        TezPlanContainer.resetScope();
    }

    @Test
    public void testFilter() throws Exception {
        String query =
                "a = load 'file:///tmp/input' as (x:int, y:int);" +
                "b = filter a by x > 0;" +
                "c = foreach b generate y;" +
                "store c into 'file:///tmp/output';";

        run(query, "test/org/apache/pig/test/data/GoldenFiles/tez/TEZC-Filter-1.gld");
    }

    @Test
    public void testGroupBy() throws Exception {
        String query =
                "a = load 'file:///tmp/input' as (x:int, y:int);" +
                "b = group a by x;" +
                "c = foreach b generate group, COUNT(a.x);" +
                "store c into 'file:///tmp/output';";

        run(query, "test/org/apache/pig/test/data/GoldenFiles/tez/TEZC-Group-1.gld");
    }

    @Test
    public void testJoin() throws Exception {
        String query =
                "a = load 'file:///tmp/input1' as (x:int, y:int);" +
                "b = load 'file:///tmp/input2' as (x:int, z:int);" +
                "c = join a by x, b by x;" +
                "d = foreach c generate a::x as x, y, z;" +
                "store d into 'file:///tmp/output';";

        run(query, "test/org/apache/pig/test/data/GoldenFiles/tez/TEZC-Join-1.gld");
    }

    @Test
    public void testSelfJoin() throws Exception {
        String query =
                "a = load 'file:///tmp/input1' as (x:int, y:int);" +
                "b = filter a by x < 5;" +
                "c = filter a by x == 10;" +
                "d = filter a by x > 10;" +
                "e = join b by x, c by x, d by x;" +
                "store e into 'file:///tmp/output';";

        run(query, "test/org/apache/pig/test/data/GoldenFiles/tez/TEZC-SelfJoin-1.gld");
    }

    @Test
    public void testSelfJoinSkewed() throws Exception {
        String query =
                "a = load 'file:///tmp/input1' as (x:int, y:int);" +
                "b = filter a by x < 5;" +
                "c = filter a by x == 10;" +
                "d = join b by x, c by x using 'skewed';" +
                "store d into 'file:///tmp/output';";

        run(query, "test/org/apache/pig/test/data/GoldenFiles/tez/TEZC-SelfJoin-2.gld");
    }

    @Test
    public void testSelfJoinReplicated() throws Exception {
        String query =
                "a = load 'file:///tmp/input1' as (x:int, y:int);" +
                "b = filter a by x < 5;" +
                "c = filter a by x == 10;" +
                "d = filter a by x > 10;" +
                "e = join b by x, c by x, d by x using 'replicated';" +
                "store e into 'file:///tmp/output';";

        run(query, "test/org/apache/pig/test/data/GoldenFiles/tez/TEZC-SelfJoin-3.gld");
    }

    @Test
    public void testCross() throws Exception {
        String query =
                "a = load 'file:///tmp/input1' as (x:int, y:int);" +
                "b = load 'file:///tmp/input2' as (x:int, z:int);" +
                "c = cross a, b;" +
                "d = foreach c generate a::x as x, y, z;" +
                "store d into 'file:///tmp/output';";

        run(query, "test/org/apache/pig/test/data/GoldenFiles/tez/TEZC-Cross-1.gld");
    }

    @Test
    public void testSelfCross() throws Exception {
        String query =
                "a = load 'file:///tmp/input1' as (x:int, y:int);" +
                "b = filter a by x < 5;" +
                "c = filter a by x == 10;" +
                "d = cross b, c;" +
                "store d into 'file:///tmp/output';";

        run(query, "test/org/apache/pig/test/data/GoldenFiles/tez/TEZC-Cross-2.gld");
    }

    @Test
    public void testSkewedJoin() throws Exception {
        String query =
                "a = load 'file:///tmp/input1' as (x:int, y:int);" +
                "b = load 'file:///tmp/input2' as (x:int, z:int);" +
                "c = join a by x, b by x using 'skewed';" +
                "d = foreach c generate a::x as x, y, z;" +
                "store d into 'file:///tmp/output';";

        run(query, "test/org/apache/pig/test/data/GoldenFiles/tez/TEZC-SkewJoin-1.gld");
    }

    @Test
    public void testSkewedJoinFilter() throws Exception {
        String query =
                "a = load 'file:///tmp/input1' as (x:int, y:int);" +
                "a = filter a by x == 1;" +
                "b = load 'file:///tmp/input2' as (x:int, z:int);" +
                "c = join a by x, b by x using 'skewed';" +
                "d = foreach c generate a::x as x, y, z;" +
                "store d into 'file:///tmp/output';";

        run(query, "test/org/apache/pig/test/data/GoldenFiles/tez/TEZC-SkewJoin-2.gld");
    }

    @Test
    public void testLimit() throws Exception {
        String query =
                "a = load 'file:///tmp/input' as (x:int, y:int);" +
                "b = limit a 10;" +
                "c = foreach b generate y;" +
                "store c into 'file:///tmp/output';";

        run(query, "test/org/apache/pig/test/data/GoldenFiles/tez/TEZC-Limit-1.gld");
    }

    @Test
    public void testLimitOrderby() throws Exception {
        String query =
                "a = load 'file:///tmp/input' as (x:int, y:int);" +
                "b = order a by x, y;" +
                "c = limit b 10;" +
                "store c into 'file:///tmp/output';";

        run(query, "test/org/apache/pig/test/data/GoldenFiles/tez/TEZC-Limit-2.gld");
    }

    @Test
    public void testLimitScalarOrderby() throws Exception {
        String query =
                "a = load 'file:///tmp/input' as (x:int, y:int);" +
                "b = order a by x, y;" +
                "g = group a all;" +
                "h = foreach g generate COUNT(a) as sum;" +
                "c = limit b h.sum/2;" +
                "store c into 'file:///tmp/output';";

        run(query, "test/org/apache/pig/test/data/GoldenFiles/tez/TEZC-Limit-3.gld");
    }

    @Test
    public void testDistinct() throws Exception {
        String query =
                "a = load 'file:///tmp/input' as (x:int, y:int);" +
                "b = distinct a;" +
                "c = foreach b generate y;" +
                "store c into 'file:///tmp/output';";

        run(query, "test/org/apache/pig/test/data/GoldenFiles/tez/TEZC-Distinct-1.gld");
    }

    @Test
    public void testDistinctAlgebraicUdfCombiner() throws Exception {
        String query =
                "a = load 'file:///tmp/input' as (x:int, y:int);" +
                "b = group a by x;" +
                "c = foreach b { d = distinct a; generate COUNT(d); };" +
                "store c into 'file:///tmp/output';";

        run(query, "test/org/apache/pig/test/data/GoldenFiles/tez/TEZC-Distinct-2.gld");
    }



    @Test
    public void testReplicatedJoinInMapper() throws Exception {
        String query =
                "a = load 'file:///tmp/input1' as (x:int, y:int);" +
                "b = load 'file:///tmp/input2' as (x:int, z:int);" +
                "c = load 'file:///tmp/input3' as (x:int, z:int);" +
                "d = join a by x, b by x, c by x using 'replicated';" +
                "store d into 'file:///tmp/output/d';";

        run(query, "test/org/apache/pig/test/data/GoldenFiles/tez/TEZC-FRJoin-1.gld");
    }

    @Test
    public void testReplicatedJoinInReducer() throws Exception {
        String query =
                "a = load 'file:///tmp/input1' as (x:int, y:int);" +
                "b = group a by x;" +
                "b1 = foreach b generate group, COUNT(a.y);" +
                "c = load 'file:///tmp/input2' as (x:int, z:int);" +
                "d = join b1 by group, c by x using 'replicated';" +
                "store d into 'file:///tmp/output/e';";

        run(query, "test/org/apache/pig/test/data/GoldenFiles/tez/TEZC-FRJoin-2.gld");
    }

    @Test
    public void testStream() throws Exception {
        String query =
                "a = load 'file:///tmp/input' using PigStorage(',') as (x:int, y:int);" +
                "b = stream a through `stream.pl -n 5`;" +
                "STORE b INTO 'file:///tmp/output';";

        run(query, "test/org/apache/pig/test/data/GoldenFiles/tez/TEZC-Stream-1.gld");
    }

    @Test
    public void testSecondaryKeySort() throws Exception {
        String query =
                "a = load 'file:///tmp/input' using PigStorage(',') as (x:int, y:int, z:int);" +
                "b = group a by $0;" +
                "c = foreach b { d = limit a 10; e = order d by $1; f = order e by $0; generate group, f;};"+
                "store c INTO 'file:///tmp/output';";

        run(query, "test/org/apache/pig/test/data/GoldenFiles/tez/TEZC-SecKeySort-1.gld");

        // With optimization turned off
        setProperty(PigConfiguration.PIG_EXEC_NO_SECONDARY_KEY, "true");
        run(query, "test/org/apache/pig/test/data/GoldenFiles/tez/TEZC-SecKeySort-2.gld");
    }

    @Test
    public void testOrderBy() throws Exception {
        String query =
                "a = load 'file:///tmp/input' using PigStorage(',') as (x:int, y:int);" +
                "b = order a by x;" +
                "STORE b INTO 'file:///tmp/output';";

        run(query, "test/org/apache/pig/test/data/GoldenFiles/tez/TEZC-Order-1.gld");
    }

    @Test
    public void testOrderByWithFilter() throws Exception {
        String query =
                "a = load 'file:///tmp/input' using PigStorage(',') as (x:int, y:int);" +
                "b = filter a by x == 1;" +
                "c = order b by x;" +
                "STORE c INTO 'file:///tmp/output';";

        run(query, "test/org/apache/pig/test/data/GoldenFiles/tez/TEZC-Order-2.gld");
    }

    @Test
    public void testOrderByReadOnceLoadFunc() throws Exception {
        setProperty("pig.sort.readonce.loadfuncs","org.apache.pig.backend.hadoop.hbase.HBaseStorage,org.apache.pig.backend.hadoop.accumulo.AccumuloStorage");
        String query =
                "a = load 'file:///tmp/input' using org.apache.pig.backend.hadoop.hbase.HBaseStorage(',') as (x:int, y:int);" +
                "b = order a by x;" +
                "STORE b INTO 'file:///tmp/output';";

        run(query, "test/org/apache/pig/test/data/GoldenFiles/tez/TEZC-Order-3.gld");
        setProperty("pig.sort.readonce.loadfuncs", null);
    }

    // PIG-3759, PIG-3781
    // Combiner should not be added in case of co-group
    @Test
    public void testCogroupWithAlgebraiceUDF() throws Exception {
        String query =
                "a = load 'file:///tmp/input1' as (x:int, y:int);" +
                "b = load 'file:///tmp/input2' as (x:int, z:int);" +
                "c = cogroup a by x, b by x;" +
                "d = foreach c generate group, COUNT(a.y), COUNT(b.z);" +
                "store d into 'file:///tmp/output/d';";

        run(query, "test/org/apache/pig/test/data/GoldenFiles/tez/TEZC-Cogroup-1.gld");
    }

    @Test
    public void testMulitQueryWithSplitSingleVertex() throws Exception {
        String query =
                "a = load 'file:///tmp/input' as (x:int, y:int);" +
                "split a into b if x <= 5, c if x <= 10, d if x >10;" +
                "store b into 'file:///tmp/output/b';" +
                "store c into 'file:///tmp/output/c';" +
                "store d into 'file:///tmp/output/d';";

        setProperty(PigConfiguration.PIG_OPT_MULTIQUERY, "" + true);
        run(query, "test/org/apache/pig/test/data/GoldenFiles/tez/TEZC-MQ-1.gld");
        setProperty(PigConfiguration.PIG_OPT_MULTIQUERY, "" + false);
        run(query, "test/org/apache/pig/test/data/GoldenFiles/tez/TEZC-MQ-1-OPTOFF.gld");
    }

    @Test
    public void testMulitQueryWithSplitMultiVertex() throws Exception {
        String query =
                "a = load 'file:///tmp/input' as (x:int, y:int);" +
                "split a into b if x <= 5, c if x <= 10, d if x >10;" +
                "split b into e if x < 3, f if x >= 3;" +
                // No Combiner on the edge to b1/b2 vertex as both b1 and b2 are stored
                "b1 = group b by x;" +
                "b2 = foreach b1 generate group, SUM(b.x);" +
                // Case of two outputs within a split going to same edge as input
                "c1 = join c by x, b by x;" +
                "c2 = group c by x;" +
                // Combiner on the edge to c3 vertex
                "c3 = foreach c2 generate group, SUM(c.x);" +
                "d1 = filter d by x == 5;" +
                "e1 = order e by x;" +
                // TODO: Physical plan has extra split for f1 - 1-2: Split - scope-80
                // POSplit has only 1 sub plan. Optimized and removed in MR plan.
                // Needs to be removed in Tez plan as well.
                "f1 = limit f 1;" +
                "f2 = union d1, f1;" +
                "store b1 into 'file:///tmp/output/b1';" +
                "store b2 into 'file:///tmp/output/b2';" +
                "store c1 into 'file:///tmp/output/c1';" +
                "store c3 into 'file:///tmp/output/c1';" +
                "store d1 into 'file:///tmp/output/d1';" +
                "store e1 into 'file:///tmp/output/e1';" +
                "store f1 into 'file:///tmp/output/f1';" +
                "store f2 into 'file:///tmp/output/f2';";

        setProperty(PigConfiguration.PIG_OPT_MULTIQUERY, "" + true);
        run(query, "test/org/apache/pig/test/data/GoldenFiles/tez/TEZC-MQ-2.gld");
        setProperty(PigConfiguration.PIG_OPT_MULTIQUERY, "" + false);
        run(query, "test/org/apache/pig/test/data/GoldenFiles/tez/TEZC-MQ-2-OPTOFF.gld");
    }

    @Test
    public void testMultiQueryWithGroupBy() throws Exception {
        String query =
                "a = load 'file:///tmp/input' as (x:int, y:int);" +
                "b = group a by x;" +
                "b = foreach b generate group, COUNT(a.x);" +
                "c = group a by (x,y);" +
                "c = foreach c generate group, COUNT(a.y);" +
                "store b into 'file:///tmp/output/b';" +
                "store c into 'file:///tmp/output/c';";

        setProperty(PigConfiguration.PIG_OPT_MULTIQUERY, "" + true);
        run(query, "test/org/apache/pig/test/data/GoldenFiles/tez/TEZC-MQ-3.gld");
        setProperty(PigConfiguration.PIG_OPT_MULTIQUERY, "" + false);
        run(query, "test/org/apache/pig/test/data/GoldenFiles/tez/TEZC-MQ-3-OPTOFF.gld");
    }

    @Test
    public void testMultiQueryWithJoin() throws Exception {
        String query =
                "a = load 'file:///tmp/input1' as (x:int, y:int);" +
                "b = load 'file:///tmp/input2' as (x:int, z:int);" +
                "c = join a by x, b by x;" +
                "d = foreach c generate $0, $1, $3;" +
                "e = foreach c generate $0, $1, $2, $3;" +
                "store c into 'file:///tmp/output/c';" +
                "store d into 'file:///tmp/output/d';" +
                "store e into 'file:///tmp/output/e';";

        setProperty(PigConfiguration.PIG_OPT_MULTIQUERY, "" + true);
        run(query, "test/org/apache/pig/test/data/GoldenFiles/tez/TEZC-MQ-4.gld");
        setProperty(PigConfiguration.PIG_OPT_MULTIQUERY, "" + false);
        run(query, "test/org/apache/pig/test/data/GoldenFiles/tez/TEZC-MQ-4-OPTOFF.gld");
    }

    @Test
    public void testMultiQueryWithNestedSplit() throws Exception {
        String query =
                "a = load 'file:///tmp/input' as (x:int, y:int);" +
                "b = group a by x;" + //b: {group: int,a: {(x: int,y: int)}}
                "store b into 'file:///tmp/output/b';" +
                "c = foreach b generate a.x, a.y;" + //c: {{(x: int)},{(y: int)}}
                "store c into 'file:///tmp/output/c';" +
                "d = foreach b GENERATE FLATTEN(a);" + //d: {a::x: int,a::y: int}
                "store d into 'file:///tmp/output/d';" +
                "e = foreach d GENERATE a::x, a::y;" +
                "store e into 'file:///tmp/output/e';";

        setProperty(PigConfiguration.PIG_OPT_MULTIQUERY, "" + true);
        run(query, "test/org/apache/pig/test/data/GoldenFiles/tez/TEZC-MQ-5.gld");
        setProperty(PigConfiguration.PIG_OPT_MULTIQUERY, "" + false);
        run(query, "test/org/apache/pig/test/data/GoldenFiles/tez/TEZC-MQ-5-OPTOFF.gld");
    }

    @Test
    public void testMultiQueryScalar() throws Exception {
        String query =
                "a = load 'file:///tmp/input' as (x:int, y:int);" +
                "b = group a by x;" +
                "c = foreach b generate group, COUNT(a) as cnt;" +
                "SPLIT a into d if (2 * c.cnt) < y, e OTHERWISE;" +
                "store d into 'file:///tmp/output1';" +
                "store e into 'file:///tmp/output2';";

        setProperty(PigConfiguration.PIG_OPT_MULTIQUERY, "" + true);
        run(query, "test/org/apache/pig/test/data/GoldenFiles/tez/TEZC-MQ-6.gld");
        resetScope();
        setProperty(PigConfiguration.PIG_OPT_MULTIQUERY, "" + false);
        run(query, "test/org/apache/pig/test/data/GoldenFiles/tez/TEZC-MQ-6-OPTOFF.gld");
    }

    @Test
    public void testMultiQueryMultipleReplicateJoin() throws Exception {
        String query =
                "a = load 'file:///tmp/input' as (x:int, y:int);" +
                "b = load 'file:///tmp/input' as (x:int, y:int);" +
                "c = join a by $0, b by $0 using 'replicated';" +
                "d = join a by $1, b by $1 using 'replicated';" +
                "e = union c,d;" +
                "store e into 'file:///tmp/output';";

        setProperty(PigConfiguration.PIG_OPT_MULTIQUERY, "" + true);
        run(query, "test/org/apache/pig/test/data/GoldenFiles/tez/TEZC-MQ-7.gld");
        resetScope();
        setProperty(PigConfiguration.PIG_OPT_MULTIQUERY, "" + false);
        run(query, "test/org/apache/pig/test/data/GoldenFiles/tez/TEZC-MQ-7-OPTOFF.gld");
    }

    @Test
    public void testUnionStore() throws Exception {
        String query =
                "a = load 'file:///tmp/input' as (x:int, y:chararray);" +
                "b = load 'file:///tmp/input' as (y:chararray, x:int);" +
                "c = union onschema a, b;" +
                "store c into 'file:///tmp/output';";

        setProperty(PigConfiguration.PIG_TEZ_OPT_UNION, "" + true);
        run(query, "test/org/apache/pig/test/data/GoldenFiles/tez/TEZC-Union-1.gld");
        resetScope();
        setProperty(PigConfiguration.PIG_TEZ_OPT_UNION, "" + false);
        run(query, "test/org/apache/pig/test/data/GoldenFiles/tez/TEZC-Union-1-OPTOFF.gld");
    }

    @Test
    public void testUnionUnSupportedStore() throws Exception {
        String query =
                "a = load 'file:///tmp/input' as (x:int, y:chararray);" +
                "b = load 'file:///tmp/input' as (y:chararray, x:int);" +
                "c = union onschema a, b;" +
                "store c into 'file:///tmp/output';";

        setProperty(PigConfiguration.PIG_TEZ_OPT_UNION, "" + true);
        String oldSupported = getProperty(PigConfiguration.PIG_TEZ_OPT_UNION_SUPPORTED_STOREFUNCS);
        String oldUnSupported = getProperty(PigConfiguration.PIG_TEZ_OPT_UNION_UNSUPPORTED_STOREFUNCS);
        setProperty(PigConfiguration.PIG_TEZ_OPT_UNION_UNSUPPORTED_STOREFUNCS, PigStorage.class.getName());
        // Plan should not have union optimization applied
        run(query, "test/org/apache/pig/test/data/GoldenFiles/tez/TEZC-Union-1-OPTOFF.gld");
        resetScope();
        setProperty(PigConfiguration.PIG_TEZ_OPT_UNION_UNSUPPORTED_STOREFUNCS, null);
        setProperty(PigConfiguration.PIG_TEZ_OPT_UNION_SUPPORTED_STOREFUNCS, OrcStorage.class.getName());
        query =
                "a = load 'file:///tmp/input' as (x:int, y:chararray);" +
                "b = load 'file:///tmp/input' as (y:chararray, x:int);" +
                "c = union onschema a, b;" +
                "store c into 'file:///tmp/output' using " + DummyStoreWithOutputFormat.class.getName() + "();";
        // Plan should not have union optimization applied
        run(query, "test/org/apache/pig/test/data/GoldenFiles/tez/TEZC-Union-1-DummyStore-OPTOFF.gld");
        // Restore the value
        setProperty(PigConfiguration.PIG_TEZ_OPT_UNION_SUPPORTED_STOREFUNCS, oldSupported);
        setProperty(PigConfiguration.PIG_TEZ_OPT_UNION_UNSUPPORTED_STOREFUNCS, oldUnSupported);
    }

    @Test
    public void testUnionGroupBy() throws Exception {
        String query =
                "a = load 'file:///tmp/input' as (x:int, y:int);" +
                "b = load 'file:///tmp/input' as (y:int, x:int);" +
                "c = union onschema a, b;" +
                "d = group c by x;" +
                "e = foreach d generate group, SUM(c.y);" +
                "store e into 'file:///tmp/output';";

        setProperty(PigConfiguration.PIG_TEZ_OPT_UNION, "" + true);
        run(query, "test/org/apache/pig/test/data/GoldenFiles/tez/TEZC-Union-2.gld");
        setProperty(PigConfiguration.PIG_TEZ_OPT_UNION, "" + false);
        run(query, "test/org/apache/pig/test/data/GoldenFiles/tez/TEZC-Union-2-OPTOFF.gld");
    }

    @Test
    public void testUnionJoin() throws Exception {
        String query =
                "a = load 'file:///tmp/input' as (x:int, y:chararray);" +
                "b = load 'file:///tmp/input' as (y:chararray, x:int);" +
                "c = union onschema a, b;" +
                "d = load 'file:///tmp/input1' as (x:int, z:chararray);" +
                "e = join c by x, d by x;" +
                "store e into 'file:///tmp/output';";

        setProperty(PigConfiguration.PIG_TEZ_OPT_UNION, "" + true);
        run(query, "test/org/apache/pig/test/data/GoldenFiles/tez/TEZC-Union-3.gld");
        setProperty(PigConfiguration.PIG_TEZ_OPT_UNION, "" + false);
        run(query, "test/org/apache/pig/test/data/GoldenFiles/tez/TEZC-Union-3-OPTOFF.gld");
    }


    @Test
    public void testUnionReplicateJoin() throws Exception {
        String query =
                "a = load 'file:///tmp/input' as (x:int, y:chararray);" +
                "b = load 'file:///tmp/input' as (y:chararray, x:int);" +
                "c = union onschema a, b;" +
                "d = load 'file:///tmp/input1' as (x:int, z:chararray);" +
                "e = join c by x, d by x using 'replicated';" +
                "store e into 'file:///tmp/output';";

        //TODO: PIG-3856 Not optimized
        setProperty(PigConfiguration.PIG_TEZ_OPT_UNION, "" + true);
        run(query, "test/org/apache/pig/test/data/GoldenFiles/tez/TEZC-Union-4.gld");
        setProperty(PigConfiguration.PIG_TEZ_OPT_UNION, "" + false);
        run(query, "test/org/apache/pig/test/data/GoldenFiles/tez/TEZC-Union-4-OPTOFF.gld");

        query =
                "a = load 'file:///tmp/input' as (x:int, y:chararray);" +
                "b = load 'file:///tmp/input' as (y:chararray, x:int);" +
                "c = union onschema a, b;" +
                "d = load 'file:///tmp/input1' as (x:int, z:chararray);" +
                "e = join d by x, c by x using 'replicated';" +
                "store e into 'file:///tmp/output';";

        // Optimized
        setProperty(PigConfiguration.PIG_TEZ_OPT_UNION, "" + true);
        run(query, "test/org/apache/pig/test/data/GoldenFiles/tez/TEZC-Union-5.gld");
        setProperty(PigConfiguration.PIG_TEZ_OPT_UNION, "" + false);
        run(query, "test/org/apache/pig/test/data/GoldenFiles/tez/TEZC-Union-5-OPTOFF.gld");
    }

    @Test
    public void testUnionSkewedJoin() throws Exception {
        // TODO: PIG-4574 optimization needs to be done for this as well.
        // Requires changes in UnionOptimizer
        String query =
                "a = load 'file:///tmp/input' as (x:int, y:chararray);" +
                "b = load 'file:///tmp/input' as (y:chararray, x:int);" +
                "c = union onschema a, b;" +
                "d = load 'file:///tmp/input1' as (x:int, z:chararray);" +
                "e = join c by x, d by x using 'skewed';" +
                "store e into 'file:///tmp/output';";

        setProperty(PigConfiguration.PIG_TEZ_OPT_UNION, "" + true);
        run(query, "test/org/apache/pig/test/data/GoldenFiles/tez/TEZC-Union-6.gld");
        setProperty(PigConfiguration.PIG_TEZ_OPT_UNION, "" + false);
        run(query, "test/org/apache/pig/test/data/GoldenFiles/tez/TEZC-Union-6-OPTOFF.gld");
    }

    @Test
    public void testUnionOrderby() throws Exception {
        // TODO: PIG-4574 optimization needs to be done for this as well.
        // Requires changes in UnionOptimizer
        String query =
                "a = load 'file:///tmp/input' as (x:int, y:chararray);" +
                "b = load 'file:///tmp/input' as (y:chararray, x:int);" +
                "c = union onschema a, b;" +
                "d = order c by x;" +
                "store d into 'file:///tmp/output';";

        setProperty(PigConfiguration.PIG_TEZ_OPT_UNION, "" + true);
        run(query, "test/org/apache/pig/test/data/GoldenFiles/tez/TEZC-Union-7.gld");
        setProperty(PigConfiguration.PIG_TEZ_OPT_UNION, "" + false);
        run(query, "test/org/apache/pig/test/data/GoldenFiles/tez/TEZC-Union-7-OPTOFF.gld");
    }

    //TODO: PIG-3854 Limit is too convoluted and can be simplified.
    @Test
    public void testUnionLimit() throws Exception {
        String query =
                "a = load 'file:///tmp/input' as (x:int, y:chararray);" +
                "b = load 'file:///tmp/input' as (y:chararray, x:int);" +
                "c = union onschema a, b;" +
                "d = limit c 1;" +
                "store d into 'file:///tmp/output';";

        setProperty(PigConfiguration.PIG_TEZ_OPT_UNION, "" + true);
        run(query, "test/org/apache/pig/test/data/GoldenFiles/tez/TEZC-Union-8.gld");
        setProperty(PigConfiguration.PIG_TEZ_OPT_UNION, "" + false);
        run(query, "test/org/apache/pig/test/data/GoldenFiles/tez/TEZC-Union-8-OPTOFF.gld");
    }

    @Test
    public void testUnionSplit() throws Exception {
        String query =
                "a = load 'file:///tmp/input' as (x:int, y:chararray);" +
                "b = load 'file:///tmp/input' as (y:chararray, x:int);" +
                "split a into a1 if x > 100, a2 otherwise;" +
                "c = union onschema a1, a2, b;" +
                "split c into d if x > 500, e otherwise;" +
                "store a2 into 'file:///tmp/output/a2';" +
                "store d into 'file:///tmp/output/d';" +
                "store e into 'file:///tmp/output/e';";

        setProperty(PigConfiguration.PIG_TEZ_OPT_UNION, "" + true);
        run(query, "test/org/apache/pig/test/data/GoldenFiles/tez/TEZC-Union-9.gld");
        resetScope();
        setProperty(PigConfiguration.PIG_TEZ_OPT_UNION, "" + false);
        run(query, "test/org/apache/pig/test/data/GoldenFiles/tez/TEZC-Union-9-OPTOFF.gld");
    }

    @Test
    public void testUnionUnion() throws Exception {
        String query =
                "a = load 'file:///tmp/input' as (x:int, y:chararray);" +
                "b = load 'file:///tmp/input' as (y:chararray, x:int);" +
                "c = union onschema a, b;" +
                "d = load 'file:///tmp/input1' as (x:int, y:chararray);" +
                "e = union onschema c, d;" +
                "f = group e by x;" +
                "store e into 'file:///tmp/output1';" +
                "store f into 'file:///tmp/output2';";

        setProperty(PigConfiguration.PIG_TEZ_OPT_UNION, "" + true);
        run(query, "test/org/apache/pig/test/data/GoldenFiles/tez/TEZC-Union-10.gld");
        resetScope();
        setProperty(PigConfiguration.PIG_TEZ_OPT_UNION, "" + false);
        run(query, "test/org/apache/pig/test/data/GoldenFiles/tez/TEZC-Union-10-OPTOFF.gld");

    }

    @Test
    public void testUnionUnionStore() throws Exception {
        String query =
                "a = load 'file:///tmp/input' as (x:int, y:chararray);" +
                "b = load 'file:///tmp/input' as (y:chararray, x:int);" +
                "c = union onschema a, b;" +
                "d = load 'file:///tmp/input1' as (x:int, y:chararray);" +
                "e = union onschema c, d;" +
                "store e into 'file:///tmp/output';";

        setProperty(PigConfiguration.PIG_TEZ_OPT_UNION, "" + true);
        run(query, "test/org/apache/pig/test/data/GoldenFiles/tez/TEZC-Union-11.gld");
        resetScope();
        setProperty(PigConfiguration.PIG_TEZ_OPT_UNION, "" + false);
        run(query, "test/org/apache/pig/test/data/GoldenFiles/tez/TEZC-Union-11-OPTOFF.gld");
    }

    @Test
    public void testMultipleUnionSplitJoin() throws Exception {
        String query =
                "a = load 'file:///tmp/input' as (x:int, y:chararray);" +
                "b = filter a by x == 2;" +
                "b1 = foreach b generate *;" +
                "b2 = foreach b generate *;" +
                "b3 = union onschema b1, b2;" +
                "c = filter a by x == 3;" +
                "c1 = foreach c generate y, x;" +
                "c2 = foreach c generate y, x;" +
                "c3 = union c1, c2;" +
                "a1 = union onschema b3, c3;" +
                "store a1 into 'file:///tmp/output1';" +
                "d = load 'file:///tmp/input1' as (x:int, z:chararray);" +
                "e = join a1 by x, d by x using 'skewed';" +
                "store e into 'file:///tmp/output2';";

        setProperty(PigConfiguration.PIG_TEZ_OPT_UNION, "" + true);
        run(query, "test/org/apache/pig/test/data/GoldenFiles/tez/TEZC-Union-12.gld");
        resetScope();
        setProperty(PigConfiguration.PIG_TEZ_OPT_UNION, "" + false);
        run(query, "test/org/apache/pig/test/data/GoldenFiles/tez/TEZC-Union-12-OPTOFF.gld");
    }

    @Test
    public void testUnionSplitReplicateJoin() throws Exception {
        String query =
                "a = load 'file:///tmp/input' as (x:int, y:chararray);" +
                "b = filter a by x == 2;" +
                "c = union onschema a, b;" +
                "d = load 'file:///tmp/input1' as (x:int, z:chararray);" +
                "e = join c by x, d by x using 'replicated';" +
                "store e into 'file:///tmp/output';";

        setProperty(PigConfiguration.PIG_TEZ_OPT_UNION, "" + true);
        run(query, "test/org/apache/pig/test/data/GoldenFiles/tez/TEZC-Union-13.gld");
        resetScope();
        setProperty(PigConfiguration.PIG_TEZ_OPT_UNION, "" + false);
        run(query, "test/org/apache/pig/test/data/GoldenFiles/tez/TEZC-Union-13-OPTOFF.gld");

        query =
                "a = load 'file:///tmp/input' as (x:int, y:chararray);" +
                "b = filter a by x == 2;" +
                "c = union onschema a, b;" +
                "d = load 'file:///tmp/input1' as (x:int, z:chararray);" +
                "e = join d by x, c by x using 'replicated';" +
                "store e into 'file:///tmp/output';";

        resetScope();
        setProperty(PigConfiguration.PIG_TEZ_OPT_UNION, "" + true);
        run(query, "test/org/apache/pig/test/data/GoldenFiles/tez/TEZC-Union-14.gld");
        resetScope();
        setProperty(PigConfiguration.PIG_TEZ_OPT_UNION, "" + false);
        run(query, "test/org/apache/pig/test/data/GoldenFiles/tez/TEZC-Union-14-OPTOFF.gld");
    }

    @Test
    public void testUnionSplitSkewedJoin() throws Exception {
        String query =
                "a = load 'file:///tmp/input' as (x:int, y:chararray);" +
                "b = filter a by x == 2;" +
                "c = union onschema a, b;" +
                "d = load 'file:///tmp/input1' as (x:int, z:chararray);" +
                "e = join c by x, d by x using 'skewed';" +
                "store e into 'file:///tmp/output';";

        setProperty(PigConfiguration.PIG_TEZ_OPT_UNION, "" + true);
        run(query, "test/org/apache/pig/test/data/GoldenFiles/tez/TEZC-Union-15.gld");
        resetScope();
        setProperty(PigConfiguration.PIG_TEZ_OPT_UNION, "" + false);
        run(query, "test/org/apache/pig/test/data/GoldenFiles/tez/TEZC-Union-15-OPTOFF.gld");

        query =
                "a = load 'file:///tmp/input' as (x:int, y:chararray);" +
                "b = filter a by x == 2;" +
                "c = union onschema a, b;" +
                "d = load 'file:///tmp/input1' as (x:int, z:chararray);" +
                "e = join d by x, c by x using 'skewed';" +
                "store e into 'file:///tmp/output';";

        resetScope();
        setProperty(PigConfiguration.PIG_TEZ_OPT_UNION, "" + true);
        run(query, "test/org/apache/pig/test/data/GoldenFiles/tez/TEZC-Union-16.gld");
        resetScope();
        setProperty(PigConfiguration.PIG_TEZ_OPT_UNION, "" + false);
        run(query, "test/org/apache/pig/test/data/GoldenFiles/tez/TEZC-Union-16-OPTOFF.gld");
    }

    @Test
    public void testUnionScalar() throws Exception {
        String query =
                "a = load 'file:///tmp/input' as (x:int, y:chararray);" +
                "b = load 'file:///tmp/input' as (y:chararray, x:int);" +
                "c = union onschema a, b;" +
                "d = load 'file:///tmp/input1' as (x:int, z:chararray);" +
                "e = filter c by x == d.x;" +
                "store e into 'file:///tmp/output';";

        setProperty(PigConfiguration.PIG_TEZ_OPT_UNION, "" + true);
        run(query, "test/org/apache/pig/test/data/GoldenFiles/tez/TEZC-Union-17.gld");
        resetScope();
        setProperty(PigConfiguration.PIG_TEZ_OPT_UNION, "" + false);
        run(query, "test/org/apache/pig/test/data/GoldenFiles/tez/TEZC-Union-17-OPTOFF.gld");

        query =
                "a = load 'file:///tmp/input' as (x:int, y:chararray);" +
                "b = load 'file:///tmp/input' as (y:chararray, x:int);" +
                "c = union onschema a, b;" +
                "d = load 'file:///tmp/input1' as (x:int, z:chararray);" +
                "e = filter d by x == c.x;" +
                "store e into 'file:///tmp/output';";

        resetScope();
        setProperty(PigConfiguration.PIG_TEZ_OPT_UNION, "" + true);
        run(query, "test/org/apache/pig/test/data/GoldenFiles/tez/TEZC-Union-18.gld");
        resetScope();
        setProperty(PigConfiguration.PIG_TEZ_OPT_UNION, "" + false);
        run(query, "test/org/apache/pig/test/data/GoldenFiles/tez/TEZC-Union-18-OPTOFF.gld");
    }

    @Test
    public void testRank() throws Exception {
        String query =
                "a = load 'file:///tmp/input1' as (x:int, y:int);" +
                "b = rank a;" +
                "store b into 'file:///tmp/output/d';";

        run(query, "test/org/apache/pig/test/data/GoldenFiles/tez/TEZC-Rank-1.gld");
    }

    @Test
    public void testRankBy() throws Exception {
        //TODO: Physical plan (affects both MR and Tez) has extra job before order by. Does not look right.
        String query =
                "a = load 'file:///tmp/input1' as (x:int, y:int);" +
                "b = rank a by x;" +
                "store b into 'file:///tmp/output/d';";

        run(query, "test/org/apache/pig/test/data/GoldenFiles/tez/TEZC-Rank-2.gld");
    }

    private String getProperty(String property) {
        return pigServer.getPigContext().getProperties().getProperty(property);
    }

    private void setProperty(String property, String value) {
        if (value == null) {
            pigServer.getPigContext().getProperties().remove(property);
        } else {
            pigServer.getPigContext().getProperties().setProperty(property, value);
        }
    }

    private void run(String query, String expectedFile) throws Exception {
        PhysicalPlan pp = Util.buildPp(pigServer, query);
        TezLauncher launcher = new TezLauncher();
        pc.inExplain = true;
        TezPlanContainer tezPlanContainer = launcher.compile(pp, pc);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(baos);
        TezPlanContainerPrinter printer = new TezPlanContainerPrinter(ps, tezPlanContainer);
        printer.visit();
        String compiledPlan = baos.toString();
        System.out.println();
        System.out.println("<<<" + compiledPlan + ">>>");

        if (generate) {
            FileOutputStream fos = new FileOutputStream(expectedFile);
            fos.write(baos.toByteArray());
            fos.close();
            return;
        }
        FileInputStream fis = new FileInputStream(expectedFile);
        byte[] b = new byte[MAX_SIZE];
        int len = fis.read(b);
        fis.close();
        String goldenPlan = new String(b, 0, len);
        if (goldenPlan.charAt(len-1) == '\n') {
            goldenPlan = goldenPlan.substring(0, len-1);
        }

        System.out.println("-------------");
        System.out.println("Golden");
        System.out.println("<<<" + goldenPlan + ">>>");
        System.out.println("-------------");

        String goldenPlanClean = Util.standardizeNewline(goldenPlan).trim();
        String compiledPlanClean = Util.standardizeNewline(compiledPlan).trim();
        assertEquals(TestHelper.sortUDFs(Util.removeSignature(goldenPlanClean)),
                TestHelper.sortUDFs(Util.removeSignature(compiledPlanClean)));
    }
}


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

import java.io.StringReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import junit.framework.Assert;
import junit.framework.TestCase;

import org.apache.pig.ExecType;
import org.apache.pig.PigException;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecJob;
import org.apache.pig.backend.executionengine.util.ExecTools;
import org.apache.pig.backend.hadoop.executionengine.HExecutionEngine;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MRCompiler;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MapReduceLauncher;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROperPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.logicalLayer.LogicalPlan;
import org.apache.pig.impl.plan.Operator;
import org.apache.pig.impl.plan.OperatorPlan;
import org.apache.pig.tools.grunt.GruntParser;
import org.apache.pig.tools.grunt.Utils;
import org.apache.pig.tools.pigscript.parser.ParseException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestMultiQuery extends TestCase {

    private static final MiniCluster cluster = MiniCluster.buildCluster();

    private PigServer myPig;

    @Before
    public void setUp() throws Exception {
        myPig = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
    }

    @After
    public void tearDown() throws Exception {
        myPig = null;
    }

    @Test
    public void testMultiQueryWithTwoStores() {

        System.out.println("===== test multi-query with 2 stores =====");

        try {
            myPig.setBatchOn();

            myPig.registerQuery("a = load 'file:test/org/apache/pig/test/data/passwd' " +
                                "using PigStorage(':') as (uname:chararray, passwd:chararray, uid:int,gid:int);");
            myPig.registerQuery("b = filter a by uid > 5;");
            myPig.registerQuery("store b into '/tmp/output1';");
            myPig.registerQuery("c = group b by gid;");
            myPig.registerQuery("store c into '/tmp/output2';");

            LogicalPlan lp = checkLogicalPlan(1, 2, 9);

            PhysicalPlan pp = checkPhysicalPlan(lp, 1, 2, 11);

            checkMRPlan(pp, 1, 2, 3);

            Assert.assertTrue(executePlan(pp));

        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        } finally {
            deleteOutputFiles();
        }
    }

    @Test
    public void testMultiQueryWithTwoStores2() {

        System.out.println("===== test multi-query with 2 stores (2) =====");

        try {
            myPig.setBatchOn();

            myPig.registerQuery("a = load 'file:test/org/apache/pig/test/data/passwd' " +
                                "using PigStorage(':') as (uname:chararray, passwd:chararray, uid:int,gid:int);");
            myPig.registerQuery("b = filter a by uid > 5;");
            myPig.registerQuery("store b into '/tmp/output1';");
            myPig.registerQuery("c = group b by gid;");
            myPig.registerQuery("store c into '/tmp/output2';");

            myPig.executeBatch();

        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        } finally {
            deleteOutputFiles();
        }
    }

    @Test
    public void testMultiQueryWithThreeStores() {

        System.out.println("===== test multi-query with 3 stores =====");

        try {
            myPig.setBatchOn();

            myPig.registerQuery("a = load 'file:test/org/apache/pig/test/data/passwd' " +
                                "using PigStorage(':') as (uname:chararray, passwd:chararray, uid:int,gid:int);");
            myPig.registerQuery("b = filter a by uid > 5;");
            myPig.registerQuery("store b into '/tmp/output1';");
            myPig.registerQuery("c = filter b by uid > 10;");
            myPig.registerQuery("store c into '/tmp/output2';");
            myPig.registerQuery("d = filter c by uid > 15;");
            myPig.registerQuery("store d into '/tmp/output3';");

            LogicalPlan lp = checkLogicalPlan(1, 3, 14);

            PhysicalPlan pp = checkPhysicalPlan(lp, 1, 3, 14);

            checkMRPlan(pp, 1, 3, 5);

            Assert.assertTrue(executePlan(pp));

        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        } finally {
            deleteOutputFiles();
        }
    }

    @Test
    public void testMultiQueryWithThreeStores2() {

        System.out.println("===== test multi-query with 3 stores (2) =====");

        try {
            myPig.setBatchOn();

            myPig.registerQuery("a = load 'file:test/org/apache/pig/test/data/passwd' " +
                                "using PigStorage(':') as (uname:chararray, passwd:chararray, uid:int,gid:int);");
            myPig.registerQuery("b = filter a by uid > 5;");
            myPig.registerQuery("store b into '/tmp/output1';");
            myPig.registerQuery("c = filter b by uid > 10;");
            myPig.registerQuery("store c into '/tmp/output2';");
            myPig.registerQuery("d = filter c by uid > 15;");
            myPig.registerQuery("store d into '/tmp/output3';");

            myPig.executeBatch();

        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        } finally {
            deleteOutputFiles();
        }
    }

    @Test
    public void testMultiQueryWithTwoLoads() {

        System.out.println("===== test multi-query with two loads =====");

        try {
            myPig.setBatchOn();

            myPig.registerQuery("a = load 'file:test/org/apache/pig/test/data/passwd' " +
                                "using PigStorage(':') as (uname:chararray, passwd:chararray, uid:int,gid:int);");
            myPig.registerQuery("b = load 'file:test/org/apache/pig/test/data/passwd2' " +
                                "using PigStorage(':') as (uname:chararray, passwd:chararray, uid:int,gid:int);");
            myPig.registerQuery("c = filter a by uid > 5;");
            myPig.registerQuery("d = filter b by uid > 10;");
            myPig.registerQuery("store c into '/tmp/output1';");
            myPig.registerQuery("store d into '/tmp/output2';");
            myPig.registerQuery("e = cogroup c by uid, d by uid;");
            myPig.registerQuery("store e into '/tmp/output3';");

            LogicalPlan lp = checkLogicalPlan(2, 3, 16);

            PhysicalPlan pp = checkPhysicalPlan(lp, 2, 3, 19);

            checkMRPlan(pp, 2, 3, 5);

            Assert.assertTrue(executePlan(pp));

        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        } finally {
            deleteOutputFiles();
        }
    }

    @Test
    public void testMultiQueryWithTwoLoads2() {

        System.out.println("===== test multi-query with two loads (2) =====");

        try {
            myPig.setBatchOn();

            myPig.registerQuery("a = load 'file:test/org/apache/pig/test/data/passwd' " +
                                "using PigStorage(':') as (uname:chararray, passwd:chararray, uid:int,gid:int);");
            myPig.registerQuery("b = load 'file:test/org/apache/pig/test/data/passwd2' " +
                                "using PigStorage(':') as (uname:chararray, passwd:chararray, uid:int,gid:int);");
            myPig.registerQuery("c = filter a by uid > 5;");
            myPig.registerQuery("d = filter b by uid > 10;");
            myPig.registerQuery("store c into '/tmp/output1';");
            myPig.registerQuery("store d into '/tmp/output2';");
            myPig.registerQuery("e = cogroup c by uid, d by uid;");
            myPig.registerQuery("store e into '/tmp/output3';");

            myPig.executeBatch();

        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        } finally {
            deleteOutputFiles();
        }
    }

    @Test
    public void testMultiQueryWithNoStore() {

        System.out.println("===== test multi-query with no store =====");

        try {
            myPig.setBatchOn();

            myPig.registerQuery("a = load 'file:test/org/apache/pig/test/data/passwd' " +
                                "using PigStorage(':') as (uname:chararray, passwd:chararray, uid:int,gid:int);");
            myPig.registerQuery("b = filter a by uid > 5;");
            myPig.registerQuery("group b by gid;");

            LogicalPlan lp = checkLogicalPlan(0, 0, 0);

            PhysicalPlan pp = checkPhysicalPlan(lp, 0, 0, 0);

            //checkMRPlan(pp, 1, 1, 1);

            //Assert.assertTrue(executePlan(pp));

        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testMultiQueryWithNoStore2() {

        System.out.println("===== test multi-query with no store (2) =====");

        try {
            myPig.setBatchOn();

            myPig.registerQuery("a = load 'file:test/org/apache/pig/test/data/passwd' " +
                                "using PigStorage(':') as (uname:chararray, passwd:chararray, uid:int,gid:int);");
            myPig.registerQuery("b = filter a by uid > 5;");
            myPig.registerQuery("group b by gid;");

            myPig.executeBatch();

        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
    }


    @Test
    public void testMultiQueryWithExplain() {

        System.out.println("===== test multi-query with explain =====");

        try {
            String script = "a = load 'file:test/org/apache/pig/test/data/passwd' "
                          + "using PigStorage(':') as (uname:chararray, passwd:chararray, uid:int,gid:int);"
                          + "b = filter a by uid > 5;"
                          + "explain b;"
                          + "store b into '/tmp/output1';\n";
            
            GruntParser parser = new GruntParser(new StringReader(script));
            parser.setInteractive(false);
            parser.setParams(myPig);
            parser.parseStopOnError();

        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        } finally {
            deleteOutputFiles();
        }
    }

    @Test
    public void testMultiQueryWithDump() {

        System.out.println("===== test multi-query with dump =====");

        try {
            String script = "a = load 'file:test/org/apache/pig/test/data/passwd' "
                          + "using PigStorage(':') as (uname:chararray, passwd:chararray, uid:int,gid:int);"
                          + "b = filter a by uid > 5;"
                          + "dump b;"
                          + "store b into '/tmp/output1';\n";
            
            GruntParser parser = new GruntParser(new StringReader(script));
            parser.setInteractive(false);
            parser.setParams(myPig);
            parser.parseStopOnError();

        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        } finally {
            deleteOutputFiles();
        }
    }

    @Test
    public void testMultiQueryWithDescribe() {

        System.out.println("===== test multi-query with describe =====");

        try {
            String script = "a = load 'file:test/org/apache/pig/test/data/passwd' "
                          + "using PigStorage(':') as (uname:chararray, passwd:chararray, uid:int,gid:int);"
                          + "b = filter a by uid > 5;"
                          + "describe b;"
                          + "store b into '/tmp/output1';\n";
            
            GruntParser parser = new GruntParser(new StringReader(script));
            parser.setInteractive(false);
            parser.setParams(myPig);
            parser.parseStopOnError();

        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        } finally {
            deleteOutputFiles();
        }
    }

    @Test
    public void testMultiQueryWithIllustrate() {

        System.out.println("===== test multi-query with illustrate =====");

        try {
            String script = "a = load 'file:test/org/apache/pig/test/data/passwd' "
                          + "using PigStorage(':') as (uname:chararray, passwd:chararray, uid:int,gid:int);"
                          + "b = filter a by uid > 5;"
                          + "illustrate b;"
                          + "store b into '/tmp/output1';\n";
            
            GruntParser parser = new GruntParser(new StringReader(script));
            parser.setInteractive(false);
            parser.setParams(myPig);
            parser.parseStopOnError();

        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        } finally {
            deleteOutputFiles();
        }
    }

    // --------------------------------------------------------------------------
    // Helper methods

    private <T extends OperatorPlan<? extends Operator<?>>> 
    void showPlanOperators(T p) {
        System.out.println("Operators:");

        ArrayList<Operator<?>> ops = new ArrayList<Operator<?>>(p.getKeys()
                .values());
        Collections.sort(ops);
        for (Operator<?> op : ops) {
            System.out.println("    op: " + op.name());
        }
        System.out.println();
    }

    private LogicalPlan checkLogicalPlan(int expectedRoots,
            int expectedLeaves, int expectedSize) throws IOException,
            ParseException {

        System.out.println("===== check logical plan =====");
        
        LogicalPlan lp = null;

        try {
            java.lang.reflect.Method compileLp = myPig.getClass()
                    .getDeclaredMethod("compileLp",
                            new Class[] { String.class });

            compileLp.setAccessible(true);

            lp = (LogicalPlan) compileLp.invoke(myPig, new Object[] { null });

            Assert.assertNotNull(lp);

        } catch (Exception e) {
            PigException pe = Utils.getPigException(e);
            if (pe != null) {
                throw pe;
            } else {
                e.printStackTrace();
                Assert.fail();
            }
        }

        Assert.assertEquals(expectedRoots, lp.getRoots().size());
        Assert.assertEquals(expectedLeaves, lp.getLeaves().size());
        Assert.assertEquals(expectedSize, lp.size());

        showPlanOperators(lp);

        return lp;
    }

    private PhysicalPlan checkPhysicalPlan(LogicalPlan lp, int expectedRoots,
            int expectedLeaves, int expectedSize) throws IOException {

        System.out.println("===== check physical plan =====");

        PhysicalPlan pp = myPig.getPigContext().getExecutionEngine().compile(
                lp, null);

        Assert.assertEquals(expectedRoots, pp.getRoots().size());
        Assert.assertEquals(expectedLeaves, pp.getLeaves().size());
        Assert.assertEquals(expectedSize, pp.size());

        showPlanOperators(pp);

        return pp;
    }

    private MROperPlan checkMRPlan(PhysicalPlan pp, int expectedRoots,
            int expectedLeaves, int expectedSize) throws IOException {

        System.out.println("===== check map-reduce plan =====");

        ExecTools.checkLeafIsStore(pp, myPig.getPigContext());
        
        MRCompiler mrcomp = new MRCompiler(pp, myPig.getPigContext());
        MROperPlan mrp = mrcomp.compile();

        Assert.assertEquals(expectedRoots, mrp.getRoots().size());
        Assert.assertEquals(expectedLeaves, mrp.getLeaves().size());
        Assert.assertEquals(expectedSize, mrp.size());

        showPlanOperators(mrp);

        return mrp;
    }

    private boolean executePlan(PhysicalPlan pp) throws IOException {
        FileLocalizer.clearDeleteOnFail();
        ExecJob job = myPig.getPigContext().getExecutionEngine().execute(pp, "execute");
        boolean failed = (job.getStatus() == ExecJob.JOB_STATUS.FAILED);
        if (failed) {
            FileLocalizer.triggerDeleteOnFail();
        }
        return !failed;
    }

    private void deleteOutputFiles() {
        try {
            FileLocalizer.delete("/tmp/output1", myPig.getPigContext());
            FileLocalizer.delete("/tmp/output2", myPig.getPigContext());
            FileLocalizer.delete("/tmp/output3", myPig.getPigContext());
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

}

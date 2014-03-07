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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.Properties;

import junit.framework.Assert;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.pig.ExecType;
import org.apache.pig.PigConfiguration;
import org.apache.pig.PigException;
import org.apache.pig.PigServer;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MRExecutionEngine;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MapReduceLauncher;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigTextOutputFormat;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.plan.Operator;
import org.apache.pig.impl.plan.OperatorPlan;
import org.apache.pig.impl.util.LogUtils;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.tools.grunt.GruntParser;
import org.apache.pig.tools.pigscript.parser.ParseException;
import org.apache.pig.tools.pigstats.JobStats;
import org.apache.pig.tools.pigstats.PigStats;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestMultiQueryLocal {

    private PigServer myPig;
    private String TMP_DIR;

    @Before
    public void setUp() throws Exception {
        PigContext context = new PigContext(ExecType.LOCAL, new Properties());
        context.getProperties().setProperty("opt.multiquery", ""+true);
        myPig = new PigServer(context);
        myPig.getPigContext().getProperties().setProperty("pig.usenewlogicalplan", "false");
        myPig.getPigContext().getProperties().setProperty(PigConfiguration.PIG_TEMP_DIR, "build/test/tmp/");
        TMP_DIR = FileLocalizer.getTemporaryPath(myPig.getPigContext()).toUri().getPath();
        deleteOutputFiles();
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

            myPig.registerQuery("a = load 'test/org/apache/pig/test/data/passwd' " +
                                "using PigStorage(':') as (uname:chararray, passwd:chararray, uid:int,gid:int);");
            myPig.registerQuery("b = filter a by uid > 5;");
            myPig.registerQuery("store b into '" + TMP_DIR + "/Pig-TestMultiQueryLocal1';");
            myPig.registerQuery("c = group b by gid;");
            myPig.registerQuery("store c into '" + TMP_DIR + "/Pig-TestMultiQueryLocal2';");

            LogicalPlan lp = checkLogicalPlan(1, 2, 8);

            // XXX Physical plan has one less node in the local case
            PhysicalPlan pp = checkPhysicalPlan(lp, 1, 2, 11);

            Assert.assertTrue(executePlan(pp));

        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        } finally {
            deleteOutputFiles();
        }
    }

    @Test
    public void testEmptyExecute() {
        System.out.println("=== test empty execute ===");
        
        try {
            myPig.setBatchOn();
            myPig.executeBatch();
            myPig.executeBatch();
            myPig.discardBatch();
        }
        catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testMultiQueryWithTwoStores2() {

        System.out.println("===== test multi-query with 2 stores (2) =====");

        try {
            myPig.setBatchOn();

            myPig.registerQuery("a = load 'test/org/apache/pig/test/data/passwd' " +
                                "using PigStorage(':') as (uname:chararray, passwd:chararray, uid:int,gid:int);");
            myPig.registerQuery("b = filter a by uid > 5;");
            myPig.registerQuery("store b into '" + TMP_DIR + "/Pig-TestMultiQueryLocal1';");
            myPig.registerQuery("c = group b by gid;");
            myPig.registerQuery("store c into '" + TMP_DIR + "/Pig-TestMultiQueryLocal2';");

            myPig.executeBatch();

        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        } finally {
            deleteOutputFiles();
        }
    }

    @Test
    public void testMultiQueryWithTwoStores2Execs() {

        System.out.println("===== test multi-query with 2 stores (2) =====");
        
        try {
            myPig.setBatchOn();

            myPig.registerQuery("a = load 'test/org/apache/pig/test/data/passwd' " +
                                "using PigStorage(':') as (uname:chararray, passwd:chararray, uid:int,gid:int);");
            myPig.registerQuery("b = filter a by uid > 5;");
            myPig.executeBatch();
            myPig.registerQuery("store b into '" + TMP_DIR + "/Pig-TestMultiQueryLocal1';");
            myPig.executeBatch();
            myPig.registerQuery("c = group b by gid;");
            myPig.registerQuery("store c into '" + TMP_DIR + "/Pig-TestMultiQueryLocal2';");

            myPig.executeBatch();
            myPig.discardBatch();

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

            myPig.registerQuery("a = load 'test/org/apache/pig/test/data/passwd' " +
                                "using PigStorage(':') as (uname:chararray, passwd:chararray, uid:int,gid:int);");
            myPig.registerQuery("b = filter a by uid > 5;");
            myPig.registerQuery("store b into '" + TMP_DIR + "/Pig-TestMultiQueryLocal1';");
            myPig.registerQuery("c = filter b by uid > 10;");
            myPig.registerQuery("store c into '" + TMP_DIR + "/Pig-TestMultiQueryLocal2';");
            myPig.registerQuery("d = filter c by uid > 15;");
            myPig.registerQuery("store d into '" + TMP_DIR + "/Pig-TestMultiQueryLocal3';");

            LogicalPlan lp = checkLogicalPlan(1, 3, 13);

            PhysicalPlan pp = checkPhysicalPlan(lp, 1, 3, 14);

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

            myPig.registerQuery("a = load 'test/org/apache/pig/test/data/passwd' " +
                                "using PigStorage(':') as (uname:chararray, passwd:chararray, uid:int,gid:int);");
            myPig.registerQuery("b = filter a by uid > 5;");
            myPig.registerQuery("store b into '" + TMP_DIR + "/Pig-TestMultiQueryLocal1';");
            myPig.registerQuery("c = filter b by uid > 10;");
            myPig.registerQuery("store c into '" + TMP_DIR + "/Pig-TestMultiQueryLocal2';");
            myPig.registerQuery("d = filter c by uid > 15;");
            myPig.registerQuery("store d into '" + TMP_DIR + "/Pig-TestMultiQueryLocal3';");

            myPig.executeBatch();
            myPig.discardBatch();

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

            myPig.registerQuery("a = load 'test/org/apache/pig/test/data/passwd' " +
                                "using PigStorage(':') as (uname:chararray, passwd:chararray, uid:int,gid:int);");
            myPig.registerQuery("b = load 'test/org/apache/pig/test/data/passwd2' " +
                                "using PigStorage(':') as (uname:chararray, passwd:chararray, uid:int,gid:int);");
            myPig.registerQuery("c = filter a by uid > 5;");
            myPig.registerQuery("d = filter b by uid > 10;");
            myPig.registerQuery("store c into '" + TMP_DIR + "/Pig-TestMultiQueryLocal1';");
            myPig.registerQuery("store d into '" + TMP_DIR + "/Pig-TestMultiQueryLocal2';");
            myPig.registerQuery("e = cogroup c by uid, d by uid;");
            myPig.registerQuery("store e into '" + TMP_DIR + "/Pig-TestMultiQueryLocal3';");

            LogicalPlan lp = checkLogicalPlan(2, 3, 14);

            // XXX the total number of ops is one less in the local case
            PhysicalPlan pp = checkPhysicalPlan(lp, 2, 3, 19);

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

            myPig.registerQuery("a = load 'test/org/apache/pig/test/data/passwd' " +
                                "using PigStorage(':') as (uname:chararray, passwd:chararray, uid:int,gid:int);");
            myPig.registerQuery("b = load 'test/org/apache/pig/test/data/passwd2' " +
                                "using PigStorage(':') as (uname:chararray, passwd:chararray, uid:int,gid:int);");
            myPig.registerQuery("c = filter a by uid > 5;");
            myPig.registerQuery("d = filter b by uid > 10;");
            myPig.registerQuery("store c into '" + TMP_DIR + "/Pig-TestMultiQueryLocal1';");
            myPig.registerQuery("store d into '" + TMP_DIR + "/Pig-TestMultiQueryLocal2';");
            myPig.registerQuery("e = cogroup c by uid, d by uid;");
            myPig.registerQuery("store e into '" + TMP_DIR + "/Pig-TestMultiQueryLocal3';");

            myPig.executeBatch();
            myPig.discardBatch();

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

            myPig.registerQuery("a = load 'test/org/apache/pig/test/data/passwd' " +
                                "using PigStorage(':') as (uname:chararray, passwd:chararray, uid:int,gid:int);");
            myPig.registerQuery("b = filter a by uid > 5;");
            myPig.registerQuery("group b by gid;");

            LogicalPlan lp = checkLogicalPlan(0, 0, 0);
            
            // XXX Physical plan has one less node in the local case
            PhysicalPlan pp = checkPhysicalPlan(lp, 0, 0, 0);
            
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

            myPig.registerQuery("a = load 'test/org/apache/pig/test/data/passwd' " +
                                "using PigStorage(':') as (uname:chararray, passwd:chararray, uid:int,gid:int);");
            myPig.registerQuery("b = filter a by uid > 5;");
            myPig.registerQuery("group b by gid;");

            myPig.executeBatch();
            myPig.discardBatch();

        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    public static class PigStorageWithConfig extends PigStorage {

        private static final String key = "test.key";
        private String suffix;

        public PigStorageWithConfig(String s) {
            this.suffix = s;
        }

        @Override
        public void setStoreLocation(String location, Job job) throws IOException {
            super.setStoreLocation(location, job);
            Assert.assertNull(job.getConfiguration().get(key));
        }

        @Override
        public OutputFormat getOutputFormat() {
            return new PigTextOutputFormatWithConfig();
        }

        @Override
        public void putNext(Tuple f) throws IOException {
            try {
                Tuple t = TupleFactory.getInstance().newTuple();
                for (Object obj : f.getAll()) {
                    t.append(obj);
                }
                t.append(suffix);
                writer.write(null, t);
            } catch (InterruptedException e) {
                throw new IOException(e);
            }
        }
    }

    private static class PigTextOutputFormatWithConfig extends PigTextOutputFormat {

        public PigTextOutputFormatWithConfig() {
            super((byte) '\t');
        }

        @Override
        public synchronized OutputCommitter getOutputCommitter(TaskAttemptContext context)
                throws IOException {
            context.getConfiguration().set(PigStorageWithConfig.key, "mapred.work.output.dir");
            return super.getOutputCommitter(context);
        }
    }

    // See PIG-2912
    @Test
    public void testMultiStoreWithConfig() {

        System.out.println("===== test multi-query with competing config =====");

        try {
            myPig.setBatchOn();

            myPig.registerQuery("a = load 'test/org/apache/pig/test/data/passwd' " +
                                "using PigStorage(':') as (uname:chararray, passwd:chararray, uid:int,gid:int);");
            myPig.registerQuery("b = filter a by uid < 5;");
            myPig.registerQuery("c = filter a by uid > 5;");
            myPig.registerQuery("store b into '" + TMP_DIR + "/Pig-TestMultiQueryLocal1' using " + PigStorageWithConfig.class.getName() + "('a');");
            myPig.registerQuery("store c into '" + TMP_DIR + "/Pig-TestMultiQueryLocal2' using " + PigStorageWithConfig.class.getName() + "('b');");

            myPig.executeBatch();
            myPig.discardBatch();
            BufferedReader reader = new BufferedReader(new FileReader(TMP_DIR + "/Pig-TestMultiQueryLocal1/part-m-00000"));
            String line;
            while ((line = reader.readLine())!=null) {
                Assert.assertTrue(line.endsWith("a"));
            }
            reader = new BufferedReader(new FileReader(TMP_DIR + "/Pig-TestMultiQueryLocal2/part-m-00000"));
            while ((line = reader.readLine())!=null) {
                Assert.assertTrue(line.endsWith("b"));
            }

        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testMultiQueryWithExplain() {

        System.out.println("===== test multi-query with explain =====");

        try {
            String script = "a = load 'test/org/apache/pig/test/data/passwd' "
                          + "using PigStorage(':') as (uname:chararray, passwd:chararray, uid:int,gid:int);"
                          + "b = filter a by uid > 5;"
                          + "explain b;"
                          + "store b into '" + TMP_DIR + "/Pig-TestMultiQueryLocal1';\n";

            GruntParser parser = new GruntParser(new StringReader(script), myPig);
            parser.setInteractive(false);
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
            String script = "a = load 'test/org/apache/pig/test/data/passwd' "
                          + "using PigStorage(':') as (uname:chararray, passwd:chararray, uid:int,gid:int);"
                          + "b = filter a by uid > 5;"
                          + "dump b;"
                          + "store b into '" + TMP_DIR + "/Pig-TestMultiQueryLocal1';\n";

            GruntParser parser = new GruntParser(new StringReader(script), myPig);
            parser.setInteractive(false);
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
            String script = "a = load 'test/org/apache/pig/test/data/passwd' "
                          + "using PigStorage(':') as (uname:chararray, passwd:chararray, uid:int,gid:int);"
                          + "b = filter a by uid > 5;"
                          + "describe b;"
                          + "store b into '" + TMP_DIR + "/Pig-TestMultiQueryLocal1';\n";

            GruntParser parser = new GruntParser(new StringReader(script), myPig);
            parser.setInteractive(false);
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
            String script = "a = load 'test/org/apache/pig/test/data/passwd' "
                          + "using PigStorage(':') as (uname:chararray, passwd:chararray, uid:int,gid:int);"
                          + "b = filter a by uid > 5;"
                          + "illustrate b;"
                          + "store b into '" + TMP_DIR + "/Pig-TestMultiQueryLocal1';\n";

            GruntParser parser = new GruntParser(new StringReader(script), myPig);
            parser.setInteractive(false);
            myPig.getPigContext().getProperties().setProperty("pig.usenewlogicalplan", "true");
            parser.parseStopOnError();

        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        } finally {
            deleteOutputFiles();
            myPig.getPigContext().getProperties().setProperty("pig.usenewlogicalplan", "false");
        }
    }

    @Test
    public void testStoreOrder() {
        System.out.println("===== multi-query store order =====");
        
        try {
            myPig.setBatchOn();
            myPig.registerQuery("a = load 'test/org/apache/pig/test/data/passwd';");
            myPig.registerQuery("store a into '" + TMP_DIR + "/Pig-TestMultiQueryLocal1' using BinStorage();");
            myPig.registerQuery("a = load '" + TMP_DIR + "/Pig-TestMultiQueryLocal1';");
            myPig.registerQuery("store a into '" + TMP_DIR + "/Pig-TestMultiQueryLocal2';");
            myPig.registerQuery("a = load '" + TMP_DIR + "/Pig-TestMultiQueryLocal1';");
            myPig.registerQuery("store a into '" + TMP_DIR + "/Pig-TestMultiQueryLocal3';");
            myPig.registerQuery("a = load '" + TMP_DIR + "/Pig-TestMultiQueryLocal2' using BinStorage();");
            myPig.registerQuery("store a into '" + TMP_DIR + "/Pig-TestMultiQueryLocal4';");
            myPig.registerQuery("a = load '" + TMP_DIR + "/Pig-TestMultiQueryLocal2';");
            myPig.registerQuery("b = load '" + TMP_DIR + "/Pig-TestMultiQueryLocal1';");
            myPig.registerQuery("c = cogroup a by $0, b by $0;");
            myPig.registerQuery("store c into '" + TMP_DIR + "/Pig-TestMultiQueryLocal5';");

            LogicalPlan lp = checkLogicalPlan(1, 3, 12);
            PhysicalPlan pp = checkPhysicalPlan(lp, 1, 3, 15);

            myPig.executeBatch();
            myPig.discardBatch(); 

            Assert.assertTrue(new File(TMP_DIR + "/Pig-TestMultiQueryLocal1").exists());
            Assert.assertTrue(new File(TMP_DIR + "/Pig-TestMultiQueryLocal2").exists());
            Assert.assertTrue(new File(TMP_DIR + "/Pig-TestMultiQueryLocal3").exists());
            Assert.assertTrue(new File(TMP_DIR + "/Pig-TestMultiQueryLocal4").exists());
            Assert.assertTrue(new File(TMP_DIR + "/Pig-TestMultiQueryLocal5").exists());

            
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
            java.lang.reflect.Method buildLp = myPig.getClass().getDeclaredMethod("buildLp");
            buildLp.setAccessible(true);
            lp = (LogicalPlan) buildLp.invoke( myPig );

            Assert.assertNotNull(lp);

        } catch (Exception e) {
            PigException pe = LogUtils.getPigException(e);
            if (pe != null) {
                throw pe;
            } else {
                e.printStackTrace();
                Assert.fail();
            }
        }

        Assert.assertEquals(expectedRoots, lp.getSources().size());
        Assert.assertEquals(expectedLeaves, lp.getSinks().size());
        Assert.assertEquals(expectedSize, lp.size());

        TestMultiQueryCompiler.showLPOperators(lp);

        return lp;
    }

    private PhysicalPlan checkPhysicalPlan(LogicalPlan lp, int expectedRoots,
            int expectedLeaves, int expectedSize) throws IOException {

        System.out.println("===== check physical plan =====");        

        PhysicalPlan pp = ((MRExecutionEngine)myPig.getPigContext().getExecutionEngine()).compile(
                lp, null);

        Assert.assertEquals(expectedRoots, pp.getRoots().size());
        Assert.assertEquals(expectedLeaves, pp.getLeaves().size());
        Assert.assertEquals(expectedSize, pp.size());

        showPlanOperators(pp);

        return pp;
    }

    private boolean executePlan(PhysicalPlan pp) throws IOException {
        boolean failed = true;
        MapReduceLauncher launcher = new MapReduceLauncher();
        PigStats stats = null;
        try {
            stats = launcher.launchPig(pp, "execute", myPig.getPigContext());
        } catch (Exception e) {
            e.printStackTrace(System.out);
            throw new IOException(e);
        }
        Iterator<JobStats> iter = stats.getJobGraph().iterator();
        while (iter.hasNext()) {
            JobStats js = iter.next();
            failed = !js.isSuccessful();
            if (failed) {
                break;
            }
        }
        return !failed;
    }

    private void deleteOutputFiles() {
        String outputFiles[] = { TMP_DIR + "/Pig-TestMultiQueryLocal1",
                                 TMP_DIR + "/Pig-TestMultiQueryLocal2",
                                 TMP_DIR + "/Pig-TestMultiQueryLocal3",
                                 TMP_DIR + "/Pig-TestMultiQueryLocal4",
                                 TMP_DIR + "/Pig-TestMultiQueryLocal5"
                };
        try {
            for( String outputFile : outputFiles ) {
                if( isDirectory(outputFile) ) {
                    deleteDir( new File( outputFile ) );
                } else {
                    FileLocalizer.delete(outputFile, myPig.getPigContext());
                }    
            }            
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }
    
    private void deleteDir( File file ) {
        if( file.isDirectory() && file.listFiles().length != 0 ) {
            for( File innerFile : file.listFiles() ) {
                deleteDir( innerFile );
            }
        }
        file.delete();
    }
    
    private boolean isDirectory( String filepath ) {
        File file = new File( filepath );
        return file.isDirectory();
    }

}

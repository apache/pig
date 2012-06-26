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

import java.io.IOException;
import java.util.Iterator;
import java.util.Properties;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.pig.ExecType;
import org.apache.pig.IndexableLoadFunc;
import org.apache.pig.LoadCaster;
import org.apache.pig.LoadFunc;
import org.apache.pig.PigException;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROperPlan;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.LogUtils;
import org.apache.pig.test.utils.TestHelper;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

public class TestMergeJoin {

    private static final String INPUT_FILE = "testMergeJoinInput.txt";
    private static final String INPUT_FILE2 = "testMergeJoinInput2.txt";
    private PigServer pigServer;
    private static MiniCluster cluster = MiniCluster.buildCluster();

    public TestMergeJoin() throws ExecException{

        Properties props = cluster.getProperties();
        props.setProperty("mapred.map.max.attempts", "1");
        props.setProperty("mapred.reduce.max.attempts", "1");
        pigServer = new PigServer(ExecType.MAPREDUCE, props);
    }
    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
        int LOOP_SIZE = 3;
        String[] input = new String[LOOP_SIZE*LOOP_SIZE];
        int k = 0;
        for(int i = 1; i <= LOOP_SIZE; i++) {
            String si = i + "";
            for(int j=1;j<=LOOP_SIZE;j++)
                input[k++] = si + "\t" + j;
        }
        Util.createInputFile(cluster, INPUT_FILE, input);
        
        Util.createInputFile(cluster, INPUT_FILE2, new String[]{"2"});
    }

    @AfterClass
    public static void oneTimeTearDown() throws Exception {
        cluster.shutDown();
    }
    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception {
        Util.deleteFile(cluster, INPUT_FILE);
        Util.deleteFile(cluster, INPUT_FILE2);
    }

    @Test
    public void testRecursiveFileListing() throws IOException{
        Util.createInputFile(cluster, "foo/bar/test.dat", new String[]{"2"});
        pigServer.registerQuery("A = LOAD 'foo';");
        pigServer.registerQuery("B = LOAD 'foo';");
        DataBag dbMergeJoin = BagFactory.getInstance().newDefaultBag(), dbshj = BagFactory.getInstance().newDefaultBag();
        {
            pigServer.registerQuery("C = join A by $0, B by $0 using 'merge';");
            Iterator<Tuple> iter = pigServer.openIterator("C");

            while(iter.hasNext()) {
                dbMergeJoin.add(iter.next());
            }
        }
        {
            pigServer.registerQuery("C = join A by $0, B by $0;");
            Iterator<Tuple> iter = pigServer.openIterator("C");

            while(iter.hasNext()) {
                dbshj.add(iter.next());
            }
        }
        Assert.assertEquals(dbMergeJoin.size(), dbshj.size());
        Assert.assertEquals(true, TestHelper.compareBags(dbMergeJoin, dbshj));
        Util.deleteFile(cluster,"foo/bar/test.dat");
    }
    
    @Test
    public void testMergeJoinSimplest() throws IOException{
        pigServer.registerQuery("A = LOAD '" + INPUT_FILE + "';");
        pigServer.registerQuery("B = LOAD '" + INPUT_FILE + "';");
        DataBag dbMergeJoin = BagFactory.getInstance().newDefaultBag(), dbshj = BagFactory.getInstance().newDefaultBag();
        {
            pigServer.registerQuery("C = join A by $0, B by $0 using 'merge';");
            Iterator<Tuple> iter = pigServer.openIterator("C");

            while(iter.hasNext()) {
                dbMergeJoin.add(iter.next());
            }
        }
        {
            pigServer.registerQuery("C = join A by $0, B by $0;");
            Iterator<Tuple> iter = pigServer.openIterator("C");

            while(iter.hasNext()) {
                dbshj.add(iter.next());
            }
        }
        Assert.assertEquals(dbMergeJoin.size(), dbshj.size());
        Assert.assertEquals(true, TestHelper.compareBags(dbMergeJoin, dbshj));
    }

    @Test
    public void testMergeJoinOnMultiFields() throws IOException{
        pigServer.registerQuery("A = LOAD '" + INPUT_FILE + "';");
        pigServer.registerQuery("B = LOAD '" + INPUT_FILE + "';");
        DataBag dbMergeJoin = BagFactory.getInstance().newDefaultBag(), dbshj = BagFactory.getInstance().newDefaultBag();
        {
            pigServer.registerQuery("C = join A by ($0,$1), B by ($0,$1) using 'merge';");
            Iterator<Tuple> iter = pigServer.openIterator("C");

            while(iter.hasNext()) {
                dbMergeJoin.add(iter.next());
            }
        }
        {
            pigServer.registerQuery("C = join A by ($0,$1), B by ($0,$1);");
            Iterator<Tuple> iter = pigServer.openIterator("C");

            while(iter.hasNext()) {
                dbshj.add(iter.next());
            }
        }
        Assert.assertEquals(dbMergeJoin.size(), dbshj.size());
        Assert.assertEquals(true, TestHelper.compareBags(dbMergeJoin, dbshj));
    }

    @Test
    public void testMergeJoinWithExpr() throws IOException{
        pigServer.registerQuery("A = LOAD '" + INPUT_FILE + "';");
        pigServer.registerQuery("B = LOAD '" + INPUT_FILE + "';");
        DataBag dbMergeJoin = BagFactory.getInstance().newDefaultBag(), dbshj = BagFactory.getInstance().newDefaultBag();
        {
            pigServer.registerQuery("C = join A by ($0+10), B by ($0+10) using 'merge';");
            Iterator<Tuple> iter = pigServer.openIterator("C");

            while(iter.hasNext()) {
                dbMergeJoin.add(iter.next());
            }
        }
        {
            pigServer.registerQuery("C = join A by ($0+10), B by ($0+10);");
            Iterator<Tuple> iter = pigServer.openIterator("C");

            while(iter.hasNext()) {
                dbshj.add(iter.next());
            }
        }
        Assert.assertEquals(dbMergeJoin.size(), dbshj.size());
        Assert.assertEquals(true, TestHelper.compareBags(dbMergeJoin, dbshj));
    }

    @Test
    public void testMergeJoinOutWithSchema() throws IOException{

        pigServer.registerQuery("A = LOAD '" + INPUT_FILE + "' as (x:int,y:int);");
        pigServer.registerQuery("B = LOAD '" + INPUT_FILE + "' as (x:int,y:int);");
        DataBag dbMergeJoin = BagFactory.getInstance().newDefaultBag(), dbshj = BagFactory.getInstance().newDefaultBag();
        {
            pigServer.registerQuery("C = join A by $0, B by $0 using 'merge';");
            Iterator<Tuple> iter = pigServer.openIterator("C");

            while(iter.hasNext()) {
                dbMergeJoin.add(iter.next());
            }
        }
        {
            pigServer.registerQuery("C = join A by $0, B by $0;");
            Iterator<Tuple> iter = pigServer.openIterator("C");

            while(iter.hasNext()) {
                dbshj.add(iter.next());
            }
        }
        Assert.assertEquals(dbMergeJoin.size(), dbshj.size());
        Assert.assertEquals(true, TestHelper.compareBags(dbMergeJoin, dbshj));
    }

    @Test
    public void testMergeJoinOutWithFilters() throws IOException{

        pigServer.registerQuery("A = LOAD '" + INPUT_FILE + "';");
        pigServer.registerQuery("B = LOAD '" + INPUT_FILE + "';");
        pigServer.registerQuery("C = FILTER A by $1 > 1;"); 
        pigServer.registerQuery("D = FILTER B by $1 > 1;"); 
        DataBag dbMergeJoin = BagFactory.getInstance().newDefaultBag(), dbshj = BagFactory.getInstance().newDefaultBag();
        {
            pigServer.registerQuery("E = join C by $0, D by $0 using 'merge';");
            Iterator<Tuple> iter = pigServer.openIterator("E");

            while(iter.hasNext()) {
                dbMergeJoin.add(iter.next());
            }
        }
        {
            pigServer.registerQuery("E = join C by $0, D by $0;");
            Iterator<Tuple> iter = pigServer.openIterator("E");

            while(iter.hasNext()) {
                dbshj.add(iter.next());
            }
        }
        Assert.assertEquals(dbMergeJoin.size(), dbshj.size());
        Assert.assertEquals(true, TestHelper.compareBags(dbMergeJoin, dbshj));
    }

    @Test
    public void testMergeJoinOutWithProjects() throws IOException{

        pigServer.registerQuery("A = LOAD '" + INPUT_FILE + "' as (f1,f2);");
        pigServer.registerQuery("B = LOAD '" + INPUT_FILE + "'as (f1,f2);");
        pigServer.registerQuery("C = foreach A generate f1;"); 
        pigServer.registerQuery("D = foreach B generate f1;"); 
        DataBag dbMergeJoin = BagFactory.getInstance().newDefaultBag(), dbshj = BagFactory.getInstance().newDefaultBag();
        {
            pigServer.registerQuery("E = join C by f1, D by f1 using 'merge';");
            Iterator<Tuple> iter = pigServer.openIterator("E");

            while(iter.hasNext()) {
                dbMergeJoin.add(iter.next());
            }
        }
        {
            pigServer.registerQuery("E = join C by f1, D by f1;");
            Iterator<Tuple> iter = pigServer.openIterator("E");

            while(iter.hasNext()) {
                dbshj.add(iter.next());
            }
        }
        Assert.assertEquals(dbMergeJoin.size(), dbshj.size());
        Assert.assertEquals(true, TestHelper.compareBags(dbMergeJoin, dbshj));
    }

    @Test
    public void testMergeJoinOutPipeline() throws IOException{
        pigServer.registerQuery("A = LOAD '" + INPUT_FILE + "' as (f1:int,f2:int);");
        pigServer.registerQuery("B = LOAD '" + INPUT_FILE + "' as (f1:int,f2:int);");
        DataBag dbmrj = BagFactory.getInstance().newDefaultBag(), dbshj = BagFactory.getInstance().newDefaultBag();
        {
            pigServer.registerQuery("C = join A by $0, B by $0 using 'merge';");
            pigServer.registerQuery("G = LOAD '" + INPUT_FILE + "' as (f1:int,f2:int);");
            pigServer.registerQuery("H = LOAD '" + INPUT_FILE + "' as (f1:int,f2:int);");
            pigServer.registerQuery("D = join G by $0, H by $0 using 'merge';");
            pigServer.registerQuery("E = union C,D;");
            pigServer.registerQuery("F = filter E by 1 == 1;");
            Iterator<Tuple> iter = pigServer.openIterator("F");

            while(iter.hasNext()) {
                dbmrj.add(iter.next());
            }
        }
        // Note that these two queries are setup little differently. This is because if there is a Split in Plan, Merge Join fails.
        // When work in MRCompiler finishes, this query along with merge join preceded by order-by should be two test-cases for it.
        {
            pigServer.registerQuery("C = join A by $0, B by $0;");
            pigServer.registerQuery("D = join A by $0, B by $0;");
            pigServer.registerQuery("E = union C,D;");
            pigServer.registerQuery("F = filter E by 1 == 1;");
            Iterator<Tuple> iter = pigServer.openIterator("F");

            while(iter.hasNext()) {
                dbshj.add(iter.next());
            }
        }
        Assert.assertEquals(dbmrj.size(), dbshj.size());
        Assert.assertEquals(true, TestHelper.compareBags(dbmrj, dbshj));
    }

    @Test
    public void testMergeJoinWithNulls() throws IOException{

        String[] input = new String[3*3];
        input[0] = "\t2";
        input[1] = "1\t2";
        input[2] = "1\t2";
        input[3] = "\t2";
        input[4] = "3\t2";
        input[5] = "\t";
        input[6] = "5\t";
        input[7] = "7\t";
        input[8] = "7\t1";
        Util.createInputFile(cluster, "temp_file", input);
        pigServer.registerQuery("A = LOAD 'temp_file';");
        pigServer.registerQuery("B = LOAD 'temp_file';");
        DataBag dbmrj = BagFactory.getInstance().newDefaultBag(), dbshj = BagFactory.getInstance().newDefaultBag();
        {
            pigServer.registerQuery("C = join A by $0, B by $0 using 'merge';");
            Iterator<Tuple> iter = pigServer.openIterator("C");

            while(iter.hasNext()) {
                dbmrj.add(iter.next());
            }
        }
        {
            pigServer.registerQuery("C = join A by $0, B by $0;");
            Iterator<Tuple> iter = pigServer.openIterator("C");

            while(iter.hasNext()) {
                dbshj.add(iter.next());
            }
        }
        Util.deleteFile(cluster, "temp_file");
        Assert.assertEquals(dbmrj.size(),dbshj.size());
        Assert.assertEquals(true, TestHelper.compareBags(dbmrj, dbshj));
    }

    @Test
    public void testMergeJoinWithMRBoundaryLater() throws IOException{
        pigServer.registerQuery("A = LOAD '" + INPUT_FILE + "' as (x:int,y:int);");
        pigServer.registerQuery("B = LOAD '" + INPUT_FILE + "' as (x:int,y:int);");
        DataBag dbMergeJoin = BagFactory.getInstance().newDefaultBag(), dbshj = BagFactory.getInstance().newDefaultBag();
        {
            pigServer.registerQuery("C = join A by $0, B by $0 using 'merge';");
            pigServer.registerQuery("D = group C by $0;");
            pigServer.registerQuery("E = filter D by $0 > 0;");
            Iterator<Tuple> iter = pigServer.openIterator("E");

            while(iter.hasNext()) {
                dbMergeJoin.add(iter.next());
            }
        }
        {
            pigServer.registerQuery("C = join A by $0, B by $0;");
            pigServer.registerQuery("D = group C by $0 ;");
            pigServer.registerQuery("E = filter D by $0 > 0;");
            Iterator<Tuple> iter = pigServer.openIterator("E");

            while(iter.hasNext()) {
                dbshj.add(iter.next());
            }
        }
        Assert.assertEquals(dbMergeJoin.size(), dbshj.size());
        Assert.assertEquals(true, TestHelper.compareBags(dbMergeJoin, dbshj));
    }

    @Test
    public void testMergeJoin3Way() throws IOException{
        try {
            pigServer.registerQuery("A = LOAD '" + INPUT_FILE + "' as (id, name, n);");
            pigServer.registerQuery("B = LOAD '" + INPUT_FILE + "' as (id, name);");
            pigServer.registerQuery("C = LOAD '" + INPUT_FILE + "' as (id, name);");
            pigServer.registerQuery("D = join A by id, B by id, C by id using 'merge';");
        }catch(Exception e) {
            PigException pe = LogUtils.getPigException(e);
            Assert.assertTrue( pe.getMessage().contains( "Merge join can only be applied for 2-way joins" ) );
            return;
        }
        Assert.fail("Should throw exception, do not support 3 way join");
    }       

    @Test
    public void testMergeJoinWithOrderAndSplit() throws Exception{
        String query = "A = LOAD '" + INPUT_FILE + "' as (id, name, n);\n" +
                "B = LOAD '" + INPUT_FILE + "' as (id, name);\n" +
                "C = ORDER A by $0 parallel 5;\n" +
                "D = join A by id, C by id using 'merge';\n" +
                "store D into '/dev/null/1';";
        // verify that this passes parsing sanity checks.
        Util.buildPp(pigServer, query);
    }

    @Test
    public void testMergeJoinWithOrder() throws Exception{
        String query = "A = LOAD '" + INPUT_FILE + "' as (id, name, n);\n" +
                "B = LOAD '" + INPUT_FILE + "' as (id, name);\n" +
                "C = ORDER B by $0 parallel 5;\n" +
                "D = join A by id, C by id using 'merge';\n" +
                "store D into '/dev/null/1';";
        // verify that this passes parsing sanity checks.
        Util.buildPp(pigServer, query);
    }

    @Test
    public void testMergeFailWithOrderUDF() throws Exception{
        String query = "A = LOAD '" + INPUT_FILE + "' as (id, name, n);\n" +
                "B = LOAD '" + INPUT_FILE + "' as (id, name);\n" +
                "A = FOREACH A GENERATE LOWER($0) as id;\n" +
                "C = ORDER B by $0 parallel 5;\n" +
                "D = join A by id, C by id using 'merge';\n" +
                "store D into '/dev/null/1';";
        // verify that this fails parsing sanity checks.
        try {
            Util.buildPp(pigServer, query);
        } catch (Throwable t) {
            // expected to fail.
            return;
        }
        Assert.fail("Allowed a Merge Join despite a UDF");
    }       

    @Test
    public void testMergeJoinFailure2() throws IOException{
        pigServer.registerQuery("A = LOAD '" + INPUT_FILE + "' as (id, name, n);");
        pigServer.registerQuery("B = LOAD '" + INPUT_FILE + "' as (id, name);");
        pigServer.registerQuery("C = GROUP B by id;");
        pigServer.registerQuery("D = join A by id, C by $0 using 'merge';");
        try {
            pigServer.openIterator("D");
        }catch(Exception e) {
            PigException pe = LogUtils.getPigException(e);
            Assert.assertEquals(1103,pe.getErrorCode());
            return;
        }
        Assert.fail("Should fail to compile");
    }       

    @Test
    public void testEmptyRightFile() throws IOException{
        DataBag dbmrj = BagFactory.getInstance().newDefaultBag(), dbshj = BagFactory.getInstance().newDefaultBag();
        Util.createInputFile(cluster, "temp_file", new String[]{});
        pigServer.registerQuery("A = LOAD '" + INPUT_FILE + "';");
        pigServer.registerQuery("B = LOAD 'temp_file';");
        pigServer.registerQuery("C = join A by $0, B by $0 using 'merge';");
        pigServer.openIterator("C");
        {
            Iterator<Tuple> iter = pigServer.openIterator("C");

            while(iter.hasNext()) 
                dbmrj.add(iter.next());
        }
        {
            pigServer.registerQuery("C = join A by $0, B by $0;");
            Iterator<Tuple> iter = pigServer.openIterator("C");

            while(iter.hasNext()) {
                dbshj.add(iter.next());
            }
        }
        Assert.assertEquals(dbmrj.size(), dbshj.size());
        Assert.assertEquals(true, TestHelper.compareBags(dbmrj, dbshj));
        Util.deleteFile(cluster, "temp_file");
    }       

    @Test
    public void testParallelism() throws Exception{
        String query = "A = LOAD '" + INPUT_FILE + "';" +
                       "B = LOAD '" + INPUT_FILE + "';" +
                       "C = join A by $0, B by $0 using 'merge' parallel 50;" + 
                       "store C into 'out';";
	PigContext pc = new PigContext(ExecType.MAPREDUCE,cluster.getProperties());
    pc.connect();
	MROperPlan mro = Util.buildMRPlan(Util.buildPp(pigServer, query),pc);
        Assert.assertEquals(1,mro.getRoots().get(0).getRequestedParallelism());
    }

    @Test
    public void testIndexer() throws IOException{
        Util.createInputFile(cluster, "temp_file1", new String[]{1+""});
        Util.createInputFile(cluster, "temp_file2", new String[]{2+""});
        Util.createInputFile(cluster, "temp_file3", new String[]{10+""});
        pigServer.registerQuery("A = LOAD 'temp_file*' as (a:int);");
        pigServer.registerQuery("B = LOAD 'temp_file*' as (a:int);");
        DataBag dbmrj = BagFactory.getInstance().newDefaultBag(), dbshj = BagFactory.getInstance().newDefaultBag();
        {
            pigServer.registerQuery("C = join A by $0, B by $0 using 'merge';");
            Iterator<Tuple> iter = pigServer.openIterator("C");

            while(iter.hasNext()) {
                dbmrj.add(iter.next());
            }
        }
        {
            pigServer.registerQuery("C = join A by $0, B by $0;");
            Iterator<Tuple> iter = pigServer.openIterator("C");

            while(iter.hasNext()) {
                dbshj.add(iter.next());
            }
        }
        Util.deleteFile(cluster, "temp_file1");
        Util.deleteFile(cluster, "temp_file2");
        Util.deleteFile(cluster, "temp_file3");
        Assert.assertEquals(dbmrj.size(),dbshj.size());
        Assert.assertEquals(true, TestHelper.compareBags(dbmrj, dbshj));
    }
    
    @Test
    public void testExpression() throws IOException{
        Util.createInputFile(cluster, "temp_file1", new String[]{"8", "9"});
        Util.createInputFile(cluster, "temp_file2", new String[]{"10", "11"});
        Util.createInputFile(cluster, "temp_file3", new String[]{"20"});
        Util.createInputFile(cluster, "leftinput", new String[] { "9", "11"});
        pigServer.registerQuery("A = LOAD 'leftinput' as (a:int);");
        pigServer.registerQuery("B = LOAD 'temp_file*' as (a:int);");
        DataBag dbmrj = BagFactory.getInstance().newDefaultBag(), dbshj = BagFactory.getInstance().newDefaultBag();
        {
            pigServer.registerQuery("C = join A by $0 + 10, B by $0 + 10 using 'merge';");
            Iterator<Tuple> iter = pigServer.openIterator("C");

            while(iter.hasNext()) {
                dbmrj.add(iter.next());
            }
        }
        {
            pigServer.registerQuery("C = join A by $0 + 10, B by $0 + 10;");
            Iterator<Tuple> iter = pigServer.openIterator("C");

            while(iter.hasNext()) {
                dbshj.add(iter.next());
            }
        }
        Util.deleteFile(cluster, "temp_file1");
        Util.deleteFile(cluster, "temp_file2");
        Util.deleteFile(cluster, "temp_file3");
        Util.deleteFile(cluster, "leftinput");
        Assert.assertEquals(dbmrj.size(),dbshj.size());
        Assert.assertEquals(true, TestHelper.compareBags(dbmrj, dbshj));
    }
    
    @Test
    public void testExpressionFail() throws IOException{
        pigServer.registerQuery("A = LOAD 'leftinput' as (a:int);");
        pigServer.registerQuery("B = LOAD 'temp_file*' using " +
                DummyIndexableLoader.class.getName() + "() as (a:int);");
        boolean exceptionThrown = false;
        try {
            pigServer.registerQuery("C = join A by $0 + 10, B by $0 + 10 using 'merge';");
            Iterator<Tuple> iter = pigServer.openIterator("C");

        }catch (Exception e) {
            e.printStackTrace();
            PigException pe = LogUtils.getPigException(e);
            Assert.assertEquals(1106, pe.getErrorCode());
            exceptionThrown = true;
        }
        Assert.assertEquals(true, exceptionThrown);
    }
    
    @Test
    public void testMergeJoinSch1() throws IOException{
        pigServer.registerQuery("A = LOAD '" + INPUT_FILE + "' as (x:int,y:int);");
        pigServer.registerQuery("B = LOAD '" + INPUT_FILE + "' as (x:int,y:int);");
        Schema mjSch = null, shjSch = null;
        pigServer.registerQuery("C = join A by $0, B by $0 using 'merge';");
        mjSch = pigServer.dumpSchema("C");
        pigServer.registerQuery("C = join A by $0, B by $0;");
        shjSch = pigServer.dumpSchema("C");
        Assert.assertEquals(true, shjSch.equals(mjSch));
    }

    @Test
    public void testMergeJoinSch2() throws IOException{
        pigServer.registerQuery("A = LOAD '" + INPUT_FILE + "';");
        pigServer.registerQuery("B = LOAD '" + INPUT_FILE + "';");
        Schema mjSch = null, shjSch = null;
        pigServer.registerQuery("C = join A by ($0,$1), B by ($0,$1) using 'merge';");
        mjSch = pigServer.dumpSchema("C");
        pigServer.registerQuery("C = join A by ($0,$1), B by ($0,$1);");
        shjSch = pigServer.dumpSchema("C");
        Assert.assertTrue(shjSch == null);
    }
    
    @Test
    public void testMergeJoinWithCommaSeparatedFilePaths() throws IOException{

        pigServer.registerQuery("A = LOAD '" + INPUT_FILE + "';");
        pigServer.registerQuery("B = LOAD 'temp_file,righinput_file' using " +
                DummyIndexableLoader.class.getName() + "();");

        pigServer.registerQuery("C = join A by $0, B by $0 using 'merge';");
            
        Iterator<Tuple> iter = pigServer.openIterator("C");
        Assert.assertFalse(iter.hasNext());
    }
    
    @Test
    public void testMergeJoinEmptyIndex() throws IOException{
        DataBag dbMergeJoin = BagFactory.getInstance().newDefaultBag(), dbshj = BagFactory.getInstance().newDefaultBag();
        pigServer.registerQuery("A = LOAD '" + INPUT_FILE2 + "';");
        pigServer.registerQuery("B = LOAD '" + INPUT_FILE + "';");
        
        {
            pigServer.registerQuery("C = join A by $0, B by $0 using 'merge';");
            Iterator<Tuple> iter = pigServer.openIterator("C");
            while(iter.hasNext()) {
                dbMergeJoin.add(iter.next());
            }
        }
        
        {
            pigServer.registerQuery("C = join A by $0, B by $0;");
            Iterator<Tuple> iter = pigServer.openIterator("C");

            while(iter.hasNext()) {
                dbshj.add(iter.next());
            }
        }

        Assert.assertEquals(dbMergeJoin.size(), dbshj.size());
        Assert.assertEquals(true, TestHelper.compareBags(dbMergeJoin, dbshj));        
    }
    
    /**
     * A dummy loader which implements {@link IndexableLoadFunc} to test
     * that expressions are not allowed as merge join keys when the right input's
     * loader implements {@link IndexableLoadFunc}
     */
    public static class DummyIndexableLoader extends LoadFunc implements IndexableLoadFunc{

        /**
         * 
         */
        public DummyIndexableLoader() {
        }
 
        @Override
        public void close() throws IOException {
 
        }

        @Override
        public void seekNear(Tuple keys) throws IOException {
 
        }

        @Override
        public Tuple getNext() throws IOException {
            return null;
        }

        @Override
        public void initialize(Configuration conf) throws IOException {
        }

        @Override
        public InputFormat getInputFormat() throws IOException {
            return null;
        }

        @Override
        public LoadCaster getLoadCaster() throws IOException {            
            return null;
        }

        @Override
        public void prepareToRead(RecordReader reader, PigSplit split)
                throws IOException {

        }

        @Override
        public void setLocation(String location, Job job) throws IOException {

        }
        
    }
}

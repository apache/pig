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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.pig.ExecType;
import org.apache.pig.LoadFunc;
import org.apache.pig.PigServer;
import org.apache.pig.ResourceSchema;
import org.apache.pig.StoreFunc;
import org.apache.pig.backend.executionengine.ExecJob;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.tools.grunt.GruntParser;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
@RunWith(JUnit4.class)
public class TestMultiQueryBasic {

    private static final MiniCluster cluster = MiniCluster.buildCluster();

    private PigServer myPig;

    @BeforeClass
    public static void setUpBeforeClass() throws IOException {
        Util.copyFromLocalToCluster(cluster,
                "test/org/apache/pig/test/data/passwd", "passwd");
        Util.copyFromLocalToCluster(cluster,
                "test/org/apache/pig/test/data/passwd2", "passwd2");
    }
    
    @AfterClass
    public static void tearDownAfterClass() throws IOException {
        Util.deleteFile(cluster, "passwd");
        Util.deleteFile(cluster, "passwd2");
        cluster.shutDown();
    }
    
    @Before
    public void setUp() throws Exception {
        cluster.setProperty("opt.multiquery", ""+true);
        myPig = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        deleteOutputFiles();
    }

    @After
    public void tearDown() throws Exception {
        myPig = null;
    }
    
 
    @Test
    public void testMultiQueryWithTwoStores2() {

        System.out.println("===== multi-query with 2 stores (2) =====");

        try {
            myPig.setBatchOn();

            myPig.registerQuery("a = load 'passwd' " +
                                "using PigStorage(':') as (uname:chararray, passwd:chararray, uid:int,gid:int);");
            myPig.registerQuery("b = filter a by uid > 5;");
            myPig.registerQuery("store b into '/tmp/output1';");
            myPig.registerQuery("c = group b by gid;");
            myPig.registerQuery("store c into '/tmp/output2';");

            List<ExecJob> jobs = myPig.executeBatch();
            assertTrue(jobs.size() == 2);

            for (ExecJob job : jobs) {
                assertTrue(job.getStatus() == ExecJob.JOB_STATUS.COMPLETED);
            }

        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        } 
    }

    @Test
    public void testMultiQueryWithTwoLoads2() {

        System.out.println("===== multi-query with two loads (2) =====");

        try {
            myPig.setBatchOn();

            myPig.registerQuery("a = load 'passwd' " +
                                "using PigStorage(':') as (uname:chararray, passwd:chararray, uid:int,gid:int);");
            myPig.registerQuery("b = load 'passwd2' " +
                                "using PigStorage(':') as (uname:chararray, passwd:chararray, uid:int,gid:int);");
            myPig.registerQuery("c = filter a by uid > 5;");
            myPig.registerQuery("d = filter b by uid > 10;");
            myPig.registerQuery("store c into '/tmp/output1';");
            myPig.registerQuery("store d into '/tmp/output2';");
            myPig.registerQuery("e = cogroup c by uid, d by uid;");
            myPig.registerQuery("store e into '/tmp/output3';");

            myPig.executeBatch();
            myPig.discardBatch();

        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        } 
    }       
    
    @Test
    public void testMultiQueryPhase3BaseCase2() {

        System.out.println("===== multi-query phase 3 base case (2) =====");

        try {
            myPig.setBatchOn();

            myPig.registerQuery("a = load 'passwd' " +
                                 "using PigStorage(':') as (uname:chararray, passwd:chararray, uid:int, gid:int);");
            myPig.registerQuery("b = filter a by uid < 5;");
            myPig.registerQuery("c = filter a by uid >= 5 and uid < 10;");
            myPig.registerQuery("d = filter a by uid >= 10;");
            myPig.registerQuery("b1 = group b by gid;");
            myPig.registerQuery("b2 = foreach b1 generate group, COUNT(b.uid);");
            myPig.registerQuery("b3 = filter b2 by $1 > 5;");
            myPig.registerQuery("store b3 into '/tmp/output1';");
            myPig.registerQuery("c1 = group c by gid;");
            myPig.registerQuery("c2 = foreach c1 generate group, SUM(c.uid);");
            myPig.registerQuery("store c2 into '/tmp/output2';");
            myPig.registerQuery("d1 = group d by gid;");
            myPig.registerQuery("d2 = foreach d1 generate group, AVG(d.uid);");            
            myPig.registerQuery("store d2 into '/tmp/output3';");
             
            List<ExecJob> jobs = myPig.executeBatch();
            
            for (ExecJob job : jobs) {
                assertTrue(job.getStatus() == ExecJob.JOB_STATUS.COMPLETED);
            }
            
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        } 
    }         
    
    @Test
    public void testMultiQueryPhase3WithoutCombiner2() {

        System.out.println("===== multi-query phase 3 without combiner (2) =====");

        try {
            myPig.setBatchOn();

            myPig.registerQuery("a = load 'passwd' " +
                                 "using PigStorage(':') as (uname:chararray, passwd:chararray, uid:int, gid:int);");
            myPig.registerQuery("b = filter a by uid < 5;");
            myPig.registerQuery("c = filter a by uid >= 5 and uid < 10;");
            myPig.registerQuery("d = filter a by uid >= 10;");
            myPig.registerQuery("b1 = group b by gid;");
            myPig.registerQuery("b2 = foreach b1 generate group, COUNT(b.uid) + SUM(b.uid);");
            myPig.registerQuery("b3 = filter b2 by $1 > 5;");
            myPig.registerQuery("store b3 into '/tmp/output1';");
            myPig.registerQuery("c1 = group c by gid;");
            myPig.registerQuery("c2 = foreach c1 generate group, SUM(c.uid) - COUNT(c.uid);");
            myPig.registerQuery("store c2 into '/tmp/output2';");
            myPig.registerQuery("d1 = group d by gid;");           
            myPig.registerQuery("d2 = foreach d1 generate group, MAX(d.uid) - MIN(d.uid);");
            myPig.registerQuery("store d2 into '/tmp/output3';");
             
            List<ExecJob> jobs = myPig.executeBatch();

            for (ExecJob job : jobs) {
                assertTrue(job.getStatus() == ExecJob.JOB_STATUS.COMPLETED);
            }
  
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        } 
    }     
    
    @Test
    public void testMultiQueryPhase3WithMixedCombiner2() {

        System.out.println("===== multi-query phase 3 with mixed combiner (2) =====");

        try {
            myPig.setBatchOn();

            myPig.registerQuery("a = load 'passwd' " +
                                 "using PigStorage(':') as (uname:chararray, passwd:chararray, uid:int, gid:int);");
            myPig.registerQuery("b = filter a by uid < 5;");
            myPig.registerQuery("c = filter a by uid >= 5 and uid < 10;");
            myPig.registerQuery("d = filter a by uid >= 10;");
            myPig.registerQuery("b1 = group b by gid;");
            myPig.registerQuery("b2 = foreach b1 generate group, COUNT(b.uid);");
            myPig.registerQuery("b3 = filter b2 by $1 > 5;");
            myPig.registerQuery("store b3 into '/tmp/output1';");
            myPig.registerQuery("c1 = group c by gid;");
            myPig.registerQuery("c2 = foreach c1 generate group, SUM(c.uid);");
            myPig.registerQuery("store c2 into '/tmp/output2';");
            myPig.registerQuery("d1 = group d by gid;");            
            myPig.registerQuery("d2 = foreach d1 generate group, MAX(d.uid) - MIN(d.uid);");
            myPig.registerQuery("store d2 into '/tmp/output3';");
             
            List<ExecJob> jobs = myPig.executeBatch();
            assertEquals(3, jobs.size());

            for (ExecJob job : jobs) {
                assertTrue(job.getStatus() == ExecJob.JOB_STATUS.COMPLETED);
            }
            
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        } 
    }         

    @Test
    public void testMultiQueryPhase3WithDifferentMapDataTypes2() {

        System.out.println("===== multi-query phase 3 with different map datatypes (2) =====");

        try {
            myPig.setBatchOn();

            myPig.registerQuery("a = load 'passwd' " +
                                 "using PigStorage(':') as (uname:chararray, passwd:chararray, uid:int, gid:int);");
            myPig.registerQuery("b = filter a by uid < 5;");
            myPig.registerQuery("c = filter a by uid >= 5 and uid < 10;");
            myPig.registerQuery("d = filter a by uid >= 10;");
            myPig.registerQuery("b1 = group b by gid;");
            myPig.registerQuery("b2 = foreach b1 generate group, COUNT(b.uid);");
            myPig.registerQuery("b3 = filter b2 by $1 > 5;");
            myPig.registerQuery("store b3 into '/tmp/output1';");
            myPig.registerQuery("c1 = group c by $1;");
            myPig.registerQuery("c2 = foreach c1 generate group, SUM(c.uid);");
            myPig.registerQuery("store c2 into '/tmp/output2';");
            myPig.registerQuery("d1 = group d by $1;");
            myPig.registerQuery("d2 = foreach d1 generate group, COUNT(d.uid);");
            myPig.registerQuery("store d2 into '/tmp/output3';");
             
            List<ExecJob> jobs = myPig.executeBatch();
            assertEquals(3, jobs.size());

            for (ExecJob job : jobs) {
                assertTrue(job.getStatus() == ExecJob.JOB_STATUS.COMPLETED);
            }
            
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        } 
    }         
    
    @Test
    public void testMultiQueryPhase3WithDifferentMapDataTypes3() {

        System.out.println("===== multi-query phase 3 with different map datatypes (3) =====");

        try {
            myPig.setBatchOn();
            String[] inputData = {"john\t20\t3.4",
            		"john\t25\t3.4" ,
            		"henry\t23\t3.9" ,
            		"adam\t54\t2.9" ,
            		"henry\t21\t3.9"};
            Util.createInputFile(cluster, "queryInput.txt", inputData);

            myPig.registerQuery("a = load 'queryInput.txt' " +
                                 "as (name:chararray, age:int, gpa:double);");
            myPig.registerQuery("b = group a all;");
            myPig.registerQuery("c = foreach b generate group, COUNT(a);");
            myPig.registerQuery("store c into 'foo';");
            myPig.registerQuery("d = group a by (name, gpa);");
            myPig.registerQuery("e = foreach d generate flatten(group), MIN(a.age);");
            myPig.registerQuery("store e into 'bar';");
             
            myPig.executeBatch();
            
            myPig.registerQuery("a = load 'foo' as (grp:chararray, cnt:long) ;");
            Iterator<Tuple> it = myPig.openIterator("a");
            assertEquals(Util.getPigConstant("('all', 5l)"), it.next());
            assertFalse(it.hasNext());
            
            myPig.registerQuery("a = load 'bar' as (name:chararray, gpa:double, age:int);");
            it = myPig.openIterator("a");
            int i = 0;
            Map<String, Tuple> expectedResults = new HashMap<String, Tuple>();
            expectedResults.put("john", (Tuple) Util.getPigConstant("('john',3.4,20)"));
            expectedResults.put("adam", (Tuple) Util.getPigConstant("('adam',2.9,54)"));
            expectedResults.put("henry", (Tuple) Util.getPigConstant("('henry',3.9,21)"));
            while(it.hasNext()) {
                Tuple t = it.next();
                i++;
                assertEquals(expectedResults.get(t.get(0)), t);
            }
            assertEquals(3, i);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        } 
    }         
 
    @Test
    public void testMultiQueryPhase3StreamingInReducer2() {

        System.out.println("===== multi-query phase 3 with streaming in reducer (2) =====");

        try {
            myPig.setBatchOn();

            myPig.registerQuery("A = load 'passwd';");
            myPig.registerQuery("Split A into A1 if $2 > 5, A2 if $2 >= 5;");
            myPig.registerQuery("Split A1 into A3 if $0 > 'm', A4 if $0 >= 'm';");
            myPig.registerQuery("B = group A3 by $2;");
            myPig.registerQuery("C = foreach B generate flatten(A3);");
            myPig.registerQuery("D = stream B through `cat`;");
            myPig.registerQuery("store D into '/tmp/output1';");
            myPig.registerQuery("E = group A4 by $2;");
            myPig.registerQuery("F = foreach E generate group, COUNT(A4);");
            myPig.registerQuery("store F into '/tmp/output2';");            
            myPig.registerQuery("G = group A1 by $2;");
            myPig.registerQuery("H = foreach G generate group, COUNT(A1);");          
            myPig.registerQuery("store H into '/tmp/output3';");
             
            List<ExecJob> jobs = myPig.executeBatch();
            assertEquals(3, jobs.size());

            for (ExecJob job : jobs) {
                assertTrue(job.getStatus() == ExecJob.JOB_STATUS.COMPLETED);
            }
            
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        } 
    }       
    
    @Test
    public void testMultiQueryWithPigMixL12_2() {

        System.out.println("===== multi-query with PigMix L12 (2) =====");

        try {
            myPig.setBatchOn();

            myPig.registerQuery("a = load 'passwd' " +
                                "using PigStorage(':') as (uname, passwd, uid, gid);");
            myPig.registerQuery("b = foreach a generate uname, passwd, uid, gid;");
            myPig.registerQuery("split b into c1 if uid > 5, c2 if uid <= 5 ;"); 
            myPig.registerQuery("split c1 into d1 if gid < 5, d2 if gid >= 5;");
            myPig.registerQuery("e = group d1 by uname;");
            myPig.registerQuery("e1 = foreach e generate group, MAX(d1.uid);");
            myPig.registerQuery("store e1 into '/tmp/output1';");
            myPig.registerQuery("f = group c2 by uname;");
            myPig.registerQuery("f1 = foreach f generate group, SUM(c2.gid);");
            myPig.registerQuery("store f1 into '/tmp/output2';");
            myPig.registerQuery("g = group d2 by uname;");
            myPig.registerQuery("g1 = foreach g generate group, COUNT(d2);");
            myPig.registerQuery("store g1 into '/tmp/output3';");

            List<ExecJob> jobs = myPig.executeBatch();
            assertEquals(3, jobs.size());

            for (ExecJob job : jobs) {
                assertTrue(job.getStatus() == ExecJob.JOB_STATUS.COMPLETED);
            }
            
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        } 
    }
    
    @Test
    public void testMultiQueryWithCoGroup_2() {

        System.out.println("===== multi-query with CoGroup (2) =====");

        try {
            myPig.setBatchOn();

            myPig.registerQuery("a = load 'passwd' " +
                                "using PigStorage(':') as (uname, passwd, uid, gid);");
            myPig.registerQuery("store a into '/tmp/output1' using BinStorage();");
            myPig.registerQuery("b = load '/tmp/output1' using BinStorage() as (uname, passwd, uid, gid);"); 
            myPig.registerQuery("c = load 'passwd2' " +
                                "using PigStorage(':') as (uname, passwd, uid, gid);");
            myPig.registerQuery("d = cogroup b by (uname, uid) inner, c by (uname, uid) inner;");
            myPig.registerQuery("e = foreach d generate flatten(b), flatten(c);");
            myPig.registerQuery("store e into '/tmp/output2';");

            List<ExecJob> jobs = myPig.executeBatch();
            assertTrue(jobs.size() == 2);

            for (ExecJob job : jobs) {
                assertTrue(job.getStatus() == ExecJob.JOB_STATUS.COMPLETED);
            }

        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        } 
    }
 
    @Test
    public void testMultiQueryWithFJ_2() {

        System.out.println("===== multi-query with FJ (2) =====");

        try {
            myPig.setBatchOn();

            myPig.registerQuery("a = load 'passwd' " +
                                "using PigStorage(':') as (uname:chararray, passwd:chararray, uid:int, gid:int);");
            myPig.registerQuery("b = load 'passwd' " +
                                "using PigStorage(':') as (uname:chararray, passwd:chararray, uid:int, gid:int);");
            myPig.registerQuery("c = filter a by uid > 5;");
            myPig.registerQuery("store c into '/tmp/output1';");
            myPig.registerQuery("d = filter b by gid > 10;");
            myPig.registerQuery("store d into '/tmp/output2';");
            myPig.registerQuery("e = join c by gid, d by gid using \"repl\";");
            myPig.registerQuery("store e into '/tmp/output3';");

            List<ExecJob> jobs = myPig.executeBatch();
            assertEquals(3, jobs.size());

            for (ExecJob job : jobs) {
                assertTrue(job.getStatus() == ExecJob.JOB_STATUS.COMPLETED);
            }

        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        } 
    } 
 
    @Test
    public void testMultiQueryWithIntermediateStores_2() {

        System.out.println("===== multi-query with intermediate stores (2) =====");

        try {
            
            myPig.setBatchOn();
            
            myPig.registerQuery("a = load 'passwd' " +
                                "using PigStorage(':') as (uname:chararray, passwd:chararray, uid:int, gid:int);");
            myPig.registerQuery("store a into '/tmp/output1';");
            myPig.registerQuery("b = load '/tmp/output1' using PigStorage(':'); ");
            myPig.registerQuery("store b into '/tmp/output2';");

            List<ExecJob> jobs = myPig.executeBatch();
            assertTrue(jobs.size() == 2);

            for (ExecJob job : jobs) {
                assertTrue(job.getStatus() == ExecJob.JOB_STATUS.COMPLETED);
            }

        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        } 
    }         

    @Test
    public void testMultiQueryWithSplitInMapAndMultiMerge() throws Exception {

        // clean up any existing dirs/files
        String[] toClean = {"tmwsimam-input.txt", "foo1", "foo2", "foo3", "foo4" };
        for (int j = 0; j < toClean.length; j++) {
            Util.deleteFile(cluster, toClean[j]);    
        }
        
        // the data below is tab delimited
        String[] inputData = {
        "1	a	b	e	f	i	j	m	n",
        "2	a	b	e	f	i	j	m	n",
        "3	c	d	g	h	k	l	o	p",
        "4	c	d	g	h	k	l	o	p" };
        Util.createInputFile(cluster, "tmwsimam-input.txt", inputData);
        String query = 
        "A = LOAD 'tmwsimam-input.txt' " +
        "as (f0:chararray, f1:chararray, f2:chararray, f3:chararray, " +
        "f4:chararray, f5:chararray, f6:chararray, f7:chararray, f8:chararray); " +
        "B = FOREACH A GENERATE f0, f1, f2, f3, f4;" +
        "B1 = foreach B generate f0, f1, f2;" + 
        "C = GROUP B1 BY (f1, f2);" + 
        "STORE C into 'foo1' using BinStorage();" +
        "B2 = FOREACH B GENERATE f0, f3, f4;" + 
        "E = GROUP B2 BY (f3, f4);" +
        "STORE E into 'foo2'  using BinStorage();" +
        "F = FOREACH A GENERATE f0, f5, f6, f7, f8;" +
        "F1 = FOREACH F GENERATE f0, f5, f6;" +
        "G = GROUP F1 BY (f5, f6);" +
        "STORE G into 'foo3'  using BinStorage();" + 
        "F2  = FOREACH F GENERATE f0, f7, f8;" +
        "I = GROUP F2 BY (f7, f8);" +
        "STORE I into 'foo4'  using BinStorage();" +
        "explain;";
        myPig.setBatchOn();
        Util.registerMultiLineQuery(myPig, query);
        myPig.executeBatch();
        
        String templateLoad = "a = load 'foo' using BinStorage();";
        
        Map<Tuple, DataBag> expectedResults = new HashMap<Tuple, DataBag>();
        expectedResults.put((Tuple)Util.getPigConstant("('a','b')"),  
                            (DataBag)Util.getPigConstant("{('1','a','b'),('2','a','b')}"));
        expectedResults.put((Tuple)Util.getPigConstant("('c','d')"),  
                            (DataBag)Util.getPigConstant("{('3','c','d'),('4','c','d')}"));
        expectedResults.put((Tuple)Util.getPigConstant("('e','f')"),  
                            (DataBag)Util.getPigConstant("{('1','e','f'),('2','e','f')}"));
        expectedResults.put((Tuple)Util.getPigConstant("('g','h')"),  
                            (DataBag)Util.getPigConstant("{('3','g','h'),('4','g','h')}"));
        expectedResults.put((Tuple)Util.getPigConstant("('i','j')"),  
                            (DataBag)Util.getPigConstant("{('1','i','j'),('2','i','j')}"));
        expectedResults.put((Tuple)Util.getPigConstant("('k','l')"),  
                            (DataBag)Util.getPigConstant("{('3','k','l'),('4','k','l')}"));
        expectedResults.put((Tuple)Util.getPigConstant("('m','n')"),  
                            (DataBag)Util.getPigConstant("{('1','m','n'),('2','m','n')}"));
        expectedResults.put((Tuple)Util.getPigConstant("('o','p')"),  
                            (DataBag)Util.getPigConstant("{('3','o','p'),('4','o','p')}"));
        String[] outputDirs = { "foo1", "foo2", "foo3", "foo4" };
        for(int k = 0; k < outputDirs.length; k++) {
            myPig.registerQuery(templateLoad.replace("foo", outputDirs[k]));
            Iterator<Tuple> it = myPig.openIterator("a");
            int numTuples = 0;
            while(it.hasNext()) {
                Tuple t = it.next();
                assertEquals(expectedResults.get(t.get(0)), t.get(1));
                numTuples++;
            }
            assertEquals(numTuples, 2);
        }
        // cleanup
        for (int j = 0; j < toClean.length; j++) {
            Util.deleteFile(cluster, toClean[j]);    
        }
        
    }
       
    @Test
    public void testMultiQueryWithTwoStores2Execs() {

        System.out.println("===== multi-query with 2 stores execs =====");

        try {
            myPig.setBatchOn();

            myPig.registerQuery("a = load 'passwd' " +
                                "using PigStorage(':') as (uname:chararray, passwd:chararray, uid:int,gid:int);");
            myPig.registerQuery("b = filter a by uid > 5;");
            myPig.executeBatch();
            myPig.registerQuery("store b into '/tmp/output1';");
            myPig.executeBatch();
            myPig.registerQuery("c = group b by gid;");
            myPig.registerQuery("store c into '/tmp/output2';");

            myPig.executeBatch();
            myPig.discardBatch();

        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        } 
    }

    @Test
    public void testMultiQueryWithThreeStores2() {

        System.out.println("===== multi-query with 3 stores (2) =====");

        try {
            myPig.setBatchOn();

            myPig.registerQuery("a = load 'passwd' " +
                                "using PigStorage(':') as (uname:chararray, passwd:chararray, uid:int,gid:int);");
            myPig.registerQuery("b = filter a by uid > 5;");
            myPig.registerQuery("store b into '/tmp/output1';");
            myPig.registerQuery("c = filter b by uid > 10;");
            myPig.registerQuery("store c into '/tmp/output2';");
            myPig.registerQuery("d = filter c by uid > 15;");
            myPig.registerQuery("store d into '/tmp/output3';");

            myPig.executeBatch();
            myPig.discardBatch();

        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        } 
    }
 
    /**
     * Test that pig calls checkOutputSpecs() method of the OutputFormat (if the
     * StoreFunc defines an OutputFormat as the return value of 
     * {@link StoreFunc#getStorePreparationClass()} 
     * @throws IOException
     */
    @Test
    public void testMultiStoreWithOutputFormat() throws IOException {
        Util.createInputFile(cluster, "input.txt", new String[] {"hello", "bye"});
        String query = "a = load 'input.txt';" +
        		"b = filter a by $0 < 10;" +
        		"store b into 'output1' using "+DUMMY_STORE_WITH_OUTPUTFORMAT_CLASS+"();" +
        		"c = group a by $0;" +
        		"d = foreach c generate group, COUNT(a.$0);" +
        		"store d into 'output2' using "+DUMMY_STORE_WITH_OUTPUTFORMAT_CLASS+"();" ;
        myPig.setBatchOn();
        Util.registerMultiLineQuery(myPig, query);
        myPig.executeBatch();
        
        // check that files were created as a result of the
        // checkOutputSpecs() method of the OutputFormat being called
        FileSystem fs = cluster.getFileSystem();
        assertEquals(true, fs.exists(new Path("output1_checkOutputSpec_test")));
        assertEquals(true, fs.exists(new Path("output2_checkOutputSpec_test")));
        Util.deleteFile(cluster, "input.txt");
        Util.deleteFile(cluster, "output1_checkOutputSpec_test");
        Util.deleteFile(cluster, "output2_checkOutputSpec_test");
    }
        
    private static final String DUMMY_STORE_WITH_OUTPUTFORMAT_CLASS
            = "org.apache.pig.test.TestMultiQueryBasic\\$DummyStoreWithOutputFormat";

    public static class DummyStoreWithOutputFormat extends StoreFunc {
 
        public DummyStoreWithOutputFormat() {
        }

        @Override
        public void putNext(Tuple f) throws IOException {
 
        }

        @Override
        public void checkSchema(ResourceSchema s) throws IOException {
 
        }

        @Override
        public org.apache.hadoop.mapreduce.OutputFormat getOutputFormat()
                throws IOException {
            return new DummyOutputFormat();
        }

        @Override
        public void prepareToWrite(
                org.apache.hadoop.mapreduce.RecordWriter writer)
                throws IOException {
            
        }

        @Override
        public String relToAbsPathForStoreLocation(String location, Path curDir)
                throws IOException {
            return LoadFunc.getAbsolutePath(location, curDir);
        }

        @Override
        public void setStoreLocation(String location, Job job)
                throws IOException {
            Configuration conf = job.getConfiguration();
            conf.set("mapred.output.dir", location);
            
        }
        
        @Override
        public void setStoreFuncUDFContextSignature(String signature) {
        }
                
    }
    
    @SuppressWarnings({ "unchecked" })
    public static class DummyOutputFormat
    extends OutputFormat<WritableComparable, Tuple> {

        public DummyOutputFormat() {
            
        }
        @Override
        public void checkOutputSpecs(JobContext context) throws IOException,
                InterruptedException {
            Configuration conf = context.getConfiguration();
            FileSystem fs = FileSystem.get(conf);
            // create a file to test that this method got called
            fs.create(new Path(conf.get("mapred.output.dir") + "_checkOutputSpec_test"));
        }

        @Override
        public OutputCommitter getOutputCommitter(TaskAttemptContext context)
                throws IOException, InterruptedException {
            return null;
        }
        @Override
        public org.apache.hadoop.mapreduce.RecordWriter<WritableComparable, Tuple> getRecordWriter(
                TaskAttemptContext context) throws IOException,
                InterruptedException {
            return null;
        }
        
    }
    
    // --------------------------------------------------------------------------
    // Helper methods

    private void deleteOutputFiles() {
        try {
            FileLocalizer.delete("/tmp/output1", myPig.getPigContext());
            FileLocalizer.delete("/tmp/output2", myPig.getPigContext());
            FileLocalizer.delete("/tmp/output3", myPig.getPigContext());
            FileLocalizer.delete("/tmp/output4", myPig.getPigContext());
            FileLocalizer.delete("/tmp/output5", myPig.getPigContext());
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }
}

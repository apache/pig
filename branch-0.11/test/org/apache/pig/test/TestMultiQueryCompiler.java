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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

import junit.framework.Assert;

import org.apache.pig.ExecType;
import org.apache.pig.PigException;
import org.apache.pig.PigServer;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MapReduceLauncher;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MapReduceOper;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROperPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POSplit;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.backend.hadoop.executionengine.util.MapRedUtil;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.plan.Operator;
import org.apache.pig.impl.plan.OperatorPlan;
import org.apache.pig.impl.util.LogUtils;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.test.utils.SimpleCustomPartitioner;
import org.apache.pig.tools.grunt.GruntParser;
import org.apache.pig.tools.pigscript.parser.ParseException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
@RunWith(JUnit4.class)
public class TestMultiQueryCompiler {

    private static MiniCluster cluster;

    private PigServer myPig;

    @BeforeClass
    public static void setUpBeforeClass() throws IOException {
        cluster = MiniCluster.buildCluster();
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
    public void testMultiQueryJiraPig1438() {

        // test case: merge multiple distinct jobs -- one group by job, one distinct job
        
        String INPUT_FILE = "abc";
        
        try {           
            myPig.setBatchOn();
    
            myPig.registerQuery("A = load '" + INPUT_FILE + "' as (col1:int, col2:int, col3:int);");
            myPig.registerQuery("B1 = foreach A generate col1, col2;");
            myPig.registerQuery("B2 = foreach A generate col2, col3;");
            myPig.registerQuery("C1 = distinct B1;");
            myPig.registerQuery("C2 = group B2 by (col2, col3);");
            myPig.registerQuery("D1 = foreach C1 generate col1, col2;");
            myPig.registerQuery("D2 = foreach C2 generate B2.col2, B2.col3;");
            myPig.registerQuery("store D1 into '/tmp/output1';");
            myPig.registerQuery("store D2 into '/tmp/output2';");
            
            LogicalPlan lp = checkLogicalPlan(1, 2, 9);

            PhysicalPlan pp = checkPhysicalPlan(lp, 1, 2, 15);

            checkMRPlan(pp, 1, 1, 2); 
            
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
    }
        
    @Test
    public void testMultiQueryJiraPig1060() {

        // test case: 

        String INPUT_FILE = "pig-1060.txt";

        try {

            PrintWriter w = new PrintWriter(new FileWriter(INPUT_FILE));
            w.println("apple\t2");
            w.println("apple\t12");
            w.println("orange\t3");
            w.println("orange\t23");
            w.println("strawberry\t10");
            w.println("strawberry\t34");

            w.close();

            Util.copyFromLocalToCluster(cluster, INPUT_FILE, INPUT_FILE);

            myPig.setBatchOn();

            myPig.registerQuery("data = load '" + INPUT_FILE +
                                "' as (name:chararray, gid:int);");
            myPig.registerQuery("f1 = filter data by gid < 5;");
            myPig.registerQuery("g1 = group f1 by name;");
            myPig.registerQuery("p1 = foreach g1 generate group, COUNT(f1.gid);");
            myPig.registerQuery("store p1 into '/tmp/output1';");

            myPig.registerQuery("f2 = filter data by gid > 5;");
            myPig.registerQuery("g2 = group f2 by name;");
            myPig.registerQuery("p2 = foreach g2 generate group, COUNT(f2.gid);");
            myPig.registerQuery("store p2 into '/tmp/output2';");

            myPig.registerQuery("f3 = filter f2 by gid > 10;");
            myPig.registerQuery("g3 = group f3 by name;");
            myPig.registerQuery("p3 = foreach g3 generate group, COUNT(f3.gid);");
            myPig.registerQuery("store p3 into '/tmp/output3';");

            myPig.registerQuery("f4 = filter f3 by gid < 20;");
            myPig.registerQuery("g4 = group f4 by name;");
            myPig.registerQuery("p4 = foreach g4 generate group, COUNT(f4.gid);");
            myPig.registerQuery("store p4 into '/tmp/output4';");

            LogicalPlan lp = checkLogicalPlan(1, 4, 17);

            PhysicalPlan pp = checkPhysicalPlan(lp, 1, 4, 35);

            checkMRPlan(pp, 1, 1, 1);

        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        } finally {
            new File(INPUT_FILE).delete();
            try {
                Util.deleteFile(cluster, INPUT_FILE);
            } catch (IOException e) {
                e.printStackTrace();
                Assert.fail();
            }
        }
    }
     
    @Test
    public void testMultiQueryJiraPig920() {

        // test case: a simple diamond query
        try {
            myPig.setBatchOn();

            myPig.registerQuery("a = load 'passwd' " +
                                 "using PigStorage(':') as (uname:chararray, passwd:chararray, uid:int, gid:int);");
            myPig.registerQuery("b = filter a by uid < 5;");
            myPig.registerQuery("c = filter a by gid >= 5;");
            myPig.registerQuery("d = cogroup c by $0, b by $0;");
            myPig.registerQuery("e = foreach d generate group, COUNT(c), COUNT(b);");
            myPig.registerQuery("store e into '/tmp/output1';");
             
            LogicalPlan lp = checkLogicalPlan(1, 1, 6);

            PhysicalPlan pp = checkPhysicalPlan(lp, 1, 1, 13);

            checkMRPlan(pp, 1, 1, 1);
            
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        } 
    }       
 
    @Test
    public void testMultiQueryJiraPig920_1() {

        // test case: a query with two diamonds
        try {
            myPig.setBatchOn();

            myPig.registerQuery("a = load 'passwd' " +
                                 "using PigStorage(':') as (uname:chararray, passwd:chararray, uid:int, gid:int);");
            myPig.registerQuery("b = filter a by uid < 5;");
            myPig.registerQuery("c = filter a by gid >= 5;");
            myPig.registerQuery("d = filter a by uid >= 5;");
            myPig.registerQuery("e = filter a by gid < 5;");
            myPig.registerQuery("f = cogroup c by $0, b by $0;");
            myPig.registerQuery("f1 = foreach f generate group, COUNT(c), COUNT(b);");
            myPig.registerQuery("store f1 into '/tmp/output1';");
            myPig.registerQuery("g = cogroup d by $0, e by $0;");
            myPig.registerQuery("g1 = foreach g generate group, COUNT(d), COUNT(e);");
            myPig.registerQuery("store g1 into '/tmp/output2';");
             
            LogicalPlan lp = checkLogicalPlan(1, 2, 11);

            PhysicalPlan pp = checkPhysicalPlan(lp, 1, 2, 23);

            checkMRPlan(pp, 2, 2, 2);
            
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        } 
    }

    @Test
    public void testMultiQueryWithDemoCase() {

        System.out.println("===== multi-query with demo case 2 =====");

        try {
            myPig.setBatchOn();

            myPig.registerQuery("a = load 'passwd' " +
                                 "using PigStorage(':') as (uname:chararray, passwd:chararray, uid:int, gid:int);");
            myPig.registerQuery("b = foreach a generate uname, uid, gid;");
            myPig.registerQuery("c = filter b by uid < 5;");
            myPig.registerQuery("d = filter c by gid >= 5;");
            myPig.registerQuery("store d into '/tmp/output1';");
            myPig.registerQuery("e = filter b by uid >= 5;");
            myPig.registerQuery("store e into '/tmp/output2';");
            myPig.registerQuery("f = filter c by gid < 5;");
            myPig.registerQuery("g = group f by uname;");
            myPig.registerQuery("h = foreach g generate group, COUNT(f.uid);");
            myPig.registerQuery("store h into '/tmp/output3';");
             
            LogicalPlan lp = checkLogicalPlan(1, 3, 11);

            // NOTE: old way seemingly generated a useless foreach operator. Now we have one less operator. Reason unknow.
            PhysicalPlan pp = checkPhysicalPlan(lp, 1, 3, 19);

            checkMRPlan(pp, 1, 1, 1);
            
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        } 
    }         
    
    @Test
    public void testMultiQueryWithSingleMapReduceSplittee() {

        System.out.println("===== multi-query with single map reduce splittee =====");

        try {
            myPig.setBatchOn();

            myPig.registerQuery("a = load 'passwd' " +
                                "using PigStorage(':') as (uname, passwd, uid, gid);");
            myPig.registerQuery("b = foreach a generate uname, uid, gid;");
            myPig.registerQuery("split b into c1 if uid > 5, c2 if uid <= 5 ;"); 
            myPig.registerQuery("f = group c2 by uname;");
            myPig.registerQuery("f1 = foreach f generate group, SUM(c2.gid);");
            myPig.registerQuery("store f1 into '/tmp/output1';");

            LogicalPlan lp = checkLogicalPlan(1, 1, 7);

            PhysicalPlan pp = checkPhysicalPlan(lp, 1, 1, 9);

            checkMRPlan(pp, 1, 1, 1); 
            
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        } 
    }
    
    @Test
    public void testMultiQueryPhase3BaseCase() {

        System.out.println("===== multi-query phase 3 base case =====");

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
             
            LogicalPlan lp = checkLogicalPlan(1, 3, 14);

            PhysicalPlan pp = checkPhysicalPlan(lp, 1, 3, 25);

            checkMRPlan(pp, 1, 1, 1);
            
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        } 
    }     
 
    @Test
    public void testMultiQueryJiraPig983() {

        System.out.println("===== multi-query Jira Pig-983 =====");

        try {
            myPig.setBatchOn();

            myPig.registerQuery("a = load 'passwd' " +
                                 "using PigStorage(':') as (uname:chararray, passwd:chararray, uid:int, gid:int);");
            myPig.registerQuery("b = filter a by uid < 5;");
            myPig.registerQuery("c = filter a by uid >= 5;");
            myPig.registerQuery("d = join b by uname, c by uname;");
            myPig.registerQuery("e = group d by b::gid;");
            myPig.registerQuery("e1 = foreach e generate group, COUNT(d.b::uid);");
            myPig.registerQuery("store e1 into '/tmp/output1';");
            myPig.registerQuery("f = group d by c::gid;");
            myPig.registerQuery("f1 = foreach f generate group, SUM(d.c::uid);");
            myPig.registerQuery("store f1 into '/tmp/output2';");
             
            LogicalPlan lp = checkLogicalPlan(1, 2, 10);

            PhysicalPlan pp = checkPhysicalPlan(lp, 1, 2, 25);

            checkMRPlan(pp, 1, 1, 2);
            
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        } 
    }       
    
    @Test
    public void testMultiQueryPhase3WithoutCombiner() {

        System.out.println("===== multi-query phase 3 without combiner =====");

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
             
            LogicalPlan lp = checkLogicalPlan(1, 3, 14);

            PhysicalPlan pp = checkPhysicalPlan(lp, 1, 3, 25);

            checkMRPlan(pp, 1, 1, 1);
            
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        } 
    }     
    
    @Test
    public void testMultiQueryPhase3WithMixedCombiner() {

        System.out.println("===== multi-query phase 3 with mixed combiner =====");

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
            myPig.registerQuery("d2 = foreach d1 generate group, d.uname, MAX(d.uid) - MIN(d.uid);");
            myPig.registerQuery("store d2 into '/tmp/output3';");
             
            LogicalPlan lp = checkLogicalPlan(1, 3, 14);

            PhysicalPlan pp = checkPhysicalPlan(lp, 1, 3, 25);

            checkMRPlan(pp, 1, 1, 2);
            
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        } 
    }         

    @Test
    public void testMultiQueryPhase3WithDifferentMapDataTypes() {

        System.out.println("===== multi-query phase 3 with different map datatypes =====");

        try {
            myPig.setBatchOn();

            myPig.registerQuery("a = load 'passwd' " +
                                 "using PigStorage(':') as (uname:chararray, passwd:chararray, uid:int, gid:int);");
            myPig.registerQuery("b = filter a by uid < 5;");
            myPig.registerQuery("c = filter a by uid >= 5 and uid < 10;");
            myPig.registerQuery("d = filter a by uid >= 10;");
            myPig.registerQuery("b1 = group b by gid parallel 2;");
            myPig.registerQuery("b2 = foreach b1 generate group, COUNT(b.uid);");
            myPig.registerQuery("b3 = filter b2 by $1 > 5;");
            myPig.registerQuery("store b3 into '/tmp/output1';");
            myPig.registerQuery("c1 = group c by $1 parallel 3;");
            myPig.registerQuery("c2 = foreach c1 generate group, SUM(c.uid);");
            myPig.registerQuery("store c2 into '/tmp/output2';");
            myPig.registerQuery("d1 = group d by $1 parallel 4;");
            myPig.registerQuery("d2 = foreach d1 generate group, COUNT(d.uid);");
            myPig.registerQuery("store d2 into '/tmp/output3';");
             
            LogicalPlan lp = checkLogicalPlan(1, 3, 14);

            PhysicalPlan pp = checkPhysicalPlan(lp, 1, 3, 25);

            checkMRPlan(pp, 1, 1, 1);
            
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        } 
    }         
    
    @Test
    public void testMultiQueryPhase3StreamingInReducer() {

        System.out.println("===== multi-query phase 3 with streaming in reducer =====");

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
             
            LogicalPlan lp = checkLogicalPlan(1, 3, 15);

            PhysicalPlan pp = checkPhysicalPlan(lp, 1, 3, 24);

            checkMRPlan(pp, 1, 1, 2);
            
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        } 
    }           
    
    @Test
    public void testMultiQueryWithPigMixL12() {

        System.out.println("===== multi-query with PigMix L12 =====");

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

            LogicalPlan lp = checkLogicalPlan(1, 3, 17);

            PhysicalPlan pp = checkPhysicalPlan(lp, 1, 3, 23);

            checkMRPlan(pp, 1, 1, 1); 
            
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        } 
    }
    
    @Test
    public void testMultiQueryWithCoGroup() {

        System.out.println("===== multi-query with CoGroup =====");

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

            LogicalPlan lp = checkLogicalPlan(2, 1, 7);

            PhysicalPlan pp = checkPhysicalPlan(lp, 2, 1, 13);

            checkMRPlan(pp, 1, 1, 2); 

        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        } 
    }
    
    @Test
    public void testMultiQueryWithFJ() {

        System.out.println("===== multi-query with FJ =====");

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
            myPig.registerQuery("e = join c by gid, d by gid using 'repl';");
            myPig.registerQuery("store e into '/tmp/output3';");

            LogicalPlan lp = checkLogicalPlan(2, 3, 8);

            PhysicalPlan pp = checkPhysicalPlan(lp, 2, 3, 16);

            checkMRPlan(pp, 2, 1, 3);            
            
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        } 
    }
   
    @Test
    public void testMultiQueryWithExplicitSplitAndSideFiles() {

        System.out.println("===== multi-query with explicit split and side files =====");

        try {
            myPig.setBatchOn();

            myPig.registerQuery("a = load 'passwd' " +
                                "using PigStorage(':') as (uname:chararray, passwd:chararray, uid:int, gid:int);");
            myPig.registerQuery("split a into b if uid > 500, c if uid <= 500;");
            myPig.registerQuery("store b into '/tmp/output1';");
            myPig.registerQuery("store c into '/tmp/output2';");
            myPig.registerQuery("e = cogroup b by gid, c by gid;");
            myPig.registerQuery("d = foreach e generate flatten(c), flatten(b);");
            myPig.registerQuery("store d into '/tmp/output3';");

            LogicalPlan lp = checkLogicalPlan(1, 3, 9);

            PhysicalPlan pp = checkPhysicalPlan(lp, 1, 3, 19);

            checkMRPlan(pp, 1, 1, 2); 

        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        } 
    }        

    @Test
    public void testMultiQueryWithExplicitSplitAndOrderByAndSideFiles() {

        System.out.println("===== multi-query with explicit split, orderby and side files  =====");

        try {
            myPig.setBatchOn();

            myPig.registerQuery("a = load 'passwd' " +
                                "using PigStorage(':') as (uname:chararray, passwd:chararray, uid:int, gid:int);");
            myPig.registerQuery("split a into a1 if uid > 500, a2 if gid > 500;");
            myPig.registerQuery("b1 = distinct a1;");
            myPig.registerQuery("b2 = order a2 by uname;");
            myPig.registerQuery("store b1 into '/tmp/output1';");
            myPig.registerQuery("store b2 into '/tmp/output2';");
            myPig.registerQuery("c = cogroup b1 by uname, b2 by uname;");
            myPig.registerQuery("d = foreach c generate flatten(group), flatten($1), flatten($2);");
            myPig.registerQuery("store d into '/tmp/output3';");

            LogicalPlan lp = checkLogicalPlan(1, 3, 11);

            PhysicalPlan pp = checkPhysicalPlan(lp, 1, 3, 21);

            checkMRPlan(pp, 1, 1, 4); 

        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        } 
    }     
    
    @Test
    public void testMultiQueryWithIntermediateStores() {

        System.out.println("===== multi-query with intermediate stores =====");

        try {            
            myPig.setBatchOn();
            
            myPig.registerQuery("a = load 'passwd' " +
                                "using PigStorage(':') as (uname:chararray, passwd:chararray, uid:int, gid:int);");
            myPig.registerQuery("store a into '/tmp/output1';");
            myPig.registerQuery("b = load '/tmp/output1' using PigStorage(':'); ");
            myPig.registerQuery("store b into '/tmp/output2';");

            LogicalPlan lp = checkLogicalPlan(1, 1, 4);

            PhysicalPlan pp = checkPhysicalPlan(lp, 1, 1, 5);

            checkMRPlan(pp, 1, 1, 2); 

        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        } 
    }     

    @Test
    public void testMultiQueryWithImplicitSplitAndSideFiles() {

        System.out.println("===== multi-query with implicit split and side files  =====");

        try {
            myPig.setBatchOn();

            myPig.registerQuery("a = load 'passwd' " +
                                "using PigStorage(':') as (uname:chararray, passwd:chararray, uid:int, gid:int);");
            myPig.registerQuery("b = filter a by uid > 500;");
            myPig.registerQuery("c = filter a by gid > 500;");
            myPig.registerQuery("store c into '/tmp/output1';");
            myPig.registerQuery("d = cogroup b by uname, c by uname;");
            myPig.registerQuery("e = foreach d generate flatten(c), flatten(b);");
            myPig.registerQuery("store e into '/tmp/output2';");
            myPig.registerQuery("f = filter e by b::uid < 1000;");
            myPig.registerQuery("store f into '/tmp/output3';");

            LogicalPlan lp = checkLogicalPlan(1, 3, 9);

            PhysicalPlan pp = checkPhysicalPlan(lp, 1, 3, 22);

            checkMRPlan(pp, 1, 1, 2); 

        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        } 
    }        

    @Test
    public void testMultiQueryWithTwoLoadsAndTwoStores() {

        System.out.println("===== multi-query with two loads and two stores =====");

        try {
            myPig.setBatchOn();

            myPig.registerQuery("a = load 'passwd' " +
                                "using PigStorage(':') as (uname:chararray, passwd:chararray, uid:int,gid:int);");
            myPig.registerQuery("b = load 'passwd2' " +
                                "using PigStorage(':') as (uname:chararray, passwd:chararray, uid:int,gid:int);");
            myPig.registerQuery("c = filter a by uid > 5;");
            myPig.registerQuery("d = filter b by uid > 10;");
            myPig.registerQuery("e = cogroup c by uid, d by uid;");
            myPig.registerQuery("f = foreach e generate flatten(c), flatten(d);");
            myPig.registerQuery("g = group f by d::gid;");
            myPig.registerQuery("h = filter f by c::gid > 5;");
            myPig.registerQuery("store g into '/tmp/output1';");
            myPig.registerQuery("store h into '/tmp/output2';");
            
            LogicalPlan lp = checkLogicalPlan(2, 2, 10);

            PhysicalPlan pp = checkPhysicalPlan(lp, 2, 2, 20);

            checkMRPlan(pp, 1, 1, 2); 

        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        } 
    }
 
    @Test
    public void testMultiQueryWithSplitInReduce() {

        System.out.println("===== multi-query with split in reduce =====");

        try {
            myPig.setBatchOn();

            myPig.registerQuery("a = load 'passwd' " +
                                "using PigStorage(':') as (uname:chararray, passwd:chararray, uid:int, gid:int);");
            myPig.registerQuery("b = filter a by uid > 5;");
            myPig.registerQuery("c = group b by gid;");
            myPig.registerQuery("d = foreach c generate group, COUNT(b.uid);");
            myPig.registerQuery("store d into '/tmp/output1';");
            myPig.registerQuery("e = filter d by $1 > 5;");
            myPig.registerQuery("store e into '/tmp/output2';");

            LogicalPlan lp = checkLogicalPlan(1, 2, 7);

            PhysicalPlan pp = checkPhysicalPlan(lp, 1, 2, 13);

            checkMRPlan(pp, 1, 1, 1); 

        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        } 
    }
   
    @Test
    public void testMultiQueryWithSplitInReduceAndReduceSplitee() {

        System.out.println("===== multi-query with split in reduce and reduce splitee =====");

        try {
            myPig.setBatchOn();

            myPig.registerQuery("a = load 'passwd' " +
                                "using PigStorage(':') as (uname:chararray, passwd:chararray, uid:int, gid:int);");
            myPig.registerQuery("b = filter a by uid > 5;");
            myPig.registerQuery("c = group b by gid;");
            myPig.registerQuery("d = foreach c generate group, COUNT(b.uid);");
            myPig.registerQuery("store d into '/tmp/output1';");
            myPig.registerQuery("e = filter d by $1 > 5;");
            myPig.registerQuery("f = group e by $1;");
            myPig.registerQuery("g = foreach f generate group, SUM(e.$0);");
            myPig.registerQuery("store g into '/tmp/output2';");

            LogicalPlan lp = checkLogicalPlan(1, 2, 9);

            PhysicalPlan pp = checkPhysicalPlan(lp, 1, 2, 17);

            checkMRPlan(pp, 1, 1, 2); 
            
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        } 
    }    
  
    @Test
    public void testMultiQueryWithSplitInReduceAndReduceSplitees() {

        System.out.println("===== multi-query with split in reduce and reduce splitees =====");

        try {
            myPig.setBatchOn();

            myPig.registerQuery("a = load 'passwd' " +
                                "using PigStorage(':') as (uname:chararray, passwd:chararray, uid:int, gid:int);");
            myPig.registerQuery("b = filter a by uid > 5;");
            myPig.registerQuery("c = group b by gid;");
            myPig.registerQuery("d = foreach c generate group, COUNT(b.uid);");
            myPig.registerQuery("e = filter d by $1 > 5;");
            myPig.registerQuery("e1 = group e by $1;");
            myPig.registerQuery("e2 = foreach e1 generate group, SUM(e.$0);");
            myPig.registerQuery("store e2 into '/tmp/output1';");
            myPig.registerQuery("f = filter d by $1 < 5;");
            myPig.registerQuery("f1 = group f by $1;");
            myPig.registerQuery("f2 = foreach f1 generate group, COUNT(f.$0);");
            myPig.registerQuery("store f2 into '/tmp/output2';");
            
            LogicalPlan lp = checkLogicalPlan(1, 2, 12);

            PhysicalPlan pp = checkPhysicalPlan(lp, 1, 2, 22);

            checkMRPlan(pp, 1, 1, 2); 
        
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        } 
    }    

    @Test
    public void testMultiQueryWithSplitInReduceAndReduceSpliteesAndMore() {

        System.out.println("===== multi-query with split in reduce and reduce splitees and more =====");

        try {
            myPig.setBatchOn();

            myPig.registerQuery("a = load 'passwd' " +
                                "using PigStorage(':') as (uname:chararray, passwd:chararray, uid:int, gid:int);");
            myPig.registerQuery("b = filter a by uid > 500;");
            myPig.registerQuery("c = group b by gid;");
            myPig.registerQuery("d = foreach c generate group, COUNT(b.uid);");
            myPig.registerQuery("e = filter d by $1 > 5;");
            myPig.registerQuery("e1 = group e by $1;");
            myPig.registerQuery("e2 = foreach e1 generate group, SUM(e.$0);");
            myPig.registerQuery("e3 = filter e2 by $1 > 10;");
            myPig.registerQuery("e4 = group e3 by $1;");
            myPig.registerQuery("e5 = foreach e4 generate group, SUM(e3.$0);");
            myPig.registerQuery("store e5 into '/tmp/output1';");
            myPig.registerQuery("f = filter d by $1 < 5;");
            myPig.registerQuery("f1 = group f by $1;");
            myPig.registerQuery("f2 = foreach f1 generate group, COUNT(f.$0);");
            myPig.registerQuery("f3 = filter f2 by $1 < 100;");
            myPig.registerQuery("f4 = group f3 by $1;");
            myPig.registerQuery("f5 = foreach f4 generate group, COUNT(f3.$0);");
            myPig.registerQuery("store f5 into '/tmp/output2';");
            
            LogicalPlan lp = checkLogicalPlan(1, 2, 18);

            PhysicalPlan pp = checkPhysicalPlan(lp, 1, 2, 32);

            checkMRPlan(pp, 1, 2, 4);            

        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        } 
    }    
    
    @Test
    public void testMultiQueryWithSplitInMapAndReduceSplitees() {

        System.out.println("===== multi-query with split in map and reduce splitees =====");

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
             
            LogicalPlan lp = checkLogicalPlan(1, 3, 14);

            PhysicalPlan pp = checkPhysicalPlan(lp, 1, 3, 25);

            checkMRPlan(pp, 1, 1, 1);
            
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        } 
    }       
  
    @Test
    public void testMultiQueryWithTwoStores() {

        System.out.println("===== multi-query with 2 stores =====");

        try {
            myPig.setBatchOn();

            myPig.registerQuery("a = load 'passwd' " +
                                "using PigStorage(':') as (uname:chararray, passwd:chararray, uid:int,gid:int);");
            myPig.registerQuery("b = filter a by uid > 5;");
            myPig.registerQuery("store b into '/tmp/output1';");
            myPig.registerQuery("c = group b by gid;");
            myPig.registerQuery("store c into '/tmp/output2';");

            LogicalPlan lp = checkLogicalPlan(1, 2, 5);

            PhysicalPlan pp = checkPhysicalPlan(lp, 1, 2, 11);

            checkMRPlan(pp, 1, 1, 1);

        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        } 
    }

    @Test
    public void testMultiQueryWithThreeStores() {

        System.out.println("===== multi-query with 3 stores =====");

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

            LogicalPlan lp = checkLogicalPlan(1, 3, 7);

            PhysicalPlan pp = checkPhysicalPlan(lp, 1, 3, 14);

            checkMRPlan(pp, 1, 1, 1);            

        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        } 
    }

    @Test
    public void testMultiQueryWithTwoLoads() {

        System.out.println("===== multi-query with two loads =====");

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

            LogicalPlan lp = checkLogicalPlan(2, 3, 8);

            PhysicalPlan pp = checkPhysicalPlan(lp, 2, 3, 19);

            checkMRPlan(pp, 2, 1, 3);            

        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        } 
    }

    @Test
    public void testStoreOrder() {
        System.out.println("===== multi-query store order =====");
        
        try {
            myPig.setBatchOn();
            myPig.registerQuery("a = load 'passwd';");
            myPig.registerQuery("store a into '/tmp/output1' using BinStorage();");
            myPig.registerQuery("a = load '/tmp/output1';");
            myPig.registerQuery("store a into '/tmp/output2';");
            myPig.registerQuery("a = load '/tmp/output1';");
            myPig.registerQuery("store a into '/tmp/output3';");
            myPig.registerQuery("a = load '/tmp/output2' using BinStorage();");
            myPig.registerQuery("store a into '/tmp/output4';");
            myPig.registerQuery("a = load '/tmp/output2';");
            myPig.registerQuery("b = load '/tmp/output1';");
            myPig.registerQuery("c = cogroup a by $0, b by $0;");
            myPig.registerQuery("store c into '/tmp/output5';");

            LogicalPlan lp = checkLogicalPlan(1, 3, 12);
            PhysicalPlan pp = checkPhysicalPlan(lp, 1, 3, 15);
            MROperPlan mp = checkMRPlan(pp, 1, 3, 5);

            myPig.executeBatch();
            myPig.discardBatch(); 

            Assert.assertTrue(myPig.getPigContext().getDfs().isContainer("/tmp/output1"));
            Assert.assertTrue(myPig.getPigContext().getDfs().isContainer("/tmp/output2"));
            Assert.assertTrue(myPig.getPigContext().getDfs().isContainer("/tmp/output3"));
            Assert.assertTrue(myPig.getPigContext().getDfs().isContainer("/tmp/output4"));
            Assert.assertTrue(myPig.getPigContext().getDfs().isContainer("/tmp/output5"));

            
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        } 
    }
    
    @Test
    public void testUnnecessaryStoreRemoval() {
        System.out.println("===== multi-query unnecessary stores =====");
        
        try {
            myPig.setBatchOn();
            myPig.registerQuery("a = load 'passwd' " +
                                "using PigStorage(':') as (uname:chararray, passwd:chararray, uid:int,gid:int);");
            myPig.registerQuery("b = group a by uname;");
            myPig.registerQuery("store b into '/tmp/output1';");
            myPig.registerQuery("store b into '/tmp/output2';");
            myPig.registerQuery("c = load '/tmp/output1';");
            myPig.registerQuery("d = group c by $0;");
            myPig.registerQuery("e = store d into '/tmp/output3';");
            myPig.registerQuery("f = load '/tmp/output2';");
            myPig.registerQuery("g = group f by $0;");
            myPig.registerQuery("store g into '/tmp/output4';");

            LogicalPlan lp = checkLogicalPlan(1, 2, 10);
            PhysicalPlan pp = checkPhysicalPlan(lp, 1, 2, 20);
            MROperPlan mp = checkMRPlan(pp, 1, 2, 3);

            MapReduceOper mo1 = mp.getRoots().get(0);
            MapReduceOper mo2 = mp.getLeaves().get(0);
            MapReduceOper mo3 = mp.getLeaves().get(1);

            checkPhysicalPlan(mo1.mapPlan, 1, 1, 3);
            checkPhysicalPlan(mo1.reducePlan, 1, 1, 2);
            PhysicalOperator leaf = mo1.reducePlan.getLeaves().get(0);
            
            Assert.assertTrue(leaf instanceof POSplit);

            POSplit split = (POSplit)leaf;
            
            int i = 0;
            for (PhysicalPlan p: split.getPlans()) {
                checkPhysicalPlan(p, 1, 1, 1);
                ++i;
            }

            Assert.assertEquals(i,2);
            
            checkPhysicalPlan(mo2.mapPlan, 1, 1, 2);
            checkPhysicalPlan(mo2.reducePlan, 1, 1, 2);
            leaf = mo2.reducePlan.getLeaves().get(0);
            
            Assert.assertTrue(leaf instanceof POStore);

            checkPhysicalPlan(mo3.mapPlan, 1, 1, 2);
            checkPhysicalPlan(mo3.reducePlan, 1, 1, 2);
            leaf = mo3.reducePlan.getLeaves().get(0);
            
            Assert.assertTrue(leaf instanceof POStore);

            myPig.executeBatch();
            myPig.discardBatch(); 

            Assert.assertTrue(myPig.getPigContext().getDfs().isContainer("/tmp/output1"));
            Assert.assertTrue(myPig.getPigContext().getDfs().isContainer("/tmp/output2"));
            Assert.assertTrue(myPig.getPigContext().getDfs().isContainer("/tmp/output3"));
            Assert.assertTrue(myPig.getPigContext().getDfs().isContainer("/tmp/output4"));

            
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        } 
    }
    
    @Test
    public void testUnnecessaryStoreRemovalCollapseSplit() {
        System.out.println("===== multi-query unnecessary stores collapse split =====");
        
        try {
            myPig.setBatchOn();
            myPig.registerQuery("a = load 'passwd' " +
                                "using PigStorage(':') as (uname:chararray, passwd:chararray, uid:int,gid:int);");
            myPig.registerQuery("b = group a by uname;");
            myPig.registerQuery("store b into '/tmp/output1';");
            myPig.registerQuery("c = load '/tmp/output1';");
            myPig.registerQuery("d = group c by $0;");
            myPig.registerQuery("e = store d into '/tmp/output2';");

            LogicalPlan lp = checkLogicalPlan(1, 1, 6);
            PhysicalPlan pp = checkPhysicalPlan(lp, 1, 1, 11);
            MROperPlan mp = checkMRPlan(pp, 1, 1, 2);

            MapReduceOper mo1 = mp.getRoots().get(0);
            MapReduceOper mo2 = mp.getLeaves().get(0);

            checkPhysicalPlan(mo1.mapPlan, 1, 1, 3);
            checkPhysicalPlan(mo1.reducePlan, 1, 1, 2);
            PhysicalOperator leaf = mo1.reducePlan.getLeaves().get(0);
            
            Assert.assertTrue(leaf instanceof POStore);
            
            checkPhysicalPlan(mo2.mapPlan, 1, 1, 2);
            checkPhysicalPlan(mo2.reducePlan, 1, 1, 2);
            leaf = mo2.reducePlan.getLeaves().get(0);
            
            Assert.assertTrue(leaf instanceof POStore);

            myPig.executeBatch();
            myPig.discardBatch(); 

            Assert.assertTrue(myPig.getPigContext().getDfs().isContainer("/tmp/output1"));
            Assert.assertTrue(myPig.getPigContext().getDfs().isContainer("/tmp/output2"));

            
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        } 
    }
    
    @Test
    public void testEmptyFilterRemoval() {
        System.out.println("===== multi-query empty filters =====");
        
        try {
            myPig.setBatchOn();
            myPig.registerQuery("a = load 'passwd' " +
                                "using PigStorage(':') as (uname:chararray, passwd:chararray, uid:int,gid:int);");
            myPig.registerQuery("b = filter a by uid>0;");
            myPig.registerQuery("c = filter a by uid>5;");
            myPig.registerQuery("d = filter c by uid<10;");
            myPig.registerQuery("store b into '/tmp/output1';");
            myPig.registerQuery("store b into '/tmp/output2';");
            myPig.registerQuery("store b into '/tmp/output3';");

            LogicalPlan lp = checkLogicalPlan(1, 3, 5);
            PhysicalPlan pp = checkPhysicalPlan(lp, 1, 3, 10);
            MROperPlan mp = checkMRPlan(pp, 1, 1, 1);

            MapReduceOper mo = mp.getRoots().get(0);

            checkPhysicalPlan(mo.mapPlan, 1, 1, 4);
            PhysicalOperator leaf = mo.mapPlan.getLeaves().get(0);
            
            Assert.assertTrue(leaf instanceof POSplit);
            
            POSplit split = (POSplit)leaf;

            int i = 0;
            for (PhysicalPlan p: split.getPlans()) {
                checkPhysicalPlan(p, 1, 1, 1);
                ++i;
            }

            Assert.assertEquals(i,3);

            myPig.executeBatch();
            myPig.discardBatch();

        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        } 
    }
    
    @Test
    public void testMultiQueryWithDescribe() {

        System.out.println("===== multi-query with describe =====");

        try {
            String script = "a = load 'passwd' "
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
        } 
    }

    @Test
    public void testMultiQueryWithIllustrate() {

        System.out.println("===== multi-query with illustrate =====");

        try {
            String script = "a = load 'passwd' "
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
        } 
    }
    
    @Test
    public void testMultiQueryWithExplain() {

        System.out.println("===== multi-query with explain =====");

        try {
            String script = "a = load 'passwd' "
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
        } 
    }

    @Test
    public void testMultiQueryWithDump() {

        System.out.println("===== multi-query with dump =====");

        try {
            String script = "a = load 'passwd' "
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
        } 
    }
    
    @Test
    public void testEmptyExecute() {
        
        System.out.println("==== empty execute ====");
        
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
    
    // A test to verify there is no cycle in the plan if the load location is the same as the store location
    @Test
    public void testLoadStoreLoop() {
        try {
            String script = "a = load 'dummy'; b = filter a by $0 == 1; store b into 'dummy';\n";
            GruntParser parser = new GruntParser(new StringReader(script));
            parser.setInteractive(false);
            parser.setParams(myPig);
            myPig.getPigContext().inExplain = true;
            parser.parseStopOnError();
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
    }
    
    @Test
    public void testMultiQueryWithNoStore2() {

        System.out.println("===== multi-query with no store (2) =====");

        try {
            myPig.setBatchOn();

            myPig.registerQuery("a = load 'passwd' " +
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
    
    @Test
    public void testMultiQueryWithCustomPartitioner() {

        System.out.println("===== multi-query with intermediate stores =====");

        try {            
            myPig.setBatchOn();
            
            myPig.registerQuery("a = load 'passwd' " +
                                "using PigStorage(':') as (uname:chararray, passwd:chararray, uid:int, gid:int);");
            myPig.registerQuery("l = FILTER a BY uname == 'foo';");
            myPig.registerQuery("b = GROUP a BY uname PARTITION BY " + SimpleCustomPartitioner.class.getName() + " PARALLEL 3;");
            myPig.registerQuery("c = FOREACH b GENERATE FLATTEN(a) PARALLEL 3;");
            myPig.registerQuery("STORE c INTO 'output1';");
            myPig.registerQuery("STORE l INTO 'output2';");

            LogicalPlan lp = checkLogicalPlan(1, 2, 6);

            PhysicalPlan pp = checkPhysicalPlan(lp, 1, 2, 12);

            MROperPlan mrp = checkMRPlan(pp, 1, 1, 1); 
            
            MapReduceOper mrop = mrp.getRoots().get(0);
            Assert.assertTrue(mrop.getCustomPartitioner().equals(SimpleCustomPartitioner.class.getName()));

        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
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

        showLPOperators(lp);
       
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        System.out.println("Logical Plan: " + lp );

        System.out.println("===== Display Logical Plan =====");
        System.out.println(out.toString());

        Assert.assertEquals(expectedRoots, lp.getSources().size());
        Assert.assertEquals(expectedLeaves, lp.getSinks().size());
        Assert.assertEquals(expectedSize, lp.size());

        return lp;
    }

    static void showLPOperators(LogicalPlan lp) {
    	System.out.println("Operators:");
    	Iterator<org.apache.pig.newplan.Operator> it = lp.getOperators();
    	while( it.hasNext() ) {
    		System.out.println("    op: " + it.next().getName());
    	}
    	System.out.println();
	}

	private void checkPhysicalPlan(PhysicalPlan pp, int expectedRoots,
            int expectedLeaves, int expectedSize) throws IOException {

        System.out.println("===== check physical plan =====");
        
        showPlanOperators(pp);
        
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        pp.explain(out);
        
        System.out.println("===== Display Physical Plan =====");
        System.out.println(out.toString());
        
        Assert.assertEquals(expectedRoots, pp.getRoots().size());
        Assert.assertEquals(expectedLeaves, pp.getLeaves().size());
        Assert.assertEquals(expectedSize, pp.size());

    }
    
    private PhysicalPlan checkPhysicalPlan(LogicalPlan lp, int expectedRoots,
            int expectedLeaves, int expectedSize) throws IOException {

        System.out.println("===== check physical plan =====");

        PhysicalPlan pp = myPig.getPigContext().getExecutionEngine().compile(
                lp, null);

        showPlanOperators(pp);
       
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        pp.explain(out);

        System.out.println("===== Display Physical Plan =====");
        System.out.println(out.toString());

        Assert.assertEquals(expectedRoots, pp.getRoots().size());
        Assert.assertEquals(expectedLeaves, pp.getLeaves().size());
        Assert.assertEquals(expectedSize, pp.size());

        return pp;
    }

    private MROperPlan checkMRPlan(PhysicalPlan pp, int expectedRoots,
            int expectedLeaves, int expectedSize) throws IOException {

        System.out.println("===== check map-reduce plan =====");

        MapRedUtil.checkLeafIsStore(pp, myPig.getPigContext());
        
        MapReduceLauncher launcher = new MapReduceLauncher();

        MROperPlan mrp = null;

        try {
            java.lang.reflect.Method compile = launcher.getClass()
                    .getDeclaredMethod("compile",
                            new Class[] { PhysicalPlan.class, PigContext.class });

            compile.setAccessible(true);

            mrp = (MROperPlan) compile.invoke(launcher, new Object[] { pp, myPig.getPigContext() });

            Assert.assertNotNull(mrp);

        } catch (Exception e) {
            PigException pe = LogUtils.getPigException(e);
            if (pe != null) {
                throw pe;
            } else {
                e.printStackTrace();
                Assert.fail();
            }
        }        

        showPlanOperators(mrp);
        
        System.out.println("===== Display map-reduce Plan =====");
        System.out.println(mrp.toString());
        
        Assert.assertEquals(expectedRoots, mrp.getRoots().size());
        Assert.assertEquals(expectedLeaves, mrp.getLeaves().size());
        Assert.assertEquals(expectedSize, mrp.size());

        return mrp;
    }
    
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

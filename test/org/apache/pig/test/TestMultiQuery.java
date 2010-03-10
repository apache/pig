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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collections;
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
import org.apache.pig.PigException;
import org.apache.pig.PigServer;
import org.apache.pig.ResourceSchema;
import org.apache.pig.StoreFunc;
import org.apache.pig.backend.executionengine.ExecJob;
import org.apache.pig.backend.executionengine.util.ExecTools;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MapReduceLauncher;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MapReduceOper;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROperPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POSplit;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.logicalLayer.LogicalPlan;
import org.apache.pig.impl.plan.Operator;
import org.apache.pig.impl.plan.OperatorPlan;
import org.apache.pig.impl.util.LogUtils;
import org.apache.pig.tools.grunt.GruntParser;
import org.apache.pig.tools.pigscript.parser.ParseException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestMultiQuery {

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
    public void testMultiQueryJiraPig1252() {

        // test case: Problems with secondary key optimization and multiquery
        // diamond optimization
        
        String INPUT_FILE = "abc";
        
        try {
    
            PrintWriter w = new PrintWriter(new FileWriter(INPUT_FILE));
            w.println("1\t2\t3");
            w.println("2\t3\t4");
            w.println("3\t\t5");
            w.println("5\t6\t6");
            w.println("6\t\t7");
            w.close();
    
            Util.copyFromLocalToCluster(cluster, INPUT_FILE, INPUT_FILE);
           
            myPig.setBatchOn();
    
            myPig.registerQuery("A = load '" + INPUT_FILE + "' as (col1, col2, col3);");
            myPig.registerQuery("B = foreach A generate (chararray) col1, " +
            		"(chararray) ((col2 is not null) ?  " +
            		"col2 : (col3 < 6 ? col3 : '')) as splitcond;");
            myPig.registerQuery("split B into C if splitcond !=  '', D if splitcond == '';");
            myPig.registerQuery("E = group C by splitcond;");
            myPig.registerQuery("F = foreach E { orderedData = order C by $1, $0; generate flatten(orderedData); };");
       
            Iterator<Tuple> iter = myPig.openIterator("F");

            List<Tuple> expectedResults = Util.getTuplesFromConstantTupleStrings(
                    new String[] { 
                            "(1,2)",
                            "(2,3)",
                            "(3,5)",
                            "(5,6)"
                    });
            
            int counter = 0;
            while (iter.hasNext()) {
                assertEquals(expectedResults.get(counter++).toString(), iter.next().toString());                  
            }

            assertEquals(expectedResults.size(), counter);
            
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
    public void testMultiQueryJiraPig1169() {

        // test case: Problems with some top N queries
        
        String INPUT_FILE = "abc";
        
        try {
    
            PrintWriter w = new PrintWriter(new FileWriter(INPUT_FILE));
            w.println("1\t2\t3");
            w.println("2\t3\t4");
            w.println("3\t4\t5");
            w.println("5\t6\t7");
            w.println("6\t7\t8");
            w.close();
    
            Util.copyFromLocalToCluster(cluster, INPUT_FILE, INPUT_FILE);
           
            myPig.setBatchOn();
    
            myPig.registerQuery("A = load '" + INPUT_FILE 
                    + "' as (a:int, b, c);");
            myPig.registerQuery("A1 = Order A by a desc parallel 3;");
            myPig.registerQuery("A2 = limit A1 2;");
            myPig.registerQuery("store A1 into '/tmp/input1';");
            myPig.registerQuery("store A2 into '/tmp/input2';");

            myPig.executeBatch();

            myPig.registerQuery("B = load '/tmp/input2' as (a:int, b, c);");
            
            Iterator<Tuple> iter = myPig.openIterator("B");

            List<Tuple> expectedResults = Util.getTuplesFromConstantTupleStrings(
                    new String[] { 
                            "(6,7,8)",
                            "(5,6,7)"
                    });
            
            int counter = 0;
            while (iter.hasNext()) {
                assertEquals(expectedResults.get(counter++).toString(), iter.next().toString());      
            }

            assertEquals(expectedResults.size(), counter);
            
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
    public void testMultiQueryJiraPig1171() {

        // test case: Problems with some top N queries
        
        String INPUT_FILE = "abc";
        
        try {
    
            PrintWriter w = new PrintWriter(new FileWriter(INPUT_FILE));
            w.println("1\tapple\t3");
            w.println("2\torange\t4");
            w.println("3\tpersimmon\t5");
            w.close();
    
            Util.copyFromLocalToCluster(cluster, INPUT_FILE, INPUT_FILE);
           
            myPig.setBatchOn();
    
            myPig.registerQuery("A = load '" + INPUT_FILE 
                    + "' as (a:long, b, c);");
            myPig.registerQuery("A1 = Order A by a desc;");
            myPig.registerQuery("A2 = limit A1 1;");
            myPig.registerQuery("B = load '" + INPUT_FILE 
                    + "' as (a:long, b, c);");
            myPig.registerQuery("B1 = Order B by a desc;");
            myPig.registerQuery("B2 = limit B1 1;");
            
            myPig.registerQuery("C = cross A2, B2;");
            
            Iterator<Tuple> iter = myPig.openIterator("C");

            List<Tuple> expectedResults = Util.getTuplesFromConstantTupleStrings(
                    new String[] { 
                            "(3L,'persimmon',5,3L,'persimmon',5)"
                    });
            
            int counter = 0;
            while (iter.hasNext()) {
                assertEquals(expectedResults.get(counter++).toString(), iter.next().toString());      
            }

            assertEquals(expectedResults.size(), counter);
            
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
    public void testMultiQueryJiraPig1157() {

        // test case: Sucessive replicated joins do not generate Map Reduce plan and fails due to OOM
        
        String INPUT_FILE = "abc";
        String INPUT_FILE_1 = "xyz";
        
        try {
    
            PrintWriter w = new PrintWriter(new FileWriter(INPUT_FILE));
            w.println("1\tapple\t3");
            w.println("2\torange\t4");
            w.println("3\tpersimmon\t5");
            w.close();
    
            Util.copyFromLocalToCluster(cluster, INPUT_FILE, INPUT_FILE);
            Util.copyFromLocalToCluster(cluster, INPUT_FILE, INPUT_FILE_1);
    
            myPig.setBatchOn();
    
            myPig.registerQuery("A = load '" + INPUT_FILE 
                    + "' as (a:long, b, c);");
            myPig.registerQuery("A1 = FOREACH A GENERATE a;");
            myPig.registerQuery("B = GROUP A1 BY a;");
            myPig.registerQuery("C = load '" + INPUT_FILE_1 
                    + "' as (x:long, y);");
            myPig.registerQuery("D = JOIN C BY x, B BY group USING \"replicated\";");  
            myPig.registerQuery("E = JOIN A BY a, D by x USING \"replicated\";");  
            
            Iterator<Tuple> iter = myPig.openIterator("E");

            List<Tuple> expectedResults = Util.getTuplesFromConstantTupleStrings(
                    new String[] { 
                            "(1L,1L,'apple',1L,{(1L)})",
                            "(2L,2L,'orange',2L,{(2L)})",
                            "(3L,3L,'persimmon',3L,{(3L)})"
                    });
            
            int counter = 0;
            while (iter.hasNext()) {
                assertEquals(expectedResults.get(counter++).toString(), iter.next().toString());                  
            }

            assertEquals(expectedResults.size(), counter);
            
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        } finally {
            new File(INPUT_FILE).delete();
            try {
                Util.deleteFile(cluster, INPUT_FILE);
                Util.deleteFile(cluster, INPUT_FILE_1);
            } catch (IOException e) {
                e.printStackTrace();
                Assert.fail();
            }
        }
    }
    
    @Test
    public void testMultiQueryJiraPig1068() {

        // test case: COGROUP fails with 'Type mismatch in key from map: 
        // expected org.apache.pig.impl.io.NullableText, recieved org.apache.pig.impl.io.NullableTuple'

        String INPUT_FILE = "pig-1068.txt";

        try {

            PrintWriter w = new PrintWriter(new FileWriter(INPUT_FILE));
            w.println("10\tapple\tlogin\tjar");
            w.println("20\torange\tlogin\tbox");
            w.println("30\tstrawberry\tquit\tbot");

            w.close();

            Util.copyFromLocalToCluster(cluster, INPUT_FILE, INPUT_FILE);

            myPig.setBatchOn();

            myPig.registerQuery("logs = load '" + INPUT_FILE 
                    + "' as (ts:int, id:chararray, command:chararray, comments:chararray);");
            myPig.registerQuery("SPLIT logs INTO logins IF command == 'login', all_quits IF command == 'quit';");
            myPig.registerQuery("login_info = FOREACH logins { GENERATE id as id, comments AS client; };");  
            myPig.registerQuery("logins_grouped = GROUP login_info BY (id, client);");
            myPig.registerQuery("count_logins_by_client = FOREACH logins_grouped "
                    + "{ generate group.id AS id, group.client AS client, COUNT($1) AS count; };");
            myPig.registerQuery("all_quits_grouped = GROUP all_quits BY id; ");
            myPig.registerQuery("quits = FOREACH all_quits_grouped { GENERATE FLATTEN(all_quits); };");
            myPig.registerQuery("joined_session_info = COGROUP quits BY id, count_logins_by_client BY id;");
            
            Iterator<Tuple> iter = myPig.openIterator("joined_session_info");

            List<Tuple> expectedResults = Util.getTuplesFromConstantTupleStrings(
                    new String[] { 
                            "('apple',{},{('apple','jar',1L)})",
                            "('orange',{},{('orange','box',1L)})",
                            "('strawberry',{(30,'strawberry','quit','bot')},{})"
                    });
            
            int counter = 0;
            while (iter.hasNext()) {
                assertEquals(expectedResults.get(counter++).toString(), iter.next().toString());                
            }

            assertEquals(expectedResults.size(), counter);

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
    public void testMultiQueryJiraPig1108() {
        try {
            myPig.setBatchOn();

            myPig.registerQuery("a = load 'passwd' " 
                    + "using PigStorage(':') as (uname:chararray, passwd:chararray, uid:int, gid:int);");
            myPig.registerQuery("split a into plan1 if (uid > 5), plan2 if ( uid < 5);");
            myPig.registerQuery("b = group plan1 by uname;");
            myPig.registerQuery("c = foreach b { tmp = order plan1 by uid desc; " 
                    + "generate flatten(group) as foo, tmp; };");
            myPig.registerQuery("d = filter c BY foo is not null;");
            myPig.registerQuery("store d into '/tmp/output1';");
            myPig.registerQuery("store plan2 into '/tmp/output2';");
             
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
    public void testMultiQueryJiraPig1114() {

        // test case: MultiQuery optimization throws error when merging 2 level splits

        String INPUT_FILE = "data.txt";

        try {

            PrintWriter w = new PrintWriter(new FileWriter(INPUT_FILE));
            w.println("10\tjar");
            w.println("20\tbox");
            w.println("30\tbot");
            w.close();
            Util.copyFromLocalToCluster(cluster, INPUT_FILE, INPUT_FILE);

            myPig.setBatchOn();

            myPig.registerQuery("data = load '" + INPUT_FILE
                    + "' USING PigStorage as (id:int, name:chararray);");
            myPig.registerQuery("ids = FOREACH data GENERATE id;");
            myPig.registerQuery("allId = GROUP ids all;");
            myPig.registerQuery("allIdCount = FOREACH allId GENERATE group as allId, COUNT(ids) as total;");
            myPig.registerQuery("idGroup = GROUP ids by id;");
            myPig.registerQuery("idGroupCount = FOREACH idGroup GENERATE group as id, COUNT(ids) as count;");
            myPig.registerQuery("countTotal = cross idGroupCount, allIdCount;");
            myPig.registerQuery("idCountTotal = foreach countTotal generate id, count, total, (double)count / (double)total as proportion;");
            myPig.registerQuery("orderedCounts = order idCountTotal by count desc;");
            myPig.registerQuery("STORE orderedCounts INTO '/tmp/output1';");

            myPig.registerQuery("names = FOREACH data GENERATE name;");
            myPig.registerQuery("allNames = GROUP names all;");
            myPig.registerQuery("allNamesCount = FOREACH allNames GENERATE group as namesAll, COUNT(names) as total;");
            myPig.registerQuery("nameGroup = GROUP names by name;");
            myPig.registerQuery("nameGroupCount = FOREACH nameGroup GENERATE group as name, COUNT(names) as count;");
            myPig.registerQuery("namesCrossed = cross nameGroupCount, allNamesCount;");
            myPig.registerQuery("nameCountTotal = foreach namesCrossed generate name, count, total, (double)count / (double)total as proportion;");
            myPig.registerQuery("nameCountsOrdered = order nameCountTotal by count desc;");
            myPig.registerQuery("STORE nameCountsOrdered INTO '/tmp/output2';");

            List<ExecJob> jobs = myPig.executeBatch();
            for (ExecJob job : jobs) {
                assertTrue(job.getStatus() == ExecJob.JOB_STATUS.COMPLETED);
            }

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
    public void testMultiQueryJiraPig1113() {

        // test case: Diamond query optimization throws error in JOIN

        String INPUT_FILE_1 = "set1.txt";
        String INPUT_FILE_2 = "set2.txt";
        try {

            PrintWriter w = new PrintWriter(new FileWriter(INPUT_FILE_1));
            w.println("login\t0\tjar");
            w.println("login\t1\tbox");
            w.println("quit\t0\tmany");
            w.close();
            Util.copyFromLocalToCluster(cluster, INPUT_FILE_1, INPUT_FILE_1);

            PrintWriter w2 = new PrintWriter(new FileWriter(INPUT_FILE_2));
            w2.println("apple\tlogin\t{(login)}");
            w2.println("orange\tlogin\t{(login)}");
            w2.println("strawberry\tquit\t{(login)}");
            w2.close();
            Util.copyFromLocalToCluster(cluster, INPUT_FILE_2, INPUT_FILE_2);
            
            myPig.setBatchOn();

            myPig.registerQuery("set1 = load '" + INPUT_FILE_1 
                    + "' USING PigStorage as (a:chararray, b:chararray, c:chararray);");
            myPig.registerQuery("set2 = load '" + INPUT_FILE_2
                    + "' USING PigStorage as (a: chararray, b:chararray, c:bag{});");
            myPig.registerQuery("set2_1 = FOREACH set2 GENERATE a as f1, b as f2, " 
                    + "(chararray) 0 as f3;");
            myPig.registerQuery("set2_2 = FOREACH set2 GENERATE a as f1, "
                    + "FLATTEN((IsEmpty(c) ? null : c)) as f2, (chararray) 1 as f3;");  
            myPig.registerQuery("all_set2 = UNION set2_1, set2_2;");
            myPig.registerQuery("joined_sets = JOIN set1 BY (a,b), all_set2 BY (f2,f3);");
          
            List<Tuple> expectedResults = Util.getTuplesFromConstantTupleStrings(
                    new String[] { 
                            "('quit','0','many','strawberry','quit','0')",
                            "('login','0','jar','apple','login','0')",
                            "('login','0','jar','orange','login','0')",
                            "('login','1','box','apple','login','1')",
                            "('login','1','box','orange','login','1')",
                            "('login','1','box','strawberry','login','1')"
                    });
            
            Iterator<Tuple> iter = myPig.openIterator("joined_sets");
            int count = 0;
            while (iter.hasNext()) {
                assertEquals(expectedResults.get(count++).toString(), iter.next().toString());
            }
            assertEquals(expectedResults.size(), count);

        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        } finally {
            new File(INPUT_FILE_1).delete();
            new File(INPUT_FILE_2).delete();
            try {
                Util.deleteFile(cluster, INPUT_FILE_1);
                Util.deleteFile(cluster, INPUT_FILE_2);
            } catch (IOException e) {
                e.printStackTrace();
                Assert.fail();
            }
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

            LogicalPlan lp = checkLogicalPlan(1, 4, 27);

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
    public void testMultiQueryJiraPig1060_2() {

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

            List<ExecJob> jobs = myPig.executeBatch();
            assertEquals(4, jobs.size());

            for (ExecJob job : jobs) {
                assertTrue(job.getStatus() == ExecJob.JOB_STATUS.COMPLETED);
            }

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
             
            LogicalPlan lp = checkLogicalPlan(1, 1, 10);

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
             
            LogicalPlan lp = checkLogicalPlan(1, 2, 17);

            PhysicalPlan pp = checkPhysicalPlan(lp, 1, 2, 23);

            checkMRPlan(pp, 2, 2, 2);
            
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        } 
    }

    @Test
    public void testMultiQueryJiraPig920_2() {

        // test case: execution of a query with two diamonds
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
    public void testMultiQueryJiraPig920_3() {

        // test case: execution of a simple diamond query
        
        String INPUT_FILE = "pig-920.txt";
        
        try {
            
            PrintWriter w = new PrintWriter(new FileWriter(INPUT_FILE));
            w.println("apple\tapple\t100\t10");
            w.println("apple\tapple\t200\t20");
            w.println("orange\torange\t100\t10");
            w.println("orange\torange\t300\t20");
   
            w.close();
            
            Util.copyFromLocalToCluster(cluster, INPUT_FILE, INPUT_FILE);
        
            myPig.setBatchOn();

            myPig.registerQuery("a = load '" + INPUT_FILE +
                                "' as (uname:chararray, passwd:chararray, uid:int, gid:int);");
            myPig.registerQuery("b = filter a by uid < 300;");
            myPig.registerQuery("c = filter a by gid > 10;");
            myPig.registerQuery("d = cogroup c by $0, b by $0;");
            myPig.registerQuery("e = foreach d generate group, COUNT(c), COUNT(b);");
                                   
            Iterator<Tuple> iter = myPig.openIterator("e");

            List<Tuple> expectedResults = Util.getTuplesFromConstantTupleStrings(
                    new String[] { 
                            "('apple',1L,2L)",
                            "('orange',1L,1L)"
                    });
            
            int counter = 0;
            while (iter.hasNext()) {
                assertEquals(expectedResults.get(counter++).toString(), iter.next().toString());
            }

            assertEquals(expectedResults.size(), counter);
            
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
             
            LogicalPlan lp = checkLogicalPlan(1, 3, 18);

            PhysicalPlan pp = checkPhysicalPlan(lp, 1, 3, 20);

            checkMRPlan(pp, 1, 1, 1);
            
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        } 
    }         

    @Test
    public void testMultiQueryJiraPig976() {

        // test case: key ('group') isn't part of foreach output
        // and keys have the same type.

        try {
            myPig.setBatchOn();

            myPig.registerQuery("a = load 'passwd' " +
                                "using PigStorage(':') as (uname:chararray, passwd:chararray, uid:int, gid:int);");
            myPig.registerQuery("b = group a by uid;");
            myPig.registerQuery("c = group a by gid;");
            myPig.registerQuery("d = foreach b generate SUM(a.gid);");
            myPig.registerQuery("e = foreach c generate group, COUNT(a);");
            myPig.registerQuery("store d into '/tmp/output1';");
            myPig.registerQuery("store e into '/tmp/output2';");

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
    public void testMultiQueryJiraPig976_2() {

        // test case: key ('group') isn't part of foreach output 
        // and keys have different types

        try {
            myPig.setBatchOn();

            myPig.registerQuery("a = load 'passwd' " +
                                "using PigStorage(':') as (uname:chararray, passwd:chararray, uid:int, gid:int);");
            myPig.registerQuery("b = group a by uname;");
            myPig.registerQuery("c = group a by gid;");
            myPig.registerQuery("d = foreach b generate SUM(a.gid);");
            myPig.registerQuery("e = foreach c generate group, COUNT(a);");
            myPig.registerQuery("store d into '/tmp/output1';");
            myPig.registerQuery("store e into '/tmp/output2';");

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
    public void testMultiQueryJiraPig976_3() {

        // test case: group all and key ('group') isn't part of output

        try {
            myPig.setBatchOn();

            myPig.registerQuery("a = load 'passwd' " +
                                "using PigStorage(':') as (uname:chararray, passwd:chararray, uid:int, gid:int);");
            myPig.registerQuery("b = group a all;");
            myPig.registerQuery("c = group a by gid;");
            myPig.registerQuery("d = foreach b generate SUM(a.gid);");
            myPig.registerQuery("e = foreach c generate group, COUNT(a);");
            myPig.registerQuery("store d into '/tmp/output1';");
            myPig.registerQuery("store e into '/tmp/output2';");

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
    public void testMultiQueryJiraPig976_4() {

        // test case: group by multi-cols and key ('group') isn't part of output
         
        try {
            myPig.setBatchOn();

            myPig.registerQuery("a = load 'passwd' " +
                                "using PigStorage(':') as (uname:chararray, passwd:chararray, uid:int, gid:int);");
            myPig.registerQuery("b = group a by uid;");
            myPig.registerQuery("c = group a by (uname, gid);");
            myPig.registerQuery("d = foreach b generate SUM(a.gid);");
            myPig.registerQuery("e = foreach c generate group.uname, group.gid, COUNT(a);");
            myPig.registerQuery("store d into '/tmp/output1';");
            myPig.registerQuery("store e into '/tmp/output2';");

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
    public void testMultiQueryJiraPig976_5() {

        // test case: key ('group') in multiple positions.

        try {
            myPig.setBatchOn();

            myPig.registerQuery("a = load 'passwd' " +
                                "using PigStorage(':') as (uname:chararray, passwd:chararray, uid:int, gid:int);");
            myPig.registerQuery("b = group a by uid;");
            myPig.registerQuery("c = group a by (uname, gid);");
            myPig.registerQuery("d = foreach b generate SUM(a.gid), group, group as foo;");
            myPig.registerQuery("d1 = foreach d generate $1 + $2;");
            myPig.registerQuery("e = foreach c generate group, COUNT(a);");
            myPig.registerQuery("store d1 into '/tmp/output1';");
            myPig.registerQuery("store e into '/tmp/output2';");

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
    public void testMultiQueryJiraPig976_6() {

        // test case: key ('group') has null values.

        String INPUT_FILE = "pig-976.txt";
        
        try {
            
            PrintWriter w = new PrintWriter(new FileWriter(INPUT_FILE));
            w.println("apple\tapple\t100\t10");
            w.println("apple\tapple\t\t20");
            w.println("orange\torange\t100\t10");
            w.println("orange\torange\t\t20");
            w.println("strawberry\tstrawberry\t300\t10");
   
            w.close();
            
            Util.copyFromLocalToCluster(cluster, INPUT_FILE, INPUT_FILE);
        
            myPig.setBatchOn();

            myPig.registerQuery("a = load '" + INPUT_FILE +
                                "' as (uname:chararray, passwd:chararray, uid:int, gid:int);");
            myPig.registerQuery("b = group a by uid;");
            myPig.registerQuery("c = group a by gid;");
            myPig.registerQuery("d = foreach b generate group, SUM(a.gid);");
            myPig.registerQuery("e = foreach c generate COUNT(a), group;");
            myPig.registerQuery("store d into '/tmp/output1';");
            myPig.registerQuery("store e into '/tmp/output2';");

            List<ExecJob> jobs = myPig.executeBatch();
            assertTrue(jobs.size() == 2);
            
            for (ExecJob job : jobs) {
                assertTrue(job.getStatus() == ExecJob.JOB_STATUS.COMPLETED);
            }
            
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

            LogicalPlan lp = checkLogicalPlan(1, 1, 6);

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
             
            LogicalPlan lp = checkLogicalPlan(1, 3, 19);

            PhysicalPlan pp = checkPhysicalPlan(lp, 1, 3, 25);

            checkMRPlan(pp, 1, 1, 1);
            
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
             
            LogicalPlan lp = checkLogicalPlan(1, 2, 17);

            PhysicalPlan pp = checkPhysicalPlan(lp, 1, 2, 25);

            checkMRPlan(pp, 1, 1, 2);
            
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        } 
    }       
    
    @Test
    public void testMultiQueryJiraPig983_2() {

        System.out.println("===== multi-query Jira Pig-983_2 =====");

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
             
            LogicalPlan lp = checkLogicalPlan(1, 3, 19);

            PhysicalPlan pp = checkPhysicalPlan(lp, 1, 3, 25);

            checkMRPlan(pp, 1, 1, 1);
            
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
            myPig.registerQuery("d2 = foreach d1 generate group, MAX(d.uid) - MIN(d.uid);");
            myPig.registerQuery("store d2 into '/tmp/output3';");
             
            LogicalPlan lp = checkLogicalPlan(1, 3, 19);

            PhysicalPlan pp = checkPhysicalPlan(lp, 1, 3, 25);

            checkMRPlan(pp, 1, 1, 2);
            
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
             
            LogicalPlan lp = checkLogicalPlan(1, 3, 19);

            PhysicalPlan pp = checkPhysicalPlan(lp, 1, 3, 25);

            checkMRPlan(pp, 1, 1, 1);
            
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
             
            LogicalPlan lp = checkLogicalPlan(1, 3, 16);

            PhysicalPlan pp = checkPhysicalPlan(lp, 1, 3, 24);

            checkMRPlan(pp, 1, 1, 2);
            
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

            LogicalPlan lp = checkLogicalPlan(1, 3, 15);

            PhysicalPlan pp = checkPhysicalPlan(lp, 1, 3, 23);

            checkMRPlan(pp, 1, 1, 1); 
            
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

            PhysicalPlan pp = checkPhysicalPlan(lp, 2, 1, 10);

            checkMRPlan(pp, 1, 1, 2); 

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
            myPig.registerQuery("e = join c by gid, d by gid using \"repl\";");
            myPig.registerQuery("store e into '/tmp/output3';");

            LogicalPlan lp = checkLogicalPlan(2, 3, 16);

            PhysicalPlan pp = checkPhysicalPlan(lp, 2, 3, 16);

            checkMRPlan(pp, 2, 1, 3);            
            
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

            LogicalPlan lp = checkLogicalPlan(1, 3, 15);

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

            LogicalPlan lp = checkLogicalPlan(1, 3, 17);

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

            LogicalPlan lp = checkLogicalPlan(1, 1, 5);

            PhysicalPlan pp = checkPhysicalPlan(lp, 1, 1, 5);

            checkMRPlan(pp, 1, 1, 2); 

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

            LogicalPlan lp = checkLogicalPlan(1, 3, 19);

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
            
            LogicalPlan lp = checkLogicalPlan(2, 2, 15);

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

            LogicalPlan lp = checkLogicalPlan(1, 2, 11);

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

            LogicalPlan lp = checkLogicalPlan(1, 2, 13);

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
            
            LogicalPlan lp = checkLogicalPlan(1, 2, 16);

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
            
            LogicalPlan lp = checkLogicalPlan(1, 2, 22);

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
             
            LogicalPlan lp = checkLogicalPlan(1, 3, 19);

            PhysicalPlan pp = checkPhysicalPlan(lp, 1, 3, 25);

            checkMRPlan(pp, 1, 1, 1);
            
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

            LogicalPlan lp = checkLogicalPlan(1, 2, 9);

            PhysicalPlan pp = checkPhysicalPlan(lp, 1, 2, 11);

            checkMRPlan(pp, 1, 1, 1);

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

            LogicalPlan lp = checkLogicalPlan(1, 3, 14);

            PhysicalPlan pp = checkPhysicalPlan(lp, 1, 3, 14);

            checkMRPlan(pp, 1, 1, 1);            

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

            LogicalPlan lp = checkLogicalPlan(2, 3, 16);

            PhysicalPlan pp = checkPhysicalPlan(lp, 2, 3, 19);

            checkMRPlan(pp, 2, 1, 3);            

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

            LogicalPlan lp = checkLogicalPlan(1, 3, 10);
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

            LogicalPlan lp = checkLogicalPlan(1, 2, 14);
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

            LogicalPlan lp = checkLogicalPlan(1, 1, 7);
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
        		"store b into 'output1' using "+DummyStoreWithOutputFormat.class.getName()+"();" +
        		"c = group a by $0;" +
        		"d = foreach c generate group, COUNT(a.$0);" +
        		"store d into 'output2' using "+DummyStoreWithOutputFormat.class.getName()+"();" ;
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

    public static class DummyStoreWithOutputFormat extends StoreFunc {
        
        /**
         * 
         */
        public DummyStoreWithOutputFormat() {
            // TODO Auto-generated constructor stub
        }

        
        /* (non-Javadoc)
         * @see org.apache.pig.StoreFunc#putNext(org.apache.pig.data.Tuple)
         */
        @Override
        public void putNext(Tuple f) throws IOException {
            // TODO Auto-generated method stub
            
        }


        /* (non-Javadoc)
         * @see org.apache.pig.StoreFunc#checkSchema(org.apache.pig.ResourceSchema)
         */
        @Override
        public void checkSchema(ResourceSchema s) throws IOException {
            // TODO Auto-generated method stub
            
        }


        /* (non-Javadoc)
         * @see org.apache.pig.StoreFunc#getOutputFormat()
         */
        @Override
        public org.apache.hadoop.mapreduce.OutputFormat getOutputFormat()
                throws IOException {
            return new DummyOutputFormat();
        }


        /* (non-Javadoc)
         * @see org.apache.pig.StoreFunc#prepareToWrite(org.apache.hadoop.mapreduce.RecordWriter)
         */
        @Override
        public void prepareToWrite(
                org.apache.hadoop.mapreduce.RecordWriter writer)
                throws IOException {
            
        }


        /* (non-Javadoc)
         * @see org.apache.pig.StoreFunc#relToAbsPathForStoreLocation(java.lang.String, org.apache.hadoop.fs.Path)
         */
        @Override
        public String relToAbsPathForStoreLocation(String location, Path curDir)
                throws IOException {
            return LoadFunc.getAbsolutePath(location, curDir);
        }


        /* (non-Javadoc)
         * @see org.apache.pig.StoreFunc#setStoreLocation(java.lang.String, org.apache.hadoop.mapreduce.Job)
         */
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
    
    @SuppressWarnings({ "deprecation", "unchecked" })
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
            PigException pe = LogUtils.getPigException(e);
            if (pe != null) {
                throw pe;
            } else {
                e.printStackTrace();
                Assert.fail();
            }
        }

        showPlanOperators(lp);
       
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        lp.explain(out, System.out);

        System.out.println("===== Display Logical Plan =====");
        System.out.println(out.toString());

        Assert.assertEquals(expectedRoots, lp.getRoots().size());
        Assert.assertEquals(expectedLeaves, lp.getLeaves().size());
        Assert.assertEquals(expectedSize, lp.size());

        return lp;
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

        ExecTools.checkLeafIsStore(pp, myPig.getPigContext());
        
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

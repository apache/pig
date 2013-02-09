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
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecJob;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TestMultiQuery {

    private static PigServer myPig;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        Util.copyFromLocalToLocal(
                "test/org/apache/pig/test/data/passwd", "passwd");
        Util.copyFromLocalToLocal(
                "test/org/apache/pig/test/data/passwd2", "passwd2");
        Properties props = new Properties();
        props.setProperty("opt.multiquery", ""+true);
        myPig = new PigServer(ExecType.LOCAL, props);
    }
    
    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        Util.deleteFile(new PigContext(ExecType.LOCAL, new Properties()), "passwd");
        Util.deleteFile(new PigContext(ExecType.LOCAL, new Properties()), "passwd2");
        deleteOutputFiles();
    }
    
    @Before
    public void setUp() throws Exception {
        deleteOutputFiles();
    }

    @After
    public void tearDown() throws Exception {

    }

    @Test
    public void testMultiQueryJiraPig1438() throws Exception {

        // test case: merge multiple distinct jobs
        
        String INPUT_FILE = "abc";
        
        String[] inputData = {
                "1\t2\t3",
                "2\t3\t4",
                "1\t2\t3",
                "2\t3\t4",
                "1\t2\t3"
        };
        
        Util.createLocalInputFile(INPUT_FILE, inputData);
       
        myPig.setBatchOn();

        myPig.registerQuery("A = load '" + INPUT_FILE + "' as (col1:int, col2:int, col3:int);");
        myPig.registerQuery("B1 = foreach A generate col1, col2;");
        myPig.registerQuery("B2 = foreach A generate col2, col3;");
        myPig.registerQuery("C1 = distinct B1;");
        myPig.registerQuery("C2 = distinct B2;");
        myPig.registerQuery("D1 = foreach C1 generate col1, col2;");
        myPig.registerQuery("D2 = foreach C2 generate col2, col3;");
        myPig.registerQuery("store D1 into 'output1';");
        myPig.registerQuery("store D2 into 'output2';");            
        
        myPig.executeBatch();
        
        myPig.registerQuery("E = load 'output1' as (a:int, b:int);");            
        Iterator<Tuple> iter = myPig.openIterator("E");

        List<Tuple> expectedResults = Util.getTuplesFromConstantTupleStrings(
                new String[] { 
                        "(1,2)",
                        "(2,3)"
                });
        
        int counter = 0;
        while (iter.hasNext()) {
            assertEquals(expectedResults.get(counter++).toString(), iter.next().toString());      
        }
        assertEquals(expectedResults.size(), counter);
                    
        myPig.registerQuery("E = load 'output2' as (a:int, b:int);");            
        iter = myPig.openIterator("E");

        expectedResults = Util.getTuplesFromConstantTupleStrings(
                new String[] { 
                        "(2,3)",
                        "(3,4)"
                });
        
        counter = 0;
        while (iter.hasNext()) {
            assertEquals(expectedResults.get(counter++).toString(), iter.next().toString());      
        }

        assertEquals(expectedResults.size(), counter);
    }
    
    @Test
    public void testMultiQueryJiraPig1252() throws Exception {

        // test case: Problems with secondary key optimization and multiquery
        // diamond optimization
        
        String INPUT_FILE = "abc";
        
        String[] inputData = {
            "1\t2\t3",
            "2\t3\t4",
            "3\t\t5",
            "5\t6\t6",
            "6\t\t7"       
        };
        
        Util.createLocalInputFile(INPUT_FILE, inputData);

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
    }

    @Test
    public void testMultiQueryJiraPig1169() throws Exception {

        // test case: Problems with some top N queries
        
        String INPUT_FILE = "abc";
        
        String[] inputData = {
                "1\t2\t3",
                "2\t3\t4",
                "3\t4\t5",
                "5\t6\t7",
                "6\t7\t8"       
        };
        
        Util.createLocalInputFile(INPUT_FILE, inputData);
       
        myPig.setBatchOn();

        myPig.registerQuery("A = load '" + INPUT_FILE 
                + "' as (a:int, b, c);");
        myPig.registerQuery("A1 = Order A by a desc parallel 3;");
        myPig.registerQuery("A2 = limit A1 2;");
        myPig.registerQuery("store A1 into 'output1';");
        myPig.registerQuery("store A2 into 'output2';");

        myPig.executeBatch();

        myPig.registerQuery("B = load 'output2' as (a:int, b, c);");
        
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
    }
  
    @Test
    public void testMultiQueryJiraPig1171() throws Exception {

        // test case: Problems with some top N queries
        
        String INPUT_FILE = "abc";
        
        String[] inputData = {
            "1\tapple\t3",
            "2\torange\t4",
            "3\tpersimmon\t5"    
        };
        
        Util.createLocalInputFile(INPUT_FILE, inputData);

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
    }
    
    @Test
    public void testMultiQueryJiraPig1157() throws Exception {

        // test case: Sucessive replicated joins do not generate Map Reduce plan and fails due to OOM
        
        String INPUT_FILE = "abc";
        String INPUT_FILE_1 = "abc";
        
        String[] inputData = {
                "1\tapple\t3",
                "2\torange\t4",
                "3\tpersimmon\t5"    
        };
            
        Util.createLocalInputFile(INPUT_FILE, inputData);

        myPig.setBatchOn();

        myPig.registerQuery("A = load '" + INPUT_FILE 
                + "' as (a:long, b, c);");
        myPig.registerQuery("A1 = FOREACH A GENERATE a;");
        myPig.registerQuery("B = GROUP A1 BY a;");
        myPig.registerQuery("C = load '" + INPUT_FILE_1 
                + "' as (x:long, y);");
        myPig.registerQuery("D = JOIN C BY x, B BY group USING 'replicated';");  
        myPig.registerQuery("E = JOIN A BY a, D by x USING 'replicated';");  
        
        Iterator<Tuple> iter = myPig.openIterator("E");

        List<Tuple> expectedResults = Util.getTuplesFromConstantTupleStrings(
                new String[] { 
                        "(1L,'apple',3,1L,'apple',1L,{(1L)})",
                        "(2L,'orange',4,2L,'orange',2L,{(2L)})",
                        "(3L,'persimmon',5,3L,'persimmon',3L,{(3L)})"
                });
        
        int counter = 0;
        while (iter.hasNext()) {
            assertEquals(expectedResults.get(counter++).toString(), iter.next().toString());                  
        }

        assertEquals(expectedResults.size(), counter);
    }

    @Test
    public void testMultiQueryJiraPig1068() throws Exception {

        // test case: COGROUP fails with 'Type mismatch in key from map: 
        // expected org.apache.pig.impl.io.NullableText, recieved org.apache.pig.impl.io.NullableTuple'

        String INPUT_FILE = "pig-1068.txt";

        String[] inputData = {
            "10\tapple\tlogin\tjar",
            "20\torange\tlogin\tbox",
            "30\tstrawberry\tquit\tbot"    
        };
            
        Util.createLocalInputFile(INPUT_FILE, inputData);

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
    }

    @Test
    public void testMultiQueryJiraPig1108() throws Exception {

        myPig.setBatchOn();

        myPig.registerQuery("a = load 'passwd' " 
                + "using PigStorage(':') as (uname:chararray, passwd:chararray, uid:int, gid:int);");
        myPig.registerQuery("split a into plan1 if (uid > 5), plan2 if ( uid < 5);");
        myPig.registerQuery("b = group plan1 by uname;");
        myPig.registerQuery("c = foreach b { tmp = order plan1 by uid desc; " 
                + "generate flatten(group) as foo, tmp; };");
        myPig.registerQuery("d = filter c BY foo is not null;");
        myPig.registerQuery("store d into 'output1';");
        myPig.registerQuery("store plan2 into 'output2';");
         
        List<ExecJob> jobs = myPig.executeBatch();
        for (ExecJob job : jobs) {
            assertTrue(job.getStatus() == ExecJob.JOB_STATUS.COMPLETED);
        }
    }    
    
    @Test
    public void testMultiQueryJiraPig1114() throws Exception {

        // test case: MultiQuery optimization throws error when merging 2 level splits

        String INPUT_FILE = "data.txt";

        String[] inputData = {
            "10\tjar",
            "20\tbox",
            "30\tbot"   
        };
                
        Util.createLocalInputFile(INPUT_FILE, inputData);

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
        myPig.registerQuery("STORE orderedCounts INTO 'output1';");

        myPig.registerQuery("names = FOREACH data GENERATE name;");
        myPig.registerQuery("allNames = GROUP names all;");
        myPig.registerQuery("allNamesCount = FOREACH allNames GENERATE group as namesAll, COUNT(names) as total;");
        myPig.registerQuery("nameGroup = GROUP names by name;");
        myPig.registerQuery("nameGroupCount = FOREACH nameGroup GENERATE group as name, COUNT(names) as count;");
        myPig.registerQuery("namesCrossed = cross nameGroupCount, allNamesCount;");
        myPig.registerQuery("nameCountTotal = foreach namesCrossed generate name, count, total, (double)count / (double)total as proportion;");
        myPig.registerQuery("nameCountsOrdered = order nameCountTotal by count desc;");
        myPig.registerQuery("STORE nameCountsOrdered INTO 'output2';");

        List<ExecJob> jobs = myPig.executeBatch();
        for (ExecJob job : jobs) {
            assertTrue(job.getStatus() == ExecJob.JOB_STATUS.COMPLETED);
        }
    }

/** Test case is disabled due to PIG-2038
    @Test
    public void testMultiQueryJiraPig1113() throws Exception {

        // test case: Diamond query optimization throws error in JOIN

        String INPUT_FILE_1 = "set1.txt";
        String INPUT_FILE_2 = "set2.txt";
        

        String[] inputData_1 = {
            "login\t0\tjar",
            "login\t1\tbox",
            "quit\t0\tmany" 
        };
                
        Util.createLocalInputFile(INPUT_FILE_1, inputData_1);
        
        String[] inputData_2 = {
            "apple\tlogin\t{(login)}",
            "orange\tlogin\t{(login)}",
            "strawberry\tquit\t{(login)}"  
        };
                
        Util.createLocalInputFile(INPUT_FILE_2, inputData_2);
            
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
    }
 
    @Test
    public void testMultiQueryJiraPig1060_2() throws Exception {

        // test case: 

        String INPUT_FILE = "pig-1060.txt";

        String[] inputData = {
            "apple\t2",
            "apple\t12",
            "orange\t3",
            "orange\t23",
            "strawberry\t10",
            "strawberry\t34"  
        };
                    
        Util.createLocalInputFile(INPUT_FILE, inputData);

        myPig.setBatchOn();

        myPig.registerQuery("data = load '" + INPUT_FILE +
        "' as (name:chararray, gid:int);");
        myPig.registerQuery("f1 = filter data by gid < 5;");
        myPig.registerQuery("g1 = group f1 by name;");
        myPig.registerQuery("p1 = foreach g1 generate group, COUNT(f1.gid);");
        myPig.registerQuery("store p1 into 'output1';");

        myPig.registerQuery("f2 = filter data by gid > 5;");
        myPig.registerQuery("g2 = group f2 by name;");
        myPig.registerQuery("p2 = foreach g2 generate group, COUNT(f2.gid);");
        myPig.registerQuery("store p2 into 'output2';");

        myPig.registerQuery("f3 = filter f2 by gid > 10;");
        myPig.registerQuery("g3 = group f3 by name;");
        myPig.registerQuery("p3 = foreach g3 generate group, COUNT(f3.gid);");
        myPig.registerQuery("store p3 into 'output3';");

        myPig.registerQuery("f4 = filter f3 by gid < 20;");
        myPig.registerQuery("g4 = group f4 by name;");
        myPig.registerQuery("p4 = foreach g4 generate group, COUNT(f4.gid);");
        myPig.registerQuery("store p4 into 'output4';");

        List<ExecJob> jobs = myPig.executeBatch();
        assertEquals(4, jobs.size());

        for (ExecJob job : jobs) {
            assertTrue(job.getStatus() == ExecJob.JOB_STATUS.COMPLETED);
        }
    } 

    @Test
    public void testMultiQueryJiraPig920_2() throws Exception {

        // test case: execution of a query with two diamonds

        myPig.setBatchOn();

        myPig.registerQuery("a = load 'passwd' " +
                             "using PigStorage(':') as (uname:chararray, passwd:chararray, uid:int, gid:int);");
        myPig.registerQuery("b = filter a by uid < 5;");
        myPig.registerQuery("c = filter a by gid >= 5;");
        myPig.registerQuery("d = filter a by uid >= 5;");
        myPig.registerQuery("e = filter a by gid < 5;");
        myPig.registerQuery("f = cogroup c by $0, b by $0;");
        myPig.registerQuery("f1 = foreach f generate group, COUNT(c), COUNT(b);");
        myPig.registerQuery("store f1 into 'output1';");
        myPig.registerQuery("g = cogroup d by $0, e by $0;");
        myPig.registerQuery("g1 = foreach g generate group, COUNT(d), COUNT(e);");
        myPig.registerQuery("store g1 into 'output2';");
         
        List<ExecJob> jobs = myPig.executeBatch();
        for (ExecJob job : jobs) {
            assertTrue(job.getStatus() == ExecJob.JOB_STATUS.COMPLETED);
        }
    }            
    
    @Test
    public void testMultiQueryJiraPig920_3() throws Exception {

        // test case: execution of a simple diamond query
        
        String INPUT_FILE = "pig-920.txt";
        
        String[] inputData = {
            "apple\tapple\t100\t10",
            "apple\tapple\t200\t20",
            "orange\torange\t100\t10",
            "orange\torange\t300\t20"  
        };
                        
        Util.createLocalInputFile(INPUT_FILE, inputData);

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
    }        

    @Test
    public void testMultiQueryJiraPig976() throws Exception {

        // test case: key ('group') isn't part of foreach output
        // and keys have the same type.

        myPig.setBatchOn();

        myPig.registerQuery("a = load 'passwd' " +
                            "using PigStorage(':') as (uname:chararray, passwd:chararray, uid:int, gid:int);");
        myPig.registerQuery("b = group a by uid;");
        myPig.registerQuery("c = group a by gid;");
        myPig.registerQuery("d = foreach b generate SUM(a.gid);");
        myPig.registerQuery("e = foreach c generate group, COUNT(a);");
        myPig.registerQuery("store d into 'output1';");
        myPig.registerQuery("store e into 'output2';");

        List<ExecJob> jobs = myPig.executeBatch();
        for (ExecJob job : jobs) {
            assertTrue(job.getStatus() == ExecJob.JOB_STATUS.COMPLETED);
        }
    }

    @Test
    public void testMultiQueryJiraPig976_2() throws Exception {

        // test case: key ('group') isn't part of foreach output 
        // and keys have different types

        myPig.setBatchOn();

        myPig.registerQuery("a = load 'passwd' " +
                            "using PigStorage(':') as (uname:chararray, passwd:chararray, uid:int, gid:int);");
        myPig.registerQuery("b = group a by uname;");
        myPig.registerQuery("c = group a by gid;");
        myPig.registerQuery("d = foreach b generate SUM(a.gid);");
        myPig.registerQuery("e = foreach c generate group, COUNT(a);");
        myPig.registerQuery("store d into 'output1';");
        myPig.registerQuery("store e into 'output2';");

        List<ExecJob> jobs = myPig.executeBatch();
        for (ExecJob job : jobs) {
            assertTrue(job.getStatus() == ExecJob.JOB_STATUS.COMPLETED);
        }
    }

    @Test
    public void testMultiQueryJiraPig976_3() throws Exception {

        // test case: group all and key ('group') isn't part of output

        myPig.setBatchOn();

        myPig.registerQuery("a = load 'passwd' " +
                            "using PigStorage(':') as (uname:chararray, passwd:chararray, uid:int, gid:int);");
        myPig.registerQuery("b = group a all;");
        myPig.registerQuery("c = group a by gid;");
        myPig.registerQuery("d = foreach b generate SUM(a.gid);");
        myPig.registerQuery("e = foreach c generate group, COUNT(a);");
        myPig.registerQuery("store d into 'output1';");
        myPig.registerQuery("store e into 'output2';");

        List<ExecJob> jobs = myPig.executeBatch();
        for (ExecJob job : jobs) {
            assertTrue(job.getStatus() == ExecJob.JOB_STATUS.COMPLETED);
        }
    }

    @Test
    public void testMultiQueryJiraPig976_4() throws Exception {

        // test case: group by multi-cols and key ('group') isn't part of output
     
        myPig.setBatchOn();

        myPig.registerQuery("a = load 'passwd' " +
                            "using PigStorage(':') as (uname:chararray, passwd:chararray, uid:int, gid:int);");
        myPig.registerQuery("b = group a by uid;");
        myPig.registerQuery("c = group a by (uname, gid);");
        myPig.registerQuery("d = foreach b generate SUM(a.gid);");
        myPig.registerQuery("e = foreach c generate group.uname, group.gid, COUNT(a);");
        myPig.registerQuery("store d into 'output1';");
        myPig.registerQuery("store e into 'output2';");

        List<ExecJob> jobs = myPig.executeBatch();
        for (ExecJob job : jobs) {
            assertTrue(job.getStatus() == ExecJob.JOB_STATUS.COMPLETED);
        }
    }
   
    @Test
    public void testMultiQueryJiraPig976_5() throws Exception {

        // test case: key ('group') in multiple positions.

        myPig.setBatchOn();

        myPig.registerQuery("a = load 'passwd' " +
                            "using PigStorage(':') as (uname:chararray, passwd:chararray, uid:int, gid:int);");
        myPig.registerQuery("b = group a by uid;");
        myPig.registerQuery("c = group a by (uname, gid);");
        myPig.registerQuery("d = foreach b generate SUM(a.gid), group, group as foo;");
        myPig.registerQuery("d1 = foreach d generate $1 + $2;");
        myPig.registerQuery("e = foreach c generate group, COUNT(a);");
        myPig.registerQuery("store d1 into 'output1';");
        myPig.registerQuery("store e into 'output2';");

        List<ExecJob> jobs = myPig.executeBatch();
        for (ExecJob job : jobs) {
            assertTrue(job.getStatus() == ExecJob.JOB_STATUS.COMPLETED);
        }
    }
*/
    @Test
    public void testMultiQueryJiraPig976_6() throws Exception {

        // test case: key ('group') has null values.

        String INPUT_FILE = "pig-976.txt";
        
        String[] inputData = {
            "apple\tapple\t100\t10",
            "apple\tapple\t\t20",
            "orange\torange\t100\t10",
            "orange\torange\t\t20",
            "strawberry\tstrawberry\t300\t10"
        };
                            
        Util.createLocalInputFile(INPUT_FILE, inputData);
    
        myPig.setBatchOn();

        myPig.registerQuery("a = load '" + INPUT_FILE +
                            "' as (uname:chararray, passwd:chararray, uid:int, gid:int);");
        myPig.registerQuery("b = group a by uid;");
        myPig.registerQuery("c = group a by gid;");
        myPig.registerQuery("d = foreach b generate group, SUM(a.gid);");
        myPig.registerQuery("e = foreach c generate COUNT(a), group;");
        myPig.registerQuery("store d into 'output1';");
        myPig.registerQuery("store e into 'output2';");

        List<ExecJob> jobs = myPig.executeBatch();
        assertTrue(jobs.size() == 2);
        
        for (ExecJob job : jobs) {
            assertTrue(job.getStatus() == ExecJob.JOB_STATUS.COMPLETED);
        }
    }    

    @Test
    public void testMultiQueryJiraPig983_2() throws Exception {

        System.out.println("===== multi-query Jira Pig-983_2 =====");

        myPig.setBatchOn();

        myPig.registerQuery("a = load 'passwd' " +
                             "using PigStorage(':') as (uname:chararray, passwd:chararray, uid:int, gid:int);");
        myPig.registerQuery("b = filter a by uid < 5;");
        myPig.registerQuery("c = filter a by uid >= 5;");
        myPig.registerQuery("d = join b by uname, c by uname;");
        myPig.registerQuery("e = group d by b::gid;");
        myPig.registerQuery("e1 = foreach e generate group, COUNT(d.b::uid);");
        myPig.registerQuery("store e1 into 'output1';");
        myPig.registerQuery("f = group d by c::gid;");
        myPig.registerQuery("f1 = foreach f generate group, SUM(d.c::uid);");
        myPig.registerQuery("store f1 into 'output2';");
         
        List<ExecJob> jobs = myPig.executeBatch();

        assertTrue(jobs.size() == 2);
        
        for (ExecJob job : jobs) {
            assertTrue(job.getStatus() == ExecJob.JOB_STATUS.COMPLETED);
        }
    }     

    // --------------------------------------------------------------------------
    // Helper methods

    private static void deleteOutputFiles() {
        Util.deleteDirectory(new File("output1"));
        Util.deleteDirectory(new File("output2"));
        Util.deleteDirectory(new File("output3"));
        Util.deleteDirectory(new File("output4"));
        Util.deleteDirectory(new File("output5"));
    }
}

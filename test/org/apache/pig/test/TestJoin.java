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

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.apache.pig.ExecType;
import org.apache.pig.PigException;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.LogUtils;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.logical.relational.LOJoin;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.newplan.logical.relational.LOJoin.JOINTYPE;
import org.apache.pig.parser.ParserException;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import junit.framework.TestCase;

/**
 * Test cases to test join statement
 */

@RunWith(JUnit4.class)
public class TestJoin extends TestCase {
    
    static MiniCluster cluster;
    private PigServer pigServer;

    TupleFactory mTf = TupleFactory.getInstance();
    BagFactory mBf = BagFactory.getInstance();
    ExecType[] execTypes = new ExecType[] {ExecType.LOCAL, ExecType.MAPREDUCE};
    
    @Before
    @Override
    public void setUp() throws Exception{
        FileLocalizer.setR(new Random());
    }
    
    @AfterClass
    public static void oneTimeTearDown() throws Exception {
        cluster.shutDown();
    }
    
    private void setUp(ExecType execType) throws ExecException {
        // cause a reinitialization of FileLocalizer's
        // internal state
        FileLocalizer.setInitialized(false);
        if(execType == ExecType.MAPREDUCE) {
            cluster =  MiniCluster.buildCluster();
            pigServer = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        } else if(execType == ExecType.LOCAL) {
            pigServer = new PigServer(ExecType.LOCAL);       
        }
    }
    
    private String createInputFile(ExecType execType, String fileNameHint, String[] data) throws IOException {
        String fileName = "";
        if(execType == ExecType.MAPREDUCE) {
            Util.createInputFile(cluster, fileNameHint, data);
            fileName = fileNameHint;
        } else if (execType == ExecType.LOCAL) {
            File f = Util.createInputFile("test", fileNameHint, data);
            fileName = Util.generateURI(Util.encodeEscape(f.getAbsolutePath()), pigServer.getPigContext());
        }
        return fileName;
    }
    
    private void deleteInputFile(ExecType execType, String fileName) throws IOException {
        if(execType == ExecType.MAPREDUCE) {
            Util.deleteFile(cluster, fileName);
        } else if(execType == ExecType.LOCAL){
            fileName = fileName.replace("file://", "");
            new File(fileName).delete();
        }
    }

    @Test
    public void testJoinWithMissingFieldsInTuples() throws IOException{
        setUp(ExecType.MAPREDUCE);
        String[] input1 = {
                "ff ff ff",
                "",
                "",
                "",
                "",
                "ff ff ff",
                "",
                ""
                };
        String[] input2 = {
                "",
                "",
                "",
                "",
                ""
                };
        
        String firstInput = createInputFile(ExecType.MAPREDUCE, "a.txt", input1);
        String secondInput = createInputFile(ExecType.MAPREDUCE, "b.txt", input2);
        String script = "a = load 'a.txt'  using PigStorage(' ');" +
        "b = load 'b.txt'  using PigStorage('\u0001');" +
        "c = join a by $0, b by $0;";
        Util.registerMultiLineQuery(pigServer, script);
        Iterator<Tuple> it = pigServer.openIterator("c");
        assertFalse(it.hasNext());
        deleteInputFile(ExecType.MAPREDUCE, firstInput);
        deleteInputFile(ExecType.MAPREDUCE, secondInput);
    }
    
    @Test
    public void testJoinUnkownSchema() throws Exception {
        // If any of the input schema is unknown, the resulting schema should be unknown as well
        for (ExecType execType : execTypes) {
            setUp(execType);
            String script = "a = load 'a.txt';" +
                    "b = load 'b.txt'; " +
                    "c = join a by $0, b by $0;";
            Util.registerMultiLineQuery(pigServer, script);
            Schema schema = pigServer.dumpSchema("c");
            assertTrue(schema == null);
        }
    }

    @Test
    public void testDefaultJoin() throws IOException, ParserException {
        for (ExecType execType : execTypes) {
            setUp(execType);
            String[] input1 = {
                    "hello\t1",
                    "bye\t2",
                    "\t3"
            };
            String[] input2 = {
                    "hello\tworld",
                    "good\tmorning",
                    "\tevening"
            };
            
            String firstInput = createInputFile(execType, "a.txt", input1);
            String secondInput = createInputFile(execType, "b.txt", input2);
            Tuple expectedResult = (Tuple)Util.getPigConstant("('hello',1,'hello','world')");
            
            // with schema
            String script = "a = load '"+ Util.encodeEscape(firstInput) +"' as (n:chararray, a:int); " +
            		"b = load '"+ Util.encodeEscape(secondInput) +"' as (n:chararray, m:chararray); " +
            		"c = join a by $0, b by $0;";
            Util.registerMultiLineQuery(pigServer, script);
            Iterator<Tuple> it = pigServer.openIterator("c");
            assertEquals(true, it.hasNext());
            assertEquals(expectedResult, it.next());
            assertEquals(false, it.hasNext());
            
            // without schema
            script = "a = load '"+ Util.encodeEscape(firstInput) + "'; " +
            "b = load '" + Util.encodeEscape(secondInput) + "'; " +
            "c = join a by $0, b by $0;";
            Util.registerMultiLineQuery(pigServer, script);
            it = pigServer.openIterator("c");
            assertEquals(true, it.hasNext());
            assertEquals(expectedResult.toString(), it.next().toString());
            assertEquals(false, it.hasNext());
            deleteInputFile(execType, firstInput);
            deleteInputFile(execType, secondInput);
        }
    }
    
    
    @Test
    public void testJoinSchema() throws Exception {
        for (ExecType execType : execTypes) {
            setUp(execType);
            String[] input1 = {
                    "1\t2",
                    "2\t3",
                    "3\t4"
            };
            String[] input2 = {
                    "1\thello",
                    "4\tbye",
            };
            
            String firstInput = createInputFile(execType, "a.txt", input1);
            String secondInput = createInputFile(execType, "b.txt", input2);
            Tuple expectedResult = (Tuple)Util.getPigConstant("(1,2,1,'hello',1,2,1,'hello')");
            
            // with schema
            String script = "a = load '"+ Util.encodeEscape(firstInput) +"' as (i:int, j:int); " +
                    "b = load '"+ Util.encodeEscape(secondInput) +"' as (k:int, l:chararray); " +
                    "c = join a by $0, b by $0;" +
                    "d = foreach c generate i,j,k,l,a::i as ai,a::j as aj,b::k as bk,b::l as bl;";
            Util.registerMultiLineQuery(pigServer, script);
            Iterator<Tuple> it = pigServer.openIterator("d");
            assertEquals(true, it.hasNext());
            assertEquals(expectedResult, it.next());
            assertEquals(false, it.hasNext());
            
            // schema with duplicates
            script = "a = load '"+ Util.encodeEscape(firstInput) +"' as (i:int, j:int); " +
            "b = load '"+ Util.encodeEscape(secondInput) +"' as (i:int, l:chararray); " +
            "c = join a by $0, b by $0;" +
            "d = foreach c generate i,j,l,a::i as ai,a::j as aj,b::i as bi,b::l as bl;";
            boolean exceptionThrown = false;
            try{
                Util.registerMultiLineQuery(pigServer, script);
                pigServer.openIterator("d");
            }catch (Exception e) {
                PigException pe = LogUtils.getPigException(e);
                assertEquals(1025, pe.getErrorCode());
                exceptionThrown = true;
            }
            assertEquals(true, exceptionThrown);
            
            // schema with duplicates with resolution
            script = "a = load '"+ Util.encodeEscape(firstInput) +"' as (i:int, j:int); " +
            "b = load '"+ Util.encodeEscape(secondInput) +"' as (i:int, l:chararray); " +
            "c = join a by $0, b by $0;" +
            "d = foreach c generate a::i as ai1,j,b::i as bi1,l,a::i as ai2,a::j as aj2,b::i as bi3,b::l as bl3;";
            Util.registerMultiLineQuery(pigServer, script);
            it = pigServer.openIterator("d");
            assertEquals(true, it.hasNext());
            assertEquals(expectedResult, it.next());
            assertEquals(false, it.hasNext());
            deleteInputFile(execType, firstInput);
            deleteInputFile(execType, secondInput);
        }
    }
    
    @Test
    public void testJoinSchema2() throws Exception {
        // test join where one load does not have schema
        ExecType execType = ExecType.LOCAL;
        setUp(execType );
        String[] input1 = {
                "1\t2",
                "2\t3",
                "3\t4"
        };
        String[] input2 = {
                "1\thello",
                "4\tbye",
        };
        
        String firstInput = createInputFile(execType, "a.txt", input1);
        String secondInput = createInputFile(execType, "b.txt", input2);
        Tuple expectedResultCharArray =
            (Tuple)Util.getPigConstant("('1','2','1','hello','1','2','1','hello')");
        
        Tuple expectedResult = TupleFactory.getInstance().newTuple();
        for(Object field : expectedResultCharArray.getAll()){
            expectedResult.append(new DataByteArray(field.toString()));
        }
        
        // with schema
        String script = "a = load '"+ Util.encodeEscape(firstInput) +"' ; " +
        //re-using alias a for new operator below, doing this intentionally 
        // because such use case has been seen
        "a = foreach a generate $0 as i, $1 as j ;" +
        "b = load '"+ Util.encodeEscape(secondInput) +"' as (k, l); " +
        "c = join a by $0, b by $0;" +
        "d = foreach c generate i,j,k,l,a::i as ai,a::j as aj,b::k as bk,b::l as bl;";
        Util.registerMultiLineQuery(pigServer, script);
        Iterator<Tuple> it = pigServer.openIterator("d");
        assertEquals(true, it.hasNext());
        Tuple res = it.next();
        assertEquals(expectedResult, res);
        assertEquals(false, it.hasNext());
        deleteInputFile(execType, firstInput);
        deleteInputFile(execType, secondInput);
        
    }
    
    @Test
    public void testLeftOuterJoin() throws Exception {
        for (ExecType execType : execTypes) {
            setUp(execType);
            String[] input1 = {
                    "hello\t1",
                    "bye\t2",
                    "\t3"
            };
            String[] input2 = {
                    "hello\tworld",
                    "good\tmorning",
                    "\tevening"
    
            };
            
            String firstInput = createInputFile(execType, "a.txt", input1);
            String secondInput = createInputFile(execType, "b.txt", input2);
            List<Tuple> expectedResults = Util.getTuplesFromConstantTupleStrings(
                    new String[] { 
                            "('hello',1,'hello','world')",
                            "('bye',2,null,null)",
                            "(null,3,null,null)"
                    });
            
            // with and without optional outer
            for(int i = 0; i < 2; i++) {
                //with schema
                String script = "a = load '"+ Util.encodeEscape(firstInput) +"' as (n:chararray, a:int); " +
                        "b = load '"+ Util.encodeEscape(secondInput) +"' as (n:chararray, m:chararray); ";
                if(i == 0) {
                    script +=  "c = join a by $0 left outer, b by $0;" ;
                } else {
                    script +=  "c = join a by $0 left, b by $0;" ;
                }
                script += "d = order c by $1;";
                // ensure we parse correctly
                Util.buildLp(pigServer, script);
                
                // run query and test results only once
                if(i == 0) {
                    Util.registerMultiLineQuery(pigServer, script);
                    Iterator<Tuple> it = pigServer.openIterator("d");
                    int counter= 0;
                    while(it.hasNext()) {
                        assertEquals(expectedResults.get(counter++), it.next());
                    }
                    assertEquals(expectedResults.size(), counter);
                    
                    // without schema
                    script = "a = load '"+ Util.encodeEscape(firstInput) +"'; " +
                    "b = load '"+ Util.encodeEscape(secondInput) +"'; ";
                    if(i == 0) {
                        script +=  "c = join a by $0 left outer, b by $0;" ;
                    } else {
                        script +=  "c = join a by $0 left, b by $0;" ;
                    }
                    try {
                        Util.registerMultiLineQuery(pigServer, script);
                    } catch (Exception e) {
                        PigException pe = LogUtils.getPigException(e);
                        assertEquals(1105, pe.getErrorCode());
                    }
                }
            }
            deleteInputFile(execType, firstInput);
            deleteInputFile(execType, secondInput);
        }
    }

    @Test
    public void testRightOuterJoin() throws Exception {
        for (ExecType execType : execTypes) {
            setUp(execType);
            String[] input1 = {
                    "hello\t1",
                    "bye\t2",
                    "\t3"
            };
            String[] input2 = {
                    "hello\tworld",
                    "good\tmorning",
                    "\tevening"
    
            };
            
            String firstInput = createInputFile(execType, "a.txt", input1);
            String secondInput = createInputFile(execType, "b.txt", input2);
            List<Tuple> expectedResults = Util.getTuplesFromConstantTupleStrings(
                    new String[] {
                            "(null,null,null,'evening')",
                            "(null,null,'good','morning')",
                            "('hello',1,'hello','world')"
                                           });
            // with and without optional outer
            for(int i = 0; i < 2; i++) {
                // with schema
                String script = "a = load '"+ Util.encodeEscape(firstInput) +"' as (n:chararray, a:int); " +
                        "b = load '"+ Util.encodeEscape(secondInput) +"' as (n:chararray, m:chararray); ";
                if(i == 0) {
                    script +=  "c = join a by $0 right outer, b by $0;" ;
                } else {
                    script +=  "c = join a by $0 right, b by $0;" ;
                }
                script += "d = order c by $3;";
                // ensure we parse correctly
                Util.buildLp(pigServer, script);
                
                // run query and test results only once
                if(i == 0) {
                    Util.registerMultiLineQuery(pigServer, script);
                    Iterator<Tuple> it = pigServer.openIterator("d");
                    int counter= 0;
                    while(it.hasNext()) {
                        assertEquals(expectedResults.get(counter++), it.next());
                    }
                    assertEquals(expectedResults.size(), counter);
                    
                    // without schema
                    script = "a = load '"+ Util.encodeEscape(firstInput) +"'; " +
                    "b = load '"+ Util.encodeEscape(secondInput) +"'; " ;
                    if(i == 0) {
                        script +=  "c = join a by $0 right outer, b by $0;" ;
                    } else {
                        script +=  "c = join a by $0 right, b by $0;" ;
                    }
                    try {
                        Util.registerMultiLineQuery(pigServer, script);
                    } catch (Exception e) {
                        PigException pe = LogUtils.getPigException(e);
                        assertEquals(1105, pe.getErrorCode());
                    }
                }
            }
            deleteInputFile(execType, firstInput);
            deleteInputFile(execType, secondInput);
        }
    }
    
    @Test
    public void testFullOuterJoin() throws Exception {
        for (ExecType execType : execTypes) {
            setUp(execType);
            String[] input1 = {
                    "hello\t1",
                    "bye\t2",
                    "\t3"
            };
            String[] input2 = {
                    "hello\tworld",
                    "good\tmorning",
                    "\tevening"
    
            };
            
            String firstInput = createInputFile(execType, "a.txt", input1);
            String secondInput = createInputFile(execType, "b.txt", input2);
            List<Tuple> expectedResults = Util.getTuplesFromConstantTupleStrings(
                    new String[] {
                            "(null,null,null,'evening')" ,
                            "(null,null,'good','morning')" ,
                            "('hello',1,'hello','world')" ,
                            "('bye',2,null,null)" ,
                            "(null,3,null,null)"
                                           });
            // with and without optional outer
            for(int i = 0; i < 2; i++) {
                // with schema
                String script = "a = load '"+ Util.encodeEscape(firstInput) +"' as (n:chararray, a:int); " +
                        "b = load '"+ Util.encodeEscape(secondInput) +"' as (n:chararray, m:chararray); ";
                if(i == 0) {
                    script +=  "c = join a by $0 full outer, b by $0;" ;
                } else {
                    script +=  "c = join a by $0 full, b by $0;" ;
                }
                script += "d = order c by $1, $3;";
                // ensure we parse correctly
                Util.buildLp(pigServer, script);
                
                // run query and test results only once
                if(i == 0) {
                    Util.registerMultiLineQuery(pigServer, script);
                    Iterator<Tuple> it = pigServer.openIterator("d");
                    int counter= 0;
                    while(it.hasNext()) {
                        assertEquals(expectedResults.get(counter++), it.next());
                    }
                    assertEquals(expectedResults.size(), counter);
                    
                    // without schema
                    script = "a = load '"+ Util.encodeEscape(firstInput) +"'; " +
                    "b = load '"+ Util.encodeEscape(secondInput) +"'; " ;
                    if(i == 0) {
                        script +=  "c = join a by $0 full outer, b by $0;" ;
                    } else {
                        script +=  "c = join a by $0 full, b by $0;" ;
                    }
                    try {
                        Util.registerMultiLineQuery(pigServer, script);
                    } catch (Exception e) {
                        PigException pe = LogUtils.getPigException(e);
                        assertEquals(1105, pe.getErrorCode());
                    }
                }
            }
            deleteInputFile(execType, firstInput);
            deleteInputFile(execType, secondInput);
        }
    }
    
    @Test
    public void testMultiOuterJoinFailure() throws ExecException {
    	setUp(ExecType.LOCAL);
        String[] types = new String[] { "left", "right", "full" };
        String query = "a = load 'a.txt' as (n:chararray, a:int);\n" +
        "b = load 'b.txt' as (n:chararray, m:chararray);\n"+
        "c = load 'c.txt' as (n:chararray, m:chararray);\n";
        for (int i = 0; i < types.length; i++) {
            boolean errCaught = false;
            try {
            	String q = query + 
            	           "d = join a by $0 " + types[i] + " outer, b by $0, c by $0;" +
            	           "store d into 'output';";
            	Util.buildLp(pigServer, q);
            } catch(Exception e) {
                errCaught = true;
                assertTrue(e.getMessage().contains("mismatched input ',' expecting SEMI_COLON"));
            }
            assertEquals(true, errCaught);
            
        }
        
    }
    
    @Test
    public void testNonRegularOuterJoinFailure() throws ExecException {
    	setUp(ExecType.LOCAL);
        String query = "a = load 'a.txt' as (n:chararray, a:int); "+
        "b = load 'b.txt' as (n:chararray, m:chararray); ";
        String[] types = new String[] { "left", "right", "full" };
        String[] joinTypes = new String[] { "replicated", "repl"};
        for (int i = 0; i < types.length; i++) {
            for(int j = 0; j < joinTypes.length; j++) {
                boolean errCaught = false;
                try {
                	String q = query + "d = join a by $0 " + 
                    types[i] + " outer, b by $0 using '" + joinTypes[j] +"';" +
                    "store d into 'output';";
                    Util.buildLp(pigServer, q);
                    
                } catch(Exception e) {
                    errCaught = true;
                     // This after adding support of LeftOuter Join to replicated Join
                        assertEquals(true, e.getMessage().contains("does not support (right|full) outer joins"));   
                }
                assertEquals( i == 0 ? false : true, errCaught);
            }
        }
    }
    
    @Test
    public void testJoinTupleFieldKey() throws Exception{
        for (ExecType execType : execTypes) {
            setUp(execType);
            String[] input1 = {
                    "(1,a)",
                    "(2,aa)"
            };
            String[] input2 = {
                    "(1,b)",
                    "(2,bb)"
            };
            
            String firstInput = createInputFile(execType, "a.txt", input1);
            String secondInput = createInputFile(execType, "b.txt", input2);
            
            String script = "a = load '"+ Util.encodeEscape(firstInput) +"' as (a:tuple(a1:int, a2:chararray));" +
                    "b = load '"+ Util.encodeEscape(secondInput) +"' as (b:tuple(b1:int, b2:chararray));" +
                    "c = join a by a.a1, b by b.b1;";
            Util.registerMultiLineQuery(pigServer, script);
            Iterator<Tuple> it = pigServer.openIterator("c");
            
            assertTrue(it.hasNext());
            Tuple t = it.next();
            assertTrue(t.toString().equals("((1,a),(1,b))"));
            
            assertTrue(it.hasNext());
            t = it.next();
            assertTrue(t.toString().equals("((2,aa),(2,bb))"));
            
            deleteInputFile(execType, firstInput);
            deleteInputFile(execType, secondInput);
        }
    }

    @Test
    public void testJoinNullTupleFieldKey() throws Exception{
        for (ExecType execType : execTypes) {
            setUp(execType);
            String[] input1 = {
                    "1\t",
                    "2\taa"
            };
            String[] input2 = {
                    "1\t",
                    "2\taa"
            };
            
            String firstInput = createInputFile(execType, "a.txt", input1);
            String secondInput = createInputFile(execType, "b.txt", input2);
            
            String script = "a = load '"+ Util.encodeEscape(firstInput) +"' as (a1:int, a2:chararray);" +
                    "b = load '"+ Util.encodeEscape(secondInput) +"' as (b1:int, b2:chararray);" +
                    "c = join a by (a1, a2), b by (b1, b2);";
            Util.registerMultiLineQuery(pigServer, script);
            Iterator<Tuple> it = pigServer.openIterator("c");
            
            assertTrue(it.hasNext());
            Tuple t = it.next();
            assertTrue(t.toString().equals("(2,aa,2,aa)"));
            
            assertFalse(it.hasNext());
            
            deleteInputFile(execType, firstInput);
            deleteInputFile(execType, secondInput);
        }
    }
    
    @Test
    public void testLiteralsForJoinAlgoSpecification1() throws Exception {
    	setUp(ExecType.LOCAL);
        String query = "a = load 'A'; " +
                       "b = load 'B'; " +
                       "c = Join a by $0, b by $0 using 'merge';" +
                       "store c into 'output';";
        LogicalPlan lp = Util.buildLp(pigServer, query);
        Operator store = lp.getSinks().get(0);
        LOJoin join = (LOJoin)lp.getPredecessors( store ).get(0);
        assertEquals(JOINTYPE.MERGE, join.getJoinType());
    }
    
    @Test
    public void testLiteralsForJoinAlgoSpecification2() throws Exception {
    	setUp(ExecType.LOCAL);
        String query = "a = load 'A'; " +
                       "b = load 'B'; " +
                       "c = Join a by $0, b by $0 using 'hash'; "+
                       "store c into 'output';";
        LogicalPlan lp = Util.buildLp(pigServer, query);
        Operator store = lp.getSinks().get(0);
        LOJoin join = (LOJoin) lp.getPredecessors( store ).get(0);
        assertEquals(JOINTYPE.HASH, join.getJoinType());
    }
    
    @Test
    public void testLiteralsForJoinAlgoSpecification5() throws Exception {
    	setUp(ExecType.LOCAL);
        String query = "a = load 'A'; " +
                       "b = load 'B'; " + 
                       "c = Join a by $0, b by $0 using 'default'; "+
                       "store c into 'output';";
        LogicalPlan lp = Util.buildLp(pigServer, query);
        Operator store = lp.getSinks().get(0);
        LOJoin join = (LOJoin) lp.getPredecessors( store ).get(0);
       assertEquals(JOINTYPE.HASH, join.getJoinType());
    }
    
    @Test
    public void testLiteralsForJoinAlgoSpecification3() throws Exception {
    	setUp(ExecType.LOCAL);
        String query = "a = load 'A'; " +
                       "b = load 'B'; " +
                       "c = Join a by $0, b by $0 using 'repl'; "+
                       "store c into 'output';";
        LogicalPlan lp = Util.buildLp(pigServer, query);
        Operator store = lp.getSinks().get(0);
        LOJoin join = (LOJoin) lp.getPredecessors( store ).get(0);
        assertEquals(JOINTYPE.REPLICATED, join.getJoinType());
    }
    
    @Test
    public void testLiteralsForJoinAlgoSpecification4() throws Exception {
    	setUp(ExecType.LOCAL);
        String query = "a = load 'A'; " + 
                       "b = load 'B'; " +
                       "c = Join a by $0, b by $0 using 'replicated'; "+
                       "store c into 'output';";
        LogicalPlan lp = Util.buildLp(pigServer, query);
        Operator store = lp.getSinks().get(0);
        LOJoin join = (LOJoin) lp.getPredecessors( store ).get(0);
       assertEquals(JOINTYPE.REPLICATED, join.getJoinType());
    }
}

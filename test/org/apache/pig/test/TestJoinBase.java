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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.pig.PigException;
import org.apache.pig.PigServer;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.LogUtils;
import org.junit.Ignore;
import org.junit.Test;

@Ignore
abstract public class TestJoinBase {
    protected PigServer pigServer;

    TupleFactory mTf = TupleFactory.getInstance();
    BagFactory mBf = BagFactory.getInstance();

    abstract protected String createInputFile(String fileNameHint, String[] data) throws IOException;

    abstract protected void deleteInputFile(String fileName) throws Exception;

    @Test
    public void testJoinUnkownSchema() throws Exception {
        // If any of the input schema is unknown, the resulting schema should be unknown as well
        String script = "a = load 'a.txt';" +
                "b = load 'b.txt'; " +
                "c = join a by $0, b by $0;";
        Util.registerMultiLineQuery(pigServer, script);
        Schema schema = pigServer.dumpSchema("c");
        assertNull(schema);
    }

    @Test
    public void testDefaultJoin() throws Exception {
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

        String firstInput = createInputFile("a.txt", input1);
        String secondInput = createInputFile("b.txt", input2);
        Tuple expectedResult = (Tuple)Util.getPigConstant("('hello',1,'hello','world')");

        // with schema
        String script = "a = load '"+ Util.encodeEscape(firstInput) +"' as (n:chararray, a:int); " +
                "b = load '"+ Util.encodeEscape(secondInput) +"' as (n:chararray, m:chararray); " +
                "c = join a by $0, b by $0;";
        Util.registerMultiLineQuery(pigServer, script);
        Iterator<Tuple> it = pigServer.openIterator("c");
        assertTrue(it.hasNext());
        assertEquals(expectedResult, it.next());
        assertFalse(it.hasNext());

        // without schema
        script = "a = load '"+ Util.encodeEscape(firstInput) + "'; " +
        "b = load '" + Util.encodeEscape(secondInput) + "'; " +
        "c = join a by $0, b by $0;";
        Util.registerMultiLineQuery(pigServer, script);
        it = pigServer.openIterator("c");
        assertTrue(it.hasNext());
        assertEquals(expectedResult.toString(), it.next().toString());
        assertFalse(it.hasNext());
        deleteInputFile(firstInput);
        deleteInputFile(secondInput);
    }


    @Test
    public void testJoinSchema() throws Exception {
        String[] input1 = {
                "1\t2",
                "2\t3",
                "3\t4"
        };
        String[] input2 = {
                "1\thello",
                "4\tbye",
        };

        String firstInput = createInputFile("a.txt", input1);
        String secondInput = createInputFile("b.txt", input2);
        Tuple expectedResult = (Tuple)Util.getPigConstant("(1,2,1,'hello',1,2,1,'hello')");

        // with schema
        String script = "a = load '"+ Util.encodeEscape(firstInput) +"' as (i:int, j:int); " +
                "b = load '"+ Util.encodeEscape(secondInput) +"' as (k:int, l:chararray); " +
                "c = join a by $0, b by $0;" +
                "d = foreach c generate i,j,k,l,a::i as ai,a::j as aj,b::k as bk,b::l as bl;";
        Util.registerMultiLineQuery(pigServer, script);
        Iterator<Tuple> it = pigServer.openIterator("d");
        assertTrue(it.hasNext());
        assertEquals(expectedResult, it.next());
        assertFalse(it.hasNext());

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
        assertTrue(exceptionThrown);

        // schema with duplicates with resolution
        script = "a = load '"+ Util.encodeEscape(firstInput) +"' as (i:int, j:int); " +
        "b = load '"+ Util.encodeEscape(secondInput) +"' as (i:int, l:chararray); " +
        "c = join a by $0, b by $0;" +
        "d = foreach c generate a::i as ai1,j,b::i as bi1,l,a::i as ai2,a::j as aj2,b::i as bi3,b::l as bl3;";
        Util.registerMultiLineQuery(pigServer, script);
        it = pigServer.openIterator("d");
        assertTrue(it.hasNext());
        assertEquals(expectedResult, it.next());
        assertFalse(it.hasNext());
        deleteInputFile(firstInput);
        deleteInputFile(secondInput);
    }

    @Test
    public void testLeftOuterJoin() throws Exception {
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

        String firstInput = createInputFile("a.txt", input1);
        String secondInput = createInputFile("b.txt", input2);
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
        deleteInputFile(firstInput);
        deleteInputFile(secondInput);
    }

    @Test
    public void testRightOuterJoin() throws Exception {
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

        String firstInput = createInputFile("a.txt", input1);
        String secondInput = createInputFile("b.txt", input2);
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
        deleteInputFile(firstInput);
        deleteInputFile(secondInput);
    }

    @Test
    public void testFullOuterJoin() throws Exception {
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

        String firstInput = createInputFile("a.txt", input1);
        String secondInput = createInputFile("b.txt", input2);
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
        deleteInputFile(firstInput);
        deleteInputFile(secondInput);
    }


    @Test
    public void testJoinTupleFieldKey() throws Exception{
        String[] input1 = {
                "(1,a)",
                "(2,aa)"
        };
        String[] input2 = {
                "(1,b)",
                "(2,bb)"
        };

        String firstInput = createInputFile("a.txt", input1);
        String secondInput = createInputFile("b.txt", input2);

        String script = "a = load '"+ Util.encodeEscape(firstInput) +"' as (a:tuple(a1:int, a2:chararray));" +
                "b = load '"+ Util.encodeEscape(secondInput) +"' as (b:tuple(b1:int, b2:chararray));" +
                "c = join a by a.a1, b by b.b1;";
        Util.registerMultiLineQuery(pigServer, script);
        Iterator<Tuple> it = pigServer.openIterator("c");

        List<Tuple> expectedResults = Util.getTuplesFromConstantTupleStrings(
                new String[] {
                        "((1,'a'),(1,'b'))",
                        "((2,'aa'),(2,'bb'))"
                        });
        Util.checkQueryOutputsAfterSort(it, expectedResults);

        deleteInputFile(firstInput);
        deleteInputFile(secondInput);
    }

    @Test
    public void testJoinNullTupleFieldKey() throws Exception{
        String[] input1 = {
                "1\t",
                "2\taa"
        };
        String[] input2 = {
                "1\t",
                "2\taa"
        };

        String firstInput = createInputFile("a.txt", input1);
        String secondInput = createInputFile("b.txt", input2);

        String script = "a = load '"+ Util.encodeEscape(firstInput) +"' as (a1:int, a2:chararray);" +
                "b = load '"+ Util.encodeEscape(secondInput) +"' as (b1:int, b2:chararray);" +
                "c = join a by (a1, a2), b by (b1, b2);";
        Util.registerMultiLineQuery(pigServer, script);
        Iterator<Tuple> it = pigServer.openIterator("c");

        List<Tuple> expectedResults = Util
                .getTuplesFromConstantTupleStrings(new String[] { "(2,'aa',2,'aa')" });
        Util.checkQueryOutputs(it, expectedResults);

        deleteInputFile(firstInput);
        deleteInputFile(secondInput);
    }

}

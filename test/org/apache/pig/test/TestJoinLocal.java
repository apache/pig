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
import static org.apache.pig.builtin.mock.Storage.resetData;
import static org.apache.pig.builtin.mock.Storage.tuple;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.pig.PigServer;
import org.apache.pig.builtin.mock.Storage.Data;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.util.Utils;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.logical.relational.LOJoin;
import org.apache.pig.newplan.logical.relational.LOJoin.JOINTYPE;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.junit.Before;
import org.junit.Test;
import org.python.google.common.collect.Sets;

public class TestJoinLocal extends TestJoinBase {

    @Before
    public void setUp() throws Exception {
        pigServer = new PigServer(Util.getLocalTestMode());
    }
    @Override
    protected String createInputFile(String fileNameHint, String[] data)
            throws IOException {
        String fileName = "";
        File f = Util.createInputFile("test", fileNameHint, data);
        fileName = Util.generateURI(f.getAbsolutePath(), pigServer.getPigContext());
        return fileName;
    }

    @Override
    protected void deleteInputFile(String fileName) throws Exception {
        fileName = fileName.replace("file://", "");
        new File(fileName).delete();
    }

    @Test
    public void testJoinSchema2() throws Exception {
        // test join where one load does not have schema
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
        assertTrue(it.hasNext());
        Tuple res = it.next();
        assertEquals(expectedResult, res);
        assertFalse(it.hasNext());
        deleteInputFile(firstInput);
        deleteInputFile(secondInput);

    }

    @Test
    public void testMultiOuterJoinFailure() throws Exception {
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
            assertTrue(errCaught);

        }

    }

    @Test
    public void testNonRegularOuterJoinFailure() throws Exception {
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
                        assertTrue(e.getMessage().contains("does not support (right|full) outer joins"));
                }
                assertEquals( i == 0 ? false : true, errCaught);
            }
        }
    }

    @Test
    public void testLiteralsForJoinAlgoSpecification1() throws Exception {
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
        String query = "a = load 'A'; " +
                       "b = load 'B'; " +
                       "c = Join a by $0, b by $0 using 'replicated'; "+
                       "store c into 'output';";
        LogicalPlan lp = Util.buildLp(pigServer, query);
        Operator store = lp.getSinks().get(0);
        LOJoin join = (LOJoin) lp.getPredecessors( store ).get(0);
        assertEquals(JOINTYPE.REPLICATED, join.getJoinType());
    }

    // See: https://issues.apache.org/jira/browse/PIG-3093
    @Test
    public void testIndirectSelfJoinRealias() throws Exception {
        Data data = resetData(pigServer);

        Set<Tuple> tuples = Sets.newHashSet(tuple("a"), tuple("b"), tuple("c"));
        data.set("foo", Utils.getSchemaFromString("field1:chararray"), tuples);
        pigServer.registerQuery("A = load 'foo' using mock.Storage();");
        pigServer.registerQuery("B = foreach A generate *;");
        pigServer.registerQuery("C = join A by field1, B by field1;");
        assertEquals(Utils.getSchemaFromString("A::field1:chararray, B::field1:chararray"), pigServer.dumpSchema("C"));
        pigServer.registerQuery("D = foreach C generate B::field1, A::field1 as field2;");
        assertEquals(Utils.getSchemaFromString("B::field1:chararray, field2:chararray"), pigServer.dumpSchema("D"));
        pigServer.registerQuery("E = foreach D generate field1, field2;");
        assertEquals(Utils.getSchemaFromString("B::field1:chararray, field2:chararray"), pigServer.dumpSchema("E"));
        pigServer.registerQuery("F = foreach E generate field2;");
        pigServer.registerQuery("store F into 'foo_out' using mock.Storage();");
        List<Tuple> out = data.get("foo_out");
        assertEquals("Expected size was "+tuples.size()+" but was "+out.size(), tuples.size(), out.size());
        for (Tuple t : out) {
            assertTrue("Should have found tuple "+t+" in expected: "+tuples, tuples.remove(t));
        }
        assertTrue("All expected tuples should have been found, remaining: "+tuples, tuples.isEmpty());
    }

    @Test
    public void testIndirectSelfJoinData() throws Exception {
        Data data = resetData(pigServer);

        Set<Tuple> tuples = Sets.newHashSet(tuple("a", 1), tuple("b", 2), tuple("c", 3));
        data.set("foo", Utils.getSchemaFromString("field1:chararray,field2:int"), tuples);
        pigServer.registerQuery("A = load 'foo' using mock.Storage();");
        pigServer.registerQuery("B = foreach A generate field1, field2*2 as field2;");
        pigServer.registerQuery("C = join A by field1, B by field1;");
        pigServer.registerQuery("D = foreach C generate A::field1 as field1_a, B::field1 as field1_b, A::field2 as field2_a, B::field2 as field2_b;");
        pigServer.registerQuery("store D into 'foo_out' using mock.Storage();");

        Set<Tuple> expected = Sets.newHashSet(tuple("a", "a", 1, 2), tuple("b", "b", 2, 4), tuple("c", "c", 3, 6));
        List<Tuple> out = data.get("foo_out");
        assertEquals("Expected size was "+expected.size()+" but was "+out.size(), expected.size(), out.size());
        for (Tuple t : out) {
            assertTrue("Should have found tuple "+t+" in expected: "+expected, expected.remove(t));
        }
        assertTrue("All expected tuples should have been found, remaining: "+expected, expected.isEmpty());
    }
}

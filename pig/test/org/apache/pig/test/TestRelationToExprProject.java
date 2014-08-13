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

import static org.apache.pig.builtin.mock.Storage.resetData;
import static org.apache.pig.builtin.mock.Storage.tuple;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.builtin.mock.Storage.Data;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DefaultTuple;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.parser.ParserException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test PORelationToExprProject which is a special project
 * introduced to handle the following case:
 * This project is Project(*) introduced after a relational operator
 * to supply a bag as output (as an expression). This project is either
 * providing the bag as input to a successor expression operator or is
 * itself the leaf in a inner plan
 * If the predecessor relational operator sends an EOP
 * then send an empty bag first to signal "empty" output
 * and then send an EOP

 * NOTE: A Project(*) of return type BAG whose predecessor is
 * from an outside plan (i.e. not in the same inner plan as the project)
 * will NOT lead us here. So a query like:
 * a = load 'baginp.txt' as (b:bag{t:tuple()}); b = foreach a generate $0; dump b;
 * will go through a regular project (without the following flag)
 */
public class TestRelationToExprProject {
    private PigServer pigServer;
    private static final String TEST_FILTER_COUNT3_INPUT="test/org/apache/pig/test/data/TestRelationToExprProjectInput.txt";

    /* (non-Javadoc)
     * @see junit.framework.TestCase#setUp()
     */
    @Before
    public void setUp() throws Exception {
        pigServer = new PigServer(ExecType.LOCAL);
    }

    /* (non-Javadoc)
     * @see junit.framework.TestCase#tearDown()
     */
    @After
    public void tearDown() throws Exception {
        pigServer.shutdown();
    }

    // based on the script provided in the jira issue:PIG-514
    // tests that when a filter inside a foreach filters away all tuples
    // for a group, an empty bag is still provided to udfs whose
    // input is the filter
    @Test
    public void testFilterCount1() throws IOException, ParserException {
        Data data = resetData(pigServer);
        data.set("foo", tuple(1,1,3), tuple(1,2,3), tuple(2,1,3), tuple(2,1,3));
        String script = "test   = load 'foo' using mock.Storage() as (col1: int, col2: int, col3: int);" +
                "test2 = group test by col1;" +
                "test3 = foreach test2 {" +
                "        filter_one    = filter test by (col2==1);" +
                "        filter_notone = filter test by (col2!=1);" +
                "        generate group as col1, COUNT(filter_one) as cnt_one, COUNT(filter_notone) as cnt_notone;};";
        pigServer.registerQuery(script);
        Iterator<Tuple> it = pigServer.openIterator("test3");
        Tuple[] expected = new DefaultTuple[2];
        expected[0] = (Tuple) Util.getPigConstant("(1,1L,1L)");
        expected[1] = (Tuple) Util.getPigConstant("(2,2L,0L)");
        Object[] results = new Object[2];
        int i = 0;
        while(it.hasNext()) {
            if(i == 2) {
                fail("Got more tuples than expected!");
            }
            Tuple t = it.next();
            if(t.get(0).equals(1)) {
                // this is the first tuple
                results[0] = t;
            } else {
                results[1] = t;
            }
            i++;
        }
        for (int j = 0; j < expected.length; j++) {
            assertTrue(expected[j].equals(results[j]));
        }
    }

    // based on jira PIG-710
    // tests that when a filter inside a foreach filters away all tuples
    // for a group, an empty bag is still provided to udfs whose
    // input is the filter
    @Test
    public void testFilterCount2() throws IOException, ParserException {
        Data data = resetData(pigServer);
        data.set("foo",
                tuple("a", "hello"),
                tuple("a", "goodbye"),
                tuple("b", "goodbye"),
                tuple("c", "hello"),
                tuple("c", "hello"),
                tuple("c", "hello"),
                tuple("d", "what")
                );
        String query = "A = load 'foo' using mock.Storage() as ( id:chararray, str:chararray );" +
                "B = group A by ( id );" +
                "Cfiltered = foreach B {" +
                "        D = filter A by (" +
                "                str matches 'hello'" +
                "                );" +
                "        matchedcount = COUNT(D);" +
                "        generate" +
                "                group," +
                "                matchedcount as matchedcount," +
                "                A.str;" +
                "        };";
        pigServer.registerQuery(query);
        Iterator<Tuple> it = pigServer.openIterator("Cfiltered");
        Map<String, Tuple> expected = new HashMap<String, Tuple>();
        expected.put("a", (Tuple) Util.getPigConstant("('a',1L,{('hello'),('goodbye')})"));
        expected.put("b", (Tuple) Util.getPigConstant("('b',0L,{('goodbye')})"));
        expected.put("c", (Tuple) Util.getPigConstant("('c',3L,{('hello'),('hello'),('hello')})"));
        expected.put("d", (Tuple) Util.getPigConstant("('d',0L,{('what')})"));
        int i = 0;
        while(it.hasNext()) {
            Tuple actual = it.next();
            assertEquals(expected.get(actual.get(0)), actual);
            i++;
        }
        assertEquals(4, i);
    }

    // based on jira PIG-739
    // tests that when a filter inside a foreach filters away all tuples
    // for a group, an empty bag is still provided to udfs whose
    // input is the filter
    @Test
    public void testFilterCount3() throws IOException, ParserException {
        String query = "TESTDATA =  load '"+TEST_FILTER_COUNT3_INPUT+"' using PigStorage() as (timestamp:chararray, testid:chararray, userid: chararray, sessionid:chararray, value:long, flag:int);" +
                "TESTDATA_FILTERED = filter TESTDATA by (timestamp gte '1230800400000' and timestamp lt '1230804000000' and value != 0);" +
                "TESTDATA_GROUP = group TESTDATA_FILTERED by testid;" +
                "TESTDATA_AGG = foreach TESTDATA_GROUP {" +
                "                        A = filter TESTDATA_FILTERED by (userid eq sessionid);" +
                "                        C = distinct A.userid;" +
                "                        generate group as testid, COUNT(TESTDATA_FILTERED) as counttestdata, COUNT(C) as distcount, SUM(TESTDATA_FILTERED.flag) as total_flags;" +
                "                }" +
                "TESTDATA_AGG_1 = group TESTDATA_AGG ALL;" +
                "TESTDATA_AGG_2 = foreach TESTDATA_AGG_1 generate COUNT(TESTDATA_AGG);" ;
        pigServer.registerQuery(query);
        Iterator<Tuple> it = pigServer.openIterator("TESTDATA_AGG_2");

        int i = 0;
        while(it.hasNext()) {
            Tuple actual = it.next();
            assertEquals(20l, actual.get(0));
            i++;
        }
        assertEquals(1, i);
    }

    // test case where RelationToExprProject is present in the
    // single inner plan of foreach - this will test that it does
    // send an EOP eventually for each input of the foreach
    @Test
    public void testFilter1() throws IOException, ParserException {
        Data data = resetData(pigServer);
        data.set("foo", tuple(1,1,3), tuple(1,2,3), tuple(2,1,3), tuple(2,1,3), tuple(3,4,4));

        String script = "test   = load 'foo' using mock.Storage() as (col1: int, col2: int, col3: int);" +
                "test2 = group test by col1;" +
                "test3 = foreach test2 {" +
                "        filter_one    = filter test by (col2==1);" +
                "        generate filter_one;};";
        pigServer.registerQuery(script);
        Iterator<Tuple> it = pigServer.openIterator("test3");
        Map<Tuple, Integer> expected = new HashMap<Tuple, Integer>();
        expected.put((Tuple) Util.getPigConstant("({(1,1,3)})"), 0);
        expected.put((Tuple) Util.getPigConstant("({(2,1,3),(2,1,3)})"), 0);
        Tuple t = TupleFactory.getInstance().newTuple();
        t.append(BagFactory.getInstance().newDefaultBag());
        expected.put(t, 0);
        int i = 0;
        while(it.hasNext()) {
            if(i == 3) {
                fail("Got more tuples than expected!");
            }
            t = it.next();
            assertTrue(expected.containsKey(t));
            int occurences = expected.get(t);
            occurences++;
            expected.put(t, occurences);
            i++;
        }
        for (Integer occurences : expected.values()) {
            assertEquals(new Integer(1), occurences);
        }
    }

    // test case where RelationToExprProject is present in a
    // different inner plan along with another plan to project the group
    // in foreach - this will test that reset() correctly resets
    // the state that empty bags need to be sent on EOP if no non-EOP
    // input has been seen on a fresh input from foreach.
    @Test
    public void testFilter2() throws IOException, ParserException {
        Data data = resetData(pigServer);
        data.set("foo", tuple(1,1,3), tuple(1,2,3), tuple(2,1,3), tuple(2,1,3), tuple(3,4,4));

        String script = "test   = load 'foo' using mock.Storage() as (col1: int, col2: int, col3: int);" +
                "test2 = group test by col1;" +
                "test3 = foreach test2 {" +
                "        filter_one    = filter test by (col2==1);" +
                "        generate group, filter_one;};";
        Util.registerMultiLineQuery(pigServer, script);
        Iterator<Tuple> it = pigServer.openIterator("test3");
        Map<Tuple, Integer> expected = new HashMap<Tuple, Integer>();
        expected.put((Tuple) Util.getPigConstant("(1,{(1,1,3)})"), 0);
        expected.put((Tuple) Util.getPigConstant("(2,{(2,1,3),(2,1,3)})"), 0);
        Tuple t = TupleFactory.getInstance().newTuple();
        t.append(new Integer(3));
        t.append(BagFactory.getInstance().newDefaultBag());
        expected.put(t, 0);
        int i = 0;
        while(it.hasNext()) {
            if(i == 3) {
                fail("Got more tuples than expected!");
            }
            t = it.next();
            assertTrue(expected.containsKey(t));
            int occurences = expected.get(t);
            occurences++;
            expected.put(t, occurences);
            i++;
        }
        for (Integer occurences : expected.values()) {
            assertEquals(Integer.valueOf(1), occurences);
        }
    }
}

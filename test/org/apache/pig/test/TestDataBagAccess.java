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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.builtin.BinStorage;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DefaultTuple;
import org.apache.pig.data.NonSpillableDataBag;
import org.apache.pig.data.SingleTupleBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.util.MultiMap;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import junit.framework.TestCase;

/**
 *
 */
@RunWith(JUnit4.class)
public class TestDataBagAccess extends TestCase {
    private PigServer pigServer;

    @Before
    @Override
    public void setUp() throws Exception{
        pigServer = new PigServer(ExecType.LOCAL, new Properties());
    }

    @Test
    public void testSingleTupleBagAcess() throws Exception {
        Tuple inputTuple = new DefaultTuple();
        inputTuple.append("a");
        inputTuple.append("b");

        SingleTupleBag bg = new SingleTupleBag(inputTuple);
        Iterator<Tuple> it = bg.iterator();
        assertEquals(inputTuple, it.next());
        assertFalse(it.hasNext());
    }

    @Test
    public void testNonSpillableDataBag() throws Exception {
        String[][] tupleContents = new String[][] {{"a", "b"},{"c", "d" }, { "e", "f"} };
        NonSpillableDataBag bg = new NonSpillableDataBag();
        for (int i = 0; i < tupleContents.length; i++) {
            bg.add(Util.createTuple(tupleContents[i]));
        }
        Iterator<Tuple> it = bg.iterator();
        int j = 0;
        while(it.hasNext()) {
            Tuple t = it.next();
            assertEquals(Util.createTuple(tupleContents[j]), t);
            j++;
        }
        assertEquals(tupleContents.length, j);
    }

    @Test
    public void testBagConstantAccess() throws IOException, ExecException {
        File input = Util.createInputFile("tmp", "",
                new String[] {"sampledata\tnot_used"});
        pigServer.registerQuery("a = load '"
                + Util.generateURI(Util.encodeEscape(input.toString()), pigServer.getPigContext()) + "';");
        pigServer.registerQuery("b = foreach a generate {(16, 4.0e-2, 'hello', -101)} as mybag:{t:(i: int, d: double, c: chararray, e : int)};");
        pigServer.registerQuery("c = foreach b generate mybag.i, mybag.d, mybag.c, mybag.e;");
        Iterator<Tuple> it = pigServer.openIterator("c");
        Tuple t = it.next();
        Object[] results = new Object[] { new Integer(16), new Double(4.0e-2), "hello", new Integer( -101 ) };
        Class[] resultClasses = new Class[] { Integer.class, Double.class, String.class, Integer.class };
        assertEquals(results.length, t.size());
        for (int i = 0; i < results.length; i++) {
            DataBag bag = (DataBag)t.get(i);
            assertEquals(results[i], bag.iterator().next().get(0));
            assertEquals(resultClasses[i], bag.iterator().next().get(0).getClass());
        }
    }

    @Test
    public void testBagConstantAccessFailure() throws IOException, ExecException {
        File input = Util.createInputFile("tmp", "",
                new String[] {"sampledata\tnot_used"});
        boolean exceptionOccured = false;
        pigServer.setValidateEachStatement(true);
        try {
            pigServer.registerQuery("a = load '"
                    + Util.generateURI(Util.encodeEscape(input.toString()), pigServer.getPigContext()) + "';");
            pigServer.registerQuery("b = foreach a generate {(16, 4.0e-2, 'hello')} as mybag:{t:(i: int, d: double, c: chararray)};");
            pigServer.registerQuery("c = foreach b generate mybag.t;");
            pigServer.explain("c", System.out);
        } catch(FrontendException e) {
            exceptionOccured = true;
            String msg = e.getMessage();
            Util.checkStrContainsSubStr(msg, "Cannot find field t in i:int,d:double,c:chararray");
        }
        assertTrue(exceptionOccured);
    }

    @Test
    public void testBagConstantFlatten1() throws IOException, ExecException {
        File input = Util.createInputFile("tmp", "",
                new String[] {"sampledata\tnot_used"});
        pigServer.registerQuery("A = load '"
                + Util.generateURI(Util.encodeEscape(input.toString()), pigServer.getPigContext()) + "';");
        pigServer.registerQuery("B = foreach A generate {(('p1-t1-e1', 'p1-t1-e2'),('p1-t2-e1', 'p1-t2-e2'))," +
                "(('p2-t1-e1', 'p2-t1-e2'), ('p2-t2-e1', 'p2-t2-e2'))};");
        pigServer.registerQuery("C = foreach B generate $0 as pairbag : { pair: ( t1: (e1, e2), t2: (e1, e2) ) };");
        pigServer.registerQuery("D = foreach C generate FLATTEN(pairbag);");
        pigServer.registerQuery("E = foreach D generate t1.e2 as t1e2, t2.e1 as t2e1;");
        Iterator<Tuple> it = pigServer.openIterator("E");
        // We should get the following two tuples as the result:
        // (p1-t1-e2,p1-t2-e1)
        // (p2-t1-e2,p2-t2-e1)
        Tuple t = it.next();
        assertEquals("p1-t1-e2", (String)t.get(0));
        assertEquals("p1-t2-e1", (String)t.get(1));
        t = it.next();
        assertEquals("p2-t1-e2", (String)t.get(0));
        assertEquals("p2-t2-e1", (String)t.get(1));
        assertFalse(it.hasNext());
    }

    @Test
    public void testBagConstantFlatten2() throws IOException, ExecException {
        File input = Util.createInputFile("tmp", "",
                new String[] {"somestring\t10\t{(a,10),(b,20)}"});
        pigServer.registerQuery("a = load '"
                + Util.generateURI(Util.encodeEscape(input.toString()), pigServer.getPigContext())
                + "' " + "as (str:chararray, intval:int, bg:bag{t:tuple(s:chararray, i:int)});");
        pigServer.registerQuery("b = foreach a generate str, intval, flatten(bg);");
        pigServer.registerQuery("c = foreach b generate str, intval, s, i;");
        Iterator<Tuple> it = pigServer.openIterator("c");
        int i = 0;
        Object[][] results = new Object[][] { {"somestring", new Integer(10), "a", new Integer(10)},
                {"somestring", new Integer(10), "b", new Integer(20) }};
        Class[] resultClasses = new Class[] { String.class, Integer.class, String.class, Integer.class };
        while(it.hasNext()) {
            Tuple t = it.next();
            for (int j = 0; j < resultClasses.length; j++) {
                assertEquals(results[i][j], t.get(j));
                assertEquals(resultClasses[j], t.get(j).getClass());
            }
            i++;
        }
        assertEquals(results.length, i);

        pigServer.registerQuery("c = foreach b generate str, intval, bg::s, bg::i;");
        it = pigServer.openIterator("c");
        i = 0;
        while(it.hasNext()) {
            Tuple t = it.next();
            for (int j = 0; j < resultClasses.length; j++) {
                assertEquals(results[i][j], t.get(j));
                assertEquals(resultClasses[j], t.get(j).getClass());
            }
            i++;
        }
        assertEquals(results.length, i);
    }

    @Test
    public void testBagStoreLoad() throws IOException, ExecException {
        File input = Util.createInputFile("tmp", "",
                new String[] {"a\tid1", "a\tid2", "a\tid3", "b\tid4", "b\tid5", "b\tid6"});
        pigServer.registerQuery("a = load '"
                + Util.generateURI(Util.encodeEscape(input.toString()), pigServer.getPigContext())
                + "' " + "as (s:chararray, id:chararray);");
        pigServer.registerQuery("b = group a by s;");
        Class[] loadStoreClasses = new Class[] { BinStorage.class, PigStorage.class };
        for (int i = 0; i < loadStoreClasses.length; i++) {
            String output = "TestDataBagAccess-testBagStoreLoad-" +
                             loadStoreClasses[i].getName() + ".txt";
            pigServer.deleteFile(output);
            pigServer.store("b", output, loadStoreClasses[i].getName());
            pigServer.registerQuery("c = load '" + output + "' using " + loadStoreClasses[i].getName() + "() AS " +
                    "(gp: chararray, bg:bag { t: tuple (sReLoaded: chararray, idReLoaded: chararray)});;");
            Iterator<Tuple> it = pigServer.openIterator("c");
            MultiMap<Object, Object> results = new MultiMap<Object, Object>();
            results.put("a", "id1");
            results.put("a", "id2");
            results.put("a", "id3");
            results.put("b", "id4");
            results.put("b", "id5");
            results.put("b", "id6");
            int j = 0;
            while(it.hasNext()) {
                Tuple t = it.next();
                Object groupKey = t.get(0);
                DataBag groupBag = (DataBag)t.get(1);
                Iterator<Tuple> bgIt = groupBag.iterator();
                int k = 0;
                while(bgIt.hasNext()) {
                    // a hash to make sure we don't see the
                    // same "ids" twice
                    HashMap<Object, Boolean> seen = new HashMap<Object, Boolean>();
                    Tuple bgt = bgIt.next();
                    // the first col is the group by key
                    assertTrue(bgt.get(0).equals(groupKey));
                    Collection<Object> values = results.get(groupKey);
                    // check that the second column is one
                    // of the "id" values associated with this
                    // group by key
                    assertTrue(values.contains(bgt.get(1)));
                    // check that we have not seen the same "id" value
                    // before
                    if(seen.containsKey(bgt.get(1)))
                        fail("LoadStoreClass used : " + loadStoreClasses[i].getName() + " " +
                        		", duplicate value (" + bgt.get(1) + ")");
                    else
                        seen.put(bgt.get(1), true);
                    k++;
                }
                // check that we saw 3 tuples in each group bag
                assertEquals(3, k);
                j++;
            }
            // make sure we saw the right number of high
            // level tuples
            assertEquals(results.keySet().size(), j);

            pigServer.registerQuery("d = foreach c generate gp, flatten(bg);");
            // results should be
            // a a id1
            // a a id2
            // a a id3
            // b b id4
            // b b id5
            // b b id6
            // However order is not guaranteed
            List<Tuple> resultTuples = new ArrayList<Tuple>();
            resultTuples.add(Util.createTuple(new String[] { "a", "a", "id1"}));
            resultTuples.add(Util.createTuple(new String[] { "a", "a", "id2"}));
            resultTuples.add(Util.createTuple(new String[] { "a", "a", "id3"}));
            resultTuples.add(Util.createTuple(new String[] { "b", "b", "id4"}));
            resultTuples.add(Util.createTuple(new String[] { "b", "b", "id5"}));
            resultTuples.add(Util.createTuple(new String[] { "b", "b", "id6"}));
            it = pigServer.openIterator("d");
            j = 0;
            HashMap<Tuple, Boolean> seen = new HashMap<Tuple, Boolean>();
            while(it.hasNext()) {
                Tuple t = it.next();
                assertTrue(resultTuples.contains(t));
                if(seen.containsKey(t)) {
                    fail("LoadStoreClass used : " + loadStoreClasses[i].getName() + " " +
                                ", duplicate tuple (" + t + ") encountered.");
                } else {
                    seen.put(t, true);
                }
                j++;
            }
            // check we got expected number of tuples
            assertEquals(resultTuples.size(), j);

            // same output as above - but projection based on aliases
            pigServer.registerQuery("e = foreach d generate gp, sReLoaded, idReLoaded;");
            it = pigServer.openIterator("e");
            j = 0;
            seen = new HashMap<Tuple, Boolean>();
            while(it.hasNext()) {
                Tuple t = it.next();
                assertTrue(resultTuples.contains(t));
                if(seen.containsKey(t)) {
                    fail("LoadStoreClass used : " + loadStoreClasses[i].getName() + " " +
                                ", duplicate tuple (" + t + ") encountered.");
                } else {
                    seen.put(t, true);
                }
                j++;
            }
            // check we got expected number of tuples
            assertEquals(resultTuples.size(), j);

            // same result as above but projection based on position specifiers
            pigServer.registerQuery("f = foreach d generate $0, $1, $2;");
            it = pigServer.openIterator("f");
            j = 0;
            seen = new HashMap<Tuple, Boolean>();
            while(it.hasNext()) {
                Tuple t = it.next();
                assertTrue(resultTuples.contains(t));
                if(seen.containsKey(t)) {
                    fail("LoadStoreClass used : " + loadStoreClasses[i].getName() + " " +
                                ", duplicate tuple (" + t + ") encountered.");
                } else {
                    seen.put(t, true);
                }
                j++;
            }
            // check we got expected number of tuples
            assertEquals(resultTuples.size(), j);


        }
    }
}

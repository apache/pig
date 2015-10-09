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
package org.apache.pig.piggybank.test.evaluation.string;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.pig.EvalFunc;
import org.apache.pig.PigServer;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.piggybank.evaluation.string.REPLACE_MULTI;
import org.apache.pig.test.Util;
import org.junit.Before;
import org.junit.Test;

public class TestReplaceMulti {

    private static PigServer pigServer;

    @Before
    public void setUp() throws Exception {
        Properties properties = new Properties();
        properties.put("udf.import.list", "org.apache.pig.builtin:org.apache.pig.piggybank.evaluation.string");
        pigServer = new PigServer(Util.getLocalTestMode(), properties);
    }

    @Test
    public void testNullSourceStringToReplaceMultiUDF() throws Exception {
        Tuple input;
        String output;

        String inputStr = null;

        List<Object> list = new LinkedList<Object>();
        EvalFunc<String> strFunc = new REPLACE_MULTI();
        list.add(inputStr);
        Map<String, String> map = new HashMap<String, String>();
        list.add(map);
        input = TupleFactory.getInstance().newTuple(list);
        output = strFunc.exec(input);
        assertNull(output);

    }

    @Test
    public void testSearchReplacementMapNullToReplaceMultiUDF()
            throws Exception {
        Tuple input;
        String output;

        String inputStr = "Hello World!";
        String expected = "Hello World!";

        List<Object> list = new LinkedList<Object>();
        EvalFunc<String> strFunc = new REPLACE_MULTI();
        list.add(inputStr);
        Map<String, String> map = null;
        list.add(map);
        input = TupleFactory.getInstance().newTuple(list);
        output = strFunc.exec(input);
        assertTrue(output.equals(expected));

    }

    @Test
    public void testEmptyMapToReplaceMultiUDF() throws Exception {
        Tuple input;
        String output;

        String inputStr = "Hello World!";
        String expected = "Hello World!";

        List<Object> list = new LinkedList<Object>();
        EvalFunc<String> strFunc = new REPLACE_MULTI();
        list.add(inputStr);
        Map<String, String> map = new HashMap<String, String>();
        list.add(map);
        input = TupleFactory.getInstance().newTuple(list);
        output = strFunc.exec(input);
        assertTrue(output.equals(expected));

    }

    @Test
    public void testReplaceMultiUDF() throws Exception {
        Tuple input;
        String output;

        String inputStr = "Hello World!";
        String expected = "Halloo_Woorld";

        List<Object> list = new LinkedList<Object>();
        EvalFunc<String> strFunc = new REPLACE_MULTI();
        list.add(inputStr);
        Map<String, String> map = new HashMap<String, String>();
        map.put(" ", "_");
        map.put("e", "a");
        map.put("!", "");
        map.put("o", "oo");
        list.add(map);
        input = TupleFactory.getInstance().newTuple(list);
        output = strFunc.exec(input);
        assertTrue(output.equals(expected));

    }

    @Test
    public void testPigScriptForReplaceMultiUDFNullParameters()
            throws Exception {
        String inputStr = "Hello World!";

        File inputFile = Util.createInputFile("tmp", "testReplaceMultiIn.txt",
                new String[] { inputStr });

        // test typed data
        pigServer.registerQuery("A = LOAD '"
                + Util.encodeEscape(inputFile.getAbsolutePath())
                + "' AS (name: chararray);");
        pigServer
                .registerQuery("B = FOREACH A GENERATE org.apache.pig.piggybank.evaluation.string.REPLACE_MULTI(NULL, NULL);");

        Iterator<Tuple> it = pigServer.openIterator("B");
        assertTrue(it.hasNext());
        Tuple t = it.next();
        assertNull(t.get(0));
    }

    @Test
    public void testPigScriptForReplaceMultiUDFNullMap() throws Exception {
        String inputStr = "Hello World!";

        File inputFile = Util.createInputFile("tmp", "testReplaceMultiIn.txt",
                new String[] { inputStr });

        // test typed data
        pigServer.registerQuery("A = LOAD '"
                + Util.encodeEscape(inputFile.getAbsolutePath())
                + "' AS (name: chararray);");
        pigServer
                .registerQuery("B = FOREACH A GENERATE REPLACE_MULTI(name, NULL);");

        Iterator<Tuple> it = pigServer.openIterator("B");
        assertTrue(it.hasNext());
        Tuple t = it.next();
        assertEquals(inputStr, t.get(0));
    }

    @Test
    public void testPigScriptForReplaceMultiUDF() throws Exception {
        String inputStr = "Hello World!";

        File inputFile = Util.createInputFile("tmp", "testReplaceMultiIn.txt",
                new String[] { inputStr });

        // test typed data
        pigServer.registerQuery("A = LOAD '"
                + Util.encodeEscape(inputFile.getAbsolutePath())
                + "' AS (name: chararray);");
        pigServer
                .registerQuery("B = FOREACH A GENERATE REPLACE_MULTI(name, [' '#'_','e'#'a','o'#'oo','!'#'']);");

        Iterator<Tuple> it = pigServer.openIterator("B");
        assertTrue(it.hasNext());
        Tuple t = it.next();
        assertEquals("Halloo_Woorld", t.get(0));
    }

}

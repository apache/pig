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
package org.apache.pig.piggybank.test.evaluation;

import java.io.File;
import java.io.PrintWriter;
import java.util.Iterator;
import java.util.Properties;
import java.util.LinkedList;
import java.util.List;

import junit.framework.TestCase;

import org.junit.Test;

import org.apache.pig.data.Tuple;
import org.apache.pig.data.DefaultTupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import org.apache.pig.piggybank.evaluation.string.*;

// This class tests all string eval functions.

public class TestEvalString extends TestCase {
	
    @Test
    public void testUPPER() throws Exception {
        UPPER func = new UPPER();

        // test excution
        String in = "Hello World!";
        String expected = "HELLO WORLD!";

        Tuple input = DefaultTupleFactory.getInstance().newTuple(in);

        String output = func.exec(input);
        assertTrue(output.equals(expected));
        
        String in2 = null;
        Tuple input2 = DefaultTupleFactory.getInstance().newTuple(in2);
        assertEquals("upper null value",null, func.exec(input2));
        
        // test schema creation

        // FIXME
        //Schema outSchema = func.outputSchema(tupleSchema);
        //assertTrue(outSchema.toString().equals("upper_" + fieldName));

    }

    @Test
    public void testLOWER() throws Exception {
        LOWER func = new LOWER();

        // test excution
        String in = "Hello World!";
        String expected = "hello world!";

        Tuple input = DefaultTupleFactory.getInstance().newTuple(in);

        String output = func.exec(input);
        assertTrue(output.equals(expected));
    }

    @Test
    public void testINDEXOF() throws Exception {
        INDEXOF func = new INDEXOF();

        // test excution
        List l = new LinkedList();
        l.add("Hello World!");
        l.add("o");

        Tuple input = DefaultTupleFactory.getInstance().newTuple(l);

        Integer output = func.exec(input);
        assertTrue(output.intValue()==4);
    }

    @Test
    public void testLASTINDEXOF() throws Exception {
        LASTINDEXOF func = new LASTINDEXOF();

        // test excution
        List l = new LinkedList();
        l.add("Hello World!");
        l.add("o");

        Tuple input = DefaultTupleFactory.getInstance().newTuple(l);

        Integer output = func.exec(input);
        assertTrue(output.intValue()==7);
    }

    @Test
    public void testREPLACE() throws Exception {
        REPLACE func = new REPLACE();

        // test excution
        List l = new LinkedList();
        l.add("Hello World!");
        l.add("o");
        l.add("a");
        String expected = "Hella Warld!";

        Tuple input = DefaultTupleFactory.getInstance().newTuple(l);

        String output = func.exec(input);
        assertTrue(output.equals(expected));
    }

    @Test
    public void testSUBSTRING() throws Exception {
        SUBSTRING func = new SUBSTRING();

        // test excution
        List l = new LinkedList();
        l.add("Hello World!");
        l.add(1);
        l.add(5);
        String expected = "ello";

        Tuple input = DefaultTupleFactory.getInstance().newTuple(l);

        String output = func.exec(input);
        assertTrue(output.equals(expected));
    }

}

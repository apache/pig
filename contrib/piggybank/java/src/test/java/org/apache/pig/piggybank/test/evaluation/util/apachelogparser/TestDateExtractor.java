/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the
 * NOTICE file distributed with this work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.pig.piggybank.test.evaluation.util.apachelogparser;

import java.util.ArrayList;

import junit.framework.TestCase;

import org.apache.pig.data.DataAtom;
import org.apache.pig.data.Datum;
import org.apache.pig.data.Tuple;
import org.apache.pig.piggybank.evaluation.util.apachelogparser.DateExtractor;
import org.junit.Test;

public class TestDateExtractor extends TestCase {
    @Test
    public void testInstantiation() {
        assertNotNull(new DateExtractor());
    }

    @Test
    public void testDefaultFormatters() {
        DateExtractor dayExtractor = new DateExtractor();

        Tuple input = new Tuple(new DataAtom("20/Sep/2008:23:53:04 -0600"));
        DataAtom output = new DataAtom();
        dayExtractor.exec(input, output);
        assertEquals("09-20-2008", output.toString());
    }

    @Test
    public void testFailureThenSuccess() {
        DateExtractor dayExtractor = new DateExtractor();

        Tuple input = new Tuple(new DataAtom("dud"));
        DataAtom output = new DataAtom();
        dayExtractor.exec(input, output);
        assertEquals("", output.toString());

        input = new Tuple(new DataAtom("20/Sep/2008:23:53:04 -0600"));
        output = new DataAtom();
        dayExtractor.exec(input, output);
        assertEquals("09-20-2008", output.toString());
    }

    @Test
    public void testPassedOutputFormatter() {
        DateExtractor dayExtractor = new DateExtractor("MM-dd-yyyy");

        ArrayList<Datum> input = new ArrayList<Datum>();
        input.add(new DataAtom("20/Sep/2008:23:53:04 -0600"));

        DataAtom output = new DataAtom();
        dayExtractor.exec(new Tuple(input), output);
        assertEquals("09-20-2008", output.toString());
    }

    @Test
    public void testPassedInputOutputFormatter() {
        DateExtractor dayExtractor = new DateExtractor("dd/MMM/yyyy:HH:mm:ss", "MM~dd~yyyy");

        ArrayList<Datum> input = new ArrayList<Datum>();
        input.add(new DataAtom("20/Sep/2008:23:53:04"));

        DataAtom output = new DataAtom();
        dayExtractor.exec(new Tuple(input), output);
        assertEquals("09~20~2008", output.toString());
    }

    @Test
    public void testPassedOutputInputFormatterYear() {
        DateExtractor dayExtractor = new DateExtractor("dd/MMM/yyyy:HH:mm:ss", "yyyy");

        ArrayList<Datum> input = new ArrayList<Datum>();
        input.add(new DataAtom("20/Sep/2008:23:53:04"));

        DataAtom output = new DataAtom();
        dayExtractor.exec(new Tuple(input), output);
        assertEquals("2008", output.toString());
    }

    @Test
    public void testPassedOutputFormatterYear() {
        DateExtractor dayExtractor = new DateExtractor("yyyy");

        ArrayList<Datum> input = new ArrayList<Datum>();
        input.add(new DataAtom("20/Sep/2008:23:53:04 -0600"));

        DataAtom output = new DataAtom();
        dayExtractor.exec(new Tuple(input), output);
        assertEquals("2008", output.toString());
    }
}

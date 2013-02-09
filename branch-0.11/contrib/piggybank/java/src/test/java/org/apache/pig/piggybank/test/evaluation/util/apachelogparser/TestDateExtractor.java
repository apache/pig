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

import junit.framework.TestCase;

import org.apache.pig.data.DefaultTupleFactory;
import org.apache.pig.data.Tuple;
import org.apache.pig.piggybank.evaluation.util.apachelogparser.DateExtractor;
import org.junit.Test;

public class TestDateExtractor extends TestCase {
    
    private Tuple input=DefaultTupleFactory.getInstance().newTuple(1);
    
  
   @Test
    public void testInstantiation() {
        assertNotNull(new DateExtractor());
    }

    @Test
    public void testDefaultFormatters() throws Exception {
        DateExtractor dayExtractor = new DateExtractor();
        // test that GMT conversion moves the day
        input.set(0, "20/Sep/2008:23:53:04 -0600");
        assertEquals("2008-09-21", dayExtractor.exec(input));
        
        // test that if the string is already in GMT, nothing moves
        input.set(0, "20/Sep/2008:23:53:04 -0000");
        assertEquals("2008-09-20", dayExtractor.exec(input));
    }

    @Test
    public void testMZFormatters() throws Exception {
        DateExtractor extractor = new DateExtractor("dd/MMM/yyyy:HH:mm:ss Z", "yyyy-MM-dd", "PST");
        input.set(0, "20/Sep/2008:23:53:04 -0700");
        assertEquals("2008-09-20", extractor.exec(input));
    }
    
    @Test
    public void testFailureThenSuccess() throws Exception {
        DateExtractor dayExtractor = new DateExtractor();
        input.set(0,"dud");
        assertEquals(null, dayExtractor.exec(input));
        input.set(0,"20/Sep/2008:23:53:04 -0000");
        assertEquals("2008-09-20", dayExtractor.exec(input));
    }

    @Test
    public void testPassedOutputFormatter() throws Exception {
        DateExtractor dayExtractor = new DateExtractor("MM-dd-yyyy");
        input.set(0,"20/Sep/2008:23:53:04 -0000");
        assertEquals("09-20-2008", dayExtractor.exec(input));
    }

    @Test
    public void testPassedInputOutputFormatter() throws Exception {
        DateExtractor dayExtractor = new DateExtractor("dd/MMM/yyyy:HH:mm:ss", "MM~dd~yyyy");
        input.set(0,"20/Sep/2008:23:53:04");
        assertEquals("09~20~2008", dayExtractor.exec(input));
    }

    @Test
    public void testPassedOutputInputFormatterYear() throws Exception {
        DateExtractor dayExtractor = new DateExtractor("dd/MMM/yyyy:HH:mm:ss", "yyyy");
        input.set(0,"20/Sep/2008:23:53:04");
        assertEquals("2008", dayExtractor.exec(input));
    }

    @Test
    public void testPassedOutputFormatterYear() throws Exception {
        DateExtractor dayExtractor = new DateExtractor("yyyy");
        input.set(0, "20/Sep/2008:23:53:04 -0600");
        assertEquals("2008", dayExtractor.exec(input));
    }
}

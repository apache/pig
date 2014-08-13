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

import java.io.IOException;

import junit.framework.TestCase;

import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.piggybank.evaluation.string.RegexExtract;
import org.apache.pig.piggybank.evaluation.string.RegexExtractAll;
import org.apache.pig.piggybank.evaluation.string.RegexMatch;
import org.junit.Test;

public class TestRegex extends TestCase {
    @Test
    public void testRegexMatch() throws Exception {
        Tuple t1 = TupleFactory.getInstance().newTuple(2);
        t1.set(0, "/search/iy/xxx");
        t1.set(1, "^\\/search\\/iy\\/.*");
        
        Tuple t2 = TupleFactory.getInstance().newTuple(2);
        t2.set(0, "http://yahoo.com");
        t2.set(1, "^\\/search\\/iy\\/.*");

        Tuple t3 = TupleFactory.getInstance().newTuple(2);
        t3.set(0, null);
        t3.set(1, "^\\/search\\/iy\\/.*");
        
        RegexMatch func = new RegexMatch();
        Integer r = func.exec(t1);
        assertTrue(r==1);
        r = func.exec(t2);
        assertTrue(r==0);
        r = func.exec(t3);
        assertTrue(r==null);
    }
    @Test
    public void testRegexExtract() throws Exception {
        Tuple t1 = TupleFactory.getInstance().newTuple(3);
        t1.set(0, "/search/iy/term1/test");
        t1.set(1, "^\\/search\\/iy\\/(.*?)\\/.*");
        t1.set(2, 1);
        
        Tuple t2 = TupleFactory.getInstance().newTuple(3);
        t2.set(0, "/search/iy/term1/test");
        t2.set(1, "^\\/search\\/iy\\/(.*?)\\/.*");
        t2.set(2, 2);
        
        Tuple t3 = TupleFactory.getInstance().newTuple(3);
        t3.set(0, null);
        t3.set(1, "^\\/search\\/iy\\/(.*?)\\/.*");
        t3.set(2, 2);

        
        RegexExtract func = new RegexExtract();
        String r = func.exec(t1);
        assertTrue(r.equals("term1"));
        r = func.exec(t2);
        assertTrue(r==null);
        r = func.exec(t3);
        assertTrue(r==null);
    }
    
    @Test
    public void testRegexExtractAll() throws IOException {
        String matchRegex = "^(.+)\\b\\s+is a\\s+\\b(.+)$";
        TupleFactory tupleFactory = TupleFactory.getInstance();
        Tuple t1 = tupleFactory.newTuple(2);
        t1.set(0,"this is a match");
        t1.set(1, matchRegex);
        
        Tuple t2 = tupleFactory.newTuple(2);
        t2.set(0, "no match");
        t2.set(1, matchRegex);
        
        Tuple t3 = tupleFactory.newTuple(2);
        t3.set(0, null);
        t3.set(1, matchRegex);
     
        RegexExtractAll func = new RegexExtractAll();
        Tuple r = func.exec(t1);
        assertEquals(r.size(), 2);
        assertEquals("this", r.get(0));
        assertEquals("match", r.get(1));
        
        r = func.exec(t2);
        assertTrue(r==null);
        
        r = func.exec(t3);
        assertTrue(r==null);
    }
}

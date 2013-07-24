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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.builtin.INDEXOF;
import org.apache.pig.builtin.LAST_INDEX_OF;
import org.apache.pig.builtin.REPLACE;
import org.apache.pig.builtin.STARTSWITH;
import org.apache.pig.builtin.ENDSWITH;
import org.apache.pig.builtin.STRSPLIT;
import org.apache.pig.builtin.SUBSTRING;
import org.apache.pig.builtin.TRIM;
import org.apache.pig.builtin.LTRIM;
import org.apache.pig.builtin.RTRIM;
import org.apache.pig.builtin.EqualsIgnoreCase;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Test;

public class TestStringUDFs {
    private static final EvalFunc<String> stringSubstr_ = new SUBSTRING();

    @Test
    public void testStringSubstr() throws IOException {
        Tuple testTuple = Util.buildTuple(null, 0, 2);
        assertNull("null is null", stringSubstr_.exec(testTuple));

        testTuple = Util.buildTuple("", 0, 2);
        assertEquals("empty string", "", stringSubstr_.exec(testTuple));

        testTuple = Util.buildTuple("abcde", 1, 3);
        assertEquals("lowercase string", "bc", stringSubstr_.exec(testTuple));

        testTuple = Util.buildTuple("abc", 0, 15);
        assertEquals("uppercase string", "abc", stringSubstr_.exec(testTuple));
    }

    @Test
    public void testStringSubstr_BI_EQ_EI() throws IOException {
        Tuple testTuple = Util.buildTuple("abc", 0, 0);
        assertEquals("Testing SUBSTIRNG, beginindex == endindex", "", stringSubstr_.exec(testTuple));
    }
    
    @Test
    public void testStringSubstr_BI_LT_EI() throws IOException {
        Tuple testTuple = Util.buildTuple("abc", -2, 2);
        assertEquals("Testing SUBSTIRNG, beginindex < endindex", null, stringSubstr_.exec(testTuple));
    }
    
    @Test
    public void testStringSubstr_BI_LT_ZERO() throws IOException {
        Tuple testTuple = Util.buildTuple("abc", -1, 2);
        assertEquals("Testing SUBSTIRNG, beginindex < 0", null, stringSubstr_.exec(testTuple));
    }
       
    @Test
    public void testStringSubstr_BI_GT_EI() throws IOException {
        Tuple testTuple = Util.buildTuple("abc", 10, 2);
        assertEquals("Testing SUBSTIRNG, beginindex > endindex", null, stringSubstr_.exec(testTuple));
    }
    
    @Test
    public void testStringSubstr_EI_LT_ZERO() throws IOException {
        Tuple testTuple = Util.buildTuple("abc", 0, -2);
        assertEquals("Testing SUBSTIRNG, endindex < 0", null, stringSubstr_.exec(testTuple));
    }

    @Test
    public void testIndexOf() throws IOException {
        INDEXOF indexOf = new INDEXOF();
        Tuple testTuple = Util.buildTuple("xyz", "");
        assertEquals( ((Integer) "xyz".indexOf("")), indexOf.exec(testTuple));
        
        testTuple = Util.buildTuple(null, null);
        assertNull(indexOf.exec(testTuple));
        
        testTuple = Util.buildTuple("xyz", "y");
        assertEquals( ((Integer) "xyz".indexOf("y")), indexOf.exec(testTuple));
        
        testTuple = Util.buildTuple("xyz", "abc");
        assertEquals( ((Integer) "xyz".indexOf("abc")), indexOf.exec(testTuple));
    }
    
    @Test
    public void testLastIndexOf() throws IOException {
        LAST_INDEX_OF lastIndexOf = new LAST_INDEX_OF();
        Tuple testTuple = Util.buildTuple("xyz", "");
        assertEquals( ((Integer) "xyz".lastIndexOf("")), lastIndexOf.exec(testTuple));
        
        testTuple = Util.buildTuple(null, null);
        assertNull(lastIndexOf.exec(testTuple));
        
        testTuple = Util.buildTuple("xyzyy", "y");
        assertEquals( ((Integer) "xyzyy".lastIndexOf("y")), lastIndexOf.exec(testTuple));
        
        testTuple = Util.buildTuple("xyz", "abc");
        assertEquals( ((Integer) "xyz".lastIndexOf("abc")), lastIndexOf.exec(testTuple));
    }
    
    @Test
    public void testReplace() throws IOException {
        REPLACE replace = new REPLACE();
        Tuple testTuple = Util.buildTuple("foobar", "z", "x");
        assertEquals("foobar".replace("z", "x"), replace.exec(testTuple));
        
        testTuple = Util.buildTuple("foobar", "oo", "aa");
        assertEquals("foobar".replace("oo", "aa"), replace.exec(testTuple));
    }
    
    @Test
    public void testTrim() throws IOException {
        TRIM trim = new TRIM();
        Tuple testTuple = Util.buildTuple("nospaces");
        assertEquals("nospaces".trim(), trim.exec(testTuple));
        
        testTuple = Util.buildTuple("spaces right    ");
        assertEquals("spaces right", trim.exec(testTuple));
        
        testTuple = Util.buildTuple("    spaces left");
        assertEquals("spaces left", trim.exec(testTuple));
        
        testTuple = Util.buildTuple("    spaces both    ");
        assertEquals("spaces both", trim.exec(testTuple));
        
        testTuple = TupleFactory.getInstance().newTuple();
        assertNull(trim.exec(testTuple));
    }

    @Test
    public void testLtrim() throws IOException {
        LTRIM trim = new LTRIM();
        Tuple testTuple = Util.buildTuple("nospaces");
        assertEquals("nospaces", trim.exec(testTuple));
        
        testTuple = Util.buildTuple("spaces right    ");
        assertEquals("spaces right    ", trim.exec(testTuple));
        
        testTuple = Util.buildTuple("    spaces left");
        assertEquals("spaces left", trim.exec(testTuple));
        
        testTuple = Util.buildTuple("    spaces both    ");
        assertEquals("spaces both    ", trim.exec(testTuple));
        
        testTuple = TupleFactory.getInstance().newTuple();
        assertNull(trim.exec(testTuple));
    }
    
    @Test
    public void testRtrim() throws IOException {
        RTRIM trim = new RTRIM();
        Tuple testTuple = Util.buildTuple("nospaces");
        assertEquals("nospaces", trim.exec(testTuple));
        
        testTuple = Util.buildTuple("spaces right    ");
        assertEquals("spaces right", trim.exec(testTuple));
        
        testTuple = Util.buildTuple("    spaces left");
        assertEquals("    spaces left", trim.exec(testTuple));
        
        testTuple = Util.buildTuple("    spaces both    ");
        assertEquals("    spaces both", trim.exec(testTuple));
        
        testTuple = TupleFactory.getInstance().newTuple();
        assertNull(trim.exec(testTuple));
    }

    @Test 
    public void testSplit() throws IOException {
        STRSPLIT splitter = new STRSPLIT();
       // test no delims
        Tuple testTuple = Util.buildTuple("foo", ":");
        testTuple.set(0, "foo");
        testTuple.set(1, ":");
        Tuple splits = splitter.exec(testTuple);
        assertEquals("no matches should return tuple with original string", 1, splits.size());
        assertEquals("no matches should return tuple with original string", "foo", 
                splits.get(0));
        
        // test default delimiter
        testTuple = Util.buildTuple("f ooo bar");
        splits = splitter.exec(testTuple);
        assertEquals("split on default value ", 3, splits.size());
        assertEquals("f", splits.get(0));
        assertEquals("ooo", splits.get(1));
        assertEquals("bar", splits.get(2));
        
        // test trimming of whitespace
        testTuple = Util.buildTuple("foo bar  ");
        splits = splitter.exec(testTuple);
        assertEquals("whitespace trimmed if no length arg", 2, splits.size());
        
        // test forcing null matches with length param
        testTuple = Util.buildTuple("foo bar   ", "\\s", 10);
        splits = splitter.exec(testTuple);
        assertEquals("length forces empty string matches on end", 5, splits.size());
        
        // test limiting results with limit
        testTuple = Util.buildTuple("foo:bar:baz", ":", 2);
        splits = splitter.exec(testTuple);
        assertEquals(2, splits.size());
        assertEquals("foo", splits.get(0));
        assertEquals("bar:baz", splits.get(1));
    }

    @Test
    public void testStartsWith() throws IOException {
        STARTSWITH startsWith = new STARTSWITH();
        Tuple testTuple1 = Util.buildTuple("foo", "bar");
        assertFalse("String prefix should not match", startsWith.exec(testTuple1));
        Tuple testTuple2 = Util.buildTuple("foobaz", "foo");
        assertTrue("String prefix should match", startsWith.exec(testTuple2));
    }
    
    @Test
    public void testEndsWith() throws IOException {
        ENDSWITH endsWith = new ENDSWITH();
        Tuple testTuple1 = Util.buildTuple("foo", "bar");
        assertFalse("String suffix should not match", endsWith.exec(testTuple1));
        Tuple testTuple2 = Util.buildTuple("foobaz", "foo");
        assertFalse("String suffix should not match", endsWith.exec(testTuple2));
        Tuple testTuple3 = Util.buildTuple("foobaz", "baz");
        assertTrue("String suffix should match", endsWith.exec(testTuple3));
        Tuple testTuple4 = Util.buildTuple(null, "bar");
        assertNull("Should return null", endsWith.exec(testTuple4));
    }

    @Test
    public void testEqualsIgnoreCase() throws IOException {
        EqualsIgnoreCase equalsIgnoreCase = new EqualsIgnoreCase ();
        Tuple testTuple = Util.buildTuple("ABC","abc");
        assertEquals("Strings are NOT equalsIgnoreCase", "ABC".equalsIgnoreCase("abc"), equalsIgnoreCase.exec(testTuple));
        testTuple = Util.buildTuple("ABC", "aBC");
        assertEquals("strings are NOT equalsIgnoreCase", "ABC".equalsIgnoreCase("aBC"), equalsIgnoreCase.exec(testTuple));
        testTuple = Util.buildTuple("abc", "abc");
        assertEquals("strings are NOT equalsIgnoreCase", "abc".equalsIgnoreCase("abc"), equalsIgnoreCase.exec(testTuple));
        testTuple = Util.buildTuple("abcd", "abc");
        assertEquals("strings are NOT equalsIgnoreCase", "abcd".equalsIgnoreCase("abc"), equalsIgnoreCase.exec(testTuple));
       
    }


}

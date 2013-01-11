/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.pig.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.text.ParseException;

import org.apache.pig.tools.cmdline.CmdLineParser;
import org.junit.Test;

public class TestCmdLineParser {

    @Test(expected = AssertionError.class)
    public void testRegisterDash() {
        String[] args = { "a", "b", "c" };
        CmdLineParser p = new CmdLineParser(args);
        try {
            p.registerOpt('-', "alpha", CmdLineParser.ValueExpected.NOT_ACCEPTED);
        } catch (AssertionError e) {
            assertEquals(e.getMessage(),
                    "CmdLineParser:  '-' is not a legal single character designator.");
            throw e;
        }
    }

    @Test(expected = AssertionError.class)
    public void testDoubleRegisterShort() {
        String[] args = { "a", "b", "c" };
        CmdLineParser p = new CmdLineParser(args);
        try {
            p.registerOpt('a', "alpha", CmdLineParser.ValueExpected.NOT_ACCEPTED);
            p.registerOpt('a', "beta", CmdLineParser.ValueExpected.NOT_ACCEPTED);
            fail("Should have thrown an AssertionError");
        } catch (AssertionError e) {
            assertEquals(e.getMessage(),
                    "CmdLineParser:  You have already registered option a");
            throw e;
        }
    }

    @Test(expected = AssertionError.class)
    public void testDoubleRegisterLong() {
        String[] args = { "a", "b", "c" };
        CmdLineParser p = new CmdLineParser(args);
        try {
            p.registerOpt('a', "alpha", CmdLineParser.ValueExpected.NOT_ACCEPTED);
            p.registerOpt('b', "alpha", CmdLineParser.ValueExpected.NOT_ACCEPTED);
        } catch (AssertionError e) {
            assertEquals(e.getMessage(),
                    "CmdLineParser:  You have already registered option alpha");
            throw e;
        }
    }

    @Test
    public void testRegister() {
        String[] args = { "a", "b", "c" };
        CmdLineParser p = new CmdLineParser(args);
        p.registerOpt('a', "alpha", CmdLineParser.ValueExpected.NOT_ACCEPTED);
        p.registerOpt('b', "beta", CmdLineParser.ValueExpected.REQUIRED);
        p.registerOpt('c', null, CmdLineParser.ValueExpected.OPTIONAL);
    }

    @Test
    public void testParseNoArgs() throws ParseException {
        String[] args = {};
        CmdLineParser p = new CmdLineParser(args);
        p.registerOpt('a', "alpha", CmdLineParser.ValueExpected.NOT_ACCEPTED);
        assertEquals(CmdLineParser.EndOfOpts, p.getNextOpt());
    }

    @Test
    public void testParseNoDash() throws ParseException {
        String[] args = { "a" };
        CmdLineParser p = new CmdLineParser(args);
        p.registerOpt('a', "alpha", CmdLineParser.ValueExpected.NOT_ACCEPTED);
        assertEquals(p.getNextOpt(), CmdLineParser.EndOfOpts);
        String[] remainders = p.getRemainingArgs();
        assertEquals("a", remainders[0]);
    }

    @Test
    public void testParseLongShortNoLeftover() throws ParseException {
        String[] args = { "-a", "--beta", "beth", "--c" };
        CmdLineParser p = new CmdLineParser(args);
        p.registerOpt('a', "alpha", CmdLineParser.ValueExpected.NOT_ACCEPTED);
        p.registerOpt('b', "beta", CmdLineParser.ValueExpected.REQUIRED);
        p.registerOpt('c', null, CmdLineParser.ValueExpected.OPTIONAL);
        assertEquals('a', p.getNextOpt());
        assertEquals('b', p.getNextOpt());
        assertEquals("beth", p.getValStr());
        assertEquals('c', p.getNextOpt());
        assertNull(p.getValStr());
        assertEquals(CmdLineParser.EndOfOpts, p.getNextOpt());
        assertNull(p.getRemainingArgs());
    }

    @Test
    public void testParseLongShortLeftover1() throws ParseException {
        String[] args = { "-a", "--beta", "beth", "--c", "gimel", "-", "hi", "i'm", "left",
                        "over" };
        CmdLineParser p = new CmdLineParser(args);
        p.registerOpt('a', "alpha", CmdLineParser.ValueExpected.NOT_ACCEPTED);
        p.registerOpt('b', "beta", CmdLineParser.ValueExpected.REQUIRED);
        p.registerOpt('c', null, CmdLineParser.ValueExpected.OPTIONAL);
        assertEquals('a', p.getNextOpt());
        assertEquals('b', p.getNextOpt());
        assertEquals("beth", p.getValStr());
        assertEquals('c', p.getNextOpt());
        assertEquals("gimel", p.getValStr());
        assertEquals(CmdLineParser.EndOfOpts, p.getNextOpt());
        String[] r = p.getRemainingArgs();
        assertEquals(4, r.length);
    }

    // has two dashes instead of one for end of args
    @Test
    public void testParseLongShortLeftover2() throws ParseException {
        String[] args = { "-a", "-beta", "beth", "--c", "gimel", "--", "hi", "i'm", "left",
                        "over" };
        CmdLineParser p = new CmdLineParser(args);
        p.registerOpt('a', "alpha", CmdLineParser.ValueExpected.NOT_ACCEPTED);
        p.registerOpt('b', "beta", CmdLineParser.ValueExpected.REQUIRED);
        p.registerOpt('c', null, CmdLineParser.ValueExpected.OPTIONAL);
        assertEquals(p.getNextOpt(), 'a');
        assertEquals(p.getNextOpt(), 'b');
        assertEquals(p.getValStr(), "beth");
        assertEquals(p.getNextOpt(), 'c');
        assertEquals(p.getValStr(), "gimel");
        assertEquals(p.getNextOpt(), CmdLineParser.EndOfOpts);
        String[] r = p.getRemainingArgs();
        assertEquals(r.length, 4);
    }

    @Test
    public void testParseLongShortLeftover3() throws ParseException {
        String[] args = { "-a", "--beta", "5", "--c", "--" };
        CmdLineParser p = new CmdLineParser(args);
        p.registerOpt('a', "alpha", CmdLineParser.ValueExpected.NOT_ACCEPTED);
        p.registerOpt('b', "beta", CmdLineParser.ValueExpected.REQUIRED);
        p.registerOpt('c', null, CmdLineParser.ValueExpected.OPTIONAL);
        assertEquals('a', p.getNextOpt());
        assertEquals('b', p.getNextOpt());
        Integer ii = p.getValInt();
        assertEquals(5, ii.intValue());
        assertEquals('c', p.getNextOpt());
        assertNull(p.getValInt());
        assertEquals(CmdLineParser.EndOfOpts, p.getNextOpt());
        String[] r = p.getRemainingArgs();
        assertNull(p.getRemainingArgs());
    }

    @Test
    public void testParseValueNotAcceptedProvided1() throws ParseException {
        String[] args = { "-a", "aleph" };
        CmdLineParser p = new CmdLineParser(args);
        p.registerOpt('a', "alpha", CmdLineParser.ValueExpected.NOT_ACCEPTED);
        assertEquals('a', p.getNextOpt());
        String[] r = p.getRemainingArgs();
        assertEquals(1, r.length);
        assertEquals("aleph", r[0]);
    }

    @Test
    public void testParseValueNotAcceptedProvided2() throws ParseException {
        String[] args = { "-alpha", "aleph" };
        CmdLineParser p = new CmdLineParser(args);
        p.registerOpt('a', "alpha", CmdLineParser.ValueExpected.NOT_ACCEPTED);
        assertEquals('a', p.getNextOpt());
        String[] r = p.getRemainingArgs();
        assertEquals(1, r.length);
        assertEquals("aleph", r[0]);
    }

    @Test(expected = ParseException.class)
    public void testParseValueRequiredNotProvided1() throws Exception {
        String[] args = { "-a" };
        CmdLineParser p = new CmdLineParser(args);
        p.registerOpt('a', "alpha", CmdLineParser.ValueExpected.REQUIRED);
        try {
            p.getNextOpt();
        } catch (ParseException e) {
            assertEquals(e.getMessage(),
                    "Option -a requires a value but you did not provide one.");
            throw e;
        }
    }

    @Test(expected = ParseException.class)
    public void testParseValueRequiredNotProvided2() throws Exception {
        String[] args = { "--alpha", "-b" };
        CmdLineParser p = new CmdLineParser(args);
        p.registerOpt('a', "alpha", CmdLineParser.ValueExpected.REQUIRED);
        p.registerOpt('b', "beta", CmdLineParser.ValueExpected.NOT_ACCEPTED);
        try {
            p.getNextOpt();
        } catch (ParseException e) {
            assertEquals(e.getMessage(),
                    "Option --alpha requires a value but you did not provide one.");
            throw e;
        }
    }

    @Test(expected = NumberFormatException.class)
    public void testParseValueStrForInt() throws ParseException {
        String[] args = { "-alpha", "b" };
        CmdLineParser p = new CmdLineParser(args);
        p.registerOpt('a', "alpha", CmdLineParser.ValueExpected.REQUIRED);
        p.getNextOpt();
        Integer ii = p.getValInt();
    }

    @Test(expected = ParseException.class)
    public void testParseUnknownShort() throws Exception {
        String[] args = { "-alpha", "b", "-z" };
        CmdLineParser p = new CmdLineParser(args);
        p.registerOpt('a', "alpha", CmdLineParser.ValueExpected.REQUIRED);
        try {
            p.getNextOpt();
            assertEquals("b", p.getValStr());
            p.getNextOpt();
        } catch (ParseException e) {
            assertEquals(e.getMessage(),
                    "Found unknown option (-z) at position 3");
            throw e;
        }
    }

    @Test(expected = ParseException.class)
    public void testParseUnknownLong() throws Exception {
        String[] args = { "--zeta" };
        CmdLineParser p = new CmdLineParser(args);
        p.registerOpt('a', "alpha", CmdLineParser.ValueExpected.REQUIRED);
        try {
            p.getNextOpt();
        } catch (ParseException e) {
            assertEquals(e.getMessage(),
                    "Found unknown option (--zeta) at position 1");
            throw e;
        }
    }
}

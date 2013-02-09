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

import java.text.ParseException;

import junit.framework.AssertionFailedError;
import junit.framework.TestCase;

import org.junit.Test;

import org.apache.pig.tools.cmdline.CmdLineParser;

public class TestCmdLineParser extends TestCase
{

@Test
public void testRegisterDash()
{
    String[] args = {"a", "b", "c"};
    CmdLineParser p = new CmdLineParser(args);
    try {
        p.registerOpt('-', "alpha", CmdLineParser.ValueExpected.NOT_ACCEPTED);
        fail("Should have thrown an AssertionError");
    } catch (AssertionError e) {
        assertEquals(e.getMessage(),
            "CmdLineParser:  '-' is not a legal single character designator.");
    }
}

@Test
public void testDoubleRegisterShort()
{
    String[] args = {"a", "b", "c"};
    CmdLineParser p = new CmdLineParser(args);
    try {
        p.registerOpt('a', "alpha", CmdLineParser.ValueExpected.NOT_ACCEPTED);
        p.registerOpt('a', "beta", CmdLineParser.ValueExpected.NOT_ACCEPTED);
        fail("Should have thrown an AssertionError");
    } catch (AssertionError e) {
        assertEquals(e.getMessage(),
            "CmdLineParser:  You have already registered option a");
    }
}

@Test
public void testDoubleRegisterLong()
{
    String[] args = {"a", "b", "c"};
    CmdLineParser p = new CmdLineParser(args);
    try {
        p.registerOpt('a', "alpha", CmdLineParser.ValueExpected.NOT_ACCEPTED);
        p.registerOpt('b', "alpha", CmdLineParser.ValueExpected.NOT_ACCEPTED);
        fail("Should have thrown an AssertionError");
    } catch (AssertionError e) {
        assertEquals(e.getMessage(),
            "CmdLineParser:  You have already registered option alpha");
    }
}

@Test
public void testRegister()
{
    String[] args = {"a", "b", "c"};
    CmdLineParser p = new CmdLineParser(args);
    p.registerOpt('a', "alpha", CmdLineParser.ValueExpected.NOT_ACCEPTED);
    p.registerOpt('b', "beta", CmdLineParser.ValueExpected.REQUIRED);
    p.registerOpt('c', null, CmdLineParser.ValueExpected.OPTIONAL);
}

@Test
public void testParseNoArgs() throws ParseException
{
    String[] args = {};
    CmdLineParser p = new CmdLineParser(args);
    p.registerOpt('a', "alpha", CmdLineParser.ValueExpected.NOT_ACCEPTED);
    assertEquals(p.getNextOpt(), CmdLineParser.EndOfOpts);
}

@Test
public void testParseNoDash() throws ParseException
{
    String[] args = {"a"};
    CmdLineParser p = new CmdLineParser(args);
    p.registerOpt('a', "alpha", CmdLineParser.ValueExpected.NOT_ACCEPTED);
    assertEquals(p.getNextOpt(), CmdLineParser.EndOfOpts);
    String[] remainders = p.getRemainingArgs();
    assertEquals(remainders[0], "a");
}

@Test
public void testParseLongShortNoLeftover() throws ParseException
{
    String[] args = {"-a", "--beta", "beth", "--c"};
    CmdLineParser p = new CmdLineParser(args);
    p.registerOpt('a', "alpha", CmdLineParser.ValueExpected.NOT_ACCEPTED);
    p.registerOpt('b', "beta", CmdLineParser.ValueExpected.REQUIRED);
    p.registerOpt('c', null, CmdLineParser.ValueExpected.OPTIONAL);
    assertEquals(p.getNextOpt(), 'a');
    assertEquals(p.getNextOpt(), 'b');
    assertEquals(p.getValStr(), "beth");
    assertEquals(p.getNextOpt(), 'c');
    assertNull(p.getValStr());
    assertEquals(p.getNextOpt(), CmdLineParser.EndOfOpts);
    assertNull(p.getRemainingArgs());
}

@Test
public void testParseLongShortLeftover1() throws ParseException
{
    String[] args = {"-a", "--beta", "beth", "--c", "gimel", "-", "hi", "i'm", "left",
    "over"};
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

// has two dashes instead of one for end of args
@Test
public void testParseLongShortLeftover2() throws ParseException
{
    String[] args = {"-a", "-beta", "beth", "--c", "gimel", "--", "hi", "i'm", "left",
    "over"};
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
public void testParseLongShortLeftover3() throws ParseException
{
    String[] args = {"-a", "--beta", "5", "--c", "--"};
    CmdLineParser p = new CmdLineParser(args);
    p.registerOpt('a', "alpha", CmdLineParser.ValueExpected.NOT_ACCEPTED);
    p.registerOpt('b', "beta", CmdLineParser.ValueExpected.REQUIRED);
    p.registerOpt('c', null, CmdLineParser.ValueExpected.OPTIONAL);
    assertEquals(p.getNextOpt(), 'a');
    assertEquals(p.getNextOpt(), 'b');
    Integer ii = p.getValInt();
    assertEquals(ii.intValue(), 5);
    assertEquals(p.getNextOpt(), 'c');
    assertNull(p.getValInt());
    assertEquals(p.getNextOpt(), CmdLineParser.EndOfOpts);
    String[] r = p.getRemainingArgs();
    assertNull(p.getRemainingArgs());
}

@Test
public void testParseValueNotAcceptedProvided1() throws ParseException
{
    String[] args = {"-a", "aleph"};
    CmdLineParser p = new CmdLineParser(args);
    p.registerOpt('a', "alpha", CmdLineParser.ValueExpected.NOT_ACCEPTED);
    assertEquals(p.getNextOpt(), 'a');
    String[] r = p.getRemainingArgs();
    assertEquals(r.length, 1);
    assertEquals(r[0], "aleph");
}

@Test
public void testParseValueNotAcceptedProvided2() throws ParseException
{
    String[] args = {"-alpha", "aleph"};
    CmdLineParser p = new CmdLineParser(args);
    p.registerOpt('a', "alpha", CmdLineParser.ValueExpected.NOT_ACCEPTED);
    assertEquals(p.getNextOpt(), 'a');
    String[] r = p.getRemainingArgs();
    assertEquals(r.length, 1);
    assertEquals(r[0], "aleph");
}

@Test
public void testParseValueRequiredNotProvided1()
{
    String[] args = {"-a"};
    CmdLineParser p = new CmdLineParser(args);
    p.registerOpt('a', "alpha", CmdLineParser.ValueExpected.REQUIRED);
    try {
        p.getNextOpt();
        fail("Should have thrown a ParseException");
    } catch (ParseException e) {
        assertEquals(e.getMessage(),
            "Option -a requires a value but you did not provide one.");
    }
}

@Test
public void testParseValueRequiredNotProvided2()
{
    String[] args = {"--alpha", "-b"};
    CmdLineParser p = new CmdLineParser(args);
    p.registerOpt('a', "alpha", CmdLineParser.ValueExpected.REQUIRED);
    p.registerOpt('b', "beta", CmdLineParser.ValueExpected.NOT_ACCEPTED);
    try {
        p.getNextOpt();
        fail("Should have thrown a ParseException");
    } catch (ParseException e) {
        assertEquals(e.getMessage(),
            "Option --alpha requires a value but you did not provide one.");
    }
}

@Test
public void testParseValueStrForInt() throws ParseException
{
    String[] args = {"-alpha", "b"};
    CmdLineParser p = new CmdLineParser(args);
    p.registerOpt('a', "alpha", CmdLineParser.ValueExpected.REQUIRED);
    try {
        p.getNextOpt();
        Integer ii = p.getValInt();
        fail("Should have thrown a NumberFormatException");
    } catch (NumberFormatException e) {
    }
}

@Test
public void testParseUnknownShort()
{
    String[] args = {"-alpha", "b", "-z"};
    CmdLineParser p = new CmdLineParser(args);
    p.registerOpt('a', "alpha", CmdLineParser.ValueExpected.REQUIRED);
    try {
        p.getNextOpt();
        assertEquals(p.getValStr(), "b");
        p.getNextOpt();
        fail("Should have thrown a ParseException");
    } catch (ParseException e) {
        assertEquals(e.getMessage(),
            "Found unknown option (-z) at position 3");
    }
}

@Test
public void testParseUnknownLong()
{
    String[] args = {"--zeta"};
    CmdLineParser p = new CmdLineParser(args);
    p.registerOpt('a', "alpha", CmdLineParser.ValueExpected.REQUIRED);
    try {
        p.getNextOpt();
        fail("Should have thrown a ParseException");
    } catch (ParseException e) {
        assertEquals(e.getMessage(),
            "Found unknown option (--zeta) at position 1");
    }
}

}


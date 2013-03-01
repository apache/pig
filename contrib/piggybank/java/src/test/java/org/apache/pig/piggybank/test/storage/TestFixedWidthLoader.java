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

package org.apache.pig.piggybank.test.storage;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import junit.framework.TestCase;

import org.apache.commons.lang.StringUtils;

import org.apache.pig.ExecType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.pigunit.pig.PigServer;
import org.apache.pig.test.Util;
import org.apache.pig.tools.parameters.ParseException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestFixedWidthLoader {

    private static final String dataDir = "build/test/tmpdata/";
    private static final String testFile = "fixed_width_data";

    private PigServer pig;

    @Before
    public void setup() throws IOException {
        pig = new PigServer(ExecType.LOCAL);

        Util.deleteDirectory(new File(dataDir));
        try {
            pig.mkdirs(dataDir);

            Util.createLocalInputFile(dataDir + testFile,
                new String[] {
                    "  int            long   float     double bit boolean              datetime  string  string   extra",
                    "12345   1234567890000   2.718   3.141593   0    true  2007-04-05T14:30:10Z   avertwolowolo",
                    "12345   1234567890000   2.718   3.141593   1   false  2007-04-05T14:30:10Z   avertwolowolo   moose",
                    "        1234567890000           3.141593        true                         avert        ",
                    "        1234567890000           3.141593       false",
                    "        1234567890000           cerulean        true"
            });
        } catch (IOException e) {};
    }

    @After
    public void cleanup() throws IOException {
        Util.deleteDirectory(new File(dataDir));
        pig.shutdown();
    }

    @Test
    public void defaultSchema() throws IOException, ParseException {
        pig.registerQuery(
            "data = load '" + dataDir + testFile + "' " +
            "using org.apache.pig.piggybank.storage.FixedWidthLoader('-5, 9-21, 25-29 , 33 - 40, 44-44, 48-52, 55-74, 78-82, 83-90', 'USE_HEADER');"
        );

        Iterator<Tuple> data = pig.openIterator("data");
        String[] expected = {
            "(int,long,float,double,t,olean,datetime,tring,string)", // "bit", "boolean", and first "string" fields cut off properly
            "(12345,1234567890000,2.718,3.141593,0,true,2007-04-05T14:30:10Z,avert,wolowolo)",
            "(12345,1234567890000,2.718,3.141593,1,false,2007-04-05T14:30:10Z,avert,wolowolo)",
            "(,1234567890000,,3.141593,,true,,avert,)",
            "(,1234567890000,,3.141593,,false,,,)",
            "(,1234567890000,,cerulean,,true,,,)"                   // no problem with this since loaded as a bytearray
        };

        Assert.assertEquals(StringUtils.join(expected, "\n"), StringUtils.join(data, "\n"));
    }

    @Test
    public void userSchema() throws IOException, ParseException {
        pig.registerQuery(
            "data = load '" + dataDir + testFile + "' " +
            "using org.apache.pig.piggybank.storage.FixedWidthLoader(" +
                "'-5, 9-21, 25-29, 33-40, 44, 48-52, 55-74, 78-82, 83-90', " + 
                "'SKIP_HEADER', " + 
                "'i: int, l: long, f: float, d: double, bit: int, b: boolean, dt: datetime, c_arr: chararray, b_arr: bytearray'" + 
            ");"
        );

        Iterator<Tuple> data = pig.openIterator("data");
        String[] expected = {
            // Header skipped
            "(12345,1234567890000,2.718,3.141593,0,true,2007-04-05T14:30:10.000Z,avert,wolowolo)",  // scalar types
            "(12345,1234567890000,2.718,3.141593,1,false,2007-04-05T14:30:10.000Z,avert,wolowolo)", // ignore extra field "moose" after beryl
            "(,1234567890000,,3.141593,,true,,avert,)",                                             // nulls fields (all spaces)
            "(,1234567890000,,3.141593,,false,,,)",                                                 // missing fields (line break earlier than expected)
            "(,1234567890000,,,,true,,,)"                                                           // invalid double field "cerulean" turns to null   
        };

        Assert.assertEquals(StringUtils.join(expected, "\n"), StringUtils.join(data, "\n"));
    }

    @Test 
    public void userSchemaFewerFieldsThanColumns() throws IOException, ParseException {
        pig.registerQuery(
            "data = load '" + dataDir + testFile + "' " +
            "using org.apache.pig.piggybank.storage.FixedWidthLoader(" + 
                "'-5, 9-21, 25-29, 33-40, 44, 48-52, 55-74, 78-82, 83-90', " + 
                "'SKIP_HEADER', " +
                "'i: int, l: long, f: float, d: double'" +
            ");"
        );

        Iterator<Tuple> data = pig.openIterator("data");
        String[] expected = {
            "(12345,1234567890000,2.718,3.141593)",
            "(12345,1234567890000,2.718,3.141593)",
            "(,1234567890000,,3.141593)",
            "(,1234567890000,,3.141593)",
            "(,1234567890000,,)"
        };

        Assert.assertEquals(StringUtils.join(expected, "\n"), StringUtils.join(data, "\n"));
    }

    @Test(expected=FrontendException.class)
    public void doesNotSupportObjectTypes() throws IOException, ParseException {
        pig.registerQuery(
            "data = load '" + dataDir + testFile + "' " +
            "using org.apache.pig.piggybank.storage.FixedWidthLoader(" +
                "'-5, 9-21, 25-29, 33-40, 44, 48-52, 55-74, 78-82, 83-90', 'SKIP_HEADER', 'i: (j: int, k: int)'" + 
            ");"
        );

        Iterator<Tuple> data = pig.openIterator("data");
    }

    @Test(expected=FrontendException.class)
    public void fewerColumnsThanSchemaFields() throws IOException, ParseException {
        pig.registerQuery(
            "data = load '" + dataDir + testFile + "' " +
            "using org.apache.pig.piggybank.storage.FixedWidthLoader(" +
                "'1-5, 9-21, 25-29, 33-40', 'SKIP_HEADER', 'i: int, l: long, f: float, d: double, bit: int, c: chararray, b: bytearray') " + 
            ");"
        );

        Iterator<Tuple> data = pig.openIterator("data");
    }

    @Test(expected=FrontendException.class)
    public void columnStartsAtZero() throws IOException, ParseException {
        pig.registerQuery(
            "data = load '" + dataDir + testFile + "' " +
            "using org.apache.pig.piggybank.storage.FixedWidthLoader(" +
                "'0-5', 'SKIP_HEADER', 'i: int'" + 
            ");"
        );

        Iterator<Tuple> data = pig.openIterator("data");
    }

    @Test(expected=FrontendException.class)
    public void columnEndLessThanStart() throws IOException, ParseException {
        pig.registerQuery(
            "data = load '" + dataDir + testFile + "' " +
            "using org.apache.pig.piggybank.storage.FixedWidthLoader(" +
                "'5-0', 'SKIP_HEADER', 'i: int'" + 
            ");"
        );

        Iterator<Tuple> data = pig.openIterator("data");
    }

    @Test
    public void pushProjection() throws IOException, ParseException {
        pig.registerQuery(
            "data = load '" + dataDir + testFile + "' " +
            "using org.apache.pig.piggybank.storage.FixedWidthLoader(" +
                "'-5, 9-21, 25-29 , 33 - 40, 44-44, 48-52, 55-74, 78-82, 83-90', " + 
                "'SKIP_HEADER', " + 
                "'i: int, l: long, f: float, d: double, bit: int, b: boolean, dt: datetime, c_arr: chararray, b_arr: bytearray'" + 
            ");"
        );

        pig.registerQuery(
            "projection = foreach data generate $1, $3, $7;"
        );

        Iterator<Tuple> projection = pig.openIterator("projection");
        String[] expected = {
            "(1234567890000,3.141593,avert)",
            "(1234567890000,3.141593,avert)",
            "(1234567890000,3.141593,avert)",
            "(1234567890000,3.141593,)",
            "(1234567890000,,)" 
        };

        Assert.assertEquals(StringUtils.join(expected, "\n"), StringUtils.join(projection, "\n"));
    }
}
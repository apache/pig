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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.pig.PigServer;
import org.apache.pig.backend.hadoop.DateTimeWritable;
import org.apache.pig.builtin.ToDate;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.util.UDFContext;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import static org.junit.Assert.assertEquals;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TestDateTimeLocal {

    private static PigServer pigServer;
    private static PigServer pigServerLocal;
    private static File tmpFile;
    private static DateTimeZone currentDTZ;

    @BeforeClass
    public static void setUp() throws Exception {
        pigServerLocal = new PigServer(Util.getLocalTestMode(), new Properties());
        currentDTZ = DateTimeZone.getDefault();

        tmpFile = File.createTempFile("test", "txt");
        PrintStream ps = new PrintStream(new FileOutputStream(tmpFile));
        ps.println("1970-01-01T00:00:00.000");
        ps.println("1970-01-01T00:00:00.000Z");
        ps.println("1970-01-03T00:00:00.000");
        ps.println("1970-01-03T00:00:00.000Z");
        ps.println("1970-01-05T00:00:00.000");
        ps.println("1970-01-05T00:00:00.000Z");
        // for testing DST
        ps.println("2014-02-01T00:00:00.000"); // EST
        ps.println("2014-06-01T00:00:00.000"); // EDT
        ps.close();
    }

    @AfterClass
    public static void oneTimeTearDown() throws Exception {
        tmpFile.delete();
    }

    @After
    public void restoreDefaultTZ() throws Exception {
        DateTimeZone.setDefault(currentDTZ);
    }

    @Before
    public void cleanUpTmpFiles() throws Exception {
        FileLocalizer.deleteTempFiles();
    }

    @Test
    public void testLocalExecution() throws Exception {
        Iterator<Tuple> expectedItr = generateExpectedResults(DateTimeZone
                .forOffsetMillis(DateTimeZone.forID("+08:00").getOffset(null)));
        pigServerLocal.getPigContext().getProperties().setProperty("pig.datetime.default.tz", "+08:00");
        pigServerLocal.registerQuery("a = load '"
                + Util.encodeEscape(Util.generateURI(tmpFile.toString(), pigServerLocal.getPigContext()))
                + "' as (test:datetime);");
        pigServerLocal.registerQuery("b = filter a by test < ToDate('1970-01-04T00:00:00.000');");
        Iterator<Tuple> actualItr = pigServerLocal.openIterator("b");
        while (expectedItr.hasNext() && actualItr.hasNext()) {
            Tuple expectedTuple = expectedItr.next();
            Tuple actualTuple = actualItr.next();
            assertEquals(expectedTuple, actualTuple);
        }
        assertEquals(expectedItr.hasNext(), actualItr.hasNext());
    }


    @Test
    public void testZoneDST() throws Exception {
        String defaultDTZ = "America/New_York"; // a timezone that uses DST
        pigServerLocal.getPigContext().getProperties().setProperty("pig.datetime.default.tz", defaultDTZ);
        pigServerLocal.registerQuery("a = load '"
                + Util.encodeEscape(Util.generateURI(tmpFile.toString(), pigServerLocal.getPigContext()))
                + "' as (test:datetime);");
        pigServerLocal.registerQuery("b = filter a by test > ToDate('2014-01-01T00:00:00.000');");
        pigServerLocal.registerQuery("c = foreach b generate ToString(test, 'Z') as tz;");
        Iterator<Tuple> actualItr = pigServerLocal.openIterator("c");

        Tuple est = actualItr.next();
        assertEquals(Util.buildTuple("-0500"), est);
        Tuple edt = actualItr.next();
        assertEquals(Util.buildTuple("-0400"), edt);
    }

    private static Iterator<Tuple> generateExpectedResults(DateTimeZone dtz)
            throws Exception {
        List<Tuple> expectedResults = new ArrayList<Tuple>();
        expectedResults.add(Util.buildTuple(new DateTime(
                "1970-01-01T00:00:00.000", dtz)));
        expectedResults.add(Util.buildTuple(new DateTime(
                "1970-01-01T00:00:00.000", DateTimeZone.UTC)));
        expectedResults.add(Util.buildTuple(new DateTime(
                "1970-01-03T00:00:00.000", dtz)));
        expectedResults.add(Util.buildTuple(new DateTime(
                "1970-01-03T00:00:00.000", DateTimeZone.UTC)));
        return expectedResults.iterator();
    }
}

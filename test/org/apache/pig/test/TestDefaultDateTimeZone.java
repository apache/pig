/**
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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import junit.framework.TestCase;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.builtin.ToDate;
import org.apache.pig.data.Tuple;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestDefaultDateTimeZone extends TestCase {

    private File tmpFile;

    @Before
    public void setUp() throws Exception {
        tmpFile = File.createTempFile("test", "txt");
        PrintStream ps = new PrintStream(new FileOutputStream(tmpFile));
        ps.println("1970-01-01T00:00:00.000");
        ps.println("1970-01-01T00:00:00.000Z");
        ps.println("1970-01-03T00:00:00.000");
        ps.println("1970-01-03T00:00:00.000Z");
        ps.println("1970-01-05T00:00:00.000");
        ps.println("1970-01-05T00:00:00.000Z");
        ps.close();

    }

    @After
    public void tearDown() throws Exception {
        tmpFile.delete();
    }

    @Test
    public void testLocalExecution() throws Exception {
        Iterator<Tuple> expectedItr = generateExpectedResults(DateTimeZone
                .forOffsetMillis(DateTimeZone.forID("+08:00").getOffset(null)));
        Properties config = new Properties();
        config.setProperty("pig.datetime.default.tz", "+08:00");
        PigServer pig = new PigServer(ExecType.LOCAL, config);
        pig.registerQuery("a = load '"
                + Util.encodeEscape(Util.generateURI(tmpFile.toString(), pig.getPigContext()))
                + "' as (test:datetime);");
        pig.registerQuery("b = filter a by test < ToDate('1970-01-04T00:00:00.000');");
        Iterator<Tuple> actualItr = pig.openIterator("b");
        while (expectedItr.hasNext() && actualItr.hasNext()) {
            Tuple expectedTuple = expectedItr.next();
            Tuple actualTuple = actualItr.next();
            assertEquals(expectedTuple, actualTuple);
        }
        assertEquals(expectedItr.hasNext(), actualItr.hasNext());
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

    public void testTimeZone() throws IOException {
        // Usually set through "pig.datetime.default.tz"
        String defaultDTZ = "+03:00";
        DateTimeZone.setDefault(DateTimeZone.forOffsetMillis(DateTimeZone.forID(defaultDTZ)
                .getOffset(null)));
        String[] inputs = {
                "1970-01-01T00:00:00.000-08:00",
                "1970-01-01T00:00",
                "1970-01-01T00",
                "1970-01-01T",
                "1970-01T",
                "1970T",
                "1970-01-01T00:00-08:00",
                "1970-01-01T00-05:00",
                "1970-01-01T-08:00",
                "1970-01T-08:00",
                //"1970T+8:00", //Invalid format
                "1970-01-01",
                "1970-01",
                "1970",
        };
        String[] expectedDTZOutputs = {
                "-08:00",
                defaultDTZ,
                defaultDTZ,
                defaultDTZ,
                defaultDTZ,
                defaultDTZ,
                "-08:00",
                "-05:00",
                "-08:00",
                "-08:00",
                defaultDTZ,
                defaultDTZ,
                defaultDTZ
        };
        String[] expectedDTOutputs = {
                "1970-01-01T00:00:00.000-08:00",
                "1970-01-01T00:00:00.000" + defaultDTZ,
                "1970-01-01T00:00:00.000" + defaultDTZ,
                "1970-01-01T00:00:00.000" + defaultDTZ,
                "1970-01-01T00:00:00.000" + defaultDTZ,
                "1970-01-01T00:00:00.000" + defaultDTZ,
                "1970-01-01T00:00:00.000-08:00",
                "1970-01-01T00:00:00.000-05:00",
                "1970-01-01T00:00:00.000-08:00",
                "1970-01-01T00:00:00.000-08:00",
                "1970-01-01T00:00:00.000" + defaultDTZ,
                "1970-01-01T00:00:00.000" + defaultDTZ,
                "1970-01-01T00:00:00.000" + defaultDTZ
        };

        for( int i = 0; i < inputs.length; i++ ) {
            DateTimeZone dtz = ToDate.extractDateTimeZone( inputs[i] );
            assertEquals( expectedDTZOutputs[i], dtz.toString() );
            DateTime dt = ToDate.extractDateTime( inputs[i] );
            assertEquals( expectedDTOutputs[i], dt.toString() );
            System.out.println( "\"" + dt + "\"," );
        }

    }

}

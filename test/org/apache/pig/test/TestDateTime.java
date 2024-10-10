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
public class TestDateTime {

    private static MiniGenericCluster cluster;
    private static PigServer pigServer;
    private static File tmpFile;
    private static DateTimeZone currentDTZ;

    @BeforeClass
    public static void setUp() throws Exception {
        cluster = MiniGenericCluster.buildCluster();
        pigServer = new PigServer(cluster.getExecType(), cluster.getProperties());
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
        cluster.shutDown();
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
    public void testDateTimeWritables() throws IOException {
        Configuration jobConf = new Configuration();

        DateTimeWritable.setupAvailableZoneIds();
        UDFContext.getUDFContext().addJobConf(jobConf);

        String[] testZones = new String[]{"America/Los_Angeles", "Europe/London", "Europe/Budapest", "Japan"};

        DateTime testInputBase = DateTime.now();
        for (String zoneId : testZones){
            DateTimeZone customTZ = DateTimeZone.forID(zoneId);
            //Test with timezone name as zone ID
            testDateTimeForZone(new DateTime(testInputBase).withZoneRetainFields(customTZ));
            //Test with offset as zone ID
            testDateTimeForZone(new DateTime(testInputBase, DateTimeZone.forOffsetMillis(customTZ.getOffset(testInputBase))));
        }

    }

    private void testDateTimeForZone(DateTime testIn) throws IOException {
        DateTimeWritable in = new DateTimeWritable(testIn);

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        DataOutputStream dataOut = new DataOutputStream(outputStream);
        in.write(dataOut);
        dataOut.flush();

        // read from byte[]
        DateTimeWritable out = new DateTimeWritable();
        ByteArrayInputStream inputStream = new ByteArrayInputStream(
                outputStream.toByteArray());
        DataInputStream dataIn = new DataInputStream(inputStream);
        out.readFields(dataIn);

        assertEquals(in.get(), out.get());
    }

    /**
     * Tests DateTimeWritables on cluster
     * @throws Exception
     * Below input will trigger DateTime instances with both long zone id (UTC) and short offset (+01:00)
     */
    @Test
    public void testDateTimeZoneOnCluster() throws Exception {

        String localDateTime = "2001-01-01T01:00:00.000";
        String localDateTimeDST = "2002-07-01T01:00:00.000";

        String inputFileName = "testDateTime-input.txt";
        String[] inputData = new String[]{  "1\t1990-01-04T12:30:00.000+01:00",
                "2\t1990-01-04T11:30:00.000Z",
                "3\t"+localDateTime,
                "4\t"+localDateTimeDST,
                "5\t2017-02-02T15:19:00.000+01:00"
        };
        Util.createInputFile(cluster, inputFileName, inputData);

        String script = "A = LOAD '"+inputFileName+"' AS (foo:int, sd:datetime);" +
                "B = group A by sd;" +
                "C = foreach B generate group, MAX(A.foo);";

        Util.registerMultiLineQuery(pigServer, script);

        Iterator<Tuple> it = pigServer.openIterator("C");

        //Should return last 4 rows from input
        String tzOffsetForLocal = DateTimeZone.forOffsetMillis(DateTime.now().getZone().getOffset(new DateTime(localDateTime))).toString();
        String tzOffsetDSTForLocal = DateTimeZone.forOffsetMillis(DateTime.now().getZone().getOffset(new DateTime(localDateTimeDST))).toString();
        tzOffsetForLocal = tzOffsetForLocal.replaceAll("UTC","Z");
        tzOffsetDSTForLocal = tzOffsetDSTForLocal.replaceAll("UTC","Z");

        Util.checkQueryOutputsAfterSortRecursive(
                it,
                new String[]{
                        "(1990-01-04T11:30:00.000Z,2)",
                        "(2001-01-01T01:00:00.000"+tzOffsetForLocal+",3)",
                        "(2002-07-01T01:00:00.000"+tzOffsetDSTForLocal+",4)",
                        "(2017-02-02T15:19:00.000+01:00,5)"
                },
                org.apache.pig.newplan.logical.Util.translateSchema(pigServer.dumpSchema("C")));
    }

    @Test
    public void testTimeZone() throws IOException {
        // Usually set through "pig.datetime.default.tz"
        String defaultDTZ = "+03:00";
        DateTimeZone.setDefault(DateTimeZone.forID(defaultDTZ));
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

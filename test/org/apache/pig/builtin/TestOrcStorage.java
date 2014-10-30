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
package org.apache.pig.builtin;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.CompressionKind;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.ql.io.orc.RecordReader;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.data.BinSedesTuple;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.DefaultDataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.test.Util;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestOrcStorage {
    final protected static Log LOG = LogFactory.getLog(TestOrcStorage.class);

    final private static String basedir = "test/org/apache/pig/builtin/orc/";
    final private static String outbasedir = System.getProperty("user.dir") + "/build/test/TestOrcStorage/";

    private static String INPUT1 = outbasedir + "TestOrcStorage_1";
    private static String OUTPUT1 = outbasedir + "TestOrcStorage_2";
    private static String OUTPUT2 = outbasedir + "TestOrcStorage_3";
    private static String OUTPUT3 = outbasedir + "TestOrcStorage_4";
    private static String OUTPUT4 = outbasedir + "TestOrcStorage_5";

    private static PigServer pigServer = null;
    private static FileSystem fs;

    @BeforeClass
    public static void oneTimeSetup(){
        if(Util.WINDOWS){
            INPUT1 = INPUT1.replace("\\", "/");
            OUTPUT1 = OUTPUT1.replace("\\", "/");
            OUTPUT2 = OUTPUT2.replace("\\", "/");
            OUTPUT3 = OUTPUT3.replace("\\", "/");
            OUTPUT4 = OUTPUT4.replace("\\", "/");
        }
    }

    @Before
    public void setup() throws ExecException, IOException {
        pigServer = new PigServer(ExecType.LOCAL);
        fs = FileSystem.get(ConfigurationUtil.toConfiguration(pigServer.getPigContext().getProperties()));
        deleteTestFiles();
        pigServer.mkdirs(outbasedir);
        generateInputFiles();
    }

    @After
    public void teardown() throws IOException {
        if(pigServer != null) {
            pigServer.shutdown();
        }
        deleteTestFiles();
    }

    private void generateInputFiles() throws IOException {
        String[] input = {"65536\tworld", "1\thello"};
        Util.createLocalInputFile(INPUT1, input);
    }

    private static void deleteTestFiles() throws IOException {
        Util.deleteDirectory(new File(outbasedir));
    }

    @Test
    public void testSimpleLoad() throws Exception {
        pigServer.registerQuery("A = load '" + basedir + "orc-file-11-format.orc'" + " using OrcStorage();" );
        Schema s = pigServer.dumpSchema("A");
        assertEquals(s.toString(), "{boolean1: boolean,byte1: int,short1: int,int1: int,long1: long," +
                "float1: float,double1: double,bytes1: bytearray,string1: chararray," +
                "middle: (list: {(int1: int,string1: chararray)}),list: {(int1: int,string1: chararray)}," +
                "map: map[(int1: int,string1: chararray)],ts: datetime,decimal1: bigdecimal}");
        Iterator<Tuple> iter = pigServer.openIterator("A");

        verifyData(new Path(basedir + "orc-file-11-format.orc"), iter, fs, 7500);
    }

    @Test
    public void testJoinWithPruning() throws Exception {
        pigServer.registerQuery("A = load '" + basedir + "orc-file-11-format.orc'" + " using OrcStorage();" );
        pigServer.registerQuery("B = foreach A generate int1, string1;");
        pigServer.registerQuery("C = order B by int1;");
        pigServer.registerQuery("D = limit C 10;");
        pigServer.registerQuery("E = load '" + INPUT1 + "' as (e0:int, e1:chararray);");
        pigServer.registerQuery("F = join D by int1, E by e0;");
        Iterator<Tuple> iter = pigServer.openIterator("F");
        int count=0;
        Tuple t=null;
        while (iter.hasNext()) {
            t = iter.next();
            assertEquals(t.size(), 4);
            count++;
        }
        assertEquals(count, 10);
    }

    @Test
    // See PIG-4195
    public void testCharVarchar() throws Exception {
        pigServer.registerQuery("A = load '" + basedir + "charvarchar.orc'" + " using OrcStorage();" );
        Schema schema = pigServer.dumpSchema("A");
        assertEquals(schema.size(), 4);
        assertEquals(schema.getField(0).type, DataType.CHARARRAY);
        assertEquals(schema.getField(1).type, DataType.CHARARRAY);
        Iterator<Tuple> iter = pigServer.openIterator("A");
        int count=0;
        Tuple t=null;
        while (iter.hasNext()) {
            t = iter.next();
            assertEquals(t.size(), 4);
            assertTrue(t.get(0) instanceof String);
            assertTrue(t.get(1) instanceof String);
            assertEquals(((String)t.get(1)).length(), 20);
            count++;
        }
        assertEquals(count, 10000);
    }

    @Test
    // See PIG-4218
    public void testNullMapKey() throws Exception {
        pigServer.registerQuery("A = load '" + basedir + "nullmapkey.orc'" + " using OrcStorage();" );
        Iterator<Tuple> iter = pigServer.openIterator("A");
        assertEquals(iter.next().toString(), "([hello#world])");
        assertEquals(iter.next().toString(), "([])");
        assertFalse(iter.hasNext());
    }

    @Test
    public void testSimpleStore() throws Exception {
        pigServer.registerQuery("A = load '" + INPUT1 + "' as (a0:int, a1:chararray);");
        pigServer.store("A", OUTPUT1, "OrcStorage");
        Path outputFilePath = new Path(new Path(OUTPUT1), "part-m-00000");
        Reader reader = OrcFile.createReader(fs, outputFilePath);
        assertEquals(reader.getNumberOfRows(), 2);

        RecordReader rows = reader.rows(null);
        Object row = rows.next(null);
        StructObjectInspector soi = (StructObjectInspector)reader.getObjectInspector();
        IntWritable intWritable = (IntWritable)soi.getStructFieldData(row,
                soi.getAllStructFieldRefs().get(0));
        Text text = (Text)soi.getStructFieldData(row,
                soi.getAllStructFieldRefs().get(1));
        assertEquals(intWritable.get(), 65536);
        assertEquals(text.toString(), "world");

        row = rows.next(null);
        intWritable = (IntWritable)soi.getStructFieldData(row,
                soi.getAllStructFieldRefs().get(0));
        text = (Text)soi.getStructFieldData(row,
                soi.getAllStructFieldRefs().get(1));
        assertEquals(intWritable.get(), 1);
        assertEquals(text.toString(), "hello");

        // A bug in ORC InputFormat does not allow empty file in input directory
        fs.delete(new Path(OUTPUT1, "_SUCCESS"), true);

        // Read the output file back
        pigServer.registerQuery("A = load '" + OUTPUT1 + "' using OrcStorage();");
        Schema s = pigServer.dumpSchema("A");
        assertEquals(s.toString(), "{a0: int,a1: chararray}");
        Iterator<Tuple> iter = pigServer.openIterator("A");
        Tuple t = iter.next();
        assertEquals(t.size(), 2);
        assertEquals(t.get(0), 65536);
        assertEquals(t.get(1), "world");

        t = iter.next();
        assertEquals(t.size(), 2);
        assertEquals(t.get(0), 1);
        assertEquals(t.get(1), "hello");

        assertFalse(iter.hasNext());
        rows.close();
    }

    @Test
    public void testMultiStore() throws Exception {
        pigServer.setBatchOn();
        pigServer.registerQuery("A = load '" + INPUT1 + "' as (a0:int, a1:chararray);");
        pigServer.registerQuery("B = order A by a0;");
        pigServer.registerQuery("store B into '" + OUTPUT2 + "' using OrcStorage();");
        pigServer.registerQuery("store B into '" + OUTPUT3 +"' using OrcStorage('-c SNAPPY');");
        pigServer.executeBatch();

        Path outputFilePath = new Path(new Path(OUTPUT2), "part-r-00000");
        Reader reader = OrcFile.createReader(fs, outputFilePath);
        assertEquals(reader.getNumberOfRows(), 2);
        assertEquals(reader.getCompression(), CompressionKind.ZLIB);

        Path outputFilePath2 = new Path(new Path(OUTPUT3), "part-r-00000");
        reader = OrcFile.createReader(fs, outputFilePath2);
        assertEquals(reader.getNumberOfRows(), 2);
        assertEquals(reader.getCompression(), CompressionKind.SNAPPY);

        verifyData(outputFilePath, outputFilePath2, fs, 2);
    }

    @Test
    public void testMultipleLoadStore() throws Exception {
        pigServer.registerQuery("A = load '" + basedir + "orc-file-11-format.orc'" + " using OrcStorage();" );
        pigServer.registerQuery("store A into '" + OUTPUT1 + "' using OrcStorage();");
        pigServer.registerQuery("B = load '" + OUTPUT1 + "' using OrcStorage();");
        verifyData(new Path(basedir + "orc-file-11-format.orc"), pigServer.openIterator("B"), fs, 7500);
    }

    @Test
    public void testLoadStoreMoreDataType() throws Exception {
        pigServer.registerQuery("A = load '" + basedir + "orc-file-11-format.orc'" + " using OrcStorage();" );
        pigServer.registerQuery("B = foreach A generate boolean1..double1, '' as bytes1, string1..;");
        pigServer.store("B", OUTPUT4, "OrcStorage");

        // A bug in ORC InputFormat does not allow empty file in input directory
        fs.delete(new Path(OUTPUT4, "_SUCCESS"), true);

        pigServer.registerQuery("A = load '" + OUTPUT4 + "' using OrcStorage();" );
        Iterator<Tuple> iter = pigServer.openIterator("A");
        Tuple t = iter.next();
        assertTrue(t.toString().startsWith("(false,1,1024,65536,9223372036854775807,1.0,-15.0," +
                ",hi,({(1,bye),(2,sigh)}),{(3,good),(4,bad)},[],"));
        assertTrue(t.get(12).toString().matches("2000-03-12T15:00:00.000.*"));
        assertTrue(t.toString().endsWith(",12345678.6547456)"));
    }

    private void verifyData(Path orcFile, Iterator<Tuple> iter, FileSystem fs, int expectedTotalRows) throws Exception {

        int expectedRows = 0;
        int actualRows = 0;
        Reader orcReader = OrcFile.createReader(fs, orcFile);
        ObjectInspector oi = orcReader.getObjectInspector();
        StructObjectInspector soi = (StructObjectInspector) oi;

        RecordReader reader = orcReader.rows(null);
        Object row = null;

        while (reader.hasNext()) {
            row = reader.next(row);
            expectedRows++;
            List<?> orcRow = soi.getStructFieldsDataAsList(row);
            if (!iter.hasNext()) {
                break;
            }
            Tuple t = iter.next();
            assertEquals(orcRow.size(), t.size());
            actualRows++;

            for (int i = 0; i < orcRow.size(); i++) {
                Object expected = orcRow.get(i);
                Object actual = t.get(i);
                compareData(expected, actual);
            }
        }
        assertFalse(iter.hasNext());
        assertEquals(expectedRows, actualRows);
        assertEquals(expectedTotalRows, actualRows);

    }

    private void verifyData(Path orcFile, Path pigOrcFile, FileSystem fs, int expectedTotalRows) throws Exception {

        int expectedRows = 0;
        int actualRows = 0;
        Reader orcReaderExpected = OrcFile.createReader(fs, orcFile);
        StructObjectInspector soiExpected = (StructObjectInspector) orcReaderExpected.getObjectInspector();
        Reader orcReaderActual = OrcFile.createReader(fs, orcFile);
        StructObjectInspector soiActual = (StructObjectInspector) orcReaderActual.getObjectInspector();

        RecordReader readerExpected = orcReaderExpected.rows(null);
        Object expectedRow = null;
        RecordReader readerActual = orcReaderActual.rows(null);
        Object actualRow = null;

        while (readerExpected.hasNext()) {
            expectedRow = readerExpected.next(expectedRow);
            expectedRows++;
            List<?> orcRowExpected = soiExpected.getStructFieldsDataAsList(expectedRow);
            if (!readerActual.hasNext()) {
                break;
            }
            actualRow = readerActual.next(actualRow);
            actualRows++;
            List<?> orcRowActual = soiActual.getStructFieldsDataAsList(actualRow);
            assertEquals(orcRowExpected.size(), orcRowActual.size());

            for (int i = 0; i < orcRowExpected.size(); i++) {
                assertEquals(orcRowExpected.get(i), orcRowActual.get(i));
            }
        }
        assertFalse(readerActual.hasNext());
        assertEquals(expectedRows, actualRows);
        assertEquals(expectedTotalRows, actualRows);

        readerExpected.close();
        readerActual.close();
    }

    @SuppressWarnings("rawtypes")
    private void compareData(Object expected, Object actual) {
        if (expected instanceof Text) {
            assertEquals(String.class, actual.getClass());
            assertEquals(expected.toString(), actual);
        } else if (expected instanceof ShortWritable) {
            assertEquals(Integer.class, actual.getClass());
            assertEquals((int)((ShortWritable) expected).get(), actual);
        } else if (expected instanceof IntWritable) {
            assertEquals(Integer.class, actual.getClass());
            assertEquals(((IntWritable) expected).get(), actual);
        } else if (expected instanceof LongWritable) {
            assertEquals(Long.class, actual.getClass());
            assertEquals(((LongWritable) expected).get(), actual);
        } else if (expected instanceof FloatWritable) {
            assertEquals(Float.class, actual.getClass());
            assertEquals(((FloatWritable) expected).get(), actual);
        } else if (expected instanceof HiveDecimalWritable) {
            assertEquals(BigDecimal.class, actual.getClass());
            assertEquals(((HiveDecimalWritable) expected).toString(), actual.toString());
        } else if (expected instanceof DoubleWritable) {
            assertEquals(Double.class, actual.getClass());
            assertEquals(((DoubleWritable) expected).get(), actual);
        } else if (expected instanceof BooleanWritable) {
            assertEquals(Boolean.class, actual.getClass());
            assertEquals(((BooleanWritable) expected).get(), actual);
        } else if (expected instanceof TimestampWritable) {
            assertEquals(DateTime.class, actual.getClass());
            assertEquals(((TimestampWritable) expected).getTimestamp().getTime(),
                    ((DateTime) actual).getMillis());
        } else if (expected instanceof BytesWritable) {
            assertEquals(DataByteArray.class, actual.getClass());
            BytesWritable bw = (BytesWritable) expected;
            assertEquals(new DataByteArray(bw.getBytes(), 0, bw.getLength()), actual);
        } else if (expected instanceof ByteWritable) {
            assertEquals(Integer.class, actual.getClass());
            assertEquals((int) ((ByteWritable) expected).get(), actual);
        } else if (expected instanceof OrcStruct) {
            assertEquals(BinSedesTuple.class, actual.getClass());
            // TODO: compare actual values. No getters in OrcStruct
        } else if (expected instanceof ArrayList) {
            assertEquals(DefaultDataBag.class, actual.getClass());
            // TODO: compare actual values. No getters in OrcStruct
        } else if (expected instanceof HashMap) {
            assertEquals(HashMap.class, actual.getClass());
            assertEquals(((HashMap) expected).size(), ((HashMap) actual).size());
            // TODO: compare actual values. No getters in OrcStruct
        } else if (expected == null) {
            assertEquals(expected, actual);
        } else {
            Assert.fail("Unknown object type: " + expected.getClass().getName());
        }
    }

}

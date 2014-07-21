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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.CompressionKind;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.ql.io.orc.RecordReader;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.test.Util;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Before;
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
    
    @Before
    public void setup() throws ExecException, IOException {
        pigServer = new PigServer(ExecType.LOCAL);
        fs = FileSystem.get(ConfigurationUtil.toConfiguration(pigServer.getPigContext().getProperties()));
        deleteTestFiles();
        pigServer.mkdirs(outbasedir);
        generateInputFiles();
        if(Util.WINDOWS){
            INPUT1 = INPUT1.replace("\\", "/");
            OUTPUT1 = OUTPUT1.replace("\\", "/");
            OUTPUT2 = OUTPUT2.replace("\\", "/");
            OUTPUT3 = OUTPUT3.replace("\\", "/");
            OUTPUT4 = OUTPUT4.replace("\\", "/");
        }
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
        int count=0;
        Tuple t;
        while (iter.hasNext()) {
            t = (Tuple)iter.next();
            assertEquals(t.size(), 14);
            assertTrue(t.get(0) instanceof Boolean);
            assertTrue(t.get(1) instanceof Integer);
            assertTrue(t.get(2) instanceof Integer);
            assertTrue(t.get(3) instanceof Integer);
            assertTrue(t.get(4) instanceof Long);
            assertTrue(t.get(5) instanceof Float);
            assertTrue(t.get(6) instanceof Double);
            assertTrue(t.get(7) instanceof DataByteArray);
            assertTrue(t.get(8) instanceof String);
            assertTrue(t.get(9) instanceof Tuple);
            assertTrue(t.get(10) instanceof DataBag);
            assertTrue(t.get(11) instanceof Map);
            assertTrue(t.get(12) instanceof DateTime);
            assertTrue(t.get(13) instanceof BigDecimal);
            count++;
        }
        assertEquals(count, 7500);
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
        fs.delete(new Path(OUTPUT1, "_SUCCESS"));
        
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
        
        outputFilePath = new Path(new Path(OUTPUT3), "part-r-00000");
        reader = OrcFile.createReader(fs, outputFilePath);
        assertEquals(reader.getNumberOfRows(), 2);
        assertEquals(reader.getCompression(), CompressionKind.SNAPPY);
    }
    
    
    @Test
    public void testLoadStoreMoreDataType() throws Exception {
        pigServer.registerQuery("A = load '" + basedir + "orc-file-11-format.orc'" + " using OrcStorage();" );
        pigServer.registerQuery("B = foreach A generate boolean1..double1, '' as bytes1, string1..;");
        pigServer.store("B", OUTPUT4, "OrcStorage");
        
        // A bug in ORC InputFormat does not allow empty file in input directory
        fs.delete(new Path(OUTPUT4, "_SUCCESS"));
        
        pigServer.registerQuery("A = load '" + OUTPUT4 + "' using OrcStorage();" );
        Schema s = pigServer.dumpSchema("A");
        Iterator<Tuple> iter = pigServer.openIterator("A");
        Tuple t = iter.next();
        assertTrue(t.toString().startsWith("(false,1,1024,65536,9223372036854775807,1.0,-15.0," +
                ",hi,({(1,bye),(2,sigh)}),{(3,good),(4,bad)},[],"));
        assertTrue(t.get(12).toString().matches("2000-03-12T00:00:00.000.*"));
        assertTrue(t.toString().endsWith(",12345678.6547456)"));
    }
}

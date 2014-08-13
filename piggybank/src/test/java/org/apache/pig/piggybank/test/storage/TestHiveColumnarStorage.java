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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.regex.Pattern;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.hive.ql.io.RCFileOutputFormat;
import org.apache.hadoop.hive.ql.io.RCFileRecordReader;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe;
import org.apache.hadoop.hive.serde2.columnar.ColumnarStruct;
import org.apache.hadoop.hive.serde2.lazy.LazyArray;
import org.apache.hadoop.hive.serde2.lazy.LazyMap;
import org.apache.hadoop.hive.serde2.lazy.LazyString;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.apache.pig.piggybank.storage.hiverc.HiveRCSchemaUtil;
import org.apache.pig.test.Util;
import org.junit.Test;

public class TestHiveColumnarStorage extends TestCase {

    static Configuration conf = null;
    static private FileSystem fs;

    static File simpleDataFile = null;
    static File simpleDataDir = null;

    static int simpleDirFileCount = 3;
    static int simpleRowCount = 10;
    static int columnCount = 3;


    @Override
    public synchronized void setUp() throws Exception {

        conf = new Configuration();

        fs = LocalFileSystem.getLocal(conf);

        produceSimpleData();
       // Util.deleteDirectory(new File("testhiveColumnarStore"));
    }

    @Override
    public void tearDown() {
        Util.deleteDirectory(simpleDataDir);
        Util.deleteDirectory(new File("testhiveColumnarStore"));
        simpleDataFile.delete();
    }

    @Test
    public void testShouldStoreRowInHiveFormat() throws IOException, InterruptedException, SerDeException {
        String loadString = "org.apache.pig.piggybank.storage.HiveColumnarLoader('f1 string,f2 string,f3 string')";
        String storeString = "org.apache.pig.piggybank.storage.HiveColumnarStorage()";

        String singlePartitionedFile = simpleDataFile.getAbsolutePath();
        File outputFile = new File("testhiveColumnarStore");

        PigServer server = new PigServer(ExecType.LOCAL);
        server.setBatchOn();
        server.registerQuery("a = LOAD '" + Util.encodeEscape(singlePartitionedFile) + "' using " + loadString
                + ";");

        //when
        server.store("a", outputFile.getAbsolutePath(), storeString);

        //then
        Path outputPath = new Path(outputFile.getAbsolutePath()+"/part-m-00000.rc");

        ColumnarStruct struct = readRow(outputFile, outputPath, "f1 string,f2 string,f3 string");

        assertEquals(3, struct.getFieldsAsList().size());
        Object o =  struct.getField(0);
        assertEquals(LazyString.class, o.getClass());
        o =  struct.getField(1);
        assertEquals(LazyString.class, o.getClass());
        o =  struct.getField(2);
        assertEquals(LazyString.class, o.getClass());

    }
    @Test
    public void testShouldStoreTupleAsHiveArray() throws IOException, InterruptedException, SerDeException {
        String loadString = "org.apache.pig.piggybank.storage.HiveColumnarLoader('f1 string,f2 string,f3 string')";
        String storeString = "org.apache.pig.piggybank.storage.HiveColumnarStorage()";

        String singlePartitionedFile = simpleDataFile.getAbsolutePath();
        File outputFile = new File("testhiveColumnarStore");

        PigServer server = new PigServer(ExecType.LOCAL);
        server.setBatchOn();
        server.registerQuery("a = LOAD '" + Util.encodeEscape(singlePartitionedFile) + "' using " + loadString
                + ";");
        server.registerQuery("b = FOREACH a GENERATE f1, TOTUPLE(f2,f3);");

        //when
        server.store("b", outputFile.getAbsolutePath(), storeString);

        //then
        Path outputPath = new Path(outputFile.getAbsolutePath()+"/part-m-00000.rc");

        ColumnarStruct struct = readRow(outputFile, outputPath, "f1 string,f2 array<string>");

        assertEquals(2, struct.getFieldsAsList().size());
        Object o =  struct.getField(0);
        assertEquals(LazyString.class, o.getClass());
        o =  struct.getField(1);
        assertEquals(LazyArray.class, o.getClass());

        LazyArray arr = (LazyArray)o;
        List<Object> values = arr.getList();
        for(Object value : values) {
            assertEquals(LazyString.class, value.getClass());
            String valueStr =((LazyString) value).getWritableObject().toString();
            assertEquals("Sample value", valueStr);
        }

    }
    @Test
    public void testShouldStoreBagAsHiveArray() throws IOException, InterruptedException, SerDeException {
        String loadString = "org.apache.pig.piggybank.storage.HiveColumnarLoader('f1 string,f2 string,f3 string')";
        String storeString = "org.apache.pig.piggybank.storage.HiveColumnarStorage()";

        String singlePartitionedFile = simpleDataFile.getAbsolutePath();
        File outputFile = new File("testhiveColumnarStore");

        PigServer server = new PigServer(ExecType.LOCAL);
        server.setBatchOn();
        server.registerQuery("a = LOAD '" + Util.encodeEscape(singlePartitionedFile) + "' using " + loadString
                + ";");
        server.registerQuery("b = FOREACH a GENERATE f1, TOBAG(f2,f3);");

        //when
        server.store("b", outputFile.getAbsolutePath(), storeString);

        //then
        Path outputPath = new Path(outputFile.getAbsolutePath()+"/part-m-00000.rc");

        ColumnarStruct struct = readRow(outputFile, outputPath, "f1 string,f2 array<string>");

        assertEquals(2, struct.getFieldsAsList().size());
        Object o =  struct.getField(0);
        assertEquals(LazyString.class, o.getClass());
        o =  struct.getField(1);
        assertEquals(LazyArray.class, o.getClass());

        LazyArray arr = (LazyArray)o;
        List<Object> values = arr.getList();
        for(Object value : values) {
            assertEquals(LazyString.class, value.getClass());
            String valueStr =((LazyString) value).getWritableObject().toString();
            assertEquals("Sample value", valueStr);
        }

    }
    @Test
    public void testShouldStoreMapAsHiveMap() throws IOException, InterruptedException, SerDeException {
        String loadString = "org.apache.pig.piggybank.storage.HiveColumnarLoader('f1 string,f2 string,f3 string')";
        String storeString = "org.apache.pig.piggybank.storage.HiveColumnarStorage()";

        String singlePartitionedFile = simpleDataFile.getAbsolutePath();
        File outputFile = new File("testhiveColumnarStore");

        PigServer server = new PigServer(ExecType.LOCAL);
        server.setBatchOn();
        server.registerQuery("a = LOAD '" + Util.encodeEscape(singlePartitionedFile) + "' using " + loadString
                + ";");
        server.registerQuery("b = FOREACH a GENERATE f1, TOMAP(f2,f3);");

        //when
        server.store("b", outputFile.getAbsolutePath(), storeString);

        //then
        Path outputPath = new Path(outputFile.getAbsolutePath()+"/part-m-00000.rc");

        ColumnarStruct struct = readRow(outputFile, outputPath, "f1 string,f2 map<string,string>");

        assertEquals(2, struct.getFieldsAsList().size());
        Object o =  struct.getField(0);
        assertEquals(LazyString.class, o.getClass());
        o =  struct.getField(1);
        assertEquals(LazyMap.class, o.getClass());

        LazyMap arr = (LazyMap)o;
        Map<Object,Object> values = arr.getMap();
        for(Entry<Object,Object> entry : values.entrySet()) {
            assertEquals(LazyString.class, entry.getKey().getClass());
            assertEquals(LazyString.class, entry.getValue().getClass());

            String keyStr =((LazyString) entry.getKey()).getWritableObject().toString();
            assertEquals("Sample value", keyStr);
            String valueStr =((LazyString) entry.getValue()).getWritableObject().toString();
            assertEquals("Sample value", valueStr);
        }

    }

    private ColumnarStruct readRow(File outputFile, Path outputPath, String schema) throws IOException,
            InterruptedException, SerDeException {

        FileSplit fileSplit = new FileSplit(outputPath, 0L, outputFile.length(), (String[])null);


        Path splitPath = fileSplit.getPath();

        RCFileRecordReader<LongWritable, BytesRefArrayWritable> rcFileRecordReader = new RCFileRecordReader<LongWritable, BytesRefArrayWritable>(
            new Configuration(false), new org.apache.hadoop.mapred.FileSplit(splitPath,
                fileSplit.getStart(), fileSplit.getLength(),
                new org.apache.hadoop.mapred.JobConf(conf)));

        LongWritable key = rcFileRecordReader.createKey();
        BytesRefArrayWritable value = rcFileRecordReader.createValue();
        rcFileRecordReader.next(key, value);
        rcFileRecordReader.close();

        ColumnarStruct struct = readColumnarStruct(value, schema);
        return struct;
    }

    private ColumnarStruct readColumnarStruct(BytesRefArrayWritable buff, String schema) throws SerDeException {
        Pattern pcols = Pattern.compile("[a-zA-Z_0-9]*[ ]");
        List<String> types = HiveRCSchemaUtil.parseSchemaTypes(schema);
        List<String> cols = HiveRCSchemaUtil.parseSchema(pcols, schema);

        List<FieldSchema> fieldSchemaList = new ArrayList<FieldSchema>(
            cols.size());

        for (int i = 0; i < cols.size(); i++) {
            fieldSchemaList.add(new FieldSchema(cols.get(i), HiveRCSchemaUtil
                .findPigDataType(types.get(i))));
        }

        Properties props = new Properties();

        props.setProperty(Constants.LIST_COLUMNS,
            HiveRCSchemaUtil.listToString(cols));
        props.setProperty(Constants.LIST_COLUMN_TYPES,
            HiveRCSchemaUtil.listToString(types));

        Configuration hiveConf = new HiveConf(conf, SessionState.class);
        ColumnarSerDe serde = new ColumnarSerDe();
        serde.initialize(hiveConf, props);

        return (ColumnarStruct) serde.deserialize(buff);
   }


    /**
     * Writes out a simple temporary file with 5 columns and 100 rows.<br/>
     * Data is random numbers.
     *
     * @throws SerDeException
     * @throws IOException
     */
    private static final void produceSimpleData() throws SerDeException, IOException {
        // produce on single file
        simpleDataFile = File.createTempFile("testhiveColumnarLoader", ".txt");
        simpleDataFile.deleteOnExit();

        Path path = new Path(simpleDataFile.getPath());

        writeRCFileTest(fs, simpleRowCount, path, columnCount, new DefaultCodec(), columnCount);

        // produce a folder of simple data
        simpleDataDir = new File("simpleDataDir" + System.currentTimeMillis());
        simpleDataDir.mkdir();

        for (int i = 0; i < simpleDirFileCount; i++) {

            simpleDataFile = new File(simpleDataDir, "testhiveColumnarLoader-" + i + ".txt");

            Path filePath = new Path(simpleDataFile.getPath());

            writeRCFileTest(fs, simpleRowCount, filePath, columnCount, new DefaultCodec(),
                    columnCount);

        }

    }

    private static int writeRCFileTest(FileSystem fs, int rowCount, Path file, int columnNum,
            CompressionCodec codec, int columnCount) throws IOException {
        fs.delete(file, true);
        int rowsWritten = 0;


        RCFileOutputFormat.setColumnNumber(conf, columnNum);
        RCFile.Writer writer = new RCFile.Writer(fs, conf, file, null, codec);

        byte[][] columnRandom;

        BytesRefArrayWritable bytes = new BytesRefArrayWritable(columnNum);
        columnRandom = new byte[columnNum][];
        for (int i = 0; i < columnNum; i++) {
            BytesRefWritable cu = new BytesRefWritable();
            bytes.set(i, cu);
        }

        for (int i = 0; i < rowCount; i++) {

            bytes.resetValid(columnRandom.length);
            for (int j = 0; j < columnRandom.length; j++) {
                columnRandom[j]= "Sample value".getBytes();
                bytes.get(j).set(columnRandom[j], 0, columnRandom[j].length);
            }
            rowsWritten++;
            writer.append(bytes);
        }
        writer.close();

        return rowsWritten;
    }

}

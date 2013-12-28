/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.pig.builtin;

import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.tool.DataFileWriteTool;
import org.apache.avro.tool.Tool;
import org.apache.avro.tool.TrevniCreateRandomTool;
import org.apache.avro.tool.TrevniToJsonTool;
import org.apache.avro.util.Utf8;
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.executionengine.ExecJob;
import org.apache.pig.backend.executionengine.ExecJob.JOB_STATUS;
import org.apache.pig.builtin.mock.Storage.Data;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.avro.AvroBagWrapper;
import org.apache.pig.impl.util.avro.AvroMapWrapper;
import org.apache.pig.impl.util.avro.AvroTupleWrapper;
import org.apache.pig.test.Util;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.apache.pig.builtin.mock.Storage.resetData;
import static org.apache.pig.builtin.mock.Storage.tuple;

public class TestAvroStorage {

    final protected static Log LOG = LogFactory.getLog(TestAvroStorage.class);

    final private static String basedir = "test/org/apache/pig/builtin/avro/";
    private static String outbasedir = System.getProperty("user.dir") + "/build/test/TestAvroStorage/";
    final private static String[] datadir = {
        "data/avro",
        "data/avro/compressed",
        "data/avro/compressed/snappy",
        "data/avro/compressed/deflate",
        "data/avro/uncompressed",
        "data/avro/uncompressed/testdirectory",
        "data/json/testdirectory",
        "data/trevni",
        "data/trevni/uncompressed",
    };
    final private static String[] avroSchemas = {
        "arraysAsOutputByPig",
        "arrays",
        "recordsAsOutputByPig",
        "recordsAsOutputByPigWithDates",
        "records",
        "recordsOfArrays",
        "recordsOfArraysOfRecords",
        "recordsSubSchema",
        "recordsSubSchemaNullable",
        "recordsWithDoubleUnderscores",
        "recordsWithEnums",
        "recordsWithFixed",
        "recordsWithMaps",
        "recordsWithMapsOfRecords",
        "recordsWithNullableUnions",
        "recordWithRepeatedSubRecords",
        "recursiveRecord",
        "projectionTest",
        "recordsWithSimpleUnion",
        "recordsWithSimpleUnionOutput",
    };
    final private static String[] trevniSchemas = {
        "simpleRecordsTrevni",
    };

    private static PigServer pigServerLocal = null;

    public static final PathFilter hiddenPathFilter = new PathFilter() {
        public boolean accept(Path p) {
          String name = p.getName();
          return !name.startsWith("_") && !name.startsWith(".");
        }
      };

    private static String loadFileIntoString(String file) throws IOException {
      return FileUtils.readFileToString(new File(file)).replace("\n", " ");
    }

    @BeforeClass
    public static void setup() throws ExecException, IOException {
        pigServerLocal = new PigServer(ExecType.LOCAL);
        Util.deleteDirectory(new File(outbasedir));
        generateInputFiles();
    }

    @AfterClass
    public static void teardown() throws IOException {
        if(pigServerLocal != null) {
            pigServerLocal.shutdown();
        }
        deleteInputFiles();
    }

    /**
     * Generate input files for test cases
     */
    private static void generateInputFiles() throws IOException {
        String data;
        String json;
        String avro;
        String trevni;
        String schema;

        for (String fn : datadir) {
            FileUtils.forceMkdir(new File(basedir + fn));
        }

        for (String fn : avroSchemas) {
            schema = basedir + "schema/" + fn + ".avsc";
            json = basedir + "data/json/" + fn + ".json";
            avro = basedir + "data/avro/uncompressed/" + fn + ".avro";
            LOG.info("creating " + avro);
            generateAvroFile(schema, json, avro, null);
        }

        for (String fn : trevniSchemas) {
            schema = basedir + "schema/" + fn + ".avsc";
            trevni = basedir + "data/trevni/uncompressed/" + fn + ".trevni";
            json = basedir + "data/json/" + fn + ".json";
            LOG.info("creating " + trevni);
            generateRandomTrevniFile(schema, "1000", trevni);
            LOG.info("creating " + json);
            convertTrevniToJsonFile(schema, trevni, json);

            schema = basedir + "schema/" + fn + ".avsc";
            json = basedir + "data/json/" + fn + ".json";
            avro = basedir + "data/avro/uncompressed/" + fn + ".avro";
            LOG.info("creating " + avro);
            generateAvroFile(schema, json, avro, null);
        }

        PrintWriter w;
        int itemSum = 0;
        int recordCount = 0;
        int evenFileNameItemSum = 0;
        int evenFileNameRecordCount = 0;

        for (int i = 0; i < 8; i++) {
            schema = basedir + "schema/testDirectory.avsc";
            json = basedir + "data/json/testdirectory/part-m-0000" + i;
            avro = basedir + "data/avro/uncompressed/testdirectory/part-m-0000" + i + ".avro";
            LOG.info("creating " + json);
            w = new PrintWriter(new FileWriter(json));
            for (int j = i*1000; j < (i+1)*1000; j++) {
                itemSum += j;
                recordCount += 1;
                evenFileNameItemSum += ((i+1) % 2) * j;
                evenFileNameRecordCount += (i+1) % 2;
                w.println("{\"item\" : " + j + ", \"timestamp\" : " + System.currentTimeMillis() +  " }\n");
            }
            w.close();
            generateAvroFile(schema, json, avro, null);
        }

        schema = basedir + "schema/testDirectoryCounts.avsc";
        json = basedir + "data/json/testDirectoryCounts.json";
        avro = basedir + "data/avro/uncompressed/testDirectoryCounts.avro";
        data = "{\"itemSum\" : {\"int\" : " + itemSum + "}, \"n\" : {\"int\" : " + recordCount + "} }";
        LOG.info("creating " + json);
        FileUtils.writeStringToFile(new File(json), data);
        LOG.info("creating " + avro);
        generateAvroFile(schema, json, avro, null);

        schema = basedir + "schema/testDirectoryCounts.avsc";
        json = basedir + "data/json/evenFileNameTestDirectoryCounts.json";
        avro = basedir + "data/avro/uncompressed/evenFileNameTestDirectoryCounts.avro";
        data = "{\"itemSum\" : {\"int\" : " + evenFileNameItemSum + "}, \"n\" : {\"int\" : " + evenFileNameRecordCount + "} }";
        LOG.info("creating " + json);
        FileUtils.writeStringToFile(new File(json), data);
        LOG.info("creating " + avro);
        generateAvroFile(schema, json, avro, null);

        for (String codec : new String[] {"deflate", "snappy"}) {
            for (String fn : new String[] {"records",  "recordsAsOutputByPig"}) {
                schema = basedir + "schema/" + fn + ".avsc";
                json = basedir + "data/json/" + fn + ".json";
                avro = basedir + "data/avro/compressed/" + codec + "/" + fn + ".avro";
                LOG.info("creating " + avro);
                generateAvroFile(schema, json, avro, codec);
            }
        }
    }

    /**
     * Clean up generated input files
     */
    private static void deleteInputFiles() throws IOException {
        // Delete auto-generated directories
        for (String fn : datadir) {
            LOG.info("Deleting " + basedir + fn);
            FileUtils.deleteQuietly(new File(basedir + fn));
        }
        // Delete auto-generated json files
        String json;
        for (String fn : trevniSchemas) {
            json = basedir + "data/json/" + fn + ".json";
            LOG.info("Deleting " + json);
            FileUtils.deleteQuietly(new File(json));
        }
        json = basedir + "data/json/testDirectoryCounts.json";
        LOG.info("Deleting " + json);
        FileUtils.deleteQuietly(new File(json));
        json = basedir + "data/json/evenFileNameTestDirectoryCounts.json";
        LOG.info("Deleting " + json);
        FileUtils.deleteQuietly(new File(json));
    }

    private static void generateAvroFile(String schema, String json, String avro, String codec) throws IOException {
        Tool tool = new DataFileWriteTool();
        List<String> args = new ArrayList<String>();
        args.add("--schema-file");
        args.add(schema);
        args.add(json);
        if (codec != null) {
            args.add("--codec");
            args.add(codec);
        }
        try {
            StringBuffer sb = new StringBuffer();
            for (String a : args) {
                sb.append(a);
                sb.append(" ");
            }
            PrintStream out = new PrintStream(avro);
            tool.run(System.in, out, System.err, args);
        } catch (Exception e) {
            LOG.info("Could not generate avro file: " + avro, e);
            throw new IOException();
        }
    }

    private static void generateRandomTrevniFile(String schema, String count, String trevni) throws IOException {
        Tool tool = new TrevniCreateRandomTool();
        List<String> args = new ArrayList<String>();
        args.add(schema);
        args.add(count);
        args.add(trevni);
        try {
            tool.run(System.in, System.out, System.err, args);
        } catch (Exception e) {
            LOG.info("Could not generate trevni file: " + trevni, e);
            throw new IOException();
        }
    }

    private static void convertTrevniToJsonFile(String schema, String trevni, String json) throws IOException {
        Tool tool = new TrevniToJsonTool();
        List<String> args = new ArrayList<String>();
        args.add(trevni);
        try {
            PrintStream out = new PrintStream(json);
            tool.run(System.in, out, System.err, args);
        } catch (Exception e) {
            LOG.info("Could not generate json file: " + json, e);
            throw new IOException();
        }
    }

    private String createOutputName() {
        final StackTraceElement[] st = Thread.currentThread().getStackTrace();
        if(Util.WINDOWS){
            outbasedir = outbasedir.replace('\\','/');
        }
        return outbasedir + st[2].getMethodName();
    }

    @Test public void testLoadRecords() throws Exception {
      final String input = basedir + "data/avro/uncompressed/records.avro";
      final String check = basedir + "data/avro/uncompressed/recordsAsOutputByPig.avro";
      testAvroStorage(true, basedir + "code/pig/identity_ao2.pig",
          ImmutableMap.of(
              "INFILE",           input,
              "AVROSTORAGE_OUT_1", "records",
              "AVROSTORAGE_OUT_2", "-n org.apache.pig.test.builtin",
              "OUTFILE",          createOutputName())
        );
      verifyResults(createOutputName(),check);
    }

    @Test public void testLoadRecordsWithSimpleUnion() throws Exception {
        final String input = basedir + "data/avro/uncompressed/recordsWithSimpleUnion.avro";
        final String check = basedir + "data/avro/uncompressed/recordsWithSimpleUnionOutput.avro";
        testAvroStorage(true, basedir + "code/pig/identity_ao2.pig",
            ImmutableMap.of(
                 "INFILE",            input,
                 "AVROSTORAGE_OUT_1", "records",
                 "AVROSTORAGE_OUT_2", "-n org.apache.pig.test.builtin -f " + basedir + "schema/recordsSubSchema.avsc",
                 "OUTFILE",           createOutputName())
          );
        verifyResults(createOutputName(),check);
      }


    @Test public void testProjection() throws Exception {
        final String input = basedir + "data/avro/uncompressed/records.avro";
        final String check = basedir + "data/avro/uncompressed/projectionTest.avro";
        testAvroStorage(true, basedir + "code/pig/projection_test.pig",
            ImmutableMap.of(
                "INFILE",           input,
                "AVROSTORAGE_OUT_1", "projectionTest",
                "AVROSTORAGE_OUT_2", "-n org.apache.pig.test.builtin",
                "OUTFILE",          createOutputName())
          );
        verifyResults(createOutputName(),check);
      }


    @Test public void testDates() throws Exception {
      final String input = basedir + "data/avro/uncompressed/records.avro";
      final String check = basedir + "data/avro/uncompressed/recordsAsOutputByPigWithDates.avro";
      testAvroStorage(true, basedir + "code/pig/with_dates.pig",
          ImmutableMap.of(
              "INFILE",           input,
              "AVROSTORAGE_OUT_1", "records",
              "AVROSTORAGE_OUT_2", "-n org.apache.pig.test.builtin",
              "OUTFILE",          createOutputName())
        );
      verifyResults(createOutputName(),check);
    }

    @Test public void testLoadRecordsSpecifyFullSchema() throws Exception {
      final String input = basedir + "data/avro/uncompressed/records.avro";
      final String check = basedir + "data/avro/uncompressed/recordsAsOutputByPig.avro";
      final String schema = loadFileIntoString(basedir + "schema/records.avsc");
      testAvroStorage(true, basedir + "code/pig/identity_ai1_ao2.pig",
          ImmutableMap.of(
               "INFILE",            input,
               "OUTFILE",           createOutputName(),
               "AVROSTORAGE_OUT_1", "records",
               "AVROSTORAGE_OUT_2", "-n org.apache.pig.test.builtin",
               "AVROSTORAGE_IN_1",  schema)
        );
      verifyResults(createOutputName(),check);
    }

    @Test public void testLoadRecordsSpecifyFullSchemaFromFile() throws Exception {
      final String input = basedir + "data/avro/uncompressed/records.avro";
      final String check = basedir + "data/avro/uncompressed/recordsAsOutputByPig.avro";
      testAvroStorage(true, basedir + "code/pig/identity.pig",
          ImmutableMap.of(
               "INFILE",            input,
               "OUTFILE",           createOutputName(),
               "AVROSTORAGE_OUT_1", "records",
               "AVROSTORAGE_OUT_2", "-n org.apache.pig.test.builtin",
               "AVROSTORAGE_IN_2",  "-f " + basedir + "schema/records.avsc")
        );
      verifyResults(createOutputName(),check);
    }

    @Test public void testLoadRecordsSpecifySubSchema() throws Exception {
      final String input = basedir + "data/avro/uncompressed/records.avro";
      final String check = basedir + "data/avro/uncompressed/recordsSubSchema.avro";
      testAvroStorage(true, basedir + "code/pig/identity_ai1_ao2.pig",
          ImmutableMap.of(
               "INFILE",            input,
               "OUTFILE",           createOutputName(),
               "AVROSTORAGE_OUT_1", "records",
               "AVROSTORAGE_OUT_2", "-n org.apache.pig.test.builtin -f " + basedir + "schema/recordsSubSchema.avsc",
               "AVROSTORAGE_IN_1",  loadFileIntoString(basedir + "schema/recordsSubSchema.avsc"))
        );
      verifyResults(createOutputName(),check);
    }

    @Test public void testLoadRecordsSpecifySubSchemaFromFile() throws Exception {
      final String input = basedir + "data/avro/uncompressed/records.avro";
      final String check = basedir + "data/avro/uncompressed/recordsSubSchema.avro";
      testAvroStorage(true, basedir + "code/pig/identity_blank_first_args.pig",
          ImmutableMap.of(
               "INFILE",            input,
               "OUTFILE",           createOutputName(),
               "AVROSTORAGE_OUT_2", "-f " + basedir + "schema/recordsSubSchema.avsc",
               "AVROSTORAGE_IN_2",  "-f " + basedir + "schema/recordsSubSchema.avsc")
        );
      verifyResults(createOutputName(),check);
    }

    @Test public void testLoadRecordsSpecifySubSchemaFromExampleFile() throws Exception {
      final String input = basedir + "data/avro/uncompressed/records.avro";
      final String check = basedir + "data/avro/uncompressed/recordsSubSchema.avro";
      testAvroStorage(true, basedir + "code/pig/identity_blank_first_args.pig",
          ImmutableMap.of(
               "INFILE",            input,
               "OUTFILE",           createOutputName(),
               "AVROSTORAGE_OUT_2", "-f " + basedir + "schema/recordsSubSchema.avsc",
               "AVROSTORAGE_IN_2",  "-e " + basedir + "data/avro/uncompressed/recordsSubSchema.avro")
        );
      verifyResults(createOutputName(),check);
    }

    @Test public void testLoadRecordsOfArrays() throws Exception {
      final String input = basedir + "data/avro/uncompressed/recordsOfArrays.avro";
      final String check = input;
      testAvroStorage(true, basedir + "code/pig/identity_just_ao2.pig",
          ImmutableMap.of(
              "INFILE",             input,
              "AVROSTORAGE_OUT_2", "-f " + basedir + "schema/recordsOfArrays.avsc",
              "OUTFILE",            createOutputName())
        );
      verifyResults(createOutputName(),check);
    }

    @Test public void testLoadRecordsOfArraysOfRecords() throws Exception {
      final String input = basedir + "data/avro/uncompressed/recordsOfArraysOfRecords.avro";
      final String check = input;
      testAvroStorage(true, basedir + "code/pig/identity_just_ao2.pig",
          ImmutableMap.of(
              "INFILE",             input,
              "AVROSTORAGE_OUT_2", "-f " + basedir + "schema/recordsOfArraysOfRecords.avsc",
              "OUTFILE",            createOutputName())
        );
      verifyResults(createOutputName(),check);
    }

    @Test public void testLoadRecordsWithEnums() throws Exception {
      final String input = basedir + "data/avro/uncompressed/recordsWithEnums.avro";
      final String check = input;
      testAvroStorage(true, basedir + "code/pig/identity_just_ao2.pig",
          ImmutableMap.of(
              "INFILE",             input,
              "AVROSTORAGE_OUT_2", "-f " + basedir + "schema/recordsWithEnums.avsc",
              "OUTFILE",            createOutputName())
        );
      verifyResults(createOutputName(),check);
    }

    @Test public void testLoadRecordsWithFixed() throws Exception {
      final String input = basedir + "data/avro/uncompressed/recordsWithFixed.avro";
      final String check = input;
      testAvroStorage(true, basedir + "code/pig/identity_just_ao2.pig",
          ImmutableMap.of(
              "INFILE",             input,
              "AVROSTORAGE_OUT_2", "-f " + basedir + "schema/recordsWithFixed.avsc",
              "OUTFILE",            createOutputName())
        );
      verifyResults(createOutputName(),check);
    }

    @Test public void testLoadRecordsWithMaps() throws Exception {
      final String input = basedir + "data/avro/uncompressed/recordsWithMaps.avro";
      final String check = input;
      testAvroStorage(true, basedir + "code/pig/identity_just_ao2.pig",
          ImmutableMap.of(
              "INFILE",             input,
              "AVROSTORAGE_OUT_2", "-f " + basedir + "schema/recordsWithMaps.avsc",
              "OUTFILE",            createOutputName())
        );
      verifyResults(createOutputName(),check);
    }

    @Test public void testLoadRecordsWithMapsOfRecords() throws Exception {
      final String input = basedir + "data/avro/uncompressed/recordsWithMapsOfRecords.avro";
      final String check = input;
      testAvroStorage(true, basedir + "code/pig/identity_just_ao2.pig",
          ImmutableMap.of(
              "INFILE",             input,
              "AVROSTORAGE_OUT_2", "-f " + basedir + "schema/recordsWithMapsOfRecords.avsc",
              "OUTFILE",            createOutputName())
        );
      verifyResults(createOutputName(),check);
    }

    @Test public void testLoadRecordsWithNullableUnions() throws Exception {
      final String input = basedir + "data/avro/uncompressed/recordsWithNullableUnions.avro";
      final String check = input;
      testAvroStorage(true, basedir + "code/pig/identity_just_ao2.pig",
          ImmutableMap.of(
              "INFILE",             input,
              "AVROSTORAGE_OUT_2", "-f " + basedir + "schema/recordsWithNullableUnions.avsc",
              "OUTFILE",            createOutputName())
        );
      verifyResults(createOutputName(),check);
    }

    @Test public void testLoadDeflateCompressedRecords() throws Exception {
      final String input = basedir + "data/avro/compressed/deflate/records.avro";
      final String check = basedir + "data/avro/uncompressed/recordsAsOutputByPig.avro";
      testAvroStorage(true, basedir + "code/pig/identity_ao2.pig",
          ImmutableMap.of(
              "INFILE",           input,
              "AVROSTORAGE_OUT_1", "records",
              "AVROSTORAGE_OUT_2", "-n org.apache.pig.test.builtin",
              "OUTFILE",          createOutputName())
        );
      verifyResults(createOutputName(),check);
    }

    @Test public void testLoadSnappyCompressedRecords() throws Exception {
      final String input = basedir + "data/avro/compressed/snappy/records.avro";
      final String check = basedir + "data/avro/uncompressed/recordsAsOutputByPig.avro";
      testAvroStorage(true, basedir + "code/pig/identity_ao2.pig",
          ImmutableMap.of(
              "INFILE",           input,
              "AVROSTORAGE_OUT_1", "records",
              "AVROSTORAGE_OUT_2", "-n org.apache.pig.test.builtin",
              "OUTFILE",          createOutputName())
        );
      verifyResults(createOutputName(),check);
    }

    @Test public void testStoreDeflateCompressedRecords() throws Exception {
      final String input = basedir + "data/avro/uncompressed/records.avro";
      final String check = basedir + "data/avro/compressed/deflate/recordsAsOutputByPig.avro";
      testAvroStorage(true, basedir + "code/pig/identity_codec.pig",
          ImmutableMap.<String,String>builder()
            .put("INFILE",input)
            .put("OUTFILE",createOutputName())
            .put("AVROSTORAGE_OUT_1", "records")
            .put("AVROSTORAGE_OUT_2", "-n org.apache.pig.test.builtin")
            .put("CODEC", "deflate")
            .put("LEVEL", "6")
            .build()
           );
      verifyResults(createOutputName(),check);
    }

    @Test public void testStoreSnappyCompressedRecords() throws Exception {
      final String input = basedir + "data/avro/uncompressed/records.avro";
      final String check = basedir + "data/avro/compressed/snappy/recordsAsOutputByPig.avro";
      testAvroStorage(true, basedir + "code/pig/identity_codec.pig",
          ImmutableMap.<String,String>builder()
          .put("INFILE",input)
          .put("OUTFILE",createOutputName())
          .put("AVROSTORAGE_OUT_1", "records")
          .put("AVROSTORAGE_OUT_2", "-n org.apache.pig.test.builtin")
          .put("CODEC", "snappy")
          .put("LEVEL", "6")
          .build()
         );
     verifyResults(createOutputName(),check);
    }

    @Test public void testLoadRecursiveRecords() throws Exception {
      final String input = basedir + "data/avro/uncompressed/recursiveRecord.avro";
      testAvroStorage(false, basedir + "code/pig/recursive_tests.pig",
          ImmutableMap.of(
              "INFILE",           input,
              "AVROSTORAGE_IN_2", "-n this.is.ignored.in.a.loadFunc",
              "AVROSTORAGE_OUT_1", "recordsSubSchemaNullable",
              "AVROSTORAGE_OUT_2", "-n org.apache.pig.test.builtin",
              "OUTFILE",          createOutputName())
        );
    }

    @Test public void testLoadRecursiveRecordsOptionOn() throws Exception {
      final String input = basedir + "data/avro/uncompressed/recursiveRecord.avro";
      final String check = basedir + "data/avro/uncompressed/recordsSubSchemaNullable.avro";
      testAvroStorage(true, basedir + "code/pig/recursive_tests.pig",
          ImmutableMap.of(
              "INFILE",           input,
              "AVROSTORAGE_IN_2", "-r",
              "AVROSTORAGE_OUT_1", "recordsSubSchemaNullable",
              "AVROSTORAGE_OUT_2", "-n org.apache.pig.test.builtin",
              "OUTFILE",          createOutputName())
        );
      verifyResults(createOutputName(),check);
    }

    @Test public void testLoadRecordsWithRepeatedSubRecords() throws Exception {
      final String input = basedir + "data/avro/uncompressed/recordWithRepeatedSubRecords.avro";
      final String check = basedir + "data/avro/uncompressed/recordWithRepeatedSubRecords.avro";
      testAvroStorage(true, basedir + "code/pig/identity_just_ao2.pig",
          ImmutableMap.of(
              "INFILE",           input,
              "AVROSTORAGE_OUT_2", "-f " + basedir + "schema/recordWithRepeatedSubRecords.avsc",
              "OUTFILE",          createOutputName())
        );
      verifyResults(createOutputName(),check);
    }

    @Test public void testLoadDirectory() throws Exception {
      final String input = basedir + "data/avro/uncompressed/testdirectory";
      final String check = basedir + "data/avro/uncompressed/testDirectoryCounts.avro";
      testAvroStorage(true, basedir + "code/pig/directory_test.pig",
          ImmutableMap.of(
              "INFILE",           input,
              "AVROSTORAGE_OUT_1", "stats",
              "AVROSTORAGE_OUT_2", "-n org.apache.pig.test.builtin",
              "OUTFILE",          createOutputName())
        );
      verifyResults(createOutputName(),check);
    }

    @Test public void testLoadGlob() throws Exception {
      final String input = basedir + "data/avro/uncompressed/testdirectory/part-m-0000*";
      final String check = basedir + "data/avro/uncompressed/testDirectoryCounts.avro";
      testAvroStorage(true, basedir + "code/pig/directory_test.pig",
          ImmutableMap.of(
              "INFILE",           input,
              "AVROSTORAGE_OUT_1", "stats",
              "AVROSTORAGE_OUT_2", "-n org.apache.pig.test.builtin",
              "OUTFILE",          createOutputName())
        );
      verifyResults(createOutputName(),check);
    }

    @Test public void testPartialLoadGlob() throws Exception {
      final String input = basedir + "data/avro/uncompressed/testdirectory/part-m-0000{0,2,4,6}.avro";
      final String check = basedir + "data/avro/uncompressed/evenFileNameTestDirectoryCounts.avro";
      testAvroStorage(true, basedir + "code/pig/directory_test.pig",
          ImmutableMap.of(
              "INFILE",           input,
              "AVROSTORAGE_OUT_1", "stats",
              "AVROSTORAGE_OUT_2", "-n org.apache.pig.test.builtin",
              "OUTFILE",          createOutputName())
        );
      verifyResults(createOutputName(),check);
    }

    @Test public void testSeparatedByComma() throws Exception {
        final String temp = basedir
                + "data/avro/uncompressed/testdirectory/part-m-0000";
        StringBuffer sb = new StringBuffer();
        sb.append(temp + "0.avro");
        for (int i = 1; i <= 7; ++i) {
            sb.append(",");
            sb.append(temp + String.valueOf(i) + ".avro");
        }
        final String input = sb.toString();
        final String check = basedir
                + "data/avro/uncompressed/testDirectoryCounts.avro";
        testAvroStorage(true, basedir + "code/pig/directory_test.pig",
                ImmutableMap.of("INFILE", input, "AVROSTORAGE_OUT_1", "stats",
                        "AVROSTORAGE_OUT_2", "-n org.apache.pig.test.builtin",
                        "OUTFILE", createOutputName()));
        verifyResults(createOutputName(), check);
    }

    @Test public void testDoubleUnderscore() throws Exception {
      final String input = basedir + "data/avro/uncompressed/records.avro";
      final String check = basedir + "data/avro/uncompressed/recordsWithDoubleUnderscores.avro";
      testAvroStorage(true, basedir + "code/pig/namesWithDoubleColons.pig",
          ImmutableMap.of(
              "INFILE",           input,
              "AVROSTORAGE_OUT_1", "recordsWithDoubleUnderscores",
              "AVROSTORAGE_OUT_2", "-d -n org.apache.pig.test.builtin",
              "OUTFILE",          createOutputName())
        );
      verifyResults(createOutputName(),check);
    }

    @Test public void testDoubleUnderscoreNoFlag() throws Exception {
      final String input = basedir + "data/avro/uncompressed/records.avro";
      testAvroStorage(false, basedir + "code/pig/namesWithDoubleColons.pig",
          ImmutableMap.of(
              "INFILE",           input,
              "AVROSTORAGE_OUT_1", "recordsWithDoubleUnderscores",
              "AVROSTORAGE_OUT_2", "-n org.apache.pig.test.builtin",
              "OUTFILE",          createOutputName())
        );
    }

    @Test public void testLoadArrays() throws Exception {
      final String input = basedir + "data/avro/uncompressed/arrays.avro";
      final String check = basedir + "data/avro/uncompressed/arraysAsOutputByPig.avro";
      testAvroStorage(true, basedir + "code/pig/identity_ao2.pig",
          ImmutableMap.of(
              "INFILE",           input,
              "AVROSTORAGE_OUT_1", "arrays",
              "AVROSTORAGE_OUT_2", "-n org.apache.pig.test.builtin",
              "OUTFILE",          createOutputName())
        );
      verifyResults(createOutputName(),check);
    }

    @Test public void testLoadTrevniRecords() throws Exception {
      final String input = basedir + "data/trevni/uncompressed/simpleRecordsTrevni.trevni";
      final String check = basedir + "data/avro/uncompressed/simpleRecordsTrevni.avro";
      testAvroStorage(true, basedir + "code/pig/trevni_to_avro.pig",
          ImmutableMap.of(
              "INFILE",           input,
              "AVROSTORAGE_OUT_2", "-f " + basedir + "schema/simpleRecordsTrevni.avsc",
              "OUTFILE",          createOutputName())
        );
      verifyResults(createOutputName(),check);
    }

    @Test public void testLoadAndSaveTrevniRecords() throws Exception {
      final String input = basedir + "data/trevni/uncompressed/simpleRecordsTrevni.trevni";
      final String check = basedir + "data/avro/uncompressed/simpleRecordsTrevni.avro";

      testAvroStorage(true, basedir + "code/pig/trevni_to_trevni.pig",
          ImmutableMap.of(
              "INFILE",           input,
              "AVROSTORAGE_OUT_2", "-f " + basedir + "schema/simpleRecordsTrevni.avsc",
              "OUTFILE",          createOutputName() + "Trevni")
        );

      testAvroStorage(true, basedir + "code/pig/trevni_to_avro.pig",
          ImmutableMap.of(
              "INFILE",           createOutputName() + "Trevni",
              "AVROSTORAGE_OUT_2", "-f " + basedir + "schema/simpleRecordsTrevni.avsc",
              "OUTFILE",          createOutputName())
        );
      verifyResults(createOutputName(), check);
    }

    @Test
    public void testRetrieveDataFromMap() throws Exception {
        pigServerLocal = new PigServer(ExecType.LOCAL);
        Data data = resetData(pigServerLocal);
        Map<String, String> mapv1 = new HashMap<String, String>();
        mapv1.put("key1", "v11");
        mapv1.put("key2", "v12");
        Map<String, String> mapv2 = new HashMap<String, String>();
        mapv2.put("key1", "v21");
        mapv2.put("key2", "v22");
        data.set("testMap", "maps:map[chararray]", tuple(mapv1), tuple(mapv2));
        String schemaDescription = new String(
                "{" +
                      "\"type\": \"record\"," +
                      "\"name\": \"record\"," +
                      "\"fields\" : [" +
                      "{\"name\" : \"maps\", \"type\" :{\"type\" : \"map\", \"values\" : \"string\"}}" +
                      "]" +
                      "}");
        pigServerLocal.registerQuery("A = LOAD 'testMap' USING mock.Storage();");
        pigServerLocal.registerQuery("STORE A INTO '" + createOutputName() + "' USING AvroStorage('"+ schemaDescription +"');");
        pigServerLocal.registerQuery("B = LOAD '" + createOutputName() + "' USING AvroStorage();");
        pigServerLocal.registerQuery("C = FOREACH B generate maps#'key1';");
        pigServerLocal.registerQuery("STORE C INTO 'out' USING mock.Storage();");


        List<Tuple> out = data.get("out");
        assertEquals(tuple("v11"), out.get(0));
        assertEquals(tuple("v21"), out.get(1));
    }

    @SuppressWarnings("rawtypes")
    @Test
    public void testCompareToOfBagWrapper() throws Exception {
        final String check = basedir + "data/avro/uncompressed/arraysAsOutputByPig.avro";

        Set<GenericData.Record> records = getExpected(check);
        assertEquals(3, records.size());

        AvroBagWrapper size0 = null; // [ ]
        AvroBagWrapper size1 = null; // [ 6 ]
        AvroBagWrapper size5 = null; // [ 1, 2, 3, 4, 5 ]

        // 3 arrays in records are in an arbitrary order. We re-order them by
        // their size.
        for (GenericData.Record record : records) {
            AvroTupleWrapper tw = new AvroTupleWrapper<GenericData.Record>(record);
            if (((AvroBagWrapper)tw.get(0)).size() == 0) {
                size0 = (AvroBagWrapper)tw.get(0);
            } else if (((AvroBagWrapper)tw.get(0)).size() == 1) {
                size1 = (AvroBagWrapper)tw.get(0);
            } else if (((AvroBagWrapper)tw.get(0)).size() == 5) {
                size5 = (AvroBagWrapper)tw.get(0);
            }
        }

        assertEquals(0, size0.size());
        assertEquals(1, size1.size());
        assertEquals(5, size5.size());

        assertTrue(size0.compareTo(size0) == 0);
        assertTrue(size0.compareTo(size1) < 0);
        assertTrue(size0.compareTo(size5) < 0);
        assertTrue(size1.compareTo(size0) > 0);
        // 6 > 1, so size1 > size5 even though size1 is smaller than size5.
        assertTrue(size1.compareTo(size5) > 0);
    }

    @Test
    public void testUtf8KeyLookupFromMap() throws Exception {
        Map<CharSequence, Object> tm = new TreeMap<CharSequence, Object> ();
        tm.put(new Utf8("foo"), "foo");
        tm.put(new Utf8("bar"), "bar");

        AvroMapWrapper wrapper = new AvroMapWrapper(tm);
        String v = (String)wrapper.get(new Utf8("foo"));
        assertEquals("foo", v);
        v = (String)wrapper.get(new Utf8("bar"));
        assertEquals("bar", v);
    }

    private void testAvroStorage(boolean expectedToSucceed, String scriptFile, Map<String,String> parameterMap) throws IOException {
        pigServerLocal.setBatchOn();

        int numOfFailedJobs = 0;

        try {
          pigServerLocal.registerScript(scriptFile, parameterMap);
          for (ExecJob job : pigServerLocal.executeBatch()) {
            if (job.getStatus().equals(JOB_STATUS.FAILED)) {
                  numOfFailedJobs++;
              }
          }
        } catch (Exception e) {
          System.err.printf("Exception caught in testAvroStorage: %s\n", e);
          numOfFailedJobs++;
        }

        if (expectedToSucceed) {
          assertTrue("There was a failed job!", numOfFailedJobs == 0);
        } else {
          assertTrue("There was no failed job!", numOfFailedJobs > 0);
        }
    }

    private void verifyResults(String outPath, String expectedOutpath) throws IOException {
        verifyResults(outPath, expectedOutpath, null);
    }

    private void verifyResults(String outPath, String expectedOutpath, String expectedCodec) throws IOException {
        FileSystem fs = FileSystem.getLocal(new Configuration()) ;

        /* read in expected results*/
        Set<GenericData.Record> expected = getExpected (expectedOutpath);

        /* read in output results and compare */
        Path output = new Path(outPath);
        assertTrue("Output dir does not exists!", fs.exists(output)
                && fs.getFileStatus(output).isDir());

        Path[] paths = FileUtil.stat2Paths(fs.listStatus(output, hiddenPathFilter));
        assertTrue("Split field dirs not found!", paths != null);

        for (Path path : paths) {
          Path[] files = FileUtil.stat2Paths(fs.listStatus(path, hiddenPathFilter));
          assertTrue("No files found for path: " + path.toUri().getPath(),
                  files != null);
          for (Path filePath : files) {
            assertTrue("This shouldn't be a directory", fs.isFile(filePath));

            GenericDatumReader<GenericData.Record> reader = new GenericDatumReader<GenericData.Record>();

            DataFileStream<GenericData.Record> in = new DataFileStream<GenericData.Record>(
                                            fs.open(filePath), reader);
            assertEquals("codec", expectedCodec, in.getMetaString("avro.codec"));
            int count = 0;
            while (in.hasNext()) {
                GenericData.Record obj = in.next();
                assertTrue("Avro result object found that's not expected: Found "
                        + (obj != null ? obj.getSchema() : "null") + ", " + obj.toString()
                        + "\nExpected " + (expected != null ? expected.toString() : "null") + "\n"
                        , expected.contains(obj));
                count++;
            }
            in.close();
            assertEquals(expected.size(), count);
          }
        }
      }

    private Set<GenericData.Record> getExpected (String pathstr ) throws IOException {

        Set<GenericData.Record> ret = new TreeSet<GenericData.Record>(
                new Comparator<GenericData.Record>() {
                    @Override
                    public int compare(Record o1, Record o2) {
                        return o1.toString().compareTo(o2.toString());
                    }}
                );
        FileSystem fs = FileSystem.getLocal(new Configuration());

        /* read in output results and compare */
        Path output = new Path(pathstr);
        assertTrue("Expected output does not exists!", fs.exists(output));

        Path[] paths = FileUtil.stat2Paths(fs.listStatus(output, hiddenPathFilter));
        assertTrue("Split field dirs not found!", paths != null);

        for (Path path : paths) {
            Path[] files = FileUtil.stat2Paths(fs.listStatus(path, hiddenPathFilter));
            assertTrue("No files found for path: " + path.toUri().getPath(), files != null);
            for (Path filePath : files) {
                assertTrue("This shouldn't be a directory", fs.isFile(filePath));

                GenericDatumReader<GenericData.Record> reader = new GenericDatumReader<GenericData.Record>();

                DataFileStream<GenericData.Record> in = new DataFileStream<GenericData.Record>(fs.open(filePath), reader);

                while (in.hasNext()) {
                    GenericData.Record obj = in.next();
                    ret.add(obj);
                }
                in.close();
            }
        }
        return ret;
  }

}


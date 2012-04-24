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
package org.apache.pig.piggybank.test.storage.avro;

import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
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
import org.apache.pig.piggybank.storage.avro.PigSchema2Avro;
import org.apache.pig.test.Util;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestAvroStorage {

    protected static final Log LOG = LogFactory.getLog(TestAvroStorage.class);

    private static PigServer pigServerLocal = null;

    final private static String basedir = "src/test/java/org/apache/pig/piggybank/test/storage/avro/avro_test_files/";

    final private static String outbasedir = "/tmp/TestAvroStorage/";
    
    public static final PathFilter hiddenPathFilter = new PathFilter() {
        public boolean accept(Path p) {
          String name = p.getName();
          return !name.startsWith("_") && !name.startsWith(".");
        }
      };

    private static String getInputFile(String file) {
        return "file://" + System.getProperty("user.dir") + "/" + basedir + file;
    }

    final private String testArrayFile = getInputFile("test_array.avro");
    final private String testRecordFile = getInputFile("test_record.avro");
    final private String testRecordSchema = getInputFile("test_record.avsc");
    final private String testTextFile = getInputFile("test_record.txt");
    final private String testSingleTupleBagFile = getInputFile("messages.avro");
    final private String testNoExtensionFile = getInputFile("test_no_extension");

    @BeforeClass
    public static void setup() throws ExecException {
        pigServerLocal = new PigServer(ExecType.LOCAL);
        deleteDirectory(new File(outbasedir));
    }

    @AfterClass
    public static void teardown() {
        if(pigServerLocal != null) pigServerLocal.shutdown();
    }

    @Test
    public void testArrayDefault() throws IOException {
        String output= outbasedir + "testArrayDefault";
        String expected = basedir + "expected_testArrayDefault.avro";
        
        deleteDirectory(new File(output));
        
        String [] queries = {
           " in = LOAD '" + testArrayFile + " ' USING org.apache.pig.piggybank.storage.avro.AvroStorage ();",
           " STORE in INTO '" + output + "' USING org.apache.pig.piggybank.storage.avro.AvroStorage ();"
            };
        testAvroStorage( queries);
        verifyResults(output, expected);
    }


    @Test
    public void testArrayWithSchema() throws IOException {
        String output= outbasedir + "testArrayWithSchema";
        String expected = basedir + "expected_testArrayWithSchema.avro";
        deleteDirectory(new File(output));
        String [] queries = {
           " in = LOAD '" + testArrayFile + " ' USING org.apache.pig.piggybank.storage.avro.AvroStorage ();",
           " STORE in INTO '" + output +
               "' USING org.apache.pig.piggybank.storage.avro.AvroStorage ( "  +
               "   'schema', '{\"type\":\"array\",\"items\":\"float\"}'  );"
            };
        testAvroStorage( queries);
        verifyResults(output, expected);
    }
    
    @Test
    public void testArrayWithNotNull() throws IOException {
        String output= outbasedir + "testArrayWithNotNull";
        String expected = basedir + "expected_testArrayWithSchema.avro";
        deleteDirectory(new File(output));
        String [] queries = {
           " in = LOAD '" + testArrayFile + " ' USING org.apache.pig.piggybank.storage.avro.AvroStorage ();",
           " STORE in INTO '" + output +
               "' USING org.apache.pig.piggybank.storage.avro.AvroStorage ( "  +
               "   '{\"nullable\": false }'  );"
            };
        testAvroStorage( queries);
        verifyResults(output, expected);
    }
    
    @Test
    public void testArrayWithSame() throws IOException {
        String output= outbasedir + "testArrayWithSame";
        String expected = basedir + "expected_testArrayWithSchema.avro";
        deleteDirectory(new File(output));
        String [] queries = {
           " in = LOAD '" + testArrayFile + " ' USING org.apache.pig.piggybank.storage.avro.AvroStorage ();",
           " STORE in INTO '" + output +
               "' USING org.apache.pig.piggybank.storage.avro.AvroStorage ( "  +
               "   'same', '" + testArrayFile + "'  );"
            };
        testAvroStorage(queries);
        verifyResults(output, expected);
    }

    @Test
    public void testArrayWithSnappyCompression() throws IOException {
        String output= outbasedir + "testArrayWithSnappyCompression";
        String expected = basedir + "expected_testArrayDefault.avro";
      
        deleteDirectory(new File(output));
      
        Properties properties = new Properties();
        properties.setProperty("mapred.output.compress", "true");
        properties.setProperty("mapred.output.compression.codec", "org.apache.hadoop.io.compress.SnappyCodec");
        properties.setProperty("avro.output.codec", "snappy");
        PigServer pigServer = new PigServer(ExecType.LOCAL, properties);
        pigServer.setBatchOn();
        String [] queries = {
           " in = LOAD '" + testArrayFile + " ' USING org.apache.pig.piggybank.storage.avro.AvroStorage ();",
           " STORE in INTO '" + output + "' USING org.apache.pig.piggybank.storage.avro.AvroStorage ();"
            };
        for (String query: queries){
            pigServer.registerQuery(query);
        }
        pigServer.executeBatch();
        verifyResults(output, expected, "snappy");
    }

    @Test
    public void testRecordWithSplit() throws IOException {
        PigSchema2Avro.setTupleIndex(0);
        String output1= outbasedir + "testRecordSplit1";
        String output2= outbasedir + "testRecordSplit2";
        String expected1 = basedir + "expected_testRecordSplit1.avro";
        String expected2 = basedir + "expected_testRecordSplit2.avro";
        deleteDirectory(new File(output1));
        deleteDirectory(new File(output2));
        String [] queries = {
           " avro = LOAD '" + testRecordFile + " ' USING org.apache.pig.piggybank.storage.avro.AvroStorage ();",
           " groups = GROUP avro BY member_id;",
           " sc = FOREACH groups GENERATE group AS key, COUNT(avro) AS cnt;",
           " STORE sc INTO '" + output1 + "' " +
                 " USING org.apache.pig.piggybank.storage.avro.AvroStorage (" +
                 "'{\"index\": 1, " +
                 "  \"schema\": {\"type\":\"record\", " +
                                        " \"name\":\"result\", " +
                                       "  \"fields\":[ {\"name\":\"member_id\",\"type\":\"int\"}, " +
                                                             "{\"name\":\"count\", \"type\":\"long\"} " +
                                                          "]" +
                                         "}" +
                " }');",
            " STORE sc INTO '" + output2 +
                    " 'USING org.apache.pig.piggybank.storage.avro.AvroStorage ('index', '2');"
            };
        testAvroStorage( queries);
        verifyResults(output1, expected1);
        verifyResults(output2, expected2);
    }

    @Test
    public void testRecordWithSplitFromText() throws IOException {
        PigSchema2Avro.setTupleIndex(0);
        String output1= outbasedir + "testRecordSplitFromText1";
        String output2= outbasedir + "testRecordSplitFromText2";
        String expected1 = basedir + "expected_testRecordSplitFromText1.avro";
        String expected2 = basedir + "expected_testRecordSplitFromText2.avro";
        deleteDirectory(new File(output1));
        deleteDirectory(new File(output2));
        String [] queries = {
           " avro = LOAD '" + testTextFile + "' AS (member_id:int, browser_id:chararray, tracking_time:long, act_content:bag{inner:tuple(key:chararray, value:chararray)});",
           " groups = GROUP avro BY member_id;",
           " sc = FOREACH groups GENERATE group AS key, COUNT(avro) AS cnt;",
           " STORE sc INTO '" + output1 + "' " +
                 " USING org.apache.pig.piggybank.storage.avro.AvroStorage (" +
                 "'{\"index\": 1, " +
                 "  \"schema\": {\"type\":\"record\", " +
                                        " \"name\":\"result\", " +
                                        " \"fields\":[ {\"name\":\"member_id\",\"type\":\"int\"}, " +
                                                      "{\"name\":\"count\", \"type\":\"long\"} " +
                                                          "]" +
                                         "}" +
                " }');",
            " STORE sc INTO '" + output2 +
                    " 'USING org.apache.pig.piggybank.storage.avro.AvroStorage ('index', '2');"
            };
        testAvroStorage( queries);
        verifyResults(output1, expected1);
        verifyResults(output2, expected2);
    }

    @Test
    public void testRecordWithFieldSchema() throws IOException {
        PigSchema2Avro.setTupleIndex(1);
        String output= outbasedir + "testRecordWithFieldSchema";
        String expected = basedir + "expected_testRecordWithFieldSchema.avro";
        deleteDirectory(new File(output));
        String [] queries = {
           " avro = LOAD '" + testRecordFile + " ' USING org.apache.pig.piggybank.storage.avro.AvroStorage ();",
           " avro1 = FILTER avro BY member_id > 1211;",
           " avro2 = FOREACH avro1 GENERATE member_id, browser_id, tracking_time, act_content ;",
           " STORE avro2 INTO '" + output + "' " +
                 " USING org.apache.pig.piggybank.storage.avro.AvroStorage (" +
                 "'{\"data\":  \"" + testRecordFile + "\" ," +
                 "  \"field0\": \"int\", " +
                  " \"field1\":  \"def:browser_id\", " +
                 "  \"field3\": \"def:act_content\" " +
                " }');"
            };
        testAvroStorage( queries);
        verifyResults(output, expected);
    }

    @Test
    public void testRecordWithFieldSchemaFromText() throws IOException {
        PigSchema2Avro.setTupleIndex(1);
        String output= outbasedir + "testRecordWithFieldSchemaFromText";
        String expected = basedir + "expected_testRecordWithFieldSchema.avro";
        deleteDirectory(new File(output));
        String [] queries = {
          " avro = LOAD '" + testTextFile + "' AS (member_id:int, browser_id:chararray, tracking_time:long, act_content:bag{inner:tuple(key:chararray, value:chararray)});",
          " avro1 = FILTER avro BY member_id > 1211;",
          " avro2 = FOREACH avro1 GENERATE member_id, browser_id, tracking_time, act_content ;",
          " STORE avro2 INTO '" + output + "' " +
                " USING org.apache.pig.piggybank.storage.avro.AvroStorage (" +
                "'{\"data\":  \"" + testRecordFile + "\" ," +
                "  \"field0\": \"int\", " +
                 " \"field1\":  \"def:browser_id\", " +
                "  \"field3\": \"def:act_content\" " +
               " }');"
           };
        testAvroStorage( queries);
        verifyResults(output, expected);
    }

    @Test
    public void testRecordWithFieldSchemaFromTextWithSchemaFile() throws IOException {
        PigSchema2Avro.setTupleIndex(1);
        String output= outbasedir + "testRecordWithFieldSchemaFromTextWithSchemaFile";
        String expected = basedir + "expected_testRecordWithFieldSchema.avro";
        deleteDirectory(new File(output));
        String [] queries = {
           " avro = LOAD '" + testTextFile + "' AS (member_id:int, browser_id:chararray, tracking_time:long, act_content:bag{inner:tuple(key:chararray, value:chararray)});",
          " avro1 = FILTER avro BY member_id > 1211;",
          " avro2 = FOREACH avro1 GENERATE member_id, browser_id, tracking_time, act_content ;",
          " STORE avro2 INTO '" + output + "' " +
                " USING org.apache.pig.piggybank.storage.avro.AvroStorage (" +
                "'{\"schema_file\":  \"" + testRecordSchema + "\" ," +
                "  \"field0\": \"int\", " +
                 " \"field1\":  \"def:browser_id\", " +
                "  \"field3\": \"def:act_content\" " +
               " }');"
           };
        testAvroStorage( queries);
        verifyResults(output, expected);
    }
    
    @Test
    public void testSingleFieldTuples() throws IOException {
        String output= outbasedir + "testSingleFieldTuples";
        String expected = basedir + "expected_testSingleFieldTuples.avro";
        deleteDirectory(new File(output));
        String [] queries = {
                " messages = LOAD '" + testSingleTupleBagFile + " ' USING org.apache.pig.piggybank.storage.avro.AvroStorage ();",
                " a = foreach (group messages by user_id) { sorted = order messages by message_id DESC; GENERATE group AS user_id, sorted AS messages; };",
                " STORE a INTO '" + output + "' " +
                        " USING org.apache.pig.piggybank.storage.avro.AvroStorage ();"
        };
        testAvroStorage( queries);
    }
    
    @Test
    public void testFileWithNoExtension() throws IOException {
        String output= outbasedir + "testFileWithNoExtension";
        String expected = basedir + "expected_testFileWithNoExtension.avro";
        deleteDirectory(new File(output));
        String [] queries = {
                " avro = LOAD '" + testNoExtensionFile + " ' USING org.apache.pig.piggybank.storage.avro.AvroStorage ();",
                " avro1 = FILTER avro BY member_id > 1211;",
                " avro2 = FOREACH avro1 GENERATE member_id, browser_id, tracking_time, act_content ;",
                " STORE avro2 INTO '" + output + "' " +
                        " USING org.apache.pig.piggybank.storage.avro.AvroStorage (" +
                        "'{\"data\":  \"" + testNoExtensionFile + "\" ," +
                        "  \"field0\": \"int\", " +
                        " \"field1\":  \"def:browser_id\", " +
                        "  \"field3\": \"def:act_content\" " +
                        " }');"
        };
        testAvroStorage( queries);
        verifyResults(output, expected);
    }

    // Same as above, just without using json in the constructor
    @Test
    public void testRecordWithFieldSchemaFromTextWithSchemaFile2() throws IOException {
        PigSchema2Avro.setTupleIndex(1);
        String output= outbasedir + "testRecordWithFieldSchemaFromTextWithSchemaFile2";
        String expected = basedir + "expected_testRecordWithFieldSchema.avro";
        deleteDirectory(new File(output));
        String [] queries = {
          " avro = LOAD '" + testTextFile + "' AS (member_id:int, browser_id:chararray, tracking_time:long, act_content:bag{inner:tuple(key:chararray, value:chararray)});",
          " avro1 = FILTER avro BY member_id > 1211;",
          " avro2 = FOREACH avro1 GENERATE member_id, browser_id, tracking_time, act_content ;",
          " STORE avro2 INTO '" + output + "' " +
                " USING org.apache.pig.piggybank.storage.avro.AvroStorage (" +
                "'schema_file', '" + testRecordSchema + "'," +
                "'field0','int'," +
                "'field1','def:browser_id'," +
                "'field3','def:act_content'" +
                ");"
           };
        testAvroStorage( queries);
        verifyResults(output, expected);
    }

    private static void deleteDirectory (File path) {
        if ( path.exists()) {
            File [] files = path.listFiles();
            for (File file: files) {
                if (file.isDirectory()) 
                    deleteDirectory(file);
                file.delete();
            }
        }
    }
    
    private void testAvroStorage(String ...queries) throws IOException {
        pigServerLocal.setBatchOn();
        for (String query: queries){
            if (query != null && query.length() > 0)
                pigServerLocal.registerQuery(query);
        }
        pigServerLocal.executeBatch();
    }
    
    private void verifyResults(String outPath, String expectedOutpath) throws IOException {
      verifyResults(outPath, expectedOutpath, null);
    }

    private void verifyResults(String outPath, String expectedOutpath, String expectedCodec) throws IOException {
        // Seems compress for Avro is broken in 23. Skip this test and open Jira PIG-
        if (Util.isHadoop23())
            return;
        
        FileSystem fs = FileSystem.getLocal(new Configuration()) ; 
        
        /* read in expected results*/
        Set<Object> expected = getExpected (expectedOutpath);
        
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
            
            GenericDatumReader<Object> reader = new GenericDatumReader<Object>();
            
            DataFileStream<Object> in = new DataFileStream<Object>(
                                            fs.open(filePath), reader);
            assertEquals("codec", expectedCodec, in.getMetaString("avro.codec"));
            int count = 0;
            while (in.hasNext()) {
                Object obj = in.next();
              //System.out.println("obj = " + (GenericData.Array<Float>)obj);
              assertTrue("Avro result object found that's not expected: " + obj, expected.contains(obj));
              count++;
            }        
            in.close();
            assertEquals(count, expected.size());
          }
        }
      }
    
    private Set<Object> getExpected (String pathstr ) throws IOException {
        
        Set<Object> ret = new HashSet<Object>();
        FileSystem fs = FileSystem.getLocal(new Configuration())  ; 
                                    
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
        
                GenericDatumReader<Object> reader = new GenericDatumReader<Object>();
        
                DataFileStream<Object> in = new DataFileStream<Object>(fs.open(filePath), reader);
        
                while (in.hasNext()) {
                    Object obj = in.next();
                    ret.add(obj);
                }        
                in.close();
            }
        }
        return ret;
  }

}

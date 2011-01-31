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
package org.apache.pig.piggybank.test.storage;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import junit.framework.Assert;
import org.apache.avro.Schema;
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
import org.apache.pig.piggybank.storage.avro.ASCommons;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestAvroStorage {
    
    protected static final Log LOG = LogFactory.getLog(TestAvroStorage.class);
    private static PigServer pigServerLocal = null;
    private static String basedir ;
    private static String outbasedir  ;
    
    public static final PathFilter hiddenPathFilter = new PathFilter() {
        public boolean accept(Path p) {
          String name = p.getName();
          return !name.startsWith("_") && !name.startsWith(".");
        }
      };
      
    @BeforeClass
    public static void setup() throws ExecException {
        if (pigServerLocal == null)  
            pigServerLocal = new PigServer(ExecType.LOCAL);
        
       basedir = "src/test/java/org/apache/pig/piggybank/test/storage/TestAvroStorageData/";
       outbasedir  = "/tmp/TestAvroStorage/";
        
        deleteDirectory(new File (outbasedir));
    }

    @Test
    public void testRecursiveRecord() throws IOException {
        final String str1 = 
        		"{ \"type\" : \"record\", " +
        		  "\"name\": \"Node\" , " +
        		  "\"fields\": [ { \"name\": \"value\", \"type\":\"int\"}, " +
        		                     "{ \"name\": \"next\", \"type\": [\"null\", \"Node\"] } ] }";
        Schema s = Schema.parse(str1);
        Assert.assertTrue(ASCommons.containRecursiveRecord(s));
        
        final String str2 = "{\"type\": \"array\", \"items\": "  + str1 + "}";
        s = Schema.parse(str2);
        Assert.assertTrue(ASCommons.containRecursiveRecord(s));
        
        final String str3 ="[\"null\", " + str2 + "]"; 
        s = Schema.parse(str3);
        Assert.assertTrue(ASCommons.containRecursiveRecord(s));
        
        final String str4 = 
            "{ \"type\" : \"record\", " +
              "\"name\": \"Node\" , " +
              "\"fields\": [ { \"name\": \"value\", \"type\":\"int\"}, " +
                                 "{ \"name\": \"next\", \"type\": [\"null\", \"string\"] } ] }";
        s = Schema.parse(str4);
        Assert.assertFalse(ASCommons.containRecursiveRecord(s));
    }
    
     @Test
     public void testGenericUnion() throws IOException {
      
        final String str1 = "[ \"string\", \"int\", \"boolean\"  ]";
        Schema s = Schema.parse(str1);
        Assert.assertTrue(ASCommons.containGenericUnion(s));
        
        final String str2 = "[ \"string\", \"int\", \"null\"  ]";
        s = Schema.parse(str2);
        Assert.assertTrue(ASCommons.containGenericUnion(s));
        
        final String str3 = "[ \"string\", \"null\"  ]";
        s = Schema.parse(str3);
        Assert.assertFalse(ASCommons.containGenericUnion(s));
        Schema realSchema = ASCommons.getAcceptedType(s);
        Assert.assertEquals(ASCommons.StringSchema, realSchema);
        
        final String str4 =  "{\"type\": \"array\", \"items\": "  + str2 + "}";
        s = Schema.parse(str4);
        Assert.assertTrue(ASCommons.containGenericUnion(s));
        try {
            realSchema = ASCommons.getAcceptedType(s);
            Assert.assertFalse("Should throw a runtime exception when trying to " +
                                            "get accepted type from a unacceptable union", false);
        } catch (Exception e) {
           
        }
        
        final String str5 = 
            "{ \"type\" : \"record\", " +
              "\"name\": \"Node\" , " +
              "\"fields\": [ { \"name\": \"value\", \"type\":\"int\"}, " +
                                 "{ \"name\": \"next\", \"type\": [\"null\", \"int\"] } ] }";
        s = Schema.parse(str5);
        Assert.assertFalse(ASCommons.containGenericUnion(s));
        
        final String str6 = 
            "{ \"type\" : \"record\", " +
              "\"name\": \"Node\" , " +
              "\"fields\": [ { \"name\": \"value\", \"type\":\"int\"}, " +
                                 "{ \"name\": \"next\", \"type\": [\"string\", \"int\"] } ] }";
        s = Schema.parse(str6);
        Assert.assertTrue(ASCommons.containGenericUnion(s));
        
        final String str7 = "[ \"string\"  ]"; /*union with one type*/
        s = Schema.parse(str7);
        Assert.assertFalse(ASCommons.containGenericUnion(s));
        realSchema = ASCommons.getAcceptedType(s);
        Assert.assertEquals(ASCommons.StringSchema, realSchema);
        
        final String str8 = "[  ]"; /*union with no type*/
        s = Schema.parse(str8);
        Assert.assertFalse(ASCommons.containGenericUnion(s));
        realSchema = ASCommons.getAcceptedType(s);
        Assert.assertNull(realSchema);
    }
    
    @Test
    public void testArrayDefault() throws IOException {
        String input = basedir + "test_array.avro"; 
        String output= outbasedir + "testArrayDefault";
        String expected = basedir + "expected_testArrayDefault.avro";
        
        deleteDirectory(new File(output));
        
        String [] queries = {
       " in = LOAD '" + input + " ' USING org.apache.pig.piggybank.storage.avro.AvroStorage ();",
       " STORE in INTO '" + output + "' USING org.apache.pig.piggybank.storage.avro.AvroStorage ();" 
        };
        testAvroStorage( queries);
        verifyResults(output, expected);
    }
    
    @Test
    public void testArrayWithSchema() throws IOException {
        String input = basedir + "test_array.avro"; 
        String output= outbasedir + "testArrayWithSchema";
        String expected = basedir + "expected_testArrayWithSchema.avro";
        deleteDirectory(new File(output));
        String [] queries = {
       " in = LOAD '" + input + " ' USING org.apache.pig.piggybank.storage.avro.AvroStorage ();",
       " STORE in INTO '" + output + 
           "' USING org.apache.pig.piggybank.storage.avro.AvroStorage ( "  +
           "   'schema', '{\"type\":\"array\",\"items\":\"float\"}'  );" 
        };
        testAvroStorage( queries);
        verifyResults(output, expected);
    }
    
    @Test
    public void testArrayWithNotNull() throws IOException {
        String input = basedir + "test_array.avro"; 
        String output= outbasedir + "testArrayWithNotNull";
        String expected = basedir + "expected_testArrayWithSchema.avro";
        deleteDirectory(new File(output));
        String [] queries = {
       " in = LOAD '" + input + " ' USING org.apache.pig.piggybank.storage.avro.AvroStorage ();",
       " STORE in INTO '" + output + 
           "' USING org.apache.pig.piggybank.storage.avro.AvroStorage ( "  +
           "   '{\"nullable\": false }'  );" 
        };
        testAvroStorage( queries);
        verifyResults(output, expected);
    }
    
    @Test
    public void testArrayWithSame() throws IOException {

        String input = basedir + "test_array.avro"; 
        String output= outbasedir + "testArrayWithSame";
        String expected = basedir + "expected_testArrayWithSchema.avro";
        deleteDirectory(new File(output));
        String [] queries = {
       " in = LOAD '" + input + " ' USING org.apache.pig.piggybank.storage.avro.AvroStorage ();",
       " STORE in INTO '" + output + 
           "' USING org.apache.pig.piggybank.storage.avro.AvroStorage ( "  +
           "   'same', '" + input + "'  );" 
        };
        testAvroStorage( queries);
        verifyResults(output, expected);
    }
    
    @Test
    public void testRecordWithSplit() throws IOException {

        String input = basedir + "test_record.avro"; 
        String output1= outbasedir + "testRecordSplit1";
        String output2= outbasedir + "testRecordSplit2";
        String expected1 = basedir + "expected_testRecordSplit1.avro";
        String expected2 = basedir + "expected_testRecordSplit2.avro";
        deleteDirectory(new File(output1));
        deleteDirectory(new File(output2));
        String [] queries = {
       " avro = LOAD '" + input + " ' USING org.apache.pig.piggybank.storage.avro.AvroStorage ();",
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
    public void testRecordWithFieldSchema() throws IOException {

        String input = basedir + "test_record.avro"; 
        String output= outbasedir + "testRecordWithFieldSchema";
        String expected = basedir + "expected_testRecordWithFieldSchema.avro";
        deleteDirectory(new File(output));
        String [] queries = {
       " avro = LOAD '" + input + " ' USING org.apache.pig.piggybank.storage.avro.AvroStorage ();",
       " avro1 = FILTER avro BY member_id > 1211;",
       " avro2 = FOREACH avro1 GENERATE member_id, browser_id, tracking_time, act_content ;",
       " STORE avro2 INTO '" + output + "' " +
             " USING org.apache.pig.piggybank.storage.avro.AvroStorage (" +
             "'{\"data\":  \"" + input + "\" ," +
             "  \"field0\": \"int\", " +
              " \"field1\":  \"def:browser_id\", " +
             "  \"field3\": \"def:act_content\" " +
            " }');"
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
    
    private void testAvroStorage(String ...queries)
    throws IOException {
        
         PigServer pigServer = pigServerLocal; 
        pigServer.setBatchOn();
       for (String query: queries){
            if (query != null && query.length() > 0)
              pigServer.registerQuery(query);
        }
        
        pigServer.executeBatch();
    }
    
    private void verifyResults(String outPath,
                                    String expectedOutpath) 
    throws IOException {
        FileSystem fs = FileSystem.getLocal(new Configuration()) ; 
        
        /* read in expected results*/
        Set<Object> expected = getExpected (expectedOutpath);
        
        /* read in output results and compare */
        Path output = new Path(outPath);
        Assert.assertTrue("Output dir does not exists!", fs.exists(output)
            && fs.getFileStatus(output).isDir());
        
        Path[] paths = FileUtil.stat2Paths(fs.listStatus(output, hiddenPathFilter));
        Assert.assertTrue("Split field dirs not found!", paths != null);

        for (Path path : paths) {
          //String splitField = path.getName();
          Path[] files = FileUtil.stat2Paths(fs.listStatus(path, hiddenPathFilter));
          Assert.assertTrue("No files found for path: " + path.toUri().getPath(),
              files != null);
          for (Path filePath : files) {
            Assert.assertTrue("This shouldn't be a directory", fs.isFile(filePath));
            
            GenericDatumReader<Object> reader = new GenericDatumReader<Object>();
            
            DataFileStream<Object> in = new DataFileStream<Object>(
                                            fs.open(filePath), reader);
            
            int count = 0;
            while (in.hasNext()) {
                Object obj = in.next();
                
              Assert.assertTrue( expected.contains(obj));
              count++;
            }        
            in.close();
            Assert.assertEquals(count, expected.size());
          }
        }
      }
    
    private Set<Object> getExpected (String pathstr ) throws IOException {
        
        Set<Object> ret = new HashSet<Object>();
        FileSystem fs = FileSystem.getLocal(new Configuration())  ; 
                                    
        /* read in output results and compare */
        Path output = new Path(pathstr);
        Assert.assertTrue("Expected output does not exists!", fs.exists(output));
    
        Path[] paths = FileUtil.stat2Paths(fs.listStatus(output, hiddenPathFilter));
        Assert.assertTrue("Split field dirs not found!", paths != null);

        for (Path path : paths) {
            //String splitField = path.getName();
            Path[] files = FileUtil.stat2Paths(fs.listStatus(path, hiddenPathFilter));
            Assert.assertTrue("No files found for path: " + path.toUri().getPath(),
                                            files != null);
            for (Path filePath : files) {
                Assert.assertTrue("This shouldn't be a directory", fs.isFile(filePath));
        
                GenericDatumReader<Object> reader = new GenericDatumReader<Object>();
        
                DataFileStream<Object> in = new DataFileStream<Object>(
                                        fs.open(filePath), reader);
        
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

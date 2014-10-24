/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the
 * NOTICE file distributed with this work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.pig.piggybank.test.storage;

import static org.apache.pig.ExecType.LOCAL;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import junit.framework.TestCase;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MRConfiguration;
import org.apache.pig.data.Tuple;
import org.apache.pig.test.Util;
import org.w3c.dom.Document;

public class TestXMLLoader extends TestCase {
  public static ArrayList<String[]> data = new ArrayList<String[]>();
  static {
    data.add(new String[] { "<configuration>"});
    data.add(new String[] { "<property>"});
    data.add(new String[] { "<name> foobar </name>"});
    data.add(new String[] { "<value> barfoo </value>"});
    data.add(new String[] { "</property>"});
    data.add(new String[] { "<ignoreProperty>"});
    data.add(new String[] { "<name> foo </name>"});
    data.add(new String[] { "</ignoreProperty>"});
    data.add(new String[] { "<property>"});
    data.add(new String[] { "<name> justname </name>"});
    data.add(new String[] { "</property>"});
    data.add(new String[] { "</configuration>"});
  }

  public static ArrayList<String[]> nestedTags = new ArrayList<String[]>();
  static {
     nestedTags.add(new String[] { "<events>"});
     nestedTags.add(new String[] { "<event id='116913365'>"});
     nestedTags.add(new String[] { "<eventRank>1.000000000000</eventRank>"});
     nestedTags.add(new String[] { "<name>XY</name>"});
     nestedTags.add(new String[] { "<relatedEvents>"});
     nestedTags.add(new String[] { "<event id='116913365'>x</event>"});
     nestedTags.add(new String[] { "<event id='116913365'>y</event>"});
     nestedTags.add(new String[] { "</relatedEvents>"});
     nestedTags.add(new String[] { "</event>"});

     nestedTags.add(new String[] { "<event id='116913365'>"});
     nestedTags.add(new String[] { "<eventRank>3.0000</eventRank>"});
     nestedTags.add(new String[] { "<name>AB</name>"});
     nestedTags.add(new String[] { "<relatedEvents>"});
     nestedTags.add(new String[] { "<event id='116913365'>a</event>"});
     nestedTags.add(new String[] { "<event id='116913365'>b</event>"});
     nestedTags.add(new String[] { "</relatedEvents>"});
     nestedTags.add(new String[] { "</event>"});

     nestedTags.add(new String[] { "<event>"});
     nestedTags.add(new String[] { "<eventRank>4.0000</eventRank>"});
     nestedTags.add(new String[] { "<name>CD</name>"});
     nestedTags.add(new String[] { "<relatedEvents>"});
     nestedTags.add(new String[] { "<event>c</event>"});
     nestedTags.add(new String[] { "<event>d</event>"});
     nestedTags.add(new String[] { "</relatedEvents>"});
     nestedTags.add(new String[] { "</event>"});
     nestedTags.add(new String[] { "</events>"});
  }

  public static ArrayList<String[]> inlineClosedTags = new ArrayList<String[]>();
  static {
    inlineClosedTags.add(new String[] { "<events>"});
    inlineClosedTags.add(new String[] { "<event id='3423'/>"});
    inlineClosedTags.add(new String[] { "<event/>"});
    inlineClosedTags.add(new String[] { "<event><event/></event>"});
    inlineClosedTags.add(new String[] { "<event id='33'><tag k='a' v='b'/></event>"});
    inlineClosedTags.add(new String[] { "</events>"});
  }

    public static ArrayList<String[]> indentedXmlWithMultilineLineContent = new ArrayList<String[]>();
    static {
        indentedXmlWithMultilineLineContent.add(new String[] { "    <page>You have " });
        indentedXmlWithMultilineLineContent.add(new String[] { "not missed it</page>" });
    }

  public void testShouldReturn0TupleCountIfSearchTagIsNotFound () throws Exception {
    String filename = TestHelper.createTempFile(data, "");
    PigServer pig = new PigServer(LOCAL);
    filename = filename.replace("\\", "\\\\");
    String query = "A = LOAD '" + filename + "' USING org.apache.pig.piggybank.storage.XMLLoader('invalid') as (doc:chararray);";
    pig.registerQuery(query);
    Iterator<?> it = pig.openIterator("A");
    int tupleCount = 0;
    while (it.hasNext()) {
      Tuple tuple = (Tuple) it.next();
      if (tuple == null)
        break;
      else {
        if (tuple.size() > 0) {
            tupleCount++;
        }
      }
    }
    assertEquals(0, tupleCount);
  }

  public void testLoadXMLLoader() throws Exception {
    //ArrayList<DataByteArray[]> expected = TestHelper.getExpected(data, pattern);
    String filename = TestHelper.createTempFile(data, "");
    PigServer pig = new PigServer(LOCAL);
    filename = filename.replace("\\", "\\\\");
    String query = "A = LOAD '" + filename + "' USING org.apache.pig.piggybank.storage.XMLLoader('property') as (doc:chararray);";
    pig.registerQuery(query);
    Iterator<?> it = pig.openIterator("A");
    int tupleCount = 0;
    while (it.hasNext()) {
      Tuple tuple = (Tuple) it.next();
      if (tuple == null)
        break;
      else {
        if (tuple.size() > 0) {
            tupleCount++;
        }
      }
    }
    assertEquals(2, tupleCount);
  }

  public void testXMLLoaderShouldLoadBasicBzip2Files() throws Exception {
    String filename = TestHelper.createTempFile(data, "");
    Process bzipProc = Runtime.getRuntime().exec("bzip2 "+filename);
    int waitFor = bzipProc.waitFor();

    if(waitFor != 0) {
        fail ("Failed to create the class");
    }

    filename = filename + ".bz2";

    try {
        PigServer pigServer = new PigServer (ExecType.LOCAL);
        String loadQuery = "A = LOAD '" + Util.encodeEscape(filename) + "' USING org.apache.pig.piggybank.storage.XMLLoader('property') as (doc:chararray);";
        pigServer.registerQuery(loadQuery);

        Iterator<Tuple> it = pigServer.openIterator("A");
        int tupleCount = 0;
        while (it.hasNext()) {
            Tuple tuple = (Tuple) it.next();
            if (tuple == null)
                break;
            else {
                //TestHelper.examineTuple(expected, tuple, tupleCount);
                if (tuple.size() > 0) {
                    tupleCount++;
                }
            }
        }
        assertEquals(2, tupleCount);

    } finally {
        new File(filename).delete();
    }
   }

   public void testLoaderShouldLoadBasicGzFile() throws Exception {
    String filename = TestHelper.createTempFile(data, "");

    Process bzipProc = Runtime.getRuntime().exec("gzip "+filename);
    int waitFor = bzipProc.waitFor();

    if(waitFor != 0) {
        fail ("Failed to create the class");
    }

    filename = filename + ".gz";

    try {
        PigServer pigServer = new PigServer (ExecType.LOCAL);
        String loadQuery = "A = LOAD '" + Util.encodeEscape(filename) + "' USING org.apache.pig.piggybank.storage.XMLLoader('property') as (doc:chararray);";
        pigServer.registerQuery(loadQuery);

        Iterator<Tuple> it = pigServer.openIterator("A");
        int tupleCount = 0;
        while (it.hasNext()) {
            Tuple tuple = (Tuple) it.next();
            if (tuple == null)
                break;
            else {
                if (tuple.size() > 0) {
                    tupleCount++;
                }
            }
        }
        assertEquals(2, tupleCount);

    } finally {
        new File(filename).delete();
    }
 }

   public void testXMLLoaderShouldNotConfusedWithTagsHavingSimilarPrefix () throws Exception {
      ArrayList<String[]> testData = new ArrayList<String[]>();
      testData.add(new String[] { "<namethisalso> foobar9 </namethisalso>"});
      testData.addAll(data);
      String filename = TestHelper.createTempFile(testData, "");
      PigServer pig = new PigServer(LOCAL);
      filename = filename.replace("\\", "\\\\");
      String query = "A = LOAD '" + filename + "' USING org.apache.pig.piggybank.storage.XMLLoader('name') as (doc:chararray);";
      pig.registerQuery(query);
      Iterator<?> it = pig.openIterator("A");
      int tupleCount = 0;
      while (it.hasNext()) {
          Tuple tuple = (Tuple) it.next();
          if (tuple == null)
              break;
          else {
              if (tuple.size() > 0) {
                  tupleCount++;
              }
          }
      }
      assertEquals(3, tupleCount);
   }

   public void testShouldReturn1ForIntermediateTagData () throws Exception {
      String filename = TestHelper.createTempFile(data, "");
      PigServer pig = new PigServer(LOCAL);
      filename = filename.replace("\\", "\\\\");
      String query = "A = LOAD '" + filename + "' USING org.apache.pig.piggybank.storage.XMLLoader('ignoreProperty') as (doc:chararray);";
      pig.registerQuery(query);
      Iterator<?> it = pig.openIterator("A");
      int tupleCount = 0;
      while (it.hasNext()) {
          Tuple tuple = (Tuple) it.next();
          if (tuple == null)
              break;
          else {
              if (tuple.size() > 0) {
                  tupleCount++;
              }
          }
      }
      assertEquals(1, tupleCount);
   }

   public void testShouldReturn0TupleCountIfNoEndTagIsFound() throws Exception {
      // modify the data content to avoid end tag for </ignoreProperty>
      ArrayList<String[]> testData = new ArrayList<String[]>();
      for (String content[] : data) {
         if(!content[0].equals("</ignoreProperty>")) {
            testData.add(content);
         }
      }

      String filename = TestHelper.createTempFile(testData, "");
      PigServer pig = new PigServer(LOCAL);
      filename = filename.replace("\\", "\\\\");
      String query = "A = LOAD '" + filename + "' USING org.apache.pig.piggybank.storage.XMLLoader('ignoreProperty') as (doc:chararray);";
      pig.registerQuery(query);
      Iterator<?> it = pig.openIterator("A");
      int tupleCount = 0;
      while (it.hasNext()) {
          Tuple tuple = (Tuple) it.next();
          if (tuple == null)
              break;
          else {
              if (tuple.size() > 0) {
                  tupleCount++;
              }
          }
      }
      assertEquals(0, tupleCount);
   }

   public void testShouldReturn0TupleCountIfEmptyFileIsPassed() throws Exception {
      // modify the data content to avoid end tag for </ignoreProperty>
      ArrayList<String[]> testData = new ArrayList<String[]>();

      String filename = TestHelper.createTempFile(testData, "");
      PigServer pig = new PigServer(LOCAL);
      filename = filename.replace("\\", "\\\\");
      String query = "A = LOAD '" + filename + "' USING org.apache.pig.piggybank.storage.XMLLoader('ignoreProperty') as (doc:chararray);";
      pig.registerQuery(query);
      Iterator<?> it = pig.openIterator("A");
      int tupleCount = 0;
      while (it.hasNext()) {
          Tuple tuple = (Tuple) it.next();
          if (tuple == null)
              break;
          else {
              if (tuple.size() > 0) {
                  tupleCount++;
              }
          }
      }
      assertEquals(0, tupleCount);
   }

   public void testXMLLoaderShouldSupportNestedTagWithSameName() throws Exception {
      String filename = TestHelper.createTempFile(nestedTags, "");
      PigServer pig = new PigServer(LOCAL);
      filename = filename.replace("\\", "\\\\");
      String query = "A = LOAD '" + filename + "' USING org.apache.pig.piggybank.storage.XMLLoader('event') as (doc:chararray);";
      pig.registerQuery(query);
      Iterator<?> it = pig.openIterator("A");
      int tupleCount = 0;
      while (it.hasNext()) {
          Tuple tuple = (Tuple) it.next();
          if (tuple == null)
              break;
          else {
              if (tuple.size() > 0) {
                  tupleCount++;
              }
          }
      }
      assertEquals(3, tupleCount);
   }

   public void testXMLLoaderShouldWorkWithInlineClosedTags() throws Exception {
     String filename = TestHelper.createTempFile(inlineClosedTags, "");
     PigServer pig = new PigServer(LOCAL);
     filename = filename.replace("\\", "\\\\");
     String query = "A = LOAD '" + filename + "' USING org.apache.pig.piggybank.storage.XMLLoader('event') as (doc:chararray);";
     pig.registerQuery(query);
     Iterator<?> it = pig.openIterator("A");
     int tupleCount = 0;
     while (it.hasNext()) {
         Tuple tuple = (Tuple) it.next();
         if (tuple == null)
             break;
         else {
             if (tuple.size() > 0) {
                 tupleCount++;
             }
         }
     }
     assertEquals(4, tupleCount);
   }

    public void testXMLLoaderShouldWorkWithIndentedXmlWithMultilineContent() throws Exception {
        String filename = TestHelper.createTempFile(indentedXmlWithMultilineLineContent, "");
        PigServer pig = new PigServer(LOCAL);
        filename = filename.replace("\\", "\\\\");
        String query = "A = LOAD '" + filename + "' USING org.apache.pig.piggybank.storage.XMLLoader('page') as (doc:chararray);";
        pig.registerQuery(query);
        Iterator<?> it = pig.openIterator("A");
        int tupleCount = 0;
        while (it.hasNext()) {
            Tuple tuple = (Tuple) it.next();
            if (tuple == null)
                break;
            else {
                System.out.println(((String) tuple.get(0)));
                assertTrue(((String) tuple.get(0)).equals("<page>You have not missed it</page>"));
                tupleCount++;
            }
        }
        assertEquals(1, tupleCount);
    }

   public void testXMLLoaderShouldReturnValidXML() throws Exception {
     String filename = TestHelper.createTempFile(inlineClosedTags, "");
     PigServer pig = new PigServer(LOCAL);
     filename = filename.replace("\\", "\\\\");
     String query = "A = LOAD '" + filename + "' USING org.apache.pig.piggybank.storage.XMLLoader('event') as (doc:chararray);";
     pig.registerQuery(query);
     Iterator<?> it = pig.openIterator("A");
     while (it.hasNext()) {
         Tuple tuple = (Tuple) it.next();
         if (tuple == null)
             break;
         else {
             // Test it returns a valid XML
             DocumentBuilder docBuilder =
                     DocumentBuilderFactory.newInstance().newDocumentBuilder();
             docBuilder.parse(new ByteArrayInputStream(((String)tuple.get(0)).getBytes()));
         }
     }
   }

   /**
    * This test case test the special case when a non-matching tag spans two file
    * splits in a .bz2 compressed file. At the same time, the part that falls in
    * the first split is a prefix of the matching tag.
    * In other words, till the end of the first split, it looks like the tag is
    * matching but it is not actually matching.
    *
    * @throws Exception
    */
   public void testXMLLoaderShouldNotReturnLastNonMatchedTag() throws Exception {
     Configuration conf = new Configuration();
     long blockSize = 100 * 1024;
     conf.setLong("fs.local.block.size", blockSize);

     String tagName = "event";

     PigServer pig = new PigServer(LOCAL, conf);
     FileSystem localFs = FileSystem.getLocal(conf);
     FileStatus[] testFiles = localFs.globStatus(new Path("src/test/java/org/apache/pig/piggybank/test/evaluation/xml/data/*xml.bz2"));
     assertTrue("No test files", testFiles.length > 0);
     for (FileStatus testFile : testFiles) {
       String testFileName = testFile.getPath().toUri().getPath().replace("\\", "\\\\");
       String query = "A = LOAD '" + testFileName + "' USING org.apache.pig.piggybank.storage.XMLLoader('event') as (doc:chararray);";
       pig.registerQuery(query);
       Iterator<?> it = pig.openIterator("A");
       while (it.hasNext()) {
         Tuple tuple = (Tuple) it.next();
         if (tuple == null)
           break;
         else {
           if (tuple.size() > 0) {
             assertTrue(((String)tuple.get(0)).startsWith("<"+tagName+">"));
           }
         }
       }
     }
   }

   /**
    * This test checks that a multi-line tag spanning two splits should be
    * matched.
    * @throws Exception
    */
   public void testXMLLoaderShouldMatchTagSpanningSplits() throws Exception {
     Configuration conf = new Configuration();
     long blockSize = 512;
     conf.setLong("fs.local.block.size", blockSize);
     conf.setLong(MRConfiguration.MAX_SPLIT_SIZE, blockSize);

     String tagName = "event";
     File tempFile = File.createTempFile("long-file", ".xml");
     FileSystem localFs = FileSystem.getLocal(conf);
     FSDataOutputStream directOut = localFs.create(new Path(tempFile.getAbsolutePath()), true);

     String matchingElement = "<event>\ndata\n</event>\n";
     long pos = 0;
     int matchingCount = 0;
     PrintStream ps = new PrintStream(directOut);
     // 1- Write some elements that fit completely in the first block
     while (pos + 2 * matchingElement.length() < blockSize) {
       ps.print(matchingElement);
       pos += matchingElement.length();
       matchingCount++;
     }
     // 2- Write a long element that spans multiple lines and multiple blocks
     String longElement = matchingElement.replace("data",
         "data\ndata\ndata\ndata\ndata\ndata\ndata\ndata\ndata\ndata\ndata\n");
     ps.print(longElement);
     pos += longElement.length();
     matchingCount++;
     // 3- Write some more elements to fill in the second block completely
     while (pos < 2 * blockSize) {
       ps.print(matchingElement);
       pos += matchingElement.length();
       matchingCount++;
     }
     ps.close();

     PigServer pig = new PigServer(LOCAL, conf);
     String tempFileName = tempFile.getAbsolutePath().replace("\\", "\\\\");
     String query = "A = LOAD '" + tempFileName + "' USING org.apache.pig.piggybank.storage.XMLLoader('event') as (doc:chararray);";
     pig.registerQuery(query);
     Iterator<?> it = pig.openIterator("A");

     int count = 0;
     while (it.hasNext()) {
       Tuple tuple = (Tuple) it.next();
       if (tuple == null)
         break;
       else {
         if (tuple.size() > 0) {
           count++;
           // Make sure the returned text is a proper XML element
           DocumentBuilder docBuilder =
               DocumentBuilderFactory.newInstance().newDocumentBuilder();
           Document doc = docBuilder.parse(new ByteArrayInputStream(((String)tuple.get(0)).getBytes()));
           assertTrue(doc.getDocumentElement().getNodeName().equals(tagName));
         }
       }
     }
     assertEquals(matchingCount, count);
   }
}

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

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;

import junit.framework.TestCase;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.data.Tuple;

public class TestXMLLoader extends TestCase {
  private static String patternString = "(\\d+)!+(\\w+)~+(\\w+)";
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
  
  public void testShouldReturn0TupleCountIfSearchTagIsNotFound () throws Exception
  {
    String filename = TestHelper.createTempFile(data, "");
    PigServer pig = new PigServer(LOCAL);
    filename = filename.replace("\\", "\\\\");
    patternString = patternString.replace("\\", "\\\\");
    String query = "A = LOAD 'file:" + filename + "' USING org.apache.pig.piggybank.storage.XMLLoader('invalid') as (doc:chararray);";
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
    patternString = patternString.replace("\\", "\\\\");
    String query = "A = LOAD 'file:" + filename + "' USING org.apache.pig.piggybank.storage.XMLLoader('property') as (doc:chararray);";
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
  
    if(waitFor != 0)
    {
        fail ("Failed to create the class");
    }

    filename = filename + ".bz2";

    try
    {
        PigServer pigServer = new PigServer (ExecType.LOCAL);
        String loadQuery = "A = LOAD 'file:" + filename + "' USING org.apache.pig.piggybank.storage.XMLLoader('property') as (doc:chararray);";
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
    
    }finally
    {
        new File(filename).delete();
    }
	    
   }

   public void testLoaderShouldLoadBasicGzFile() throws Exception {
    String filename = TestHelper.createTempFile(data, "");
	  
    Process bzipProc = Runtime.getRuntime().exec("gzip "+filename);
    int waitFor = bzipProc.waitFor();
	  
    if(waitFor != 0)
    {
        fail ("Failed to create the class");
    }
	  
    filename = filename + ".gz";
    
    try
    {

    PigServer pigServer = new PigServer (ExecType.LOCAL);
    String loadQuery = "A = LOAD 'file:" + filename + "' USING org.apache.pig.piggybank.storage.XMLLoader('property') as (doc:chararray);";
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
   
    }finally
    {
        new File(filename).delete();
    }
 }
   
   public void testXMLLoaderShouldNotConfusedWithTagsHavingSimilarPrefix () throws Exception
   {
      ArrayList<String[]> testData = new ArrayList<String[]>();
      testData.add(new String[] { "<namethisalso> foobar9 </namethisalso>"});
      testData.addAll(data);
      String filename = TestHelper.createTempFile(testData, "");
      PigServer pig = new PigServer(LOCAL);
      filename = filename.replace("\\", "\\\\");
      patternString = patternString.replace("\\", "\\\\");
      String query = "A = LOAD 'file:" + filename + "' USING org.apache.pig.piggybank.storage.XMLLoader('name') as (doc:chararray);";
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
   
   public void testShouldReturn1ForIntermediateTagData () throws Exception
   {
      String filename = TestHelper.createTempFile(data, "");
      PigServer pig = new PigServer(LOCAL);
      filename = filename.replace("\\", "\\\\");
      patternString = patternString.replace("\\", "\\\\");
      String query = "A = LOAD 'file:" + filename + "' USING org.apache.pig.piggybank.storage.XMLLoader('ignoreProperty') as (doc:chararray);";
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
   
   public void testShouldReturn0TupleCountIfNoEndTagIsFound() throws Exception
   {
      // modify the data content to avoid end tag for </ignoreProperty>
      ArrayList<String[]> testData = new ArrayList<String[]>();
      for (String content[] : data) {
         
         if(false == data.equals(new String[] { "</ignoreProperty>"}))
         {
            testData.add(content);
         }
      }
      
      String filename = TestHelper.createTempFile(testData, "");
      PigServer pig = new PigServer(LOCAL);
      filename = filename.replace("\\", "\\\\");
      patternString = patternString.replace("\\", "\\\\");
      String query = "A = LOAD 'file:" + filename + "' USING org.apache.pig.piggybank.storage.XMLLoader('</ignoreProperty>') as (doc:chararray);";
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
   
   public void testShouldReturn0TupleCountIfEmptyFileIsPassed() throws Exception
   {
      // modify the data content to avoid end tag for </ignoreProperty>
      ArrayList<String[]> testData = new ArrayList<String[]>();
      
      String filename = TestHelper.createTempFile(testData, "");
      PigServer pig = new PigServer(LOCAL);
      filename = filename.replace("\\", "\\\\");
      patternString = patternString.replace("\\", "\\\\");
      String query = "A = LOAD 'file:" + filename + "' USING org.apache.pig.piggybank.storage.XMLLoader('</ignoreProperty>') as (doc:chararray);";
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
      patternString = patternString.replace("\\", "\\\\");
      String query = "A = LOAD 'file:" + filename + "' USING org.apache.pig.piggybank.storage.XMLLoader('event') as (doc:chararray);";
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
   
   
}

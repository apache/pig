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

import static org.apache.pig.ExecType.LOCAL;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import junit.framework.TestCase;
import org.junit.Test;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.pig.PigServer;
import org.apache.pig.data.Tuple;
//import org.apache.pig.test.PigExecTestCase;
import org.apache.pig.test.Util;

//public class TestSequenceFileLoader extends PigExecTestCase  {
  public class TestSequenceFileLoader extends TestCase {
  private static final String[] DATA = {
    "one, two, buckle my shoe",
    "three, four, shut the door",
    "five, six, something else" };
  
  private static final String[][] EXPECTED = {
    {"0", "one, two, buckle my shoe"},
    {"1", "three, four, shut the door"},
    {"2", "five, six, something else"}
  };
  
  private String tmpFileName;
  
  private PigServer pigServer;
  @Override
  public void setUp() throws Exception {
    pigServer = new PigServer(LOCAL);
    File tmpFile = File.createTempFile("test", ".txt");
    tmpFileName = tmpFile.getAbsolutePath();
    System.err.println("fileName: "+tmpFileName);
    Path path = new Path("file:///"+tmpFileName);
    JobConf conf = new JobConf();
    FileSystem fs = FileSystem.get(path.toUri(), conf);
    
    IntWritable key = new IntWritable();
    Text value = new Text();
    SequenceFile.Writer writer = null;
    try {
      writer = SequenceFile.createWriter(fs, conf, path, 
                                         key.getClass(), value.getClass());
      for (int i=0; i < DATA.length; i++) {
        key.set(i);
        value.set(DATA[i]);
        writer.append(key, value);
      }
    } finally {
      IOUtils.closeStream(writer);
    }
  }
  
  @Test
  public void testReadsNocast() throws IOException {
    pigServer.registerQuery("A = LOAD 'file:" + Util.encodeEscape(tmpFileName) + 
    "' USING org.apache.pig.piggybank.storage.SequenceFileLoader() AS (key, val);");
    Iterator<?> it = pigServer.openIterator("A");
    int tupleCount = 0;
    while (it.hasNext()) {
      Tuple tuple = (Tuple) it.next();
      if (tuple == null)
        break;
      else {
        System.err.println("expect:---: "+EXPECTED[tupleCount][0]);
        assertEquals(EXPECTED[tupleCount][0], tuple.get(0).toString());
        assertEquals(EXPECTED[tupleCount][1], tuple.get(1).toString());
        tupleCount++;
      }
    }
    assertEquals(DATA.length, tupleCount);
  }
  
  @Test
  public void testReadsStringCast() throws IOException {
    pigServer.registerQuery("A = LOAD 'file:" + Util.encodeEscape(tmpFileName) + 
    "' USING org.apache.pig.piggybank.storage.SequenceFileLoader() AS (key:long, val);");
    Iterator<?> it = pigServer.openIterator("A");
    int tupleCount = 0;
    while (it.hasNext()) {
      Tuple tuple = (Tuple) it.next();
      if (tuple == null)
        break;
      else {
        assertEquals(Long.parseLong(EXPECTED[tupleCount][0]), tuple.get(0));
        assertEquals(EXPECTED[tupleCount][1], tuple.get(1));
        tupleCount++;
      }
    }
    assertEquals(DATA.length, tupleCount);
  }
}

/**
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

package org.apache.hadoop.zebra.pig;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.zebra.BaseTestCase;
import org.apache.hadoop.zebra.mapreduce.ZebraOutputPartition;
import org.apache.hadoop.zebra.parser.ParseException;
import org.apache.pig.backend.executionengine.ExecJob;
import org.apache.pig.data.Tuple;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import java.util.Iterator;
import junit.framework.Assert;

/**
 * Assume the input files contain rows of word and count, separated by a space:
 * 
 * <pre>
 * us 2
 * japan 2
 * india 4
 * us 2
 * japan 1
 * india 3
 * nouse 5
 * nowhere 4
 * 
 */
public class TestMultipleOutputs1 extends BaseTestCase implements Tool {
  static String inputPath;
  static String inputFileName = "multi-input.txt";
  public static String sortKey = null;

  @Before
  public void setUp() throws Exception {
    init();
    
    inputPath = getTableFullPath(inputFileName).toString();
    
    writeToFile(inputPath);
  }
  
  @After
  public void tearDown() throws Exception {
    if (mode == TestMode.local) {
      pigServer.shutdown();
    }
  }
  
  public static void writeToFile (String inputFile) throws IOException{
    if (mode == TestMode.local) {
      FileWriter fstream = new FileWriter(inputFile);
      BufferedWriter out = new BufferedWriter(fstream);
      out.write("us\t2\n");
      out.write("japan\t2\n");
      out.write("india\t4\n");
      out.write("us\t2\n");
      out.write("japan\t1\n");
      out.write("india\t3\n");
      out.write("nouse\t5\n");
      out.write("nowhere\t4\n");
      out.close();
    }

    if (mode == TestMode.cluster) {
      FSDataOutputStream fout = fs.create(new Path (inputFile));
      fout.writeBytes("us\t2\n");
      fout.writeBytes("japan\t2\n");
      fout.writeBytes("india\t4\n");
      fout.writeBytes("us\t2\n");
      fout.writeBytes("japan\t1\n");
      fout.writeBytes("india\t3\n");
      fout.writeBytes("nouse\t5\n");
      fout.writeBytes("nowhere\t4\n");
      fout.close();
    }
  }
  
  // test no sort key;
  @Test
  public void test1() throws ParseException, IOException,
      org.apache.hadoop.zebra.parser.ParseException, Exception {
    // Load data;
    String query = "records = LOAD '" + inputPath + "' as (word:chararray, count:int);";
    System.out.println("query = " + query);
    pigServer.registerQuery(query);

    Iterator<Tuple> it = pigServer.openIterator("records");
    while (it.hasNext()) {
      Tuple cur = it.next();
      System.out.println(cur);
    }
    
    // Store using multiple outputs;
    String outputPaths = "us,india,japan";
    removeDir(getTableFullPath("us"));
    removeDir(getTableFullPath("india"));
    removeDir(getTableFullPath("japan"));

    query = "store records into '" + outputPaths + "' using org.apache.hadoop.zebra.pig.TableStorer('[word,count]'," +
        "'org.apache.hadoop.zebra.pig.TestMultipleOutputs1$OutputPartitionerClass');";
    System.out.println("query = " + query);
    pigServer.registerQuery(query);    
    
    // Validate results;
    query = "records = LOAD '" + "us"
          + "' USING org.apache.hadoop.zebra.pig.TableLoader();";

    int count = 0;
    System.out.println(query);
    pigServer.registerQuery(query);
    it = pigServer.openIterator("records");
    while (it.hasNext()) {
      count ++;
      Tuple RowValue = it.next();
      System.out.println(RowValue);
      if (count == 1) {
        Assert.assertEquals("us", RowValue.get(0));
        Assert.assertEquals(2, RowValue.get(1));       
      } else if (count == 2) {
        Assert.assertEquals("us", RowValue.get(0));
        Assert.assertEquals(2, RowValue.get(1));                 
      } else if (count == 3) {
        Assert.assertEquals("nouse", RowValue.get(0));
        Assert.assertEquals(5, RowValue.get(1));       
      } else if (count == 4) {
        Assert.assertEquals("nowhere", RowValue.get(0));
        Assert.assertEquals(4, RowValue.get(1));       
      }
    }
    Assert.assertEquals(count, 4);

    query = "records = LOAD '" + "india"
      + "' USING org.apache.hadoop.zebra.pig.TableLoader();";

    count = 0;
    System.out.println(query);
    pigServer.registerQuery(query);
    it = pigServer.openIterator("records");
    while (it.hasNext()) {
      count ++;
      Tuple RowValue = it.next();
      System.out.println(RowValue);
      if (count == 1) {
        Assert.assertEquals("india", RowValue.get(0));
        Assert.assertEquals(4, RowValue.get(1));       
      } else if (count == 2) {
        Assert.assertEquals("india", RowValue.get(0));
        Assert.assertEquals(3, RowValue.get(1));                 
      } 
    }
    Assert.assertEquals(count, 2);

    query = "records = LOAD '" + "japan"
    + "' USING org.apache.hadoop.zebra.pig.TableLoader();";

    count = 0;
    System.out.println(query);
    pigServer.registerQuery(query);
    it = pigServer.openIterator("records");
    while (it.hasNext()) {
      count ++;
      Tuple RowValue = it.next();
      System.out.println(RowValue);
      if (count == 1) {
        Assert.assertEquals("japan", RowValue.get(0));
        Assert.assertEquals(2, RowValue.get(1));       
      } else if (count == 2) {
        Assert.assertEquals("japan", RowValue.get(0));
        Assert.assertEquals(1, RowValue.get(1));                 
      } 
    }
    Assert.assertEquals(count, 2);
  }    

  //Test sort key on word;
  @Test
  public void test2() throws ParseException, IOException,
      org.apache.hadoop.zebra.parser.ParseException, Exception {
    // Load data;
    String query = "a = LOAD '" + inputPath + "' as (word:chararray, count:int);";
    System.out.println("query = " + query);
    pigServer.registerQuery(query);
    
    query = "records = order a by word;";
    System.out.println("query = " + query);
    pigServer.registerQuery(query);

    Iterator<Tuple> it = pigServer.openIterator("records");
    while (it.hasNext()) {
      Tuple cur = it.next();
      System.out.println(cur);
    }
    
    // Store using multiple outputs;
    String outputPaths = "us,india,japan";
    removeDir(getTableFullPath("us"));
    removeDir(getTableFullPath("india"));
    removeDir(getTableFullPath("japan"));
    ExecJob pigJob = pigServer
      .store(
        "records",
        outputPaths,
        TableStorer.class.getCanonicalName() +
             "('[word,count]', 'org.apache.hadoop.zebra.pig.TestMultipleOutputs1$OutputPartitionerClass')");    
    
    Assert.assertNull(pigJob.getException());
    
    // Validate results;
    query = "records = LOAD '" + "us"
          + "' USING org.apache.hadoop.zebra.pig.TableLoader();";

    int count = 0;
    System.out.println(query);
    pigServer.registerQuery(query);
    it = pigServer.openIterator("records");
    while (it.hasNext()) {
      count ++;
      Tuple RowValue = it.next();
      System.out.println(RowValue);
      if (count == 1) {
        Assert.assertEquals("nouse", RowValue.get(0));
        Assert.assertEquals(5, RowValue.get(1));       
      } else if (count == 2) {
        Assert.assertEquals("nowhere", RowValue.get(0));
        Assert.assertEquals(4, RowValue.get(1));                 
      } else if (count == 3) {
        Assert.assertEquals("us", RowValue.get(0));
        Assert.assertEquals(2, RowValue.get(1));       
      } else if (count == 4) {
        Assert.assertEquals("us", RowValue.get(0));
        Assert.assertEquals(2, RowValue.get(1));       
      }
    }
    Assert.assertEquals(count, 4);

    query = "records = LOAD '" + "india"
      + "' USING org.apache.hadoop.zebra.pig.TableLoader();";

    count = 0;
    System.out.println(query);
    pigServer.registerQuery(query);
    it = pigServer.openIterator("records");
    while (it.hasNext()) {
      count ++;
      Tuple RowValue = it.next();
      System.out.println(RowValue);
      if (count == 1) {
        Assert.assertEquals("india", RowValue.get(0));
        Assert.assertEquals(4, RowValue.get(1));       
      } else if (count == 2) {
        Assert.assertEquals("india", RowValue.get(0));
        Assert.assertEquals(3, RowValue.get(1));                 
      } 
    }
    Assert.assertEquals(count, 2);

    query = "records = LOAD '" + "japan"
    + "' USING org.apache.hadoop.zebra.pig.TableLoader();";

    count = 0;
    System.out.println(query);
    pigServer.registerQuery(query);
    it = pigServer.openIterator("records");
    while (it.hasNext()) {
      count ++;
      Tuple RowValue = it.next();
      System.out.println(RowValue);
      if (count == 1) {
        Assert.assertEquals("japan", RowValue.get(0));
        Assert.assertEquals(2, RowValue.get(1));       
      } else if (count == 2) {
        Assert.assertEquals("japan", RowValue.get(0));
        Assert.assertEquals(1, RowValue.get(1));                 
      } 
    }
    Assert.assertEquals(count, 2);
  }
  
  //Test sort key on word and count;
  @Test
  public void test3() throws ParseException, IOException,
      org.apache.hadoop.zebra.parser.ParseException, Exception {
    // Load data;
    String query = "a = LOAD '" + inputPath + "' as (word:chararray, count:int);";
    System.out.println("query = " + query);
    pigServer.registerQuery(query);
    
    query = "records = order a by word, count;";
    System.out.println("query = " + query);
    pigServer.registerQuery(query);

    Iterator<Tuple> it = pigServer.openIterator("records");
    while (it.hasNext()) {
      Tuple cur = it.next();
      System.out.println(cur);
    }
    
    // Store using multiple outputs;
    String outputPaths = "us,india,japan";
    removeDir(getTableFullPath("us"));
    removeDir(getTableFullPath("india"));
    removeDir(getTableFullPath("japan"));
    ExecJob pigJob = pigServer
      .store(
        "records",
        outputPaths,
        TableStorer.class.getCanonicalName() +
             "('[word,count]', 'org.apache.hadoop.zebra.pig.TestMultipleOutputs1$OutputPartitionerClass')");    
    
    Assert.assertNull(pigJob.getException());
    
    // Validate results;
    query = "records = LOAD '" + "us"
          + "' USING org.apache.hadoop.zebra.pig.TableLoader();";

    int count = 0;
    System.out.println(query);
    pigServer.registerQuery(query);
    it = pigServer.openIterator("records");
    while (it.hasNext()) {
      count ++;
      Tuple RowValue = it.next();
      System.out.println(RowValue);
      if (count == 1) {
        Assert.assertEquals("nouse", RowValue.get(0));
        Assert.assertEquals(5, RowValue.get(1));       
      } else if (count == 2) {
        Assert.assertEquals("nowhere", RowValue.get(0));
        Assert.assertEquals(4, RowValue.get(1));                 
      } else if (count == 3) {
        Assert.assertEquals("us", RowValue.get(0));
        Assert.assertEquals(2, RowValue.get(1));       
      } else if (count == 4) {
        Assert.assertEquals("us", RowValue.get(0));
        Assert.assertEquals(2, RowValue.get(1));       
      }
    }
    Assert.assertEquals(count, 4);

    query = "records = LOAD '" + "india"
      + "' USING org.apache.hadoop.zebra.pig.TableLoader();";

    count = 0;
    System.out.println(query);
    pigServer.registerQuery(query);
    it = pigServer.openIterator("records");
    while (it.hasNext()) {
      count ++;
      Tuple RowValue = it.next();
      System.out.println(RowValue);
      if (count == 1) {
        Assert.assertEquals("india", RowValue.get(0));
        Assert.assertEquals(3, RowValue.get(1));       
      } else if (count == 2) {
        Assert.assertEquals("india", RowValue.get(0));
        Assert.assertEquals(4, RowValue.get(1));                 
      } 
    }
    Assert.assertEquals(count, 2);

    query = "records = LOAD '" + "japan"
    + "' USING org.apache.hadoop.zebra.pig.TableLoader();";

    count = 0;
    System.out.println(query);
    pigServer.registerQuery(query);
    it = pigServer.openIterator("records");
    while (it.hasNext()) {
      count ++;
      Tuple RowValue = it.next();
      System.out.println(RowValue);
      if (count == 1) {
        Assert.assertEquals("japan", RowValue.get(0));
        Assert.assertEquals(1, RowValue.get(1));       
      } else if (count == 2) {
        Assert.assertEquals("japan", RowValue.get(0));
        Assert.assertEquals(2, RowValue.get(1));                 
      } 
    }
    Assert.assertEquals(count, 2);
  }
  
  //Negative test case: invalid partition class;
  @Test (expected = IOException.class)
  public void testNegative1() throws ParseException, IOException,
      org.apache.hadoop.zebra.parser.ParseException, Exception {
    // Load data;
    String query = "a = LOAD '" + inputPath + "' as (word:chararray, count:int);";
    System.out.println("query = " + query);
    pigServer.registerQuery(query);
    
    query = "records = order a by word, count;";
    System.out.println("query = " + query);
    pigServer.registerQuery(query);

    Iterator<Tuple> it = pigServer.openIterator("records");
    while (it.hasNext()) {
      Tuple cur = it.next();
      System.out.println(cur);
    }
    
    // Store using multiple outputs;
    String outputPaths = "us,india,japan";
    removeDir(getTableFullPath("us"));
    removeDir(getTableFullPath("india"));
    removeDir(getTableFullPath("japan"));
    pigServer
      .store(
        "records",
        outputPaths,
        TableStorer.class.getCanonicalName() +
             "('[word,count]', 'org.apache.hadoop.zebra.pig.notexistingclass')");    
  }
  
  public static class OutputPartitionerClass extends ZebraOutputPartition {

    @Override
    public int getOutputPartition(BytesWritable key, Tuple value) {
      String reg = null;
      try {
        reg = (String) (value.get(0));
      } catch (Exception e) {
        //
      }

      if (reg.equals("us"))
        return 0;
      if (reg.equals("india"))
        return 1;
      if (reg.equals("japan"))
        return 2;

      return 0;
    }
  }

  @Override
  public int run(String[] args) throws Exception {
    TestMultipleOutputs1 test = new TestMultipleOutputs1();
    
    test.setUp();
    test.test1();
    test.tearDown();
    
    test.setUp();
    test.test2();
    test.tearDown();    

    test.setUp();
    test.test3();
    test.tearDown();    

    test.setUp();
    test.testNegative1();
    test.tearDown();
    
    return 0;
  }
  
  public static void main(String[] args) throws Exception {
    conf = new Configuration();
    
    int res = ToolRunner.run(conf, new TestMultipleOutputs1(), args);
    System.out.println("PASS");
    System.exit(res);
  }
}

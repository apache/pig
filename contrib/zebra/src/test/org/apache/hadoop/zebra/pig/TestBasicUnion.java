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

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import junit.framework.Assert;
import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.zebra.io.BasicTable;
import org.apache.hadoop.zebra.io.TableInserter;
import org.apache.hadoop.zebra.io.TableScanner;
import org.apache.hadoop.zebra.io.BasicTable.Reader.RangeSplit;
import org.apache.hadoop.zebra.parser.ParseException;
import org.apache.hadoop.zebra.schema.Schema;
import org.apache.hadoop.zebra.types.TypesUtils;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.test.MiniCluster;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Note:
 * 
 * Make sure you add the build/pig-0.1.0-dev-core.jar to the Classpath of the
 * app/debug configuration, when run this from inside the Eclipse.
 * 
 */
public class TestBasicUnion {
  protected static ExecType execType = ExecType.MAPREDUCE;
  private static MiniCluster cluster;
  protected static PigServer pigServer;
  private static Path pathWorking, pathTable1, pathTable2, pathTable3,
      pathTable4, pathTable5;
  private static Configuration conf;
  final static String STR_SCHEMA1 = "a:string,b,c:string,e,f";
  final static String STR_STORAGE1 = "[a];[c]";
  final static String STR_SCHEMA2 = "a:string,b,d:string,f,e";
  final static String STR_STORAGE2 = "[a,b];[d]";
  final static String STR_SCHEMA3 = "b,a";
  final static String STR_STORAGE3 = "[a];[b]";
  final static String STR_SCHEMA4 = "b:string,a,c:string";
  final static String STR_STORAGE4 = "[a,b];[c]";
  final static String STR_SCHEMA5 = "b,a:string";
  final static String STR_STORAGE5 = "[a,b]";

  @BeforeClass
  public static void setUpOnce() throws Exception {
    if (System.getProperty("hadoop.log.dir") == null) {
      String base = new File(".").getPath(); // getAbsolutePath();
      System
          .setProperty("hadoop.log.dir", new Path(base).toString() + "./logs");
    }

    if (execType == ExecType.MAPREDUCE) {
      cluster = MiniCluster.buildCluster();
      pigServer = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
    } else {
      pigServer = new PigServer(ExecType.LOCAL);
    }

    conf = new Configuration();
    FileSystem fs = cluster.getFileSystem();
    pathWorking = fs.getWorkingDirectory();

    /*
     * create 1st basic table;
     */
    pathTable1 = new Path(pathWorking, "1");
    System.out.println("pathTable1 =" + pathTable1);

    BasicTable.Writer writer = new BasicTable.Writer(pathTable1, STR_SCHEMA1,
        STR_STORAGE1, false, conf);
    Schema schema = writer.getSchema();
    Tuple tuple = TypesUtils.createTuple(schema);

    final int numsBatch = 10;
    final int numsInserters = 2;
    TableInserter[] inserters = new TableInserter[numsInserters];
    for (int i = 0; i < numsInserters; i++) {
      inserters[i] = writer.getInserter("ins" + i, false);
    }

    for (int b = 0; b < numsBatch; b++) {
      for (int i = 0; i < numsInserters; i++) {
        TypesUtils.resetTuple(tuple);
        for (int k = 0; k < tuple.size(); ++k) {
          try {
            tuple.set(k, b + "_" + i + "" + k);
          } catch (ExecException e) {
            e.printStackTrace();
          }
        }// k
        inserters[i].insert(new BytesWritable(("key1" + i).getBytes()), tuple);
      }// i
    }// b
    for (int i = 0; i < numsInserters; i++) {
      inserters[i].close();
    }

    /*
     * create 2nd basic table;
     */
    pathTable2 = new Path(pathWorking, "2");
    System.out.println("pathTable2 =" + pathTable2);

    writer = new BasicTable.Writer(pathTable2, STR_SCHEMA2, STR_STORAGE2,
        false, conf);
    schema = writer.getSchema();
    tuple = TypesUtils.createTuple(schema);

    inserters = new TableInserter[numsInserters];
    for (int i = 0; i < numsInserters; i++) {
      inserters[i] = writer.getInserter("ins" + i, false);
    }

    for (int b = 0; b < numsBatch; b++) {
      for (int i = 0; i < numsInserters; i++) {
        TypesUtils.resetTuple(tuple);
        for (int k = 0; k < tuple.size(); ++k) {
          try {
            tuple.set(k, b + "_" + i + "" + k);
          } catch (ExecException e) {
            e.printStackTrace();
          }
        }
        inserters[i].insert(new BytesWritable(("key2" + i).getBytes()), tuple);
      }
    }
    for (int i = 0; i < numsInserters; i++) {
      inserters[i].close();
    }

    /*
     * create 3rd basic table;
     */
    pathTable3 = new Path(pathWorking, "3");
    System.out.println("pathTable3 =" + pathTable3);

    writer = new BasicTable.Writer(pathTable3, STR_SCHEMA3, STR_STORAGE3,
        false, conf);
    schema = writer.getSchema();
    tuple = TypesUtils.createTuple(schema);

    inserters = new TableInserter[numsInserters];
    for (int i = 0; i < numsInserters; i++) {
      inserters[i] = writer.getInserter("ins" + i, false);
    }

    for (int b = 0; b < numsBatch; b++) {
      for (int i = 0; i < numsInserters; i++) {
        TypesUtils.resetTuple(tuple);
        for (int k = 0; k < tuple.size(); ++k) {
          try {
            tuple.set(k, b + "_" + i + "" + k);
          } catch (ExecException e) {
            e.printStackTrace();
          }
        }
        inserters[i].insert(new BytesWritable(("key3" + i).getBytes()), tuple);
      }
    }
    for (int i = 0; i < numsInserters; i++) {
      inserters[i].close();
    }
    /*
     * create 4th basic table;
     */
    pathTable4 = new Path(pathWorking, "4");
    System.out.println("pathTable4 =" + pathTable4);

    writer = new BasicTable.Writer(pathTable4, STR_SCHEMA4, STR_STORAGE4,
        false, conf);
    schema = writer.getSchema();
    tuple = TypesUtils.createTuple(schema);

    inserters = new TableInserter[numsInserters];
    for (int i = 0; i < numsInserters; i++) {
      inserters[i] = writer.getInserter("ins" + i, false);
    }

    for (int b = 0; b < numsBatch; b++) {
      for (int i = 0; i < numsInserters; i++) {
        TypesUtils.resetTuple(tuple);
        for (int k = 0; k < tuple.size(); ++k) {
          try {
            tuple.set(k, b + "_" + i + "" + k);
          } catch (ExecException e) {
            e.printStackTrace();
          }
        }
        inserters[i].insert(new BytesWritable(("key4" + i).getBytes()), tuple);
      }
    }
    for (int i = 0; i < numsInserters; i++) {
      inserters[i].close();
    }
    /*
     * create 5th basic table;
     */
    pathTable5 = new Path(pathWorking, "5");
    System.out.println("pathTable5 =" + pathTable5);

    writer = new BasicTable.Writer(pathTable5, STR_SCHEMA5, STR_STORAGE5,
        false, conf);
    schema = writer.getSchema();
    tuple = TypesUtils.createTuple(schema);

    inserters = new TableInserter[numsInserters];
    for (int i = 0; i < numsInserters; i++) {
      inserters[i] = writer.getInserter("ins" + i, false);
    }

    for (int b = 0; b < numsBatch; b++) {
      for (int i = 0; i < numsInserters; i++) {
        TypesUtils.resetTuple(tuple);
        for (int k = 0; k < tuple.size(); ++k) {
          try {
            tuple.set(k, b + "_" + i + "" + k);
          } catch (ExecException e) {
            e.printStackTrace();
          }
        }
        inserters[i].insert(new BytesWritable(("key5" + i).getBytes()), tuple);
      }
    }
    for (int i = 0; i < numsInserters; i++) {
      inserters[i].close();
    }

  }

  @AfterClass
  public static void tearDownOnce() throws Exception {
    pigServer.shutdown();
  }

  // all fields
  public void testReader1() throws ExecException, IOException {
    /*
     * remove hdfs prefix part like "hdfs://localhost.localdomain:42540" pig
     * will fill that in.
     */
    String str1 = pathTable1.toString().substring(
        pathTable1.toString().indexOf("/", 7), pathTable1.toString().length());
    String str2 = pathTable2.toString().substring(
        pathTable2.toString().indexOf("/", 7), pathTable2.toString().length());
    String query = "records = LOAD '" + str1 + "," + str2
        + "' USING org.apache.hadoop.zebra.pig.TableLoader('a, b, c, d');";
    System.out.println(query);
    // records = LOAD '/user/jing1234/1,/user/jing1234/2' USING
    // org.apache.hadoop.zebra.pig.TableLoader('a, b, c, d');

    pigServer.registerQuery(query);
    Iterator<Tuple> it = pigServer.openIterator("records");

    int i = 0;
    int k = -1;
    Tuple cur = null;
    int t = -1;
    int j = -1;

    while (it.hasNext()) {
      cur = it.next();

      System.out.println("cur: " + cur);
      // first table
      if (i <= 9) {
        System.out.println("first table first part: " + cur.toString());
        Assert.assertEquals(i + "_00", cur.get(0));
        Assert.assertEquals(i + "_01", cur.get(1));
        Assert.assertEquals(i + "_02", cur.get(2));
        Assert.assertEquals(null, cur.get(3));
      }
      if (i >= 10) {
        k++;
      }
      if (k <= 9 && k >= 0) {
        System.out.println("first table second part:  : " + cur.toString());
        Assert.assertEquals(k + "_10", cur.get(0));
        Assert.assertEquals(k + "_11", cur.get(1));
        Assert.assertEquals(k + "_12", cur.get(2));
        Assert.assertEquals(null, cur.get(3));
      }

      // second table
      if (k >= 10) {
        t++;
      }
      if (t <= 9 && t >= 0) {
        System.out.println("second table first part: " + cur.toString());
        Assert.assertEquals(t + "_00", cur.get(0));
        Assert.assertEquals(t + "_01", cur.get(1));
        Assert.assertEquals(null, cur.get(2));
        Assert.assertEquals(t + "_02", cur.get(3));
      }
      if (t >= 10) {
        j++;
      }
      if (j <= 9 && j >= 0) {
        System.out.println("second table first part: " + cur.toString());
        Assert.assertEquals(j + "_10", cur.get(0));
        Assert.assertEquals(j + "_11", cur.get(1));
        Assert.assertEquals(null, cur.get(2));
        Assert.assertEquals(j + "_12", cur.get(3));
      }
      i++;
    }// while
    Assert.assertEquals(40, i);
  }

  public void testReaderThroughIO() throws ExecException, IOException,
    ParseException {

    String projection1 = new String("a,b,c");
    BasicTable.Reader reader = new BasicTable.Reader(pathTable1, conf);
    reader.setProjection(projection1);
    List<RangeSplit> splits = reader.rangeSplit(1);
    TableScanner scanner = reader.getScanner(splits.get(0), true);
    scanner = reader.getScanner(splits.get(0), true);
    BytesWritable key = new BytesWritable();
    Tuple RowValue = TypesUtils.createTuple(scanner.getSchema());

    scanner.getKey(key);
    // Assert.assertEquals(key, new BytesWritable("k11".getBytes()));

    System.out.println("read record or record:" + RowValue.toString());

    for (int i = 0; i <= 9; i++) {
      scanner.getValue(RowValue);
      System.out.println("read record or record:" + RowValue.toString());
      Assert.assertEquals(i + "_00", RowValue.get(0));
      Assert.assertEquals(i + "_01", RowValue.get(1));
      Assert.assertEquals(i + "_02", RowValue.get(2));
      scanner.advance();
    }
    for (int i = 0; i <= 9; i++) {
      scanner.getValue(RowValue);
      System.out.println("read record or record:" + RowValue.toString());
      Assert.assertEquals(i + "_10", RowValue.get(0));
      Assert.assertEquals(i + "_11", RowValue.get(1));
      Assert.assertEquals(i + "_12", RowValue.get(2));
      scanner.advance();
    }

    reader.close();
  }

  // all fields
  public void testReader2() throws ExecException, IOException {

    String str1 = pathTable1.toString().substring(
        pathTable1.toString().indexOf("/", 7), pathTable1.toString().length());
    String str2 = pathTable2.toString().substring(
        pathTable2.toString().indexOf("/", 7), pathTable2.toString().length());
    String query = "records = LOAD '" + str1 + "," + str2
        + "' USING org.apache.hadoop.zebra.pig.TableLoader('c');";
    System.out.println(query);

    pigServer.registerQuery(query);
    Iterator<Tuple> it = pigServer.openIterator("records");

    int i = 0;
    int k = -1;
    Tuple cur = null;
    int t = -1;
    int j = -1;

    while (it.hasNext()) {
      cur = it.next();

      System.out.println("cur: " + cur);
      if (i <= 9) {
        System.out.println("first table first part: " + cur.toString());

        Assert.assertEquals(i + "_02", cur.get(0));
        try {
          cur.get(1);
          Assert.fail("should throw index out of bound exception ");
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
      if (i >= 10) {
        k++;
      }
      if (k <= 9 && k >= 0) {
        System.out.println("first table second part:  : " + cur.toString());
        Assert.assertEquals(k + "_12", cur.get(0));
        try {
          cur.get(1);
          Assert.fail("should throw index out of bound exception ");
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
      if (k >= 10) {
        t++;
      }
      if (t <= 9 && t >= 0) {
        System.out.println("second table first part: " + cur.toString());
        Assert.assertEquals(null, cur.get(0));
        try {
          cur.get(1);
          Assert.fail("should throw index out of bound exception ");
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
      if (t >= 10) {
        j++;
      }
      if (j <= 9 && j >= 0) {
        System.out.println("second table first part: " + cur.toString());

        Assert.assertEquals(null, cur.get(0));
        try {
          cur.get(1);
          Assert.fail("should throw index out of bound exception ");
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
      i++;
    }// while
    Assert.assertEquals(40, i);
  }

  // projection for common exist colum a
  public void testReader3() throws ExecException, IOException {
    String str1 = pathTable1.toString().substring(
        pathTable1.toString().indexOf("/", 7), pathTable1.toString().length());
    String str2 = pathTable2.toString().substring(
        pathTable2.toString().indexOf("/", 7), pathTable2.toString().length());
    String query = "records = LOAD '" + str1 + "," + str2
        + "' USING org.apache.hadoop.zebra.pig.TableLoader('a');";
    System.out.println(query);

    pigServer.registerQuery(query);
    Iterator<Tuple> it = pigServer.openIterator("records");

    int i = 0;
    int k = -1;
    Tuple cur = null;
    int t = -1;
    int j = -1;

    while (it.hasNext()) {
      cur = it.next();

      System.out.println("cur: " + cur);
      // first table
      if (i <= 9) {
        System.out.println("first table first part: " + cur.toString());
        Assert.assertEquals(i + "_00", cur.get(0));
        try {
          cur.get(1);
          Assert.fail("should throw index out of bound exception ");
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
      if (i >= 10) {
        k++;
      }
      if (k <= 9 && k >= 0) {
        System.out.println("first table second part:  : " + cur.toString());
        Assert.assertEquals(k + "_10", cur.get(0));
        try {
          cur.get(1);
          Assert.fail("should throw index out of bound exception ");
        } catch (Exception e) {
          e.printStackTrace();
        }
      }

      // second table
      if (k >= 10) {
        t++;
      }
      if (t <= 9 && t >= 0) {
        System.out.println("second table first part: " + cur.toString());
        Assert.assertEquals(t + "_00", cur.get(0));
        try {
          cur.get(1);
          Assert.fail("should throw index out of bound exception ");
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
      if (t >= 10) {
        j++;
      }
      if (j <= 9 && j >= 0) {
        System.out.println("second table first part: " + cur.toString());
        Assert.assertEquals(j + "_10", cur.get(0));
        try {
          cur.get(1);
          Assert.fail("should throw index out of bound exception ");
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
      i++;
    }// while
    Assert.assertEquals(40, i);
  }

  // some common fields
  public void testReader4() throws ExecException, IOException {

    String str1 = pathTable1.toString().substring(
        pathTable1.toString().indexOf("/", 7), pathTable1.toString().length());
    String str2 = pathTable2.toString().substring(
        pathTable2.toString().indexOf("/", 7), pathTable2.toString().length());
    String query = "records = LOAD '" + str1 + "," + str2
        + "' USING org.apache.hadoop.zebra.pig.TableLoader('a, b');";
    System.out.println(query);
    // records = LOAD '/user/jing1234/1,/user/jing1234/2' USING
    // org.apache.hadoop.zebra.pig.TableLoader('a, b, c, d');

    pigServer.registerQuery(query);
    Iterator<Tuple> it = pigServer.openIterator("records");

    int i = 0;
    int k = -1;
    Tuple cur = null;
    int t = -1;
    int j = -1;

    while (it.hasNext()) {
      cur = it.next();

      System.out.println("cur: " + cur);
      // first table
      if (i <= 9) {
        System.out.println("first table first part: " + cur.toString());
        Assert.assertEquals(i + "_00", cur.get(0));
        Assert.assertEquals(i + "_01", cur.get(1));
        try {
          cur.get(2);
          Assert.fail("should throw index out of bound exception");
        } catch (Exception e) {
          e.printStackTrace();
        }

      }
      if (i >= 10) {
        k++;
      }
      if (k <= 9 && k >= 0) {
        System.out.println("first table second part:  : " + cur.toString());
        Assert.assertEquals(k + "_10", cur.get(0));
        Assert.assertEquals(k + "_11", cur.get(1));
        try {
          cur.get(2);
          Assert.fail("should throw index out of bound exception");
        } catch (Exception e) {
          e.printStackTrace();
        }
      }

      // second table
      if (k >= 10) {
        t++;
      }
      if (t <= 9 && t >= 0) {
        System.out.println("second table first part: " + cur.toString());
        Assert.assertEquals(t + "_00", cur.get(0));
        Assert.assertEquals(t + "_01", cur.get(1));
        try {
          cur.get(2);
          Assert.fail("should throw index out of bound exception");
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
      if (t >= 10) {
        j++;
      }
      if (j <= 9 && j >= 0) {
        System.out.println("second table first part: " + cur.toString());
        Assert.assertEquals(j + "_10", cur.get(0));
        Assert.assertEquals(j + "_11", cur.get(1));
        try {
          cur.get(2);
          Assert.fail("should throw index out of bound exception");
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
      i++;
    }// while
    Assert.assertEquals(40, i);
  }

  // common column, but different posion
  public void testReader5() throws ExecException, IOException {
    String str1 = pathTable1.toString().substring(
        pathTable1.toString().indexOf("/", 7), pathTable1.toString().length());
    String str2 = pathTable2.toString().substring(
        pathTable2.toString().indexOf("/", 7), pathTable2.toString().length());
    String query = "records = LOAD '" + str1 + "," + str2
        + "' USING org.apache.hadoop.zebra.pig.TableLoader('e, f');";
    System.out.println(query);
    // records = LOAD '/user/jing1234/1,/user/jing1234/2' USING
    // org.apache.hadoop.zebra.pig.TableLoader('a, b, c, d');

    pigServer.registerQuery(query);
    Iterator<Tuple> it = pigServer.openIterator("records");

    int i = 0;
    int k = -1;
    Tuple cur = null;
    int t = -1;
    int j = -1;

    while (it.hasNext()) {
      cur = it.next();

      System.out.println("cur: " + cur);
      // first table
      if (i <= 9) {
        System.out.println("first table first part: " + cur.toString());
        Assert.assertEquals(i + "_03", cur.get(0));
        Assert.assertEquals(i + "_04", cur.get(1));
        try {
          cur.get(2);
          Assert.fail("should throw out of index bound exception");
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
      if (i >= 10) {
        k++;
      }
      if (k <= 9 && k >= 0) {
        System.out.println("first table second part:  : " + cur.toString());
        Assert.assertEquals(k + "_13", cur.get(0));
        Assert.assertEquals(k + "_14", cur.get(1));

      }

      // second table
      if (k >= 10) {
        t++;
      }
      if (t <= 9 && t >= 0) {
        System.out.println("second table first part: " + cur.toString());
        Assert.assertEquals(t + "_04", cur.get(0));
        Assert.assertEquals(t + "_03", cur.get(1));
      }
      if (t >= 10) {
        j++;
      }
      if (j <= 9 && j >= 0) {
        System.out.println("second table first part: " + cur.toString());
        Assert.assertEquals(j + "_14", cur.get(0));
        Assert.assertEquals(j + "_13", cur.get(1));
      }
      i++;
    }// while
    Assert.assertEquals(40, i);
  }

  @Test
  // union two tables with different column numbers and column positions
  public void testReader6() throws ExecException, IOException {
    String str1 = pathTable1.toString().substring(
        pathTable1.toString().indexOf("/", 7), pathTable1.toString().length());
    String str5 = pathTable2.toString().substring(
        pathTable5.toString().indexOf("/", 7), pathTable5.toString().length());
    String query = "records = LOAD '" + str1 + "," + str5
        + "' USING org.apache.hadoop.zebra.pig.TableLoader('b,a');";
    System.out.println(query);

    pigServer.registerQuery(query);
    Iterator<Tuple> it = pigServer.openIterator("records");

    int i = 0;
    int k = -1;
    Tuple cur = null;
    int t = -1;
    int j = -1;

    while (it.hasNext()) {
      cur = it.next();

      System.out.println("cur: " + cur);
      // first table
      if (i <= 9) {
        System.out.println("first table first part: " + cur.toString());
        Assert.assertEquals(i + "_01", cur.get(0));
        Assert.assertEquals(i + "_00", cur.get(1));
        try {
          cur.get(2);
          Assert.fail("should throw index out of bound exception");
        } catch (Exception e) {
          e.printStackTrace();
        }

      }
      if (i >= 10) {
        k++;
      }
      if (k <= 9 && k >= 0) {
        System.out.println("first table second part:  : " + cur.toString());
        Assert.assertEquals(k + "_11", cur.get(0));
        Assert.assertEquals(k + "_10", cur.get(1));
        try {
          cur.get(2);
          Assert.fail("should throw index out of bound exception");
        } catch (Exception e) {
          e.printStackTrace();
        }

      }

      // second table
      if (k >= 10) {
        t++;
      }
      if (t <= 9 && t >= 0) {
        System.out.println("second table first part: " + cur.toString());
        Assert.assertEquals(t + "_01", cur.get(0));
        Assert.assertEquals(t + "_00", cur.get(1));
        try {
          cur.get(2);
          Assert.fail("should throw index out of bound exception");
        } catch (Exception e) {
          e.printStackTrace();
        }

      }
      if (t >= 10) {
        j++;
      }
      if (j <= 9 && j >= 0) {
        System.out.println("second table first part: " + cur.toString());
        Assert.assertEquals(j + "_11", cur.get(0));
        Assert.assertEquals(j + "_10", cur.get(1));
        try {
          cur.get(2);
          Assert.fail("should throw index out of bound exception");
        } catch (Exception e) {
          e.printStackTrace();
        }

      }
      i++;
    }// while
    Assert.assertEquals(40, i);
  }

  // both paths is hdfs://...
  public void testNeg1() throws ExecException, IOException {
    String str1 = pathTable1.toString();
    String str2 = pathTable2.toString();
    String query = "records = LOAD '" + str1 + "," + str2
        + "' USING org.apache.hadoop.zebra.pig.TableLoader('a,b,c,d');";
    System.out.println(query);
    // records = LOAD
    // 'hdfs://localhost.localdomain:39125/user/jing1234/1,hdfs://localhost.localdomain:39125/user/jing1234/2'
    // USING org.apache.hadoop.zebra.pig.TableLoader('a, b, c, d');
    pigServer.registerQuery(query);

    Iterator<Tuple> it = pigServer.openIterator("records");

    int cnt = 0;
    Tuple cur = it.next();
    cnt++;
    while (it.hasNext()) {
      cur = it.next();
      System.out.println(cur);
      cnt++;
      if (cnt == 1) {
        Assert.assertEquals("0_00", cur.get(0));
        Assert.assertEquals("0_01", cur.get(1));
        Assert.assertEquals("0_02", cur.get(2));
        Assert.assertEquals(null, cur.get(3));
      }
      if (cnt == 21) {
        Assert.assertEquals("0_00", cur.get(0));
        Assert.assertEquals("0_01", cur.get(1));
        Assert.assertEquals(null, cur.get(2));
        Assert.assertEquals("0_02", cur.get(3));
      }
    }
    Assert.assertEquals(cnt, 40);
  }

  // non-existing column
  public void testNeg2() throws ExecException, IOException {
    String str1 = pathTable1.toString().substring(
        pathTable1.toString().indexOf("/", 7), pathTable1.toString().length());
    String str2 = pathTable2.toString().substring(
        pathTable2.toString().indexOf("/", 7), pathTable2.toString().length());
    String query = "records = LOAD '" + str1 + "," + str2
        + "' USING org.apache.hadoop.zebra.pig.TableLoader('a,f');";

    System.out.println(query);
    // records = LOAD
    // 'hdfs://localhost.localdomain:39125/user/jing1234/1,hdfs://localhost.localdomain:39125/user/jing1234/2'
    // USING org.apache.hadoop.zebra.pig.TableLoader('a, b, c, d');
    pigServer.registerQuery(query);

    Iterator<Tuple> it = pigServer.openIterator("records");

    int cnt = 0;
    Tuple cur = it.next();
    cnt++;
    while (it.hasNext()) {
      cur = it.next();
      System.out.println(cur);
      cnt++;
      if (cnt == 1) {
        Assert.assertEquals("0_00", cur.get(0));
        Assert.assertEquals("0_01", cur.get(1));
        Assert.assertEquals("0_02", cur.get(2));
        Assert.assertEquals(null, cur.get(3));
      }
      if (cnt == 21) {
        Assert.assertEquals("0_00", cur.get(0));
        Assert.assertEquals("0_01", cur.get(1));
        Assert.assertEquals(null, cur.get(2));
        Assert.assertEquals("0_02", cur.get(3));
      }
    }
    Assert.assertEquals(cnt, 40);
  }

  @Test
  // 2 table with same column name but different type and different position.
  // should throw exception
  public void testNeg3() throws ExecException, IOException {
    String str1 = pathTable1.toString().substring(
        pathTable1.toString().indexOf("/", 7), pathTable1.toString().length());
    String str3 = pathTable3.toString().substring(
        pathTable3.toString().indexOf("/", 7), pathTable3.toString().length());
    String query = "records = LOAD '" + str1 + "," + str3
        + "' USING org.apache.hadoop.zebra.pig.TableLoader('a, b');";
    System.out.println(query);
    // records = LOAD '/user/jing1234/1,/user/jing1234/3' USING
    // org.apache.hadoop.zebra.pig.TableLoader('a,b');
    try {
      pigServer.registerQuery(query);
      Assert.fail("should throw exception");
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  // union table1 and table4. they have same culumn name with differnt types ,
  // should throw excepiton in union
  public void testNeg4() throws ExecException, IOException {

    String str1 = pathTable1.toString().substring(
        pathTable1.toString().indexOf("/", 7), pathTable1.toString().length());
    String str4 = pathTable4.toString().substring(
        pathTable4.toString().indexOf("/", 7), pathTable4.toString().length());
    String query = "records = LOAD '" + str1 + "," + str4
        + "' USING org.apache.hadoop.zebra.pig.TableLoader('a, b, c');";
    System.out.println(query);
    try {
      pigServer.registerQuery(query);
      Assert.fail("should throw exception");
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
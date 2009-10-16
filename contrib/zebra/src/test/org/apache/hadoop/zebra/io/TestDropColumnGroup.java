/**
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
package org.apache.hadoop.zebra.io;

import java.io.IOException;
import java.util.HashMap;

import java.util.List;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.zebra.parser.ParseException;

import org.apache.hadoop.zebra.io.BasicTable.Reader.RangeSplit;

import org.apache.hadoop.zebra.schema.Schema;
import org.apache.hadoop.zebra.types.TypesUtils;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;

import org.junit.AfterClass;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestDropColumnGroup {
  Log LOG = LogFactory.getLog(TestDropColumnGroup.class);
  private static Path path;
  private static Configuration conf;

  @BeforeClass
  public static void setUpOnce() throws IOException {
    TestBasicTable.setUpOnce();
    path = new Path(TestBasicTable.rootPath, "DropCGTest");
    conf = TestBasicTable.conf;
    Log LOG = LogFactory.getLog(TestDropColumnGroup.class);

  }

  @AfterClass
  public static void tearDown() throws IOException {
    BasicTable.drop(path, conf);
  }

  /**
   * Utitility function to open a table with a given projection and verify that
   * certain fields in the returned tuple are null and certain fields are not.
   */
  void verifyScanner(Path path, Configuration conf, String projection,
      boolean isNullExpected[], int numRowsToRead) throws IOException,
      ParseException {

    BasicTable.Reader reader = new BasicTable.Reader(path, conf);
    reader.setProjection(projection);
    TableScanner scanner = reader.getScanner(null, true);

    Tuple row = TypesUtils.createTuple(reader.getSchema());

    for (int i = 0; i < numRowsToRead; i++) {
      scanner.getValue(row);
      for (int f = 0; f < isNullExpected.length; f++) {
        if (isNullExpected[f] ^ row.get(f) == null) {
          throw new IOException("Verification failure at field " + f + " row "
              + i + " : expected " + (isNullExpected[f] ? "NULL" : "nonNULL")
              + " but got opposite.");

        }
      }
      scanner.advance();
    }

    scanner.close();
  }

  int countRows(Path path, Configuration conf, String projection)
      throws IOException, ParseException {
    BasicTable.Reader reader = new BasicTable.Reader(path, conf);
    reader.setProjection(projection);
    TableScanner scanner = reader.getScanner(null, true);
    int count = 0;
    while (!scanner.atEnd()) {
      count++;
      scanner.advance();
    }
    scanner.close();
    return count;
  }

  @Test
  public void testDropColumnGroup() throws IOException, ParseException {
    /*
     * Tests basic drop columns feature. Also tests that fields in dropped
     * columns can be read the value returned is null.
     */

    BasicTable.drop(path, conf);

    int numRows = TestBasicTable.createBasicTable(1, 10, "a, b, c, d, e, f",
        "[a, b]; [c, d]", path, true, false);

    int rowsToRead = Math.min(10, numRows);

    // normal table.
    verifyScanner(path, conf, "a, c, x", new boolean[] { false, false, true },
        rowsToRead);

    // Now delete ([c, d)
    BasicTable.dropColumnGroup(path, conf, "CG1");

    // check various read cases.
    verifyScanner(path, conf, "c, a", new boolean[] { true, false }, rowsToRead);
    verifyScanner(path, conf, "c, a", new boolean[] { true, false }, rowsToRead);

    verifyScanner(path, conf, "c, a, b, f, d, e", new boolean[] { true, false,
        false, false, true, false }, rowsToRead);

    BasicTable.dumpInfo(path.toString(), System.err, conf);

    // Drop CG0 ([a, b])
    BasicTable.dropColumnGroup(path, conf, "CG0");

    verifyScanner(path, conf, "a, b", new boolean[] { true, true }, rowsToRead);

    // Drop remaining CG2
    BasicTable.dropColumnGroup(path, conf, "CG2");

    verifyScanner(path, conf, "a, b, c, d, e, f", new boolean[] { true, true,
        true, true, true, true }, rowsToRead);

    // Now make sure the reader reports zero rows.
    Assert.assertTrue(countRows(path, conf, "c, e, b") == 0);

    // delete the table
    BasicTable.drop(path, conf);

    /*
     * Try similar tests with range splits.
     */

    // 5 splits and 50 rows
    numRows = TestBasicTable.createBasicTable(5, 50, "a, b, c, d, e, f",
        "[a, b]; [c, d]; [e] as myCG", path, true, false);

    BasicTable.dropColumnGroup(path, conf, "myCG");

    verifyScanner(path, conf, "e, c, g, b", new boolean[] { true, false, true,
        false }, numRows);

    TestBasicTable.doRangeSplit(new int[] { 4, 0, 2 }, numRows,
        "a, b, e, f, x", path);

    // Remove another CG.
    BasicTable.dropColumnGroup(path, conf, "CG0");

    TestBasicTable.doRangeSplit(new int[] { 4, 0, 2, 3, 1 }, numRows,
        "a, y, e, f, x", path);

    BasicTable.drop(path, conf);
  }

  @Test
  public void testDropColumnGroupsMixedTypes() throws IOException, ParseException {

    String mixedSchema = /* roughly borrowed from testMixedType1.java */
    "s1:bool, s2:int, s3:long, s4:float, s5:string, s6:bytes, "
        + "r1:record(f1:int, f2:long), r2:record(r3:record(f3:float, f4)),"
        + "m1:map(string),m2:map(map(int)), "
        + "c:collection(f13:double, f14:float, f15:bytes)";
    // [s1, s2]; [m1#{a}]; [r1.f1]; [s3, s4, r2.r3.f3]; [s5, s6, m2#{x|y}];
    // [r1.f2, m1#{b}]; [r2.r3.f4, m2#{z}]";
    String mixedStorageHint = "[s1, s2]                      as simpleCG; "
        + "[m1#{a}, s3]                  as mapCG; "
        + "[s4, r2.r3.f3, r1.f1]         as recordCG; "
        + "[c]                           as collectionCG; "  
        + "[r1.f2, m1#{b}, m2#{z}]       as mapRecordCG; ";

    Path path = new Path(TestBasicTable.rootPath, "DropCGTest");
    Configuration conf = TestBasicTable.conf;
    conf.set("fs.default.name", "file:///");

    BasicTable.drop(path, conf);

    // first write the table :
    BasicTable.Writer writer = new BasicTable.Writer(path, mixedSchema,
        mixedStorageHint, false, conf);
    writer.finish();

    Schema schema = writer.getSchema();
    Tuple tuple = TypesUtils.createTuple(schema);
    BasicTable.Writer writer1 = new BasicTable.Writer(path, conf);
    int part = 0;
    TableInserter inserter = writer1.getInserter("part" + part, true);
    TypesUtils.resetTuple(tuple);

    Tuple tupRecord1 = TypesUtils.createTuple(schema.getColumnSchema("r1")
        .getSchema());
    Tuple tupRecord2 = TypesUtils.createTuple(schema.getColumnSchema("r2")
        .getSchema());

    Tuple tupRecord3 = TypesUtils.createTuple(new Schema("f3:float, f4"));

    // row 1
    tuple.set(0, true); // bool
    tuple.set(1, 1); // int
    tuple.set(2, 1001L); // long
    tuple.set(3, 1.1); // float
    tuple.set(4, "hello world 1"); // string
    tuple.set(5, new DataByteArray("hello byte 1")); // byte

    // r1:record(f1:int, f2:long
    tupRecord1.set(0, 1);
    tupRecord1.set(1, 1001L);
    tuple.set(6, tupRecord1);

    // r2:record(r3:record(f3:float, f4))
    tupRecord2.set(0, tupRecord3);
    tupRecord3.set(0, 1.3);
    tupRecord3.set(1, new DataByteArray("r3 row 1 byte array "));
    tuple.set(7, tupRecord2);

    // m1:map(string)
    Map<String, String> m1 = new HashMap<String, String>();
    m1.put("a", "A");
    m1.put("b", "B");
    m1.put("c", "C");
    tuple.set(8, m1);

    // m2:map(map(int))
    HashMap<String, Map<String, Integer>> m2 = new HashMap<String, Map<String, Integer>>();
    Map<String, Integer> m3 = new HashMap<String, Integer>();
    m3.put("m311", 311);
    m3.put("m321", 321);
    m3.put("m331", 331);
    Map<String, Integer> m4 = new HashMap<String, Integer>();
    m4.put("m411", 411);
    m4.put("m421", 421);
    m4.put("m431", 431);
    m2.put("x", m3);
    m2.put("y", m4);
    tuple.set(9, m2);

    // c:collection(f13:double, f14:float, f15:bytes)
    DataBag bagColl = TypesUtils.createBag();
    Schema schColl = schema.getColumn(10).getSchema();
    Tuple tupColl1 = TypesUtils.createTuple(schColl);
    Tuple tupColl2 = TypesUtils.createTuple(schColl);
    byte[] abs1 = new byte[3];
    byte[] abs2 = new byte[4];
    tupColl1.set(0, 3.1415926);
    tupColl1.set(1, 1.6);
    abs1[0] = 11;
    abs1[1] = 12;
    abs1[2] = 13;
    tupColl1.set(2, new DataByteArray(abs1));
    bagColl.add(tupColl1);
    tupColl2.set(0, 123.456789);
    tupColl2.set(1, 100);
    abs2[0] = 21;
    abs2[1] = 22;
    abs2[2] = 23;
    abs2[3] = 24;
    tupColl2.set(2, new DataByteArray(abs2));
    bagColl.add(tupColl2);
    tuple.set(10, bagColl);

    int row = 0;
    inserter.insert(new BytesWritable(String.format("k%d%d", part + 1, row + 1)
        .getBytes()), tuple);
    // row 2
    row++;
    TypesUtils.resetTuple(tuple);
    TypesUtils.resetTuple(tupRecord1);
    TypesUtils.resetTuple(tupRecord2);
    TypesUtils.resetTuple(tupRecord3);
    m1.clear();
    m2.clear();
    m3.clear();
    m4.clear();
    tuple.set(0, false);
    tuple.set(1, 2); // int
    tuple.set(2, 1002L); // long
    tuple.set(3, 3.1); // float
    tuple.set(4, "hello world 2"); // string
    tuple.set(5, new DataByteArray("hello byte 2")); // byte

    // r1:record(f1:int, f2:long
    tupRecord1.set(0, 2);

    tupRecord1.set(1, 1002L);
    tuple.set(6, tupRecord1);

    // r2:record(r3:record(f3:float, f4))
    tupRecord2.set(0, tupRecord3);
    tupRecord3.set(0, 2.3);
    tupRecord3.set(1, new DataByteArray("r3 row2  byte array"));
    tuple.set(7, tupRecord2);

    // m1:map(string)
    m1.put("a2", "A2");
    m1.put("b2", "B2");
    m1.put("c2", "C2");
    tuple.set(8, m1);

    // m2:map(map(int))
    m3.put("m321", 321);
    m3.put("m322", 322);
    m3.put("m323", 323);
    m2.put("z", m3);
    tuple.set(9, m2);

    // c:collection(f13:double, f14:float, f15:bytes)
    bagColl.clear();
    TypesUtils.resetTuple(tupColl1);
    TypesUtils.resetTuple(tupColl2);
    tupColl1.set(0, 7654.321);
    tupColl1.set(1, 0.0001);
    abs1[0] = 31;
    abs1[1] = 32;
    abs1[2] = 33;
    tupColl1.set(2, new DataByteArray(abs1));
    bagColl.add(tupColl1);
    tupColl2.set(0, 0.123456789);
    tupColl2.set(1, 0.3333);
    abs2[0] = 41;
    abs2[1] = 42;
    abs2[2] = 43;
    abs2[3] = 44;
    tupColl2.set(2, new DataByteArray(abs2));
    bagColl.add(tupColl2);
    tuple.set(10, bagColl);

    // Write same row 10 times:
    for (int i = 0; i < 10; i++) {
      inserter.insert(new BytesWritable(String.format("k%d%d", part + 1 + i,
          row + 1 + i).getBytes()), tuple);
    }

    inserter.close();
    writer1.finish();

    writer.close();

    int numRows = 11;
    // drop mapCG: removes [m1#{a}, s3]
    BasicTable.dropColumnGroup(path, conf, "mapCG");

    verifyScanner(path, conf, "m1", new boolean[] { false }, numRows);

    verifyScanner(path, conf, "s1, m1#{a}, m1#{b}, s3, s4", new boolean[] {
        false, true, false, true, false }, numRows);

    // drop simpleCG : removes [s1, s2]
    BasicTable.dropColumnGroup(path, conf, "simpleCG");
    verifyScanner(path, conf, "s1, m1#{a}, s2, m1#{b}", new boolean[] { true,
        true, true, false }, numRows);

    // drop mapRecordCG : removes [r1.f2, m1#{b}, m2#{z}];\
    BasicTable.dropColumnGroup(path, conf, "mapRecordCG");
    verifyScanner(path, conf, "r1.f1, r1.f2, m1#{a}, s5, m1#{b}",
        new boolean[] { false, true, true, false, true }, numRows);

    // drop collectionCG : removes c;\
    BasicTable.dropColumnGroup(path, conf, "collectionCG");
    verifyScanner(path, conf, "c.f1, c.f2, c.f3", new boolean[] { true, true,
        true }, numRows);

    // clean up the table
    BasicTable.drop(path, conf);
  }

  @Test
  public void test2() throws IOException, ParseException {
    /*
     * Tests concurrent drop CGs
     */

    BasicTable.drop(path, conf);

    int numRows = TestBasicTable.createBasicTable(1, 10, "a, b, c, d, e, f",
        "[a];[b];[c];[d];[e];[f]", path, true, false);
    System.out.println("Frist dump:");
    BasicTable.dumpInfo(path.toString(), System.out, conf);
    int rowsToRead = Math.min(10, numRows);

    // normal table.
    verifyScanner(path, conf, "a, c, x", new boolean[] { false, false, true },
        rowsToRead);

    // create a thread for each dropCG
    DropThread[] threads = new DropThread[6];

    for (int i = 0; i < threads.length; i++) {

      threads[i] = new DropThread(i);
    }

    // start the threads
    for (int j = 0; j < threads.length; j++) {
      threads[j].start();
    }

    for (Thread thr : threads) {
      try {
        thr.join();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    // check various read cases.

    verifyScanner(path, conf, "c, a, b, f, d, e", new boolean[] { true, true,
        true, true, true, true }, rowsToRead);
    System.out.println("second dump");
    BasicTable.dumpInfo(path.toString(), System.out, conf);

    // Now make sure the reader reports zero rows.
    Assert.assertTrue(countRows(path, conf, "c, e, b") == 0);

    // delete the table
    BasicTable.drop(path, conf);

  }

  @Test
  public void test3() throws IOException, ParseException {
    /*
     * Tests concurrrent drop CGs while one fails
     */

    BasicTable.drop(path, conf);

    int numRows = TestBasicTable.createBasicTable(1, 10, "a, b, c, d, e, f",
        "[a];[b];[c];[d];[e];[f]", path, true, false);
    System.out.println("Frist dump:");
    BasicTable.dumpInfo(path.toString(), System.out, conf);
    int rowsToRead = Math.min(10, numRows);

    // normal table.
    verifyScanner(path, conf, "a, c, x", new boolean[] { false, false, true },
        rowsToRead);

    // create a thread for each dropCG
    DropThread[] threads = new DropThread[7];

    for (int i = 0; i < threads.length; i++) {

      threads[i] = new DropThread(i);
    }

    // start the threads
    for (int j = 0; j < threads.length; j++) {
      threads[j].start();
    }

    for (Thread thr : threads) {
      try {
        thr.join();
      } catch (InterruptedException e) {
      }
    }

    // check various read cases.

    verifyScanner(path, conf, "c, a, b, f, d, e", new boolean[] { true, true,
        true, true, true, true }, rowsToRead);
    System.out.println("second dump");
    BasicTable.dumpInfo(path.toString(), System.out, conf);

    // Now make sure the reader reports zero rows.
    Assert.assertTrue(countRows(path, conf, "c, e, b") == 0);

    // delete the table
    BasicTable.drop(path, conf);
  }

  @Test
  public void test5() throws IOException, ParseException {
    /*
     * Tests drop CGs while reading the same CGs
     */

    System.out.println("######int test 5");
    BasicTable.drop(path, conf);

    int numRows = TestBasicTable.createBasicTable(1, 100000,
        "a, b, c, d, e, f", "[a, b]; [c, d]", path, true, false);

    System.out.println("in test5 , dump infor 1");
    BasicTable.dumpInfo(path.toString(), System.out, conf);

    int minRowsToRead = 10000;
    int numOfReadThreads = 20;
    int rowsToRead = Math.min(minRowsToRead, numRows);

    // normal table.
    verifyScanner(path, conf, "a, c, x", new boolean[] { false, false, true },
        rowsToRead);

    // create a thread for each dropCG
    DropThread[] dropThreads = new DropThread[3];

    for (int i = 0; i < dropThreads.length; i++) {

      dropThreads[i] = new DropThread(i);
    }

    // start the threads
    for (int j = 0; j < dropThreads.length; j++) {
      dropThreads[j].start();
    }

    // create read threads
    ReadThread[] readThreads = new ReadThread[numOfReadThreads];

    for (int i = 0; i < readThreads.length; i++) {

      readThreads[i] = new ReadThread(i, "a, b, c, d, e, f", 1000);
    }

    // start the threads
    for (int j = 0; j < readThreads.length; j++) {
      readThreads[j].start();
    }

    for (Thread thr : dropThreads) {
      try {
        thr.join();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    for (Thread thr : readThreads) {
      try {
        thr.join();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    verifyScanner(path, conf, "c, a, b, f, d, e", new boolean[] { true, true,
        true, true, true, true }, rowsToRead);
    System.out.println("second dump");
    BasicTable.dumpInfo(path.toString(), System.out, conf);

    // Now make sure the reader reports zero rows.
    Assert.assertTrue(countRows(path, conf, "c, e, b") == 0);

    // delete the table
    BasicTable.drop(path, conf);

  }

  @Test
  public void test11() throws IOException, ParseException {

    /*
     * Tests test open non-existing table.
     */

    try {
      new BasicTable.Reader(new Path(path.toString(), "non-existing"), conf);
      Assert.fail("read none existing table should fail");
    } catch (Exception e) {

    }

  }

  @Test
  public void test12() throws IOException, ParseException {
    /*
     * Tests API, path is wrong
     */

    BasicTable.drop(path, conf);

    TestBasicTable.createBasicTable(1, 10, "a, b, c, d, e, f",
        "[a];[b];[c];[d];[e];[f]", path, true, false);
    Path wrongPath = new Path(path.toString() + "non-existing");
    try {
      BasicTable.dropColumnGroup(wrongPath, conf, "CG0");
      Assert.fail("should throw excepiton");
    } catch (Exception e) {

    }
    BasicTable.drop(path, conf);
  }

  @Test
  public void test13() throws IOException, ParseException {
    /*
     * Tests API, conf is null
     */

    Path path1 = new Path(path.toString() + "13");
    TestBasicTable.createBasicTable(1, 10, "a, b, c, d, e, f",
        "[a];[b];[c];[d];[e];[f]", path1, true, false);
    try {
      BasicTable.dropColumnGroup(path1, null, "CG0");
      Assert.fail("should throw excepiton");
    } catch (Exception e) {

    }
    BasicTable.drop(path1, conf);
  }

  @Test
  public void test14() throws IOException, ParseException {
    /*
     * Tests API, CG name is empty string
     */

    Path path1 = new Path(path.toString() + "14");
    TestBasicTable.createBasicTable(1, 10, "a, b, c, d, e, f",
        "[a];[b];[c];[d];[e];[f]", path1, true, false);
    try {
      BasicTable.dropColumnGroup(path1, conf, "");
      Assert.fail("should throw excepiton");
    } catch (Exception e) {

    }
    BasicTable.drop(path1, conf);
  }

  @Test
  public void test15() throws IOException, ParseException {
    /*
     * Tests API, CG name is null
     */

    Path path1 = new Path(path.toString() + "15");

    TestBasicTable.createBasicTable(1, 10, "a, b, c, d, e, f",
        "[a];[b];[c];[d];[e];[f]", path1, true, false);
    try {
      BasicTable.dropColumnGroup(path1, conf, null);
      Assert.fail("should throw excepiton");
    } catch (Exception e) {

    }
    BasicTable.drop(path1, conf);
  }

  @Test
  public void test16() throws IOException, ParseException {
    /*
     * Tests delete same CG multiple times
     */

    Path path1 = new Path(path.toString() + "16");

    int numRows = TestBasicTable.createBasicTable(1, 10, "a, b, c, d, e, f",
        "[a, b]; [c, d]", path1, true, false);

    int rowsToRead = Math.min(10, numRows);

    // normal table.
    verifyScanner(path1, conf, "a, c, x", new boolean[] { false, false, true },
        rowsToRead);

    // Now delete ([c, d)
    BasicTable.dropColumnGroup(path1, conf, "CG1");

    // check various read cases.
    verifyScanner(path1, conf, "c, a", new boolean[] { true, false },
        rowsToRead);

    // Now delete ([c, d)again
    BasicTable.dropColumnGroup(path1, conf, "CG1");

    verifyScanner(path1, conf, "c, a", new boolean[] { true, false },
        rowsToRead);
    BasicTable.drop(path1, conf);
  }

  @Test
  public void test17() throws IOException, ParseException {
    /*
     * test rangesplit
     */
    System.out.println("test 17");

    Path path1 = new Path(path.toString() + "17");
    TestBasicTable.createBasicTable(1, 10, "a, b, c, d, e, f", "[a,b,c,d,e,f]",
        path1, true, false);

    BasicTable.dropColumnGroup(path1, conf, "CG0");

    BasicTable.Reader reader = new BasicTable.Reader(path1, conf);
    reader.setProjection("a, b, c, d, e, f");
    List<RangeSplit> splits = reader.rangeSplit(1);
    TableScanner scanner = null;
    try {
      scanner = reader.getScanner(splits.get(0), true);
    } catch (Exception e) {
      System.out.println("in test 17, getScanner");
      e.printStackTrace();
    }

    Tuple RowValue = TypesUtils.createTuple(scanner.getSchema());

    scanner.getValue(RowValue);

    Assert.assertEquals(null, RowValue.get(0));
    Assert.assertFalse(scanner.advance());
    scanner.getValue(RowValue);
    Assert.assertEquals(null, RowValue.get(0));
    BasicTable.drop(path1, conf);
  }

  /**
   * A thread that performs a DropColumnGroup.
   */
  class DropThread extends Thread {

    private int id;

    public DropThread(int id) {

      this.id = id;

    }

    /**
     * Executes DropColumnGroup.
     */
    public void run() {
      try {
        System.out.println("Droping CG: " + id);
        BasicTable.dropColumnGroup(path, conf, "CG" + id);
      } catch (Exception e) {
        System.out.println(id + " - error: " + e);
      }
    }

  }

  /**
   * A thread that performs a ReadColumnGroup.
   */
  class ReadThread extends Thread {

    private int id;
    private String projection;
    private int numRowsToRead;

    public ReadThread(int id, String projection, int numRowsToRead) {
      this.id = id;
      this.projection = projection;
      this.numRowsToRead = numRowsToRead;

    }

    /**
     * Executes DropColumnGroup.
     */
    public void run() {
      BasicTable.Reader reader = null;
      try {
        reader = new BasicTable.Reader(path, conf);
        reader.setProjection(projection);
        TableScanner scanner = reader.getScanner(null, true);
        Tuple row = TypesUtils.createTuple(reader.getSchema());
        for (int i = 0; i < numRowsToRead; i++) {
          scanner.getValue(row);
        }
        scanner.advance();
        scanner.close();
      } catch (Exception e) {
        e.printStackTrace();
      }

    }
  }
}

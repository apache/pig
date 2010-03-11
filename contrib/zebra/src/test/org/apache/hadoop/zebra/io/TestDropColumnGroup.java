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
import org.apache.hadoop.fs.FileSystem;
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
  private static FileSystem fs;

  @BeforeClass
  public static void setUpOnce() throws IOException {
    TestBasicTable.setUpOnce();
    path = new Path(TestBasicTable.rootPath, "DropCGTest");
    conf = TestBasicTable.conf;
    Log LOG = LogFactory.getLog(TestDropColumnGroup.class);
    fs = path.getFileSystem(conf);
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

    if (fs.exists(path)) {
      BasicTable.drop(path, conf);
    }
    
    int numRows = TestBasicTable.createBasicTable(1, 10, "a, b, c, d, e, f",
                                                  "[a, b]; [c, d]", null,
                                                  path, true);

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
    numRows =  TestBasicTable.createBasicTable(5, 50, "a, b, c, d, e, f",
                                               "[a, b]; [c, d]; [e] as myCG",
                                               null, path, true);

    BasicTable.dropColumnGroup(path, conf, "myCG");

    verifyScanner(path, conf, "e, c, g, b", new boolean[] { true, false, true,
        false }, numRows);

    TestBasicTable.doRangeSplit(new int[] { 4, 0, 2 }, numRows,
        "a, b, c, e, f, x", path);

    // Remove another CG.
    BasicTable.dropColumnGroup(path, conf, "CG0");

    TestBasicTable.doRangeSplit(new int[] { 4, 0, 2, 3, 1 }, numRows,
        "a, y, c, e, f, x", path);

    BasicTable.drop(path, conf);
  }

  @Test
  public void test2() throws IOException, ParseException {
    /*
     * Tests concurrent drop CGs
     */
    if (fs.exists(path)) {
      BasicTable.drop(path, conf);
    }

    int numRows = TestBasicTable.createBasicTable(1, 10, "f1,f2,f3,f4,f5,f6,f7,f8,f9,f10," +
    		                                                 "f11,f12,f13,f14,f15,f16,f17,f18,f19,f20," +
    		                                                 "f21,f22,f23,f24,f25,f26,f27,f28,f29,f30," +
    		                                                 "f31,f32,f33,f34,f35,f36,f37,f38,f39,f40," +
    		                                                 "f41,f42,f43,f44,f45,f46,f47,f48,f49,f50",
      "[f1];[f2];[f3];[f4];[f5];[f6];[f7];[f8];[f9];[f10];" +
      "[f11];[f12];[f13];[f14];[f15];[f16];[f17];[f18];[f19];[f20];" +
      "[f21];[f22];[f23];[f24];[f25];[f26];[f27];[f28];[f29];[f30];" +
      "[f31];[f32];[f33];[f34];[f35];[f36];[f37];[f38];[f39];[f40];" +
      "[f41];[f42];[f43];[f44];[f45];[f46];[f47];[f48];[f49];[f50]",
      null, path, true);
    
    System.out.println("First dump:");
    BasicTable.dumpInfo(path.toString(), System.out, conf);
    int rowsToRead = Math.min(10, numRows);

    // normal table.
    verifyScanner(path, conf, "f1, f3, xx", new boolean[] { false, false, true },
        rowsToRead);

    // create a thread for each dropCG
    DropThread[] threads = new DropThread[50];

    for (int i = 0; i < threads.length; i++) {
      threads[i] = new DropThread(i, 50);
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

    verifyScanner(path, conf, "f3, f1, f2, f6, f4, f5", new boolean[] { true, true,
        true, true, true, true }, rowsToRead);
    System.out.println("second dump");
    BasicTable.dumpInfo(path.toString(), System.out, conf);

    // Now make sure the reader reports zero rows.
    Assert.assertTrue(countRows(path, conf, "f3, f5, f2") == 0);

    // delete the table
    BasicTable.drop(path, conf);
  }

  @Test
  public void test3() throws IOException, ParseException {
    /*
     * Tests concurrrent drop CGs while one fails
     */
    if (fs.exists(path)) {
      BasicTable.drop(path, conf);
    }

    int numRows = TestBasicTable.createBasicTable(1, 10, "f1,f2,f3,f4,f5,f6,f7,f8,f9,f10," +
        "f11,f12,f13,f14,f15,f16,f17,f18,f19,f20," +
        "f21,f22,f23,f24,f25,f26,f27,f28,f29,f30," +
        "f31,f32,f33,f34,f35,f36,f37,f38,f39,f40," +
        "f41,f42,f43,f44,f45,f46,f47,f48,f49,f50",
        "[f1];[f2];[f3];[f4];[f5];[f6];[f7];[f8];[f9];[f10];" +
        "[f11];[f12];[f13];[f14];[f15];[f16];[f17];[f18];[f19];[f20];" +
        "[f21];[f22];[f23];[f24];[f25];[f26];[f27];[f28];[f29];[f30];" +
        "[f31];[f32];[f33];[f34];[f35];[f36];[f37];[f38];[f39];[f40];" +
        "[f41];[f42];[f43];[f44];[f45];[f46];[f47];[f48];[f49];[f50]",
        null, path, true);
    
    System.out.println("First dump:");
    BasicTable.dumpInfo(path.toString(), System.out, conf);
    int rowsToRead = Math.min(10, numRows);

    // normal table.
    verifyScanner(path, conf, "f1, f3, xx", new boolean[] { false, false, true },
        rowsToRead);

    // create a thread for each dropCG
    DropThread[] threads = new DropThread[60];

    for (int i = 0; i < threads.length; i++) {
      threads[i] = new DropThread(i, 50);
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

    verifyScanner(path, conf, "f3, f1, f2, f6, f4, f5", new boolean[] { true, true,
        true, true, true, true }, rowsToRead);
    System.out.println("second dump");
    BasicTable.dumpInfo(path.toString(), System.out, conf);

    // Now make sure the reader reports zero rows.
    Assert.assertTrue(countRows(path, conf, "f3, f5, f2") == 0);

    // delete the table
    BasicTable.drop(path, conf);
  }

  @Test
  public void test5() throws IOException, ParseException {
    /*
     * Tests drop CGs while reading the same CGs
     */

    System.out.println("######int test 5");

    if (fs.exists(path)) {
      BasicTable.drop(path, conf);
    }

    int numRows = TestBasicTable.createBasicTable(1, 100000,
        "a, b, c, d, e, f, g, h, i, j, k, l, m, n", "[a, b]; [c, d]; [e]; [f]; [g]; [h]; [i]; [j]; [k]; [l]; [m]; [n]", null, path, true);

    System.out.println("in test5 , dump infor 1");
    BasicTable.dumpInfo(path.toString(), System.out, conf);

    int minRowsToRead = 10000;
    int numOfReadThreads = 20;
    int rowsToRead = Math.min(minRowsToRead, numRows);

    // normal table.
    verifyScanner(path, conf, "a, c, x", new boolean[] { false, false, true },
        rowsToRead);

    // create a thread for each dropCG
    DropThread[] dropThreads = new DropThread[12];

    for (int i = 0; i < dropThreads.length; i++) {
      dropThreads[i] = new DropThread(i, 12);
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
    if (fs.exists(path)) {
      BasicTable.drop(path, conf);
    }
    
    TestBasicTable.createBasicTable(1, 10, "a, b, c, d, e, f",
        "[a];[b];[c];[d];[e];[f]", null, path, true);
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
        "[a];[b];[c];[d];[e];[f]", null, path1, true);
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
        "[a];[b];[c];[d];[e];[f]", null, path1, true);
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
        "[a];[b];[c];[d];[e];[f]", null, path1, true);
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
        "[a, b]; [c, d]", null, path1, true);

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
        null, path1, true);

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
    private int cntCGs;

    public DropThread(int id, int cntCGs) {
      this.id = id;
      this.cntCGs = cntCGs;
    }

    /**
     * Executes DropColumnGroup.
     */
    public void run() {
      try {
        int total = cntCGs;
        int digits = 1;
        while (total >= 10) {
          ++ digits;
          total /= 10;
        }
        String formatString = "%0" + digits + "d";
        String str = "CG"  + String.format(formatString, id);

        System.out.println(id + ": Droping CG: " + str);
        BasicTable.dropColumnGroup(path, conf, str);
      } catch (Exception e) {
        System.out.println(id + " - error: " + e);
        e.printStackTrace();
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

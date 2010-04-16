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
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import junit.framework.Assert;
import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.zebra.io.BlockDistribution;
import org.apache.hadoop.zebra.io.ColumnGroup;
import org.apache.hadoop.zebra.io.TableInserter;
import org.apache.hadoop.zebra.io.TableScanner;
import org.apache.hadoop.zebra.io.ColumnGroup.Reader.CGRangeSplit;
import org.apache.hadoop.zebra.parser.ParseException;
import org.apache.hadoop.zebra.schema.Schema;
import org.apache.hadoop.zebra.types.TypesUtils;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestColumnGroupReaders {
  final static String outputFile = "TestColumnGroupReaders";
  final static private Configuration conf = new Configuration();
  private static FileSystem fs;
  private static Path path;
  private static ColumnGroup.Writer writer;

  @BeforeClass
  public static void setUpOnce() throws IOException {
    // set default file system to local file system
    conf.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");

    // must set a conf here to the underlying FS, or it barks
    RawLocalFileSystem rawLFS = new RawLocalFileSystem();
    rawLFS.setConf(conf);
    fs = new LocalFileSystem(rawLFS);
    path = new Path(fs.getWorkingDirectory(), outputFile);
    System.out.println("output file: " + path);
  }

  @AfterClass
  public static void tearDownOnce() throws IOException {
    close();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testInsert2Inserters() throws ParseException, ExecException, IOException,
      ParseException {
    System.out.println("testInsert2Inserters");
    boolean sorted = false; // true;
    writer = new ColumnGroup.Writer(path, "col1:string, colTWO:map(string)", sorted, path.getName(), "pig",
        "gz", null, null, (short) -1, true, conf);
    TableInserter ins1 = writer.getInserter("part1", false);
    TableInserter ins2 = writer.getInserter("part2", false);

    // row 1
    Tuple row = TypesUtils.createTuple(writer.getSchema());
    row.set(0, "val1");

    SortedMap<String, String> map = new TreeMap<String, String>();
    map.put("john", "boy");
    row.set(1, map);

    ins1.insert(new BytesWritable("key11".getBytes()), row);
    ins2.insert(new BytesWritable("key21".getBytes()), row);

    // row 2
    TypesUtils.resetTuple(row);
    row.set(0, "val2");
    map.put("joe", "boy");
    map.put("jane", "girl");
    // map should contain 3 k->v pairs
    row.set(1, map);

    ins2.insert(new BytesWritable("key22".getBytes()), row);
    ins2.insert(new BytesWritable("key23".getBytes()), row);
    // ins2.close();
    BytesWritable key12 = new BytesWritable("key12".getBytes());
    ins1.insert(key12, row);

    ins1.close();
    ins2.close();
    finish();

    // test without beginKey/endKey
    ColumnGroup.Reader reader = new ColumnGroup.Reader(path, conf);
    List<CGRangeSplit> listRanges = reader.rangeSplit(2);
    // TableScanner scanner = reader.getScanner(null, null, false);
    TableScanner scanner = reader.getScanner(listRanges.get(0), false);
    BytesWritable key = new BytesWritable();
    scanner.getKey(key);
    Assert
        .assertTrue(key.compareTo(new BytesWritable("key11".getBytes())) == 0);
    TypesUtils.resetTuple(row);
    scanner.getValue(row);
    Assert.assertEquals("val1", row.get(0));
    Map<String, String> mapFld2 = (Map<String, String>) row.get(1);
    Assert.assertEquals("boy", mapFld2.get("john"));
    scanner.close();
    scanner = null;

    // test with beginKey
    BytesWritable key22 = new BytesWritable("key22".getBytes());
    // TableScanner scanner2 = reader.getScanner(key22, null, true); // for
    // sorted
    TableScanner scanner2 = reader.getScanner(listRanges.get(1), true);
    BytesWritable key2 = new BytesWritable();
    scanner2.advance();// not for sorted
    scanner2.getKey(key2);
    Assert.assertEquals(key2, key22);
    TypesUtils.resetTuple(row);
    scanner2.getValue(row);
    Assert.assertEquals("val2", row.get(0));

    // test advance
    scanner2.advance();
    BytesWritable key4 = new BytesWritable();
    scanner2.getKey(key4);
    Assert
        .assertTrue(key4.compareTo(new BytesWritable("key23".getBytes())) == 0);
    TypesUtils.resetTuple(row);
    scanner2.getValue(row);
    Assert.assertEquals("val2", row.get(0));

    Map<String, String> mapFld5 = (Map<String, String>) row.get(1);
    Assert.assertEquals("girl", mapFld5.get("jane"));

    // test seekTo
    // Assert.assertTrue("failed to locate key: " + key22,
    // scanner2.seekTo(key22));
    // BytesWritable key22Read = new BytesWritable();
    // scanner2.getKey(key22Read);
    // Assert.assertEquals(new BytesWritable("key22".getBytes()), key22Read);

    // Map<String, Long> map;
    BlockDistribution dist = reader.getBlockDistribution(listRanges.get(0));
    long n = dist.getLength();

    reader.close();
    close();
  }

  @Test
  public void testMultiWriters() throws ExecException, Exception {
    System.out.println("testMultiWriters");
    ColumnGroup.Writer writer1 = writeOnePart("col1:string, col2:map(string), col3:string", 1);
    ColumnGroup.Writer writer2 = writeOnePart(null, 2);
    ColumnGroup.Writer writer3 = writeOnePart(null, 3);

    writer3.close();

    // read in parts
    readOnePart(1);
    readOnePart(2);
    readOnePart(3);

    // read in one big sequence
    readInSequence(3);

    readProjection(1);

    close();

  }

  private static ColumnGroup.Writer writeOnePart(String schemaString, int count)
      throws IOException, ExecException, ParseException {
    ColumnGroup.Writer writer = null;
    if (schemaString != null) {
      writer = new ColumnGroup.Writer(path, schemaString, false, path.getName(), "pig", "gz",
          null, null, (short) -1, true, conf);
    } else {
      writer = new ColumnGroup.Writer(path, conf);
    }
    TableInserter ins = writer.getInserter(String.format("part-%06d", count),
        false);
    Tuple row = TypesUtils.createTuple(writer.getSchema());
    SortedMap<String, String> map = new TreeMap<String, String>();
    for (int nx = 0; nx < count; nx++) {
      row.set(0, String.format("smpcol%02d%02d", count, nx + 1));

      map.put(String.format("mapcolkey%02d%02d", count, nx + 1), String.format(
          "mapcolvalue%02d%02d", count, nx + 1));
      row.set(1, map);

      row.set(2, String.format("col3_%02d%02d", count, nx + 1));

      ins.insert(new BytesWritable(String.format("key%02d%02d", count, nx + 1)
          .getBytes()), row);
    }
    ins.close();
    return writer;
  }

  private static void readInSequence(int count) throws IOException,
      ExecException, ParseException {
    ColumnGroup.Reader reader = new ColumnGroup.Reader(path, conf);
    TableScanner scanner = reader.getScanner(null, true);

    Tuple row = TypesUtils.createTuple(reader.getSchema());
    BytesWritable key = new BytesWritable();
    for (int partIdx = 0; partIdx < count; ++partIdx) {
      for (int keyIdx = 0; keyIdx < partIdx + 1; keyIdx++) {
        scanner.getKey(key);
        String keyString = String
            .format("key%02d%02d", partIdx + 1, keyIdx + 1);
        Assert.assertTrue(new String(key.get()) + " != " + keyString, key
            .equals(new BytesWritable(keyString.getBytes())));
        scanner.advance();
      }
    }

    scanner.close();
    scanner = null;
    reader.close();
  }

  private static void readOnePart(int count) throws IOException, ExecException,
    ParseException {
    ColumnGroup.Reader reader = new ColumnGroup.Reader(path, conf);
    TableScanner scanner = reader.getScanner(reader.rangeSplit(5)
        .get(count - 1), true);

    Tuple row = TypesUtils.createTuple(reader.getSchema());
    BytesWritable key = new BytesWritable();
    for (int nx = 0; nx < count; ++nx) {
      scanner.getKey(key);
      String keyString = String.format("key%02d%02d", count, nx + 1);
      Assert.assertTrue("Not equal to " + keyString, key
          .equals(new BytesWritable(keyString.getBytes())));
      TypesUtils.resetTuple(row);
      scanner.getValue(row);

      String simpVal = String.format("smpcol%02d%02d", count, nx + 1);
      Assert.assertEquals(simpVal, row.get(0));

      String mapKey = String.format("mapcolkey%02d%02d", count, nx + 1);
      String mapVal = String.format("mapcolvalue%02d%02d", count, nx + 1);
      Map<String, String> mapFld2 = (Map<String, String>) row.get(1);
      Assert.assertEquals(mapVal, mapFld2.get(mapKey));

      String val3 = String.format("col3_%02d%02d", count, nx + 1);
      Assert.assertEquals(val3, row.get(2));

      if (nx < count - 1) {
        scanner.advance();
      }
    }
    scanner.close();
    scanner = null;
    reader.close();
  }

  private static void readProjection(int count) throws IOException,
      ExecException, ParseException {
    ColumnGroup.Reader reader = new ColumnGroup.Reader(path, conf);
    reader.setProjection("col2, col3, foo, col1");
    TableScanner scanner = reader.getScanner(null, true);

    Tuple row = TypesUtils.createTuple(reader.getSchema());
    BytesWritable key = new BytesWritable();
    scanner.getKey(key);

    String keyString = String.format("key%02d%02d", 1, 1);
    Assert.assertTrue("Not equal to " + keyString, key
        .equals(new BytesWritable(keyString.getBytes())));
    TypesUtils.resetTuple(row);
    scanner.getValue(row);

    // physical 3, logical 2
    String val3 = String.format("col3_%02d%02d", 1, 1);
    Assert.assertEquals(val3, row.get(1));

    // physical 2, logical 1
    String mapKey = String.format("mapcolkey%02d%02d", 1, 1);
    String mapVal = String.format("mapcolvalue%02d%02d", 1, 1);
    Map<String, String> mapFld2 = (Map<String, String>) row.get(0);
    Assert.assertEquals(mapVal, mapFld2.get(mapKey));

    // physical NONE, logical 3
    Assert.assertNull(row.get(2));

    String simpVal = String.format("smpcol%02d%02d", 1, 1);
    Assert.assertEquals(simpVal, row.get(3));

    // move to next row
    scanner.advance();
    scanner.getValue(row);

    simpVal = String.format("smpcol%02d%02d", 2, 1);
    Assert.assertEquals(simpVal, row.get(3));

    scanner.close();
    scanner = null;
    reader.close();
  }

  private static void finish() throws IOException {
    if (writer != null) {
      writer.close();
    }
  }

  private static void close() throws IOException {
    if (writer != null) {
      writer.close();
      writer = null;
    }
    ColumnGroup.drop(path, conf);
  }
}

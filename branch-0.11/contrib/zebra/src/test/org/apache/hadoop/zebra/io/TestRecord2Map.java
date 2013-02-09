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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.StringTokenizer;

import junit.framework.Assert;
import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.zebra.io.BasicTable;
import org.apache.hadoop.zebra.io.TableInserter;
import org.apache.hadoop.zebra.io.TableScanner;
import org.apache.hadoop.zebra.io.BasicTable.Reader.RangeSplit;
import org.apache.hadoop.zebra.parser.ParseException;
import org.apache.hadoop.zebra.types.Projection;
import org.apache.hadoop.zebra.schema.Schema;
import org.apache.hadoop.zebra.types.TypesUtils;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * 
 * Test projections on complicated column types.
 * 
 */
public class TestRecord2Map {
  private static Configuration conf;
  private static Path path;
  private static FileSystem fs;
  static int count;
  static String STR_SCHEMA = "r1:record(f1:int, f2:map(long)), r2:record(r3:record(f3:double, f4:map(int)))";
  static String STR_STORAGE = "[r1.f1, r1.f2#{x|y}]; [r2.r3.f4]; [r1.f2, r2.r3.f3, r2.r3.f4#{a|c}]";

  @BeforeClass
  public static void setUpOnce() throws IOException {
    conf = new Configuration();
    conf.setInt("table.output.tfile.minBlock.size", 64 * 1024);
    conf.setInt("table.input.split.minSize", 64 * 1024);
    conf.set("table.output.tfile.compression", "none");

    RawLocalFileSystem rawLFS = new RawLocalFileSystem();
    fs = new LocalFileSystem(rawLFS);
    path = new Path(fs.getWorkingDirectory(), "TestRecord2Map");
    fs = path.getFileSystem(conf);
    // drop any previous tables
    BasicTable.drop(path, conf);
    // Build Table and column groups
    BasicTable.Writer writer = new BasicTable.Writer(path, STR_SCHEMA,
        STR_STORAGE, conf);
    writer.finish();
    Schema schema = writer.getSchema();
    Tuple tuple = TypesUtils.createTuple(schema);
    BasicTable.Writer writer1 = new BasicTable.Writer(path, conf);
    int part = 0;
    TableInserter inserter = writer1.getInserter("part" + part, true);
    TypesUtils.resetTuple(tuple);

    Tuple tupRecord1;
    try {
      tupRecord1 = TypesUtils.createTuple(schema.getColumnSchema("r1")
          .getSchema());
    } catch (ParseException e) {
      e.printStackTrace();
      throw new IOException(e);
    }

    Tuple tupRecord2;
    try {
      tupRecord2 = TypesUtils.createTuple(schema.getColumnSchema("r2")
          .getSchema());
    } catch (ParseException e) {
      e.printStackTrace();
      throw new IOException(e);
    }

    Tuple tupRecord3;
    try {
      tupRecord3 = TypesUtils.createTuple(new Schema("f3:float, f4:map(int)"));
    } catch (ParseException e) {
      e.printStackTrace();
      throw new IOException(e);
    }
    HashMap<String, Long> map1 = new HashMap<String, Long>();
    // insert data in row 1
    int row = 0;
    // r1:record(f1:int, f2:map(long))
    tupRecord1.set(0, 1);
    map1.put("x", 1001L);
    tupRecord1.set(1, map1);
    tuple.set(0, tupRecord1);

    // r2:record(r3:record(f3:float, map(int)))
    HashMap<String, Integer> map = new HashMap<String, Integer>();
    tupRecord2.set(0, tupRecord3);
    tupRecord3.set(0, 1.3);
    map.put("a", 1);
    map.put("b", 2);
    map.put("c", 3);
    tupRecord3.set(1, map);
    tuple.set(1, tupRecord2);
    inserter.insert(new BytesWritable(String.format("k%d%d", part + 1, row + 1)
        .getBytes()), tuple);

    // row 2
    map1.clear();
    row++;
    TypesUtils.resetTuple(tuple);
    TypesUtils.resetTuple(tupRecord1);
    TypesUtils.resetTuple(tupRecord2);
    TypesUtils.resetTuple(tupRecord3);
    // r1:record(f1:int, f2:map(long))
    tupRecord1.set(0, 2);
    map1.put("y", 1002L);
    tupRecord1.set(1, map1);
    tuple.set(0, tupRecord1);

    // r2:record(r3:record(f3:float, f4))
    tupRecord2.set(0, tupRecord3);
    map.clear();
    map.put("x", 11);
    map.put("y", 12);
    map.put("c", 13);

    tupRecord3.set(0, 2.3);
    tupRecord3.set(1, map);
    tuple.set(1, tupRecord2);

    inserter.insert(new BytesWritable(String.format("k%d%d", part + 1, row + 1)
        .getBytes()), tuple);

    // finish building table, closing out the inserter, writer, writer1
    inserter.close();
    writer1.finish();
    writer.close();
  }

  @AfterClass
  public static void tearDownOnce() throws IOException {
    BasicTable.drop(path, conf);
  }

  @Test
  public void testReadSimpleRecord() throws IOException, ParseException {
    // Starting read , simple projection 0, not record of record level. PASS
    String projection0 = new String("r1.f1, r1.f2");
    BasicTable.Reader reader = new BasicTable.Reader(path, conf);
    reader.setProjection(projection0);
    List<RangeSplit> splits = reader.rangeSplit(1);
    TableScanner scanner = reader.getScanner(splits.get(0), true);
    BytesWritable key = new BytesWritable();
    Tuple RowValue = TypesUtils.createTuple(scanner.getSchema());

    scanner.getKey(key);
    Assert.assertEquals(key, new BytesWritable("k11".getBytes()));
    scanner.getValue(RowValue);
    Assert.assertEquals(1, RowValue.get(0));
    Long expected = 1001L;
    Assert.assertEquals(expected, ((Map<String, Long>) RowValue.get(1))
        .get("x"));
    Assert.assertEquals(null, ((Map<String, Long>) RowValue.get(1)).get("y"));
    Assert.assertEquals(null, ((Map<String, Long>) RowValue.get(1)).get("a"));
    Assert.assertEquals(null, ((Map<String, Long>) RowValue.get(1)).get("z"));

    scanner.advance();
    scanner.getKey(key);
    Assert.assertEquals(key, new BytesWritable("k12".getBytes()));
    scanner.getValue(RowValue);
    Assert.assertEquals(2, RowValue.get(0));
    expected = 1002L;
    Assert.assertEquals(expected, ((Map<String, Long>) RowValue.get(1))
        .get("y"));
    Assert.assertEquals(null, ((Map<String, Long>) RowValue.get(1)).get("x"));
    Assert.assertEquals(null, ((Map<String, Long>) RowValue.get(1)).get("a"));
    Assert.assertEquals(null, ((Map<String, Long>) RowValue.get(1)).get("z"));

    reader.close();

  }

  /*
   * String STR_SCHEMA =
   * "r1:record(f1:int, f2:long), r2:record(r3:record(f3:float, map(int))";
   * String STR_STORAGE =
   * "[r1.f1]; [r2.r3.f4]; [r1.f2, r2.r3.f3, r2.r3.f4#{a|c}]";
   */
  @Test
  public void testReadRecordOfMap1() throws IOException, ParseException {
    String projection1 = new String("r2.r3.f4#{a|b}, r2.r3.f3");
    BasicTable.Reader reader = new BasicTable.Reader(path, conf);
    reader.setProjection(projection1);
    List<RangeSplit> splits = reader.rangeSplit(1);
    TableScanner scanner = reader.getScanner(splits.get(0), true);
    scanner = reader.getScanner(splits.get(0), true);
    BytesWritable key = new BytesWritable();
    Tuple RowValue = TypesUtils.createTuple(scanner.getSchema());

    scanner.getKey(key);
    Assert.assertEquals(key, new BytesWritable("k11".getBytes()));
    scanner.getValue(RowValue);
    Assert.assertEquals(1, ((Map) (RowValue.get(0))).get("a"));
    Assert.assertEquals(2, ((Map) RowValue.get(0)).get("b"));
    Assert.assertEquals(null, ((Map) (RowValue.get(0))).get("x"));
    Assert.assertEquals(null, ((Map) RowValue.get(0)).get("y"));
    Assert.assertEquals(1.3, RowValue.get(1));

    scanner.advance();
    scanner.getKey(key);
    Assert.assertEquals(key, new BytesWritable("k12".getBytes()));
    scanner.getValue(RowValue);
    Assert.assertEquals(null, ((Map) RowValue.get(0)).get("a"));
    Assert.assertEquals(null, ((Map) RowValue.get(0)).get("b"));
    Assert.assertEquals(null, ((Map) RowValue.get(0)).get("x"));
    Assert.assertEquals(null, ((Map) RowValue.get(0)).get("y"));
    Assert.assertEquals(null, ((Map) RowValue.get(0)).get("c"));
    Assert.assertEquals(2.3, RowValue.get(1));
    reader.close();
  }

  /*
   * String STR_SCHEMA =
   * "r1:record(f1:int, f2:long), r2:record(r3:record(f3:float, f4:map(int))";
   * String STR_STORAGE =
   * "[r1.f1]; [r2.r3.f4]; [r1.f2, r2.r3.f3, r2.r3.f4#{a|c}]";
   */
  @Test
  // test stitch, [r1.f1]; [r2.r3.f4]; [r1.f2, r2.r3.f3]
  public void testReadRecordOfRecord2() throws IOException, ParseException {
    String projection2 = new String("r1.f1, r2.r3.f3");
    BasicTable.Reader reader = new BasicTable.Reader(path, conf);
    reader.setProjection(projection2);
    List<RangeSplit> splits = reader.rangeSplit(1);
    TableScanner scanner = reader.getScanner(splits.get(0), true);
    scanner = reader.getScanner(splits.get(0), true);
    BytesWritable key = new BytesWritable();
    Tuple RowValue = TypesUtils.createTuple(scanner.getSchema());

    scanner.getKey(key);
    Assert.assertEquals(key, new BytesWritable("k11".getBytes()));
    scanner.getValue(RowValue);
    Assert.assertEquals(1, RowValue.get(0));
    Assert.assertEquals(1.3, RowValue.get(1));
    scanner.advance();

    scanner.getKey(key);
    Assert.assertEquals(key, new BytesWritable("k12".getBytes()));
    scanner.getValue(RowValue);
    Assert.assertEquals(2, RowValue.get(0));
    Assert.assertEquals(2.3, RowValue.get(1));
    reader.close();
  }

  /*
   * String STR_SCHEMA =
   * "r1:record(f1:int, f2:long), r2:record(r3:record(f3:float, f4:map(int)))" ;
   * String STR_STORAGE =
   * "[r1.f1]; [r2.r3.f4]; [r1.f2, r2.r3.f3, r2.r3.f4#{a|c}]";
   */
  // test stitch, [r1.f1]; [r2.r3.f4]; [r1.f2, r2.r3.f3]
  public void testReadRecordOfRecord3() throws IOException, ParseException {
    String projection3 = new String("r1, r2, r1.f2#{x|z}");
    BasicTable.Reader reader = new BasicTable.Reader(path, conf);
    reader.setProjection(projection3);
    List<RangeSplit> splits = reader.rangeSplit(1);
    TableScanner scanner = reader.getScanner(splits.get(0), true);
    scanner = reader.getScanner(splits.get(0), true);
    BytesWritable key = new BytesWritable();
    Tuple RowValue = TypesUtils.createTuple(scanner.getSchema());

    scanner.getKey(key);
    Assert.assertEquals(key, new BytesWritable("k11".getBytes()));
    scanner.getValue(RowValue);

    Assert.assertEquals(1, ((Tuple) RowValue.get(0)).get(0));
    Long expected = 1001L;
    Assert.assertEquals(expected, ((Map<String, Long>) RowValue.get(2))
        .get("x"));
    Assert.assertEquals(null, ((Map<String, Long>) RowValue.get(2)).get("y"));
    Assert.assertEquals(null, ((Map<String, Long>) RowValue.get(2)).get("z"));
    Tuple r2 = (Tuple) RowValue.get(1);
    Tuple r3 = (Tuple) r2.get(0);
    Map<String, Integer> f4 = (Map<String, Integer>) r3.get(1);
    Integer tmp = 1;
    Assert.assertEquals(tmp, f4.get("a"));
    tmp = 2;
    Assert.assertEquals(tmp, f4.get("b"));
    tmp = 3;
    Assert.assertEquals(tmp, f4.get("c"));
    Assert.assertEquals(1.3, ((Tuple) ((Tuple) RowValue.get(1)).get(0)).get(0));

    scanner.advance();

    scanner.getKey(key);
    Assert.assertEquals(key, new BytesWritable("k12".getBytes()));
    scanner.getValue(RowValue);
    Assert.assertEquals(2, ((Tuple) RowValue.get(0)).get(0));
    expected = 1002L;
    Assert.assertEquals(null, ((Map<String, Long>) RowValue.get(2)).get("x"));
    Assert.assertEquals(null, ((Map<String, Long>) RowValue.get(2)).get("y"));
    Assert.assertEquals(null, ((Map<String, Long>) RowValue.get(2)).get("z"));
    r2 = (Tuple) RowValue.get(1);
    r3 = (Tuple) r2.get(0);
    f4 = (Map<String, Integer>) r3.get(1);
    tmp = 11;
    Assert.assertEquals(tmp, f4.get("x"));
    tmp = 12;
    Assert.assertEquals(tmp, f4.get("y"));
    tmp = 13;
    Assert.assertEquals(tmp, f4.get("c"));
    Assert.assertEquals(2.3, ((Tuple) ((Tuple) RowValue.get(1)).get(0)).get(0));

    reader.close();
  }

  /*
   * String STR_SCHEMA =
   * "r1:record(f1:int, f2:long), r2:record(r3:record(f3:float, f4:map(int)))" ;
   * String STR_STORAGE =
   * "[r1.f1]; [r2.r3.f4]; [r1.f2, r2.r3.f3, r2.r3.f4#{a|c}]";
   */
  @Test
  // test stitch, [r1.f1]; [r2.r3.f4]; [r1.f2, r2.r3.f3]
  public void testReadRecordOfRecord4() throws IOException, ParseException {
    String projection3 = new String("r2.r3.f4");
    BasicTable.Reader reader = new BasicTable.Reader(path, conf);
    reader.setProjection(projection3);
    List<RangeSplit> splits = reader.rangeSplit(1);
    TableScanner scanner = reader.getScanner(splits.get(0), true);
    scanner = reader.getScanner(splits.get(0), true);
    BytesWritable key = new BytesWritable();
    Tuple RowValue = TypesUtils.createTuple(scanner.getSchema());

    scanner.getKey(key);
    Assert.assertEquals(key, new BytesWritable("k11".getBytes()));
    scanner.getValue(RowValue);

    Map<String, Integer> f4 = (Map<String, Integer>) RowValue.get(0);
    Integer tmp = 1;
    Assert.assertEquals(tmp, f4.get("a"));
    tmp = 2;
    Assert.assertEquals(tmp, f4.get("b"));
    tmp = 3;
    Assert.assertEquals(tmp, f4.get("c"));
    Assert.assertEquals(null, f4.get("x"));

    scanner.advance();

    scanner.getKey(key);
    Assert.assertEquals(key, new BytesWritable("k12".getBytes()));
    scanner.getValue(RowValue);

    f4 = (Map<String, Integer>) RowValue.get(0);
    tmp = 11;
    Assert.assertEquals(tmp, f4.get("x"));
    tmp = 12;
    Assert.assertEquals(tmp, f4.get("y"));
    tmp = 13;
    Assert.assertEquals(tmp, f4.get("c"));
    Assert.assertEquals(null, f4.get("a"));
    reader.close();
  }

  @Test
  public void testReadNegative1() throws IOException, ParseException {
    String projection4 = new String("r3");
    BasicTable.Reader reader = new BasicTable.Reader(path, conf);
    reader.setProjection(projection4);
    List<RangeSplit> splits = reader.rangeSplit(1);
    TableScanner scanner = reader.getScanner(splits.get(0), true);
    scanner = reader.getScanner(splits.get(0), true);
    BytesWritable key = new BytesWritable();
    Tuple RowValue = TypesUtils.createTuple(scanner.getSchema());
    scanner.getKey(key);
    Assert.assertEquals(key, new BytesWritable("k11".getBytes()));
    scanner.getValue(RowValue);
    Assert.assertEquals(null, RowValue.get(0));
    reader.close();

  }
}

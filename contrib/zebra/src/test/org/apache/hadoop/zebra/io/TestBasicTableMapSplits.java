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
import java.io.PrintStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
import org.apache.hadoop.zebra.schema.Schema;
import org.apache.hadoop.zebra.types.TypesUtils;
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
public class TestBasicTableMapSplits {
  final static String STR_SCHEMA = "f1:bool, r:record(f11:int, f12:long), m:map(string), c:collection(record(f13:double, f14:double, f15:bytes))";
  final static String STR_STORAGE = "[r.f12, f1, m#{b}]; [m#{a}, r.f11]";
  private static Configuration conf;
  private static Path path;
  private static FileSystem fs;

  @BeforeClass
  public static void setUp() throws IOException {
    conf = new Configuration();
    conf.setInt("table.output.tfile.minBlock.size", 64 * 1024);
    conf.setInt("table.input.split.minSize", 64 * 1024);
    conf.set("table.output.tfile.compression", "none");

    RawLocalFileSystem rawLFS = new RawLocalFileSystem();
    fs = new LocalFileSystem(rawLFS);
    path = new Path(fs.getWorkingDirectory(), "TestBasicTableMapSplits");
    fs = path.getFileSystem(conf);
    // drop any previous tables
    BasicTable.drop(path, conf);

    BasicTable.Writer writer = new BasicTable.Writer(path, STR_SCHEMA,
        STR_STORAGE, conf);
    writer.finish();

    Schema schema = writer.getSchema();
    Tuple tuple = TypesUtils.createTuple(schema);

    BasicTable.Writer writer1 = new BasicTable.Writer(path, conf);
    int part = 0;
    TableInserter inserter = writer1.getInserter("part" + part, true);
    TypesUtils.resetTuple(tuple);

    tuple.set(0, true);

    Tuple tupRecord;
    try {
      tupRecord = TypesUtils.createTuple(schema.getColumnSchema("r")
          .getSchema());
    } catch (ParseException e) {
      e.printStackTrace();
      throw new IOException(e);
    }
    tupRecord.set(0, 1);
    tupRecord.set(1, 1001L);
    tuple.set(1, tupRecord);

    Map<String, String> map = new HashMap<String, String>();
    map.put("a", "x");
    map.put("b", "y");
    map.put("c", "z");
    tuple.set(2, map);

    DataBag bagColl = TypesUtils.createBag();
    Schema schColl = schema.getColumn(3).getSchema().getColumn(0).getSchema();
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
    tuple.set(3, bagColl);

    int row = 0;
    inserter.insert(new BytesWritable(String.format("k%d%d", part + 1, row + 1)
        .getBytes()), tuple);
    inserter.close();
    writer1.finish();

    writer.close();
  }

  @AfterClass
  public static void tearDownOnce() throws IOException {
  }

  public void testDescribse() throws IOException {

    BasicTable.dumpInfo(path.toString(), System.out, conf);

  }

  @Test
  public void test1() throws IOException, ParseException {
    String projection = new String("r.f12, f1");
    BasicTable.Reader reader = new BasicTable.Reader(path, conf);
    reader.setProjection(projection);
    // long totalBytes = reader.getStatus().getSize();

    List<RangeSplit> splits = reader.rangeSplit(1);
    reader.close();
    reader = new BasicTable.Reader(path, conf);
    reader.setProjection(projection);
    TableScanner scanner = reader.getScanner(splits.get(0), true);
    BytesWritable key = new BytesWritable();
    Tuple value = TypesUtils.createTuple(scanner.getSchema());

    scanner.getKey(key);
    Assert.assertEquals(key, new BytesWritable("k11".getBytes()));
    scanner.getValue(value);

    Assert.assertEquals(1001L, value.get(0));
    Assert.assertEquals(true, value.get(1));
    reader.close();
  }

  @Test
  public void testStitch1() throws IOException, ParseException {
    String projection = new String("f1, m#{a|b}, r, m#{c}");
    BasicTable.Reader reader = new BasicTable.Reader(path, conf);
    reader.setProjection(projection);
    // long totalBytes = reader.getStatus().getSize();

    List<RangeSplit> splits = reader.rangeSplit(1);
    reader.close();
    reader = new BasicTable.Reader(path, conf);
    reader.setProjection(projection);
    TableScanner scanner = reader.getScanner(splits.get(0), true);
    BytesWritable key = new BytesWritable();
    Tuple value = TypesUtils.createTuple(scanner.getSchema());

    scanner.getKey(key);
    Assert.assertEquals(key, new BytesWritable("k11".getBytes()));
    scanner.getValue(value);

    Tuple recordTuple = (Tuple) value.get(2);
    Assert.assertEquals(1, recordTuple.get(0));
    Assert.assertEquals(1001L, recordTuple.get(1));
    Assert.assertEquals(true, value.get(0));

    HashMap<String, Object> mapval = (HashMap<String, Object>) value.get(1);
    Assert.assertEquals("x", mapval.get("a"));
    Assert.assertEquals("y", mapval.get("b"));
    Assert.assertEquals(null, mapval.get("c"));
    mapval = (HashMap<String, Object>) value.get(3);
    Assert.assertEquals("z", mapval.get("c"));
    Assert.assertEquals(null, mapval.get("a"));
    Assert.assertEquals(null, mapval.get("b"));
    reader.close();
  }

  @Test
  public void testStitch2() throws IOException, ParseException {
    String projection = new String("f1, m#{a}, r, m#{c}, m");
    BasicTable.Reader reader = new BasicTable.Reader(path, conf);
    reader.setProjection(projection);
    // long totalBytes = reader.getStatus().getSize();

    List<RangeSplit> splits = reader.rangeSplit(1);
    reader.close();
    reader = new BasicTable.Reader(path, conf);
    reader.setProjection(projection);
    TableScanner scanner = reader.getScanner(splits.get(0), true);
    BytesWritable key = new BytesWritable();
    Tuple value = TypesUtils.createTuple(scanner.getSchema());

    scanner.getKey(key);
    Assert.assertEquals(key, new BytesWritable("k11".getBytes()));
    scanner.getValue(value);

    Tuple recordTuple = (Tuple) value.get(2);
    Assert.assertEquals(1, recordTuple.get(0));
    Assert.assertEquals(1001L, recordTuple.get(1));
    Assert.assertEquals(true, value.get(0));

    Map<String, Object> mapval = (Map<String, Object>) value.get(1);
    Assert.assertEquals("x", mapval.get("a"));
    mapval = (Map<String, Object>) value.get(3);
    Assert.assertEquals("z", mapval.get("c"));

    Map<String, Object> map = (Map<String, Object>) (value.get(4));
    Assert.assertEquals("x", (String) map.get("a"));
    Assert.assertEquals("y", (String) map.get("b"));
    Assert.assertEquals("z", (String) map.get("c"));
    reader.close();
  }
}

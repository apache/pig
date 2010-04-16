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
import java.util.Iterator;
import java.util.Map;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.zebra.io.ColumnGroup;
import org.apache.hadoop.zebra.io.TableInserter;
import org.apache.hadoop.zebra.io.TableScanner;
import org.apache.hadoop.zebra.parser.ParseException;
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
 * Test projections on complicated(nested: record, collection, map) column
 * types.
 * 
 */
public class TestColumnGroupSplits {
  final static String outputFile = "TestColumnGroupSplits";
  final static String STR_SCHEMA = "f1:bool, r:record(f11:int, f12:long), m:map(string), c:collection(record(f13:double, f14:double, f15:bytes))";
  final static private Configuration conf = new Configuration();
  static private FileSystem fs;
  static private Path path;
  static private Schema schema;

  @BeforeClass
  public static void setUpOnce() throws IOException,
    ParseException {
    // set default file system to local file system
    conf.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");

    // must set a conf here to the underlying FS, or it barks
    RawLocalFileSystem rawLFS = new RawLocalFileSystem();
    rawLFS.setConf(conf);
    fs = new LocalFileSystem(rawLFS);
    path = new Path(fs.getWorkingDirectory(), outputFile);
    System.out.println("output file: " + path);
    
    if (fs.exists(path)) {
        ColumnGroup.drop(path, conf);
    }

    schema = new Schema(STR_SCHEMA);

    ColumnGroup.Writer writer = new ColumnGroup.Writer(path, schema, false, path.getName(),
        "pig", "gz", null, null, (short) -1, true, conf);
    TableInserter ins = writer.getInserter("part0", true);

    Tuple row = TypesUtils.createTuple(schema);
    // schema for "r:record..."
    Schema schRecord = writer.getSchema().getColumn(1).getSchema();
    Tuple tupRecord = TypesUtils.createTuple(schRecord);
    Schema schColl = schema.getColumn(3).getSchema().getColumn(0).getSchema();
    DataBag bagColl = TypesUtils.createBag();
    Tuple tupColl1 = TypesUtils.createTuple(schColl);
    Tuple tupColl2 = TypesUtils.createTuple(schColl);
    Map<String, String> map = new HashMap<String, String>();
    byte[] abs1 = new byte[3];
    byte[] abs2 = new byte[4];

    // row 1
    tupRecord.set(0, 1);
    tupRecord.set(1, 1001L);
    map.put("a", "x");
    map.put("b", "y");
    map.put("c", "z");
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
    row.set(0, true);
    row.set(1, tupRecord);
    row.set(2, map);
    row.set(3, bagColl);
    ins.insert(new BytesWritable("k1".getBytes()), row);

    // row 2
    TypesUtils.resetTuple(row);
    TypesUtils.resetTuple(tupRecord);
    map.clear();
    tupRecord.set(0, 2);
    tupRecord.set(1, 1002L);
    map.put("boy", "girl");
    map.put("adam", "amy");
    map.put("bob", "becky");
    map.put("carl", "cathy");
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
    row.set(0, false);
    row.set(1, tupRecord);
    row.set(2, map);
    row.set(3, bagColl);
    ins.insert(new BytesWritable("k2".getBytes()), row);
    ins.close();

    writer.close();
  }

  @AfterClass
  public static void tearDownOnce() throws IOException {
  }

  @Test
  public void testDescribe() throws IOException, Exception {
    ColumnGroup.dumpInfo(path, System.out, conf);
  }

  @Test
  // default projection (without any projection) should return every column
  public void testDefaultProjection() throws IOException, ExecException,
    ParseException {
    System.out.println("testDefaultProjection");
    ColumnGroup.Reader reader = new ColumnGroup.Reader(path, conf);
    TableScanner scanner = reader.getScanner(null, false);

    defTest(reader, scanner);

    scanner.close();
    reader.close();
  }

  @SuppressWarnings("unchecked")
  private void defTest(ColumnGroup.Reader reader, TableScanner scanner)
      throws IOException, ParseException {
    BytesWritable key = new BytesWritable();
    Tuple row = TypesUtils.createTuple(reader.getSchema());

    scanner.getKey(key);
    Assert.assertEquals(key, new BytesWritable("k1".getBytes()));
    scanner.getValue(row);
    Assert.assertEquals(true, row.get(0));
    Tuple tupRecord1 = (Tuple) row.get(1);
    Assert.assertEquals(1, tupRecord1.get(0));
    Assert.assertEquals(1001L, tupRecord1.get(1));
    Map<String, String> map1 = (Map<String, String>) row.get(2);
    Assert.assertEquals("x", map1.get("a"));
    Assert.assertEquals("y", map1.get("b"));
    Assert.assertEquals("z", map1.get("c"));
    DataBag bagColl = (DataBag) row.get(3);
    Iterator<Tuple> itorColl = bagColl.iterator();
    Tuple tupColl = itorColl.next();
    Assert.assertEquals(3.1415926, tupColl.get(0));
    Assert.assertEquals(1.6, tupColl.get(1));
    DataByteArray dba = (DataByteArray) tupColl.get(2);
    Assert.assertEquals(11, dba.get()[0]);
    Assert.assertEquals(12, dba.get()[1]);
    Assert.assertEquals(13, dba.get()[2]);
    tupColl = itorColl.next();
    Assert.assertEquals(123.456789, tupColl.get(0));
    Assert.assertEquals(100, tupColl.get(1));
    dba = (DataByteArray) tupColl.get(2);
    Assert.assertEquals(21, dba.get()[0]);
    Assert.assertEquals(22, dba.get()[1]);
    Assert.assertEquals(23, dba.get()[2]);
    Assert.assertEquals(24, dba.get()[3]);

    // move to next row
    scanner.advance();
    scanner.getKey(key);
    Assert.assertEquals(key, new BytesWritable("k2".getBytes()));
    TypesUtils.resetTuple(row);
    scanner.getValue(row);
    Assert.assertEquals(false, row.get(0));
    Tuple tupRecord2 = (Tuple) row.get(1);
    Assert.assertEquals(2, tupRecord2.get(0));
    Assert.assertEquals(1002L, tupRecord2.get(1));
    Map<String, String> map2 = (Map<String, String>) row.get(2);
    Assert.assertEquals("girl", map2.get("boy"));
    Assert.assertEquals("amy", map2.get("adam"));
    Assert.assertEquals("cathy", map2.get("carl"));
    bagColl = (DataBag) row.get(3);
    itorColl = bagColl.iterator();
    tupColl = itorColl.next();
    Assert.assertEquals(7654.321, tupColl.get(0));
    Assert.assertEquals(0.0001, tupColl.get(1));
    tupColl = itorColl.next();
    Assert.assertEquals(0.123456789, tupColl.get(0));
    Assert.assertEquals(0.3333, tupColl.get(1));
    dba = (DataByteArray) tupColl.get(2);
    Assert.assertEquals(41, dba.get()[0]);
    Assert.assertEquals(42, dba.get()[1]);
    Assert.assertEquals(43, dba.get()[2]);
    Assert.assertEquals(44, dba.get()[3]);
  }

  @Test
  // null projection should be same as default (fully projected) projection
  public void testNullProjection() throws IOException, ExecException,
    ParseException {
    System.out.println("testNullProjection");
    ColumnGroup.Reader reader = new ColumnGroup.Reader(path, conf);
    reader.setProjection(null);
    TableScanner scanner = reader.getScanner(null, false);

    defTest(reader, scanner);

    scanner.close();
    reader.close();
  }

  @Test
  // empty projection should project no columns at all
  public void testEmptyProjection() throws Exception {
    System.out.println("testEmptyProjection");
    // test without beginKey/endKey
    ColumnGroup.Reader reader = new ColumnGroup.Reader(path, conf);
    reader.setProjection("");

    Tuple row = TypesUtils.createTuple(reader.getSchema());
    TableScanner scanner = reader.getScanner(null, false);
    BytesWritable key = new BytesWritable();

    scanner.getKey(key);
    Assert.assertEquals(key, new BytesWritable("k1".getBytes()));
    scanner.getValue(row);
    try {
      row.get(0);
      Assert.fail("Failed to catch out of boundary exceptions.");
    } catch (IndexOutOfBoundsException e) {
      // no op, expecting out of bounds exceptions
    }

    scanner.advance();
    scanner.getKey(key);
    Assert.assertEquals(key, new BytesWritable("k2".getBytes()));
    TypesUtils.resetTuple(row);
    scanner.getValue(row);
    // Assert.assertEquals("c2", row.get(0));

    scanner.close();
    reader.close();
  }

  @Test
  // a column name that does not exist in the column group
  public void testOneNonExistentProjection() throws Exception {
    System.out.println("testOneNonExistentProjection");
    ColumnGroup.Reader reader = new ColumnGroup.Reader(path, conf);
    reader.setProjection("X");

    Tuple row = TypesUtils.createTuple(reader.getSchema());
    TableScanner scanner = reader.getScanner(null, false);
    BytesWritable key = new BytesWritable();

    scanner.getKey(key);
    Assert.assertEquals(key, new BytesWritable("k1".getBytes()));
    scanner.getValue(row);
    Assert.assertNull(row.get(0));
    try {
      row.get(1);
      Assert.fail("Failed to catch out of boundary exceptions.");
    } catch (IndexOutOfBoundsException e) {
      // no op, expecting out of bounds exceptions
    }

    scanner.advance();
    scanner.getKey(key);
    Assert.assertEquals(key, new BytesWritable("k2".getBytes()));
    TypesUtils.resetTuple(row);
    scanner.getValue(row);
    // Assert.assertEquals("c2", row.get(0));

    scanner.close();
    reader.close();
  }

  @Test
  // normal one simple column projection
  public void testOneSimpleColumnProjection() throws Exception {
    System.out.println("testOneSimpleColumnProjection");
    // test without beginKey/endKey
    ColumnGroup.Reader reader = new ColumnGroup.Reader(path, conf);
    reader.setProjection("f1");

    Tuple row = TypesUtils.createTuple(reader.getSchema());
    TableScanner scanner = reader.getScanner(null, false);
    BytesWritable key = new BytesWritable();

    scanner.getKey(key);
    Assert.assertEquals(key, new BytesWritable("k1".getBytes()));
    scanner.getValue(row);
    Assert.assertEquals(true, row.get(0));
    try {
      row.get(1);
      Assert.fail("Failed to catch 'out of boundary' exceptions.");
    } catch (IndexOutOfBoundsException e) {
      // no op, expecting out of bounds exceptions
    }

    scanner.advance();
    scanner.getKey(key);
    Assert.assertEquals(key, new BytesWritable("k2".getBytes()));
    TypesUtils.resetTuple(row);
    scanner.getValue(row);
    Assert.assertEquals(false, row.get(0));

    scanner.close();
    reader.close();
  }

  @Test
  // normal one record column projection
  public void testOneRecordColumnProjection() throws Exception {
    System.out.println("testOneRecordColumnProjection");
    ColumnGroup.Reader reader = new ColumnGroup.Reader(path, conf);
    reader.setProjection("r.f12, r.XYZ");

    Tuple row = TypesUtils.createTuple(reader.getSchema());
    TableScanner scanner = reader.getScanner(null, false);
    BytesWritable key = new BytesWritable();

    scanner.getKey(key);
    Assert.assertEquals(key, new BytesWritable("k1".getBytes()));
    scanner.getValue(row);
    Assert.assertEquals(1001L, row.get(0));
    Assert.assertNull(row.get(1));
    try {
      row.get(2);
      Assert.fail("Failed to catch 'out of boundary' exceptions.");
    } catch (IndexOutOfBoundsException e) {
      // no op, expecting out of bounds exceptions
    }

    scanner.advance();
    scanner.getKey(key);
    Assert.assertEquals(key, new BytesWritable("k2".getBytes()));
    TypesUtils.resetTuple(row);
    scanner.getValue(row);
    Assert.assertEquals(1002L, row.get(0));
    Assert.assertNull(row.get(1));

    scanner.close();
    reader.close();
  }

  @Test
  @SuppressWarnings("unchecked")
  // normal one collection column projection
  public void testMapColumnProjection() throws Exception {
    System.out.println("testMapColumnProjection");
    // test without beginKey/endKey
    ColumnGroup.Reader reader = new ColumnGroup.Reader(path, conf);
    reader.setProjection("m#{c|b}, f1, m#{a}");

    Tuple row = TypesUtils.createTuple(reader.getSchema());
    TableScanner scanner = reader.getScanner(null, false);
    BytesWritable key = new BytesWritable();

    scanner.getKey(key);
    Assert.assertEquals(key, new BytesWritable("k1".getBytes()));
    scanner.getValue(row);
    String mapVal = (String) (((Map<String, Object>) (row.get(0))).get("c"));

    boolean f1 = (Boolean) (row.get(1));
    Assert.assertEquals(true, f1);

    Assert.assertEquals("z", mapVal);
    mapVal = (String) (((Map<String, Object>) (row.get(0))).get("b"));
    Assert.assertEquals("y", mapVal);
    mapVal = (String) (((Map<String, Object>) (row.get(2))).get("a"));
    Assert.assertEquals("x", mapVal);
    try {
      row.get(3);
      Assert.fail("Failed to catch 'out of boundary' exceptions.");
    } catch (IndexOutOfBoundsException e) {
      // no op, expecting out of bounds exceptions
    }

    scanner.advance();
    scanner.getKey(key);
    Assert.assertEquals(key, new BytesWritable("k2".getBytes()));
    TypesUtils.resetTuple(row);
    scanner.getValue(row);

    f1 = (Boolean) row.get(1);
    Assert.assertEquals(false, f1);

    mapVal = (String) (((Map<String, Object>) (row.get(0))).get("c"));
    Assert.assertEquals(null, mapVal);

    scanner.close();
    reader.close();
  }
}

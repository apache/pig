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
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.zebra.BaseTestCase;
import org.apache.hadoop.zebra.io.BasicTable;
import org.apache.hadoop.zebra.io.TableInserter;
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
 * This is to test type check for writes in Zebra at IO level.
 */
public class TestTypeCheck extends BaseTestCase {
  final static String STR_STORAGE = "[r.f12, f1]; [m]";
  private static Path path;
  

  @BeforeClass
  public static void setUp() throws Exception {
    init();
  }
  
  @Test
  public void testPositive1() throws IOException, ParseException {
    path = getTableFullPath("TestTypeCheck");
    removeDir(path);
    
    String STR_SCHEMA = "f1:bool, r:record(f11:int, f12:long), m:map(string), c:collection(record(f13:double, f14:double, f15:bytes))";
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
  
  // float column sees double value;
  @Test (expected = IOException.class)
  public void testNegative1() throws IOException, ParseException {
    path = getTableFullPath("TestTypeCheck");
    removeDir(path);
    String STR_SCHEMA = "f1:bool, r:record(f11:int, f12:long), m:map(string), c:collection(record(f13:double, f14:float, f15:bytes))";
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

  // int column sees a string value;
  @Test (expected = IOException.class)
  public void testNegative2() throws IOException, ParseException {
    path = getTableFullPath("TestTypeCheck");
    removeDir(path);

    String STR_SCHEMA = "f1:bool, r:record(f11:int, f12:long), m:map(string), c:collection(record(f13:double, f14:double, f15:bytes))";
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
    tupRecord.set(0, "abc");
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
     removeDir(path);
  }
}

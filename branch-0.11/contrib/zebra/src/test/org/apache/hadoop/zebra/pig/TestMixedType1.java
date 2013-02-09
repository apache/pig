package org.apache.hadoop.zebra.pig;

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

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import junit.framework.Assert;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.zebra.io.BasicTable;
import org.apache.hadoop.zebra.io.TableInserter;
import org.apache.hadoop.zebra.parser.ParseException;
import org.apache.hadoop.zebra.schema.Schema;
import org.apache.hadoop.zebra.BaseTestCase;
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
public class TestMixedType1 extends BaseTestCase {
  final static String STR_SCHEMA = "s1:bool, s2:int, s3:long, s4:double, s5:string, s6:bytes, r1:record(f1:int, f2:long), r2:record(r3:record(f3:double, f4)), m1:map(string),m2:map(map(int)), c:collection(record(f13:double, f14:double, f15:bytes))";
  final static String STR_STORAGE = "[s1, s2]; [m1#{a}]; [r1.f1]; [s3, s4, r2.r3.f3]; [s5, s6, m2#{x|y}]; [r1.f2, m1#{b}]; [r2.r3.f4, m2#{z}]";

  private static Path path;

  @BeforeClass
  public static void setUpOnce() throws IOException, Exception {
    System.out.println("ONCE SETUP !! ---------");
    init();
    
    path = getTableFullPath("TestMixedType1");  
    removeDir(path);
    
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
      tupRecord3 = TypesUtils.createTuple(new Schema("f3:float, f4"));
    } catch (ParseException e) {
      e.printStackTrace();
      throw new IOException(e);
    }

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
    HashMap<String, Map> m2 = new HashMap<String, Map>();
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
    Schema schColl = schema.getColumn(10).getSchema().getColumn(0).getSchema();
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

    inserter.insert(new BytesWritable(String.format("k%d%d", part + 1, row + 1)
        .getBytes()), tuple);

    inserter.close();
    writer1.finish();

    writer.close();
  }

  @AfterClass
  public static void tearDownOnce() throws IOException {
    BasicTable.drop(path, conf);
  }

  @Test
  public void test1() throws IOException, ParseException {
    String query = "records = LOAD '" + path.toString()
        + "' USING org.apache.hadoop.zebra.pig.TableLoader('r1.f2, s1');";
    System.out.println(query);
    pigServer.registerQuery(query);
    Iterator<Tuple> it = pigServer.openIterator("records");
    BytesWritable key = new BytesWritable();
    int row = 0;
    while (it.hasNext()) {
      Tuple RowValue = it.next();
      System.out.println(RowValue);
      row++;
      if (row == 1) {
        Assert.assertEquals(1001L, RowValue.get(0));
        Assert.assertEquals(true, RowValue.get(1));
      }
      if (row == 2) {
        Assert.assertEquals(1002L, RowValue.get(0));
        Assert.assertEquals(false, RowValue.get(1));
      }
    }
  }

  @Test
  public void testStitch() throws IOException, ParseException {
    String query = "records = LOAD '" + path.toString()
        + "' USING org.apache.hadoop.zebra.pig.TableLoader('s1, r1');";
    System.out.println(query);
    pigServer.registerQuery(query);
    Iterator<Tuple> it = pigServer.openIterator("records");
    BytesWritable key = new BytesWritable();
    int row = 0;
    while (it.hasNext()) {
      Tuple RowValue = it.next();
      System.out.println(RowValue);
      row++;
      if (row == 1) {
        Tuple recordTuple = (Tuple) RowValue.get(1);
        Assert.assertEquals(1, recordTuple.get(0));
        Assert.assertEquals(1001L, recordTuple.get(1));
        Assert.assertEquals(true, RowValue.get(0));
      }

    }
  }
}

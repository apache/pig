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
package org.apache.hadoop.zebra.types;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import junit.framework.Assert;

import org.apache.hadoop.zebra.BaseTestCase;
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
 * This is to test type check for writes in Zebra.
 */
public class TestTypeCheck extends BaseTestCase {

  @BeforeClass
  public static void setUpOnce() throws Exception {
    init();
  }
  
  @Test
  public void testPositive1() throws ParseException, IOException {
    String STR_SCHEMA = "s1:bool, s2:int, s3:long, s4:float, s5:double, s6:string, s7:bytes";
    Schema schema = new Schema(STR_SCHEMA);

    Tuple tuple = TypesUtils.createTuple(schema);
    TypesUtils.resetTuple(tuple);

    tuple.set(0, true);
    tuple.set(1, 1);
    tuple.set(2, 1001L);
    tuple.set(3, 1.1f);
    tuple.set(4, 2.2d);
    tuple.set(5, "hello world 1");
    tuple.set(6, new DataByteArray("hello byte 1"));

    try {
      TypesUtils.checkCompatible(tuple, schema);
    } catch (Exception e) {
      Assert.fail("Should not see this: " + e.getMessage());
    }
  }
  
  @Test
  public void testPositive2() throws ParseException, IOException {
    String STR_SCHEMA = "f1:bool, r:record(f11:int, f12:long), m:map(string), c:collection(record(f13:double, f14:float, f15:bytes))";
    Schema schema = new Schema(STR_SCHEMA);

    Tuple tuple = TypesUtils.createTuple(schema);    
    TypesUtils.resetTuple(tuple);

    tuple.set(0, true);

    Tuple tupRecord;
    try {
      tupRecord = TypesUtils.createTuple(schema.getColumnSchema("r").getSchema());
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
    tupColl1.set(1, 1.6f);
    abs1[0] = 11;
    abs1[1] = 12;
    abs1[2] = 13;
    tupColl1.set(2, new DataByteArray(abs1));
    bagColl.add(tupColl1);
    tupColl2.set(0, 123.456789);
    tupColl2.set(1, 100f);
    abs2[0] = 21;
    abs2[1] = 22;
    abs2[2] = 23;
    abs2[3] = 24;
    tupColl2.set(2, new DataByteArray(abs2));
    bagColl.add(tupColl2);
    tuple.set(3, bagColl);
    
    try {
      TypesUtils.checkCompatible(tuple, schema);
    } catch (Exception e) {
      Assert.fail("Should not see this: " + e.getMessage());
    }
  }
  
  @Test
  public void testPositive3() throws IOException, ParseException {
    String STR_SCHEMA = "m1:map(string),m2:map(map(int))";
    Schema schema = new Schema(STR_SCHEMA);
    Tuple tuple = TypesUtils.createTuple(schema);
    TypesUtils.resetTuple(tuple);

    // m1:map(string)
    Map<String, String> m1 = new HashMap<String, String>();
    m1.put("a", "A");
    m1.put("b", "B");
    m1.put("c", "C");
    tuple.set(0, m1);

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
    tuple.set(1, m2);
    
    try {
      TypesUtils.checkCompatible(tuple, schema);
    } catch (Exception e) {
      Assert.fail("Should not see this: " + e.getMessage());
    }
  }
  
  @Test
  public void testPositive4() throws ParseException, IOException {
    String STR_SCHEMA = "s1:bool, s2:int, s3:long, s4:float, s5:double, s6:string, s7:bytes";
    Schema schema = new Schema(STR_SCHEMA);

    Tuple tuple = TypesUtils.createTuple(schema);
    TypesUtils.resetTuple(tuple);

    tuple.set(0, true);
    tuple.set(1, 1);
    tuple.set(2, 1001);
    tuple.set(3, 1.1f);
    tuple.set(4, 2.2f);
    tuple.set(5, "hello world 1");
    tuple.set(6, new DataByteArray("hello byte 1"));

    try {
      TypesUtils.checkCompatible(tuple, schema);
    } catch (Exception e) {
      Assert.fail("Should not see this: " + e.getMessage());
    }
  }
  
  // null is ok
  @Test
  public void testPositive5() throws ParseException, IOException {
    String STR_SCHEMA = "s1:bool, s2:int, s3:long, s4:float, s5:double, s6:string, s7:bytes";
    Schema schema = new Schema(STR_SCHEMA);

    Tuple tuple = TypesUtils.createTuple(schema);
    TypesUtils.resetTuple(tuple);

    tuple.set(0, null);
    tuple.set(1, 1);
    tuple.set(2, null);
    tuple.set(3, 1.1f);
    tuple.set(4, null);
    tuple.set(5, "hello world 1");
    tuple.set(6, new DataByteArray("hello byte 1"));

    try {
      TypesUtils.checkCompatible(tuple, schema);
    } catch (Exception e) {
      Assert.fail("Should not see this: " + e.getMessage());
    }
  }
  
  @Test
  public void testPositive6() throws ParseException, IOException {
    String STR_SCHEMA = "f1:bool, r:record(f11:int, f12:long), m:map(string), c:collection(record(f13:double, f14:float, m:map(string)))";
    Schema schema = new Schema(STR_SCHEMA);

    Tuple tuple = TypesUtils.createTuple(schema);    
    TypesUtils.resetTuple(tuple);

    tuple.set(0, true);

    Tuple tupRecord;
    try {
      tupRecord = TypesUtils.createTuple(schema.getColumnSchema("r").getSchema());
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
    tupColl1.set(0, 3.1415926);
    tupColl1.set(1, 1.6f);
    tupColl1.set(2, map);
    bagColl.add(tupColl1);
    tupColl2.set(0, 123.456789);
    tupColl2.set(1, 100f);
    tupColl2.set(2, map);
    bagColl.add(tupColl2);
    tuple.set(3, bagColl);
    
    try {
      TypesUtils.checkCompatible(tuple, schema);
    } catch (Exception e) {
      Assert.fail("Should not see this: " + e.getMessage());
    }
  }

  
  // sees an long for an integer column; 
  @Test (expected = IOException.class)
  public void testNegative1() throws ParseException, IOException {
    String STR_SCHEMA = "s1:bool, s2:int, s3:long, s4:float, s5:double, s6:string, s7:bytes";
    Schema schema = new Schema(STR_SCHEMA);

    Tuple tuple = TypesUtils.createTuple(schema);
    TypesUtils.resetTuple(tuple);

    tuple.set(0, true);
    tuple.set(1, 1L);
    tuple.set(2, 1001);
    tuple.set(3, 1.1f);
    tuple.set(4, 2.2f);
    tuple.set(5, "hello world 1");
    tuple.set(6, new DataByteArray("hello byte 1"));

    TypesUtils.checkCompatible(tuple, schema);
  }
  
  // sees an float for an integer column; 
  @Test (expected = IOException.class)
  public void testNegative2() throws ParseException, IOException {
    String STR_SCHEMA = "s1:bool, s2:int, s3:long, s4:float, s5:double, s6:string, s7:bytes";
    Schema schema = new Schema(STR_SCHEMA);

    Tuple tuple = TypesUtils.createTuple(schema);
    TypesUtils.resetTuple(tuple);

    tuple.set(0, true);
    tuple.set(1, 1.1f);
    tuple.set(2, 1001);
    tuple.set(3, 1.1f);
    tuple.set(4, 2.2f);
    tuple.set(5, "hello world 1");
    tuple.set(6, new DataByteArray("hello byte 1"));

    TypesUtils.checkCompatible(tuple, schema);
  }
  
  // sees a double for an integer column; 
  @Test (expected = IOException.class)
  public void testNegative3() throws ParseException, IOException {
    String STR_SCHEMA = "s1:bool, s2:int, s3:long, s4:float, s5:double, s6:string, s7:bytes";
    Schema schema = new Schema(STR_SCHEMA);

    Tuple tuple = TypesUtils.createTuple(schema);
    TypesUtils.resetTuple(tuple);

    tuple.set(0, true);
    tuple.set(1, 1.1);
    tuple.set(2, 1001);
    tuple.set(3, 1.1f);
    tuple.set(4, 2.2f);
    tuple.set(5, "hello world 1");
    tuple.set(6, new DataByteArray("hello byte 1"));

    TypesUtils.checkCompatible(tuple, schema);
  }

  // sees an float for an long column; 
  @Test (expected = IOException.class)
  public void testNegative4() throws ParseException, IOException {
    String STR_SCHEMA = "s1:bool, s2:int, s3:long, s4:float, s5:double, s6:string, s7:bytes";
    Schema schema = new Schema(STR_SCHEMA);

    Tuple tuple = TypesUtils.createTuple(schema);
    TypesUtils.resetTuple(tuple);

    tuple.set(0, true);
    tuple.set(1, 1);
    tuple.set(2, 1.1f);
    tuple.set(3, 1.1f);
    tuple.set(4, 2.2f);
    tuple.set(5, "hello world 1");
    tuple.set(6, new DataByteArray("hello byte 1"));

    TypesUtils.checkCompatible(tuple, schema);
  }

  // sees a double for an long column; 
  @Test (expected = IOException.class)
  public void testNegative5() throws ParseException, IOException {
    String STR_SCHEMA = "s1:bool, s2:int, s3:long, s4:float, s5:double, s6:string, s7:bytes";
    Schema schema = new Schema(STR_SCHEMA);

    Tuple tuple = TypesUtils.createTuple(schema);
    TypesUtils.resetTuple(tuple);

    tuple.set(0, true);
    tuple.set(1, 1);
    tuple.set(2, 1.1);
    tuple.set(3, 1.1f);
    tuple.set(4, 2.2f);
    tuple.set(5, "hello world 1");
    tuple.set(6, new DataByteArray("hello byte 1"));

    TypesUtils.checkCompatible(tuple, schema);
  }
  
  // sees a double for an float column; 
  @Test (expected = IOException.class)
  public void testNegative6() throws ParseException, IOException {
    String STR_SCHEMA = "s1:bool, s2:int, s3:long, s4:float, s5:double, s6:string, s7:bytes";
    Schema schema = new Schema(STR_SCHEMA);

    Tuple tuple = TypesUtils.createTuple(schema);
    TypesUtils.resetTuple(tuple);

    tuple.set(0, true);
    tuple.set(1, 1);
    tuple.set(2, 1);
    tuple.set(3, 1.1);
    tuple.set(4, 2.2);
    tuple.set(5, "hello world 1");
    tuple.set(6, new DataByteArray("hello byte 1"));

    TypesUtils.checkCompatible(tuple, schema);
  }

  // sees an float for a boolean column;
  @Test (expected = IOException.class)
  public void testNegative7() throws ParseException, IOException {
    String STR_SCHEMA = "s1:bool, s2:int, s3:long, s4:float, s5:double, s6:string, s7:bytes";
    Schema schema = new Schema(STR_SCHEMA);

    Tuple tuple = TypesUtils.createTuple(schema);
    TypesUtils.resetTuple(tuple);

    tuple.set(0, 1.1f);
    tuple.set(1, 1);
    tuple.set(2, 1.1f);
    tuple.set(3, 1.1f);
    tuple.set(4, 2.2f);
    tuple.set(5, "hello world 1");
    tuple.set(6, new DataByteArray("hello byte 1"));

    TypesUtils.checkCompatible(tuple, schema);
  }
  
  // no. of columns is wrong;
  @Test (expected = IOException.class)
  public void testNegative8() throws ParseException, IOException {
    String STR_SCHEMA = "s1:bool, s2:int, s3:long, s4:float, s5:double, s6:string, s7:bytes";
    String STR_SCHEMA1 = "s1:bool, s2:int, s3:long, s4:float, s5:double, s6:string";
    Schema schema = new Schema(STR_SCHEMA);
    Schema schema1 = new Schema(STR_SCHEMA1);

    Tuple tuple = TypesUtils.createTuple(schema1);
    TypesUtils.resetTuple(tuple);

    tuple.set(0, true);
    tuple.set(1, 1);
    tuple.set(2, 1L);
    tuple.set(3, 1.1f);
    tuple.set(4, 2.2f);
    tuple.set(5, "hello world 1");

    TypesUtils.checkCompatible(tuple, schema);
  }

  @AfterClass
  public static void tearDownOnce() throws IOException {
    
  }
}

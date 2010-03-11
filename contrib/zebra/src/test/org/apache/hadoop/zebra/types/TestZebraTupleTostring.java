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
import org.apache.hadoop.zebra.parser.ParseException;
import org.apache.hadoop.zebra.schema.Schema;
import org.apache.hadoop.zebra.types.TypesUtils;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.junit.Test;

/**
 * 
 * Test toString() methold of ZebraTuple.
 * It should generate string representations of Zebra tuples of CSV format.
 * 
 */
public class TestZebraTupleTostring {
  @Test
  public void testSimple() throws IOException, ParseException {
    String STR_SCHEMA = "s1:bool, s2:int, s3:long, s4:float, s5:string, s6:bytes";
    Schema schema = new Schema(STR_SCHEMA);
    Tuple tuple = TypesUtils.createTuple(schema);
    Assert.assertTrue(tuple instanceof ZebraTuple);
    TypesUtils.resetTuple(tuple);

    tuple.set(0, true); // bool
    tuple.set(1, 1); // int
    tuple.set(2, 1001L); // long
    tuple.set(3, 1.1); // float
    tuple.set(4, "hello\r world, 1\n"); // string
    tuple.set(5, new DataByteArray("hello byte, 1")); // bytes

    String str = tuple.toString();
    Assert.assertTrue(str.equals("T,1,1001,1.1,'hello%0D world%2C 1%0A,#hello byte, 1"));
  }
  
  @Test
  public void testRecord() throws IOException, ParseException {
    String STR_SCHEMA = "r1:record(f1:int, f2:long), r2:record(r3:record(f3:float, f4))";
    Schema schema = new Schema(STR_SCHEMA);
    Tuple tuple = TypesUtils.createTuple(schema);
    TypesUtils.resetTuple(tuple);

    Tuple tupRecord1;
    Schema r1Schema = new Schema("f1:int, f2:long");
    tupRecord1 = TypesUtils.createTuple(r1Schema);
    TypesUtils.resetTuple(tupRecord1);
    
    Tuple tupRecord2;
    Schema r2Schema = new Schema("r3:record(f3:float, f4)");
    tupRecord2 = TypesUtils.createTuple(r2Schema);
    TypesUtils.resetTuple(tupRecord2);
    
    Tuple tupRecord3;
    Schema r3Schema = new Schema("f3:float, f4");
    tupRecord3 = TypesUtils.createTuple(r3Schema);
    TypesUtils.resetTuple(tupRecord3);
    
    // r1:record(f1:int, f2:long)
    tupRecord1.set(0, 1);
    tupRecord1.set(1, 1001L);
    Assert.assertTrue(tupRecord1.toString().equals("1,1001"));
    tuple.set(0, tupRecord1);

    // r2:record(r3:record(f3:float, f4))
    tupRecord2.set(0, tupRecord3);
    tupRecord3.set(0, 1.3);
    tupRecord3.set(1, new DataByteArray("r3 row1 byte array"));
    Assert.assertTrue(tupRecord3.toString().equals("1.3,#r3 row1 byte array"));
    Assert.assertTrue(tupRecord2.toString().equals("1.3,#r3 row1 byte array"));
    tuple.set(1, tupRecord2);
    Assert.assertTrue(tuple.toString().equals("1,1001,1.3,#r3 row1 byte array"));
  }

  @Test
  public void testMap() throws IOException, ParseException {
    String STR_SCHEMA = "m1:map(string),m2:map(map(int))";
    Schema schema = new Schema(STR_SCHEMA);
    Tuple tuple = TypesUtils.createTuple(schema);
    TypesUtils.resetTuple(tuple);

    // m1:map(string)
    Map<String, String> m1 = new HashMap<String, String>();
    m1.put("a", "'A");
    m1.put("b", "B,B");
    m1.put("c", "C\n");
    tuple.set(0, m1);

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
    tuple.set(1, m2);

    Assert.assertTrue(tuple.toString().equals(
        "m{'b,'B%2CB,'c,'C%0A,'a,''A},m{'y,m{'m421,421,'m411,411,'m431,431},'x,m{'m311,311,'m321,321,'m331,331}}"));
  }
  
  @Test
  public void testCollection() throws IOException, ParseException {
    String STR_SCHEMA = 
      "c1:collection(record(a:double, b:float, c:bytes)),c2:collection(record(r1:record(f1:int, f2:string), d:string)),c3:collection(record(c3_1:collection(record(e:int,f:bool))))";
    Schema schema = new Schema(STR_SCHEMA);
    Tuple tuple = TypesUtils.createTuple(schema);
    TypesUtils.resetTuple(tuple);
    
    DataBag bag1 = TypesUtils.createBag();
    Schema schColl = new Schema("a:double, b:float, c:bytes");
    Tuple tupColl1 = TypesUtils.createTuple(schColl);
    Tuple tupColl2 = TypesUtils.createTuple(schColl);

    DataBag bag2 = TypesUtils.createBag();
    Schema schColl2 = new Schema("r1:record(f1:int, f2:string), d:string");
    Tuple tupColl2_1 = TypesUtils.createTuple(schColl2);
    Tuple tupColl2_2 = TypesUtils.createTuple(schColl2);
    
    Tuple collRecord1;
    try {
      collRecord1 = TypesUtils.createTuple(new Schema("f1:int, f2:string"));
    } catch (ParseException e) {
      e.printStackTrace();
      throw new IOException(e);
    }
    Tuple collRecord2;
    try {
      collRecord2 = TypesUtils.createTuple(new Schema("f1:int, f2:string"));
    } catch (ParseException e) {
      e.printStackTrace();
      throw new IOException(e);
    }

    // c3:collection(c3_1:collection(e:int,f:bool))
    DataBag bag3 = TypesUtils.createBag();
    DataBag bag3_1 = TypesUtils.createBag();
    DataBag bag3_2 = TypesUtils.createBag();

    Tuple tupColl3_1 = null;
    try {
      tupColl3_1 = TypesUtils.createTuple(new Schema("e:int,f:bool"));
    } catch (ParseException e) {
      e.printStackTrace();
      throw new IOException(e);
    }
    Tuple tupColl3_2;
    try {
      tupColl3_2 = TypesUtils.createTuple(new Schema("e:int,f:bool"));
    } catch (ParseException e) {
      e.printStackTrace();
      throw new IOException(e);
    }

    Tuple tupColl3_3 = null;
    try {
      tupColl3_3 = TypesUtils.createTuple(new Schema("e:int,f:bool"));
    } catch (ParseException e) {
      e.printStackTrace();
      throw new IOException(e);
    }
    Tuple tupColl3_4;
    try {
      tupColl3_4 = TypesUtils.createTuple(new Schema("e:int,f:bool"));
    } catch (ParseException e) {
      e.printStackTrace();
      throw new IOException(e);
    }

    byte[] abs1 = new byte[3];
    byte[] abs2 = new byte[4];
    tupColl1.set(0, 3.1415926);
    tupColl1.set(1, 1.6);
    abs1[0] = 'a';
    abs1[1] = 'a';
    abs1[2] = 'a';
    tupColl1.set(2, new DataByteArray(abs1));
    bag1.add(tupColl1);
    tupColl2.set(0, 123.456789);
    tupColl2.set(1, 100);
    abs2[0] = 'b';
    abs2[1] = 'c';
    abs2[2] = 'd';
    abs2[3] = 'e';
    tupColl2.set(2, new DataByteArray(abs2));
    bag1.add(tupColl2);
    tuple.set(0, bag1);

    collRecord1.set(0, 1);
    collRecord1.set(1, "record1_string1");
    tupColl2_1.set(0, collRecord1);
    tupColl2_1.set(1, "hello1");
    bag2.add(tupColl2_1);

    collRecord2.set(0, 2);
    collRecord2.set(1, "record2_string1");
    tupColl2_2.set(0, collRecord2);
    tupColl2_2.set(1, "hello2");
    bag2.add(tupColl2_2);
    tuple.set(1, bag2);

    TypesUtils.resetTuple(tupColl3_1);
    TypesUtils.resetTuple(tupColl3_2);
    tupColl3_1.set(0, 1);
    tupColl3_1.set(1, true);
    tupColl3_2.set(0, 2);
    tupColl3_2.set(1, false);
    bag3_1.add(tupColl3_1);
    bag3_1.add(tupColl3_2);
    bag3.addAll(bag3_1);

    tupColl3_3.set(0, 3);
    tupColl3_3.set(1, true);
    tupColl3_4.set(0, 4);
    tupColl3_4.set(1, false);
    bag3_2.add(tupColl3_3);
    bag3_2.add(tupColl3_4);
    bag3.addAll(bag3_2);
    tuple.set(2, bag3);

    Assert.assertTrue(tuple.toString().equals("3.1415926,1.6,#aaa\n" +
    		"123.456789,100,#bcde\n" +
    		"1,'record1_string1,'hello1\n"+
    		"2,'record2_string1,'hello2\n"+
    		"1,T\n"+
    		"2,F\n"+
    		"3,T\n"+
    		"4,F\n"));
  }
}


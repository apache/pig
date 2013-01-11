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
import java.util.Iterator;
import java.util.List;

import junit.framework.Assert;

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
public class TestCollection {
  final static String STR_SCHEMA = "c:collection(record(a:double, b:double, c:bytes)),c2:collection(record(r1:record(f1:int, f2:string), d:string)),c3:collection(record(e:int,f:bool))";

  final static String STR_STORAGE = "[c]";
  private static Configuration conf;
  private static Path path;
  private static FileSystem fs;

  @BeforeClass
  public static void setUpOnce() throws IOException {
    conf = new Configuration();
    conf.setInt("table.output.tfile.minBlock.size", 64 * 1024);
    conf.setInt("table.input.split.minSize", 64 * 1024);
    conf.set("table.output.tfile.compression", "none");

    RawLocalFileSystem rawLFS = new RawLocalFileSystem();
    fs = new LocalFileSystem(rawLFS);
    path = new Path(fs.getWorkingDirectory(), "TestCollection");
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
    DataBag bag1 = TypesUtils.createBag();
    Schema schColl = schema.getColumn(0).getSchema().getColumn(0).getSchema();
    Tuple tupColl1 = TypesUtils.createTuple(schColl);
    Tuple tupColl2 = TypesUtils.createTuple(schColl);

    DataBag bag2 = TypesUtils.createBag();
    Schema schColl2 = schema.getColumn(1).getSchema().getColumn(0).getSchema();
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
    abs1[0] = 11;
    abs1[1] = 12;
    abs1[2] = 13;
    tupColl1.set(2, new DataByteArray(abs1));
    bag1.add(tupColl1);
    tupColl2.set(0, 123.456789);
    tupColl2.set(1, 100);
    abs2[0] = 21;
    abs2[1] = 22;
    abs2[2] = 23;
    abs2[3] = 24;
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

    int row = 0;
    inserter.insert(new BytesWritable(String.format("k%d%d", part + 1, row + 1).getBytes()), tuple);

    row++;

    bag1.clear();
    bag2.clear();
    bag3.clear();
    bag3_1.clear();
    bag3_2.clear();
    TypesUtils.resetTuple(tupColl1);
    TypesUtils.resetTuple(tupColl2);
    TypesUtils.resetTuple(tupColl2_1);
    TypesUtils.resetTuple(tupColl2_2);
    TypesUtils.resetTuple(collRecord1);
    TypesUtils.resetTuple(collRecord2);
    TypesUtils.resetTuple(tupColl3_1);
    TypesUtils.resetTuple(tupColl3_2);
    TypesUtils.resetTuple(tupColl3_3);
    TypesUtils.resetTuple(tupColl3_4);

    tupColl1.set(0, 7654.321);
    tupColl1.set(1, 0.0001);
    abs1[0] = 31;
    abs1[1] = 32;
    abs1[2] = 33;
    tupColl1.set(2, new DataByteArray(abs1));
    bag1.add(tupColl1);
    tupColl2.set(0, 0.123456789);
    tupColl2.set(1, 0.3333);
    abs2[0] = 41;
    abs2[1] = 42;
    abs2[2] = 43;
    abs2[3] = 44;
    tupColl2.set(2, new DataByteArray(abs2));
    bag1.add(tupColl2);
    tuple.set(0, bag1);

    collRecord1.set(0, 3);
    collRecord1.set(1, "record1_string2");
    tupColl2_1.set(0, collRecord1);
    tupColl2_1.set(1, "hello1_2");
    bag2.add(tupColl2_1);

    collRecord2.set(0, 4);
    collRecord2.set(1, "record2_string2");
    tupColl2_2.set(0, collRecord2);
    tupColl2_2.set(1, "hello2_2");
    bag2.add(tupColl2_2);
    tuple.set(1, bag2);

    tupColl3_1.set(0, 5);
    tupColl3_1.set(1, true);
    tupColl3_2.set(0, 6);
    tupColl3_2.set(1, false);
    bag3_1.add(tupColl3_1);
    bag3_1.add(tupColl3_2);
    bag3.addAll(bag3_1);

    tupColl3_3.set(0, 7);
    tupColl3_3.set(1, true);
    tupColl3_4.set(0, 8);
    tupColl3_4.set(1, false);
    bag3_2.add(tupColl3_3);
    bag3_2.add(tupColl3_4);
    bag3.addAll(bag3_2);
    tuple.set(2, bag3);

    inserter.insert(new BytesWritable(String.format("k%d%d", part + 1, row + 1)
        .getBytes()), tuple);

    inserter.close();
    writer1.finish();

    writer.close();
  }

  @AfterClass
  public static void tearDown() throws IOException {
    BasicTable.drop(path, conf);
  }

  // read one collection
  // final static String STR_SCHEMA =
  // "c:collection(a:double, b:float, c:bytes),c2:collection(r1:record(f1:int, f2:string), d:string),c3:collection(c3_1:collection(e:int,f:bool))";
  @Test
  public void testRead1() throws IOException, ParseException {
    String projection = new String("c");
    BasicTable.Reader reader = new BasicTable.Reader(path, conf);
    reader.setProjection(projection);
    List<RangeSplit> splits = reader.rangeSplit(1);
    TableScanner scanner = reader.getScanner(splits.get(0), true);
    BytesWritable key = new BytesWritable();
    Tuple RowValue = TypesUtils.createTuple(scanner.getSchema());

    scanner.getKey(key);
    // Assert.assertEquals(key, new BytesWritable("k11".getBytes()));
    scanner.getValue(RowValue);
    System.out.println("test read 1: row: " + RowValue.toString());
    // test read 1: row: ({(3.1415926,1.6, ),(123.456789,100,)})
    Iterator<Tuple> it = ((DataBag) RowValue.get(0)).iterator();
    int list = 0;
    while (it.hasNext()) {
      Tuple cur = it.next();
      System.out.println(cur.get(0));
      list++;
      if (list == 1) {
        Assert.assertEquals(3.1415926, cur.get(0));
        Assert.assertEquals(1.6, cur.get(1));
        System.out
            .println("byte 0: " + ((DataByteArray) cur.get(2)).toString());

      }
      if (list == 2) {
        Assert.assertEquals(123.456789, cur.get(0));
        Assert.assertEquals(100, cur.get(1));
        // Assert.assertEquals(3.1415926, cur.get(2));
      }
    }
    scanner.advance();
    scanner.getValue(RowValue);
    Iterator<Tuple> it2 = ((DataBag) RowValue.get(0)).iterator();
    int list2 = 0;
    while (it2.hasNext()) {
      Tuple cur = it2.next();
      System.out.println(cur.get(0));
      list2++;
      if (list2 == 1) {
        Assert.assertEquals(7654.321, cur.get(0));
        Assert.assertEquals(0.0001, cur.get(1));
        // Assert.assertEquals(3.1415926, cur.get(2));
      }
      if (list2 == 2) {
        Assert.assertEquals(0.123456789, cur.get(0));
        Assert.assertEquals(0.3333, cur.get(1));
        // Assert.assertEquals(3.1415926, cur.get(2));
      }
    }

    reader.close();
  }

  // read second collection
  // final static String STR_SCHEMA =
  // "c:collection(a:double, b:float, c:bytes),c2:collection(r1:record(f1:int, f2:string), d:string),c3:collection(c3_1:collection(e:int,f:bool))";
  @Test
  public void testRead2() throws IOException, ParseException {
    String projection = new String("c2");
    BasicTable.Reader reader = new BasicTable.Reader(path, conf);
    reader.setProjection(projection);
    List<RangeSplit> splits = reader.rangeSplit(1);
    TableScanner scanner = reader.getScanner(splits.get(0), true);
    BytesWritable key = new BytesWritable();
    Tuple RowValue = TypesUtils.createTuple(scanner.getSchema());

    scanner.getKey(key);
    // Assert.assertEquals(key, new BytesWritable("k11".getBytes()));
    scanner.getValue(RowValue);
    System.out.println("test read 2:row: " + RowValue.toString());
    // test read 2:row:
    // ({((1,record1_string1),hello1),((2,record2_string1),hello2)})
    Iterator<Tuple> it = ((DataBag) RowValue.get(0)).iterator();
    int list = 0;
    while (it.hasNext()) {
      Tuple cur = it.next();
      System.out.println(cur.get(0)); // (1,record1_string1)
      list++;
      if (list == 1) {
        Assert.assertEquals(1, ((Tuple) cur.get(0)).get(0));
        Assert.assertEquals("record1_string1", ((Tuple) cur.get(0)).get(1));
        Assert.assertEquals("hello1", cur.get(1));

      }
      if (list == 2) {
        Assert.assertEquals(2, ((Tuple) cur.get(0)).get(0));
        Assert.assertEquals("record2_string1", ((Tuple) cur.get(0)).get(1));
        Assert.assertEquals("hello2", cur.get(1));

      }
    }
    scanner.advance();
    scanner.getValue(RowValue);
    Iterator<Tuple> it2 = ((DataBag) RowValue.get(0)).iterator();
    int list2 = 0;
    while (it2.hasNext()) {
      Tuple cur = it2.next();
      System.out.println(cur.get(0));
      list2++;
      if (list2 == 1) {
        Assert.assertEquals(3, ((Tuple) cur.get(0)).get(0));
        Assert.assertEquals("record1_string2", ((Tuple) cur.get(0)).get(1));
        Assert.assertEquals("hello1_2", cur.get(1));
      }
      if (list2 == 2) {
        Assert.assertEquals(4, ((Tuple) cur.get(0)).get(0));
        Assert.assertEquals("record2_string2", ((Tuple) cur.get(0)).get(1));
        Assert.assertEquals("hello2_2", cur.get(1));
      }
    }

    reader.close();
  }

  // read 3rd column
  // final static String STR_SCHEMA =
  // "c:collection(a:double, b:float, c:bytes),c2:collection(r1:record(f1:int, f2:string), d:string),c3:collection(c3_1:collection(e:int,f:bool))";
  @Test
  public void testRead3() throws IOException, ParseException {
    String projection = new String("c3");
    BasicTable.Reader reader = new BasicTable.Reader(path, conf);
    reader.setProjection(projection);
    List<RangeSplit> splits = reader.rangeSplit(1);
    TableScanner scanner = reader.getScanner(splits.get(0), true);
    BytesWritable key = new BytesWritable();
    Tuple RowValue = TypesUtils.createTuple(scanner.getSchema());

    scanner.getKey(key);
    Assert.assertEquals(key, new BytesWritable("k11".getBytes()));
    scanner.getValue(RowValue);
    System.out.println("test record 3: row: " + RowValue.toString());
    // test record 3: row: ({(1,true),(2,false),(3,true),(4,false)})
    Iterator<Tuple> it = ((DataBag) RowValue.get(0)).iterator();
    int list = 0;
    while (it.hasNext()) {
      Tuple cur = it.next();
      System.out.println(cur.get(0)); // 3
      list++;
      if (list == 1) {
        Assert.assertEquals(1, cur.get(0));
        Assert.assertEquals(true, cur.get(1));
      }
      if (list == 2) {
        Assert.assertEquals(2, cur.get(0));
        Assert.assertEquals(false, cur.get(1));
      }
      if (list == 3) {
        Assert.assertEquals(3, cur.get(0));
        Assert.assertEquals(true, cur.get(1));
      }
      if (list == 4) {
        Assert.assertEquals(4, cur.get(0));
        Assert.assertEquals(false, cur.get(1));
      }
    }
    scanner.advance();
    scanner.getValue(RowValue);
    System.out.println("row: " + RowValue.toString());
    // row: ({(5,true),(6,false),(7,true),(8,false)})
    Iterator<Tuple> it2 = ((DataBag) RowValue.get(0)).iterator();
    scanner.getKey(key);
    Assert.assertEquals(key, new BytesWritable("k12".getBytes()));
    int list2 = 0;
    while (it2.hasNext()) {
      Tuple cur = it2.next();
      System.out.println(cur.get(0));
      list2++;
      if (list2 == 1) {
        Assert.assertEquals(5, cur.get(0));
        Assert.assertEquals(true, cur.get(1));
      }
      if (list2 == 2) {
        Assert.assertEquals(6, cur.get(0));
        Assert.assertEquals(false, cur.get(1));
      }
      if (list2 == 3) {
        Assert.assertEquals(7, cur.get(0));
        Assert.assertEquals(true, cur.get(1));
      }
      if (list2 == 4) {
        Assert.assertEquals(8, cur.get(0));
        Assert.assertEquals(false, cur.get(1));
      }
    }

    reader.close();
  }

  // Negative none exist column
  @Test
  public void xtestReadNeg1() throws IOException, ParseException {
    String projection = new String("d");
    BasicTable.Reader reader = new BasicTable.Reader(path, conf);
    reader.setProjection(projection);
    List<RangeSplit> splits = reader.rangeSplit(1);
    TableScanner scanner = reader.getScanner(splits.get(0), true);
    BytesWritable key = new BytesWritable();
    Tuple RowValue = TypesUtils.createTuple(scanner.getSchema());

    scanner.getKey(key);
    // Assert.assertEquals(key, new BytesWritable("k11".getBytes()));
    scanner.getValue(RowValue);
    System.out.println("row: " + RowValue.toString());
    System.out.println("row1: " + RowValue.get(0));
    Assert.assertEquals(null, RowValue.get(0));
    Assert.assertEquals(1, RowValue.size());
    reader.close();
  }

  // Negative test case. Projection on fields in collection is not supported.
  @Test
  public void testRead5() throws IOException {
	  try {
		  String projection = new String("c.a");
		  BasicTable.Reader reader = new BasicTable.Reader(path, conf);
		  reader.setProjection(projection);
	  } catch(ParseException ex) {
		  System.out.println( "caught expected exception: " + ex );
		  return;
	  }
	  
	  Assert.fail( "Test case failure: projection on collection field or record field is not supported." );
  }

  // read, should support project to 3rd level TODO: construct scanner failed
  // final static String STR_SCHEMA =
  // "c:collection(a:double, b:float, c:bytes),c2:collection(r1:record(f1:int, f2:string), d:string),c3:collection(c3_1:collection(e:int,f:bool))";

  public void xtestRead7() throws IOException, ParseException {
    String projection = new String("c2.r1.f1");
    BasicTable.Reader reader = new BasicTable.Reader(path, conf);
    reader.setProjection(projection);
    List<RangeSplit> splits = reader.rangeSplit(1);
    TableScanner scanner = reader.getScanner(splits.get(0), true);
    scanner = reader.getScanner(splits.get(0), true);

    BytesWritable key = new BytesWritable();
    Tuple RowValue = TypesUtils.createTuple(scanner.getSchema());

    scanner.getKey(key);
    // Assert.assertEquals(key, new BytesWritable("k11".getBytes()));
    scanner.getValue(RowValue);
    System.out.println("row: " + RowValue.toString());
    Iterator<Tuple> it = ((DataBag) RowValue.get(0)).iterator();
    int list = 0;
    while (it.hasNext()) {
      Tuple cur = it.next();
      System.out.println(cur.get(0));
      list++;
      if (list == 1) {
        Assert.assertEquals(1, ((Tuple) cur.get(0)).get(0));
        Assert.assertEquals("record1_string1", ((Tuple) cur.get(0)).get(1));
        try {
          cur.get(1);
          Assert.fail("Should throw index out of bounds exception");
        } catch (Exception e) {
          System.out.println(e);
        }

      }
      if (list == 2) {
        Assert.assertEquals(2, ((Tuple) cur.get(0)).get(0));
        Assert.assertEquals("record2_string1", ((Tuple) cur.get(0)).get(1));
        try {
          cur.get(1);
          Assert.fail("Should throw index out of bounds exception");
        } catch (Exception e) {
          System.out.println(e);
        }
      }
    }
    scanner.advance();
    scanner.getValue(RowValue);
    Iterator<Tuple> it2 = ((DataBag) RowValue.get(0)).iterator();
    int list2 = 0;
    while (it2.hasNext()) {
      Tuple cur = it2.next();
      System.out.println(cur.get(0));
      list2++;
      if (list2 == 1) {
        Assert.assertEquals(3, ((Tuple) cur.get(0)).get(0));
        Assert.assertEquals("record1_string2", ((Tuple) cur.get(0)).get(1));
        try {
          cur.get(1);
          Assert.fail("Should throw index out of bounds exception");
        } catch (Exception e) {
          System.out.println(e);
        }
      }
      if (list2 == 2) {
        Assert.assertEquals(4, ((Tuple) cur.get(0)).get(0));
        Assert.assertEquals("record2_string2", ((Tuple) cur.get(0)).get(1));
        try {
          cur.get(1);
          Assert.fail("Should throw index out of bounds exception");
        } catch (Exception e) {
          System.out.println(e);
        }
      }
    }

    reader.close();
  }

  // read, should support project to 3rd level TODO: construct scanner failed
  // final static String STR_SCHEMA =
  // "c:collection(a:double, b:float, c:bytes),c2:collection(r1:record(f1:int, f2:string), d:string),c3:collection(c3_1:collection(e:int,f:bool))";

  public void xtestRead8() throws IOException, ParseException {
    String projection = new String("c3.c3_1.e");
    BasicTable.Reader reader = new BasicTable.Reader(path, conf);
    reader.setProjection(projection);
    List<RangeSplit> splits = reader.rangeSplit(1);
    TableScanner scanner = null;
    scanner = reader.getScanner(splits.get(0), true);

    scanner = reader.getScanner(splits.get(0), true);

    BytesWritable key = new BytesWritable();
    Tuple RowValue = TypesUtils.createTuple(scanner.getSchema());

    scanner.getKey(key);
    // Assert.assertEquals(key, new BytesWritable("k11".getBytes()));
    scanner.getValue(RowValue);
    System.out.println("row: " + RowValue.toString());
    Iterator<Tuple> it = ((DataBag) RowValue.get(0)).iterator();
    int list = 0;
    while (it.hasNext()) {
      Tuple cur = it.next();
      System.out.println(cur.get(0));
      list++;
      if (list == 1) {
        Assert.assertEquals(1, ((Tuple) cur.get(0)).get(0));
        Assert.assertEquals("record1_string1", ((Tuple) cur.get(0)).get(1));
        try {
          cur.get(1);
          Assert.fail("Should throw index out of bounds exception");
        } catch (Exception e) {
          System.out.println(e);
        }

      }
      if (list == 2) {
        Assert.assertEquals(2, ((Tuple) cur.get(0)).get(0));
        Assert.assertEquals("record2_string1", ((Tuple) cur.get(0)).get(1));
        try {
          cur.get(1);
          Assert.fail("Should throw index out of bounds exception");
        } catch (Exception e) {
          System.out.println(e);
        }
      }
    }
    scanner.advance();
    scanner.getValue(RowValue);
    Iterator<Tuple> it2 = ((DataBag) RowValue.get(0)).iterator();
    int list2 = 0;
    while (it2.hasNext()) {
      Tuple cur = it2.next();
      System.out.println(cur.get(0));
      list2++;
      if (list2 == 1) {
        Assert.assertEquals(3, ((Tuple) cur.get(0)).get(0));
        Assert.assertEquals("record1_string2", ((Tuple) cur.get(0)).get(1));
        try {
          cur.get(1);
          Assert.fail("Should throw index out of bounds exception");
        } catch (Exception e) {
          System.out.println(e);
        }
      }
      if (list2 == 2) {
        Assert.assertEquals(4, ((Tuple) cur.get(0)).get(0));
        Assert.assertEquals("record2_string2", ((Tuple) cur.get(0)).get(1));
        try {
          cur.get(1);
          Assert.fail("Should throw index out of bounds exception");
        } catch (Exception e) {
          System.out.println(e);
        }
      }
    }

    reader.close();
  }

  // Negative should not support 2nd level collection split
  @Test
  public void testSplit1() throws IOException, ParseException {
    String STR_SCHEMA = "c:collection(a:double, b:double, c:bytes),c2:collection(r1:record(f1:int, f2:string), d:string),c3:collection(c3_1:collection(e:int,f:bool))";
    String STR_STORAGE = "[c.a]";
    conf = new Configuration();
    conf.setInt("table.output.tfile.minBlock.size", 64 * 1024);
    conf.setInt("table.input.split.minSize", 64 * 1024);
    conf.set("table.output.tfile.compression", "none");

    RawLocalFileSystem rawLFS = new RawLocalFileSystem();
    fs = new LocalFileSystem(rawLFS);
    path = new Path(fs.getWorkingDirectory(), this.getClass().getSimpleName());
    fs = path.getFileSystem(conf);
    // drop any previous tables
    BasicTable.drop(path, conf);
    try {
      BasicTable.Writer writer = new BasicTable.Writer(path, STR_SCHEMA,
          STR_STORAGE, conf);
      Assert.fail("should throw exception");
    } catch (Exception e) {
      System.out.println(e);
    }
  }

  // Negative should not support none_existent column split
  @Test
  public void testSplit2() throws IOException, ParseException {
    String STR_SCHEMA = "c:collection(a:double, b:double, c:bytes),c2:collection(r1:record(f1:int, f2:string), d:string),c3:collection(c3_1:collection(e:int,f:bool))";
    String STR_STORAGE = "[d]";
    conf = new Configuration();
    conf.setInt("table.output.tfile.minBlock.size", 64 * 1024);
    conf.setInt("table.input.split.minSize", 64 * 1024);
    conf.set("table.output.tfile.compression", "none");

    RawLocalFileSystem rawLFS = new RawLocalFileSystem();
    fs = new LocalFileSystem(rawLFS);
    path = new Path(fs.getWorkingDirectory(), this.getClass().getSimpleName());
    fs = path.getFileSystem(conf);
    // drop any previous tables
    BasicTable.drop(path, conf);
    try {
      BasicTable.Writer writer = new BasicTable.Writer(path, STR_SCHEMA,
          STR_STORAGE, conf);
      Assert.fail("should throw exception");
    } catch (Exception e) {
      System.out.println(e);
    }
  }
}

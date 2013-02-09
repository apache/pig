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
public class TestRecord {
  private static Configuration conf;
  private static Path path;
  private static FileSystem fs;
  static int count;
  final static String STR_SCHEMA = "r1:record(f1:int, f2:long), r2:record(f5:string, r3:record(f3:double, f4))";
  final static String STR_STORAGE = "[r1.f1]; [r2.r3.f4]; [r1.f2, r2.r3.f3]";

  @BeforeClass
  public static void setUpOnce() throws IOException {
    conf = new Configuration();
    conf.setInt("table.output.tfile.minBlock.size", 64 * 1024);
    conf.setInt("table.input.split.minSize", 64 * 1024);
    conf.set("table.output.tfile.compression", "none");

    RawLocalFileSystem rawLFS = new RawLocalFileSystem();
    fs = new LocalFileSystem(rawLFS);
    path = new Path(fs.getWorkingDirectory(), "TestRecord");
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
      tupRecord3 = TypesUtils.createTuple(new Schema("f3:float, f4"));
    } catch (ParseException e) {
      e.printStackTrace();
      throw new IOException(e);
    }

    // insert data in row 1
    int row = 0;
    // r1:record(f1:int, f2:long
    tupRecord1.set(0, 1);
    tupRecord1.set(1, 1001L);
    tuple.set(0, tupRecord1);

    // r2:record(r3:record(f3:float, f4))
    tupRecord2.set(0, "f5 row1 byte array");

    tupRecord3.set(0, 1.3);
    tupRecord3.set(1, new DataByteArray("r3 row1 byte array"));
    tupRecord2.set(1, tupRecord3);
    tuple.set(1, tupRecord2);
    inserter.insert(new BytesWritable(String.format("k%d%d", part + 1, row + 1)
        .getBytes()), tuple);

    // row 2
    row++;
    TypesUtils.resetTuple(tuple);
    TypesUtils.resetTuple(tupRecord1);
    TypesUtils.resetTuple(tupRecord2);
    TypesUtils.resetTuple(tupRecord3);
    // r1:record(f1:int, f2:long
    tupRecord1.set(0, 2);
    tupRecord1.set(1, 1002L);
    tuple.set(0, tupRecord1);

    // r2:record(r3:record(f3:float, f4))
    tupRecord2.set(0, "f5 row2 byte array");
    tupRecord3.set(0, 2.3);
    tupRecord3.set(1, new DataByteArray("r3 row2 byte array"));
    tupRecord2.set(1, tupRecord3);
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
  // Test read , simple projection 0, not record of record level.
  public void testRead1() throws IOException, ParseException {
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
    System.out.println("read 1:" + RowValue.toString());
    // read 1:(1,1001L)
    Assert.assertEquals(1, RowValue.get(0));

    scanner.advance();
    scanner.getKey(key);
    Assert.assertEquals(key, new BytesWritable("k12".getBytes()));
    scanner.getValue(RowValue);
    Assert.assertEquals(2, RowValue.get(0));
    reader.close();

  }

  /*
   * String STR_SCHEMA =
   * "r1:record(f1:int, f2:long), r2:record(f5, r3:record(f3:float, f4))";
   * String STR_STORAGE = "[r1.f1]; [r2.r3.f4]; [r1.f2, r2.r3.f3]";
   */
  @Test
  // Test read , record of record level,
  public void testRead2() throws IOException, ParseException {
    String projection1 = new String("r2.r3.f4, r2.r3.f3");
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
    System.out.println("read 2:" + RowValue.toString());
    // read 2:(r3 row1 byte array,1.3)
    Assert.assertEquals("r3 row1 byte array", RowValue.get(0).toString());
    Assert.assertEquals(1.3, RowValue.get(1));

    scanner.advance();
    scanner.getKey(key);
    Assert.assertEquals(key, new BytesWritable("k12".getBytes()));
    scanner.getValue(RowValue);
    Assert.assertEquals("r3 row2 byte array", RowValue.get(0).toString());
    Assert.assertEquals(2.3, RowValue.get(1));
    reader.close();
  }

  /*
   * String STR_SCHEMA =
   * "r1:record(f1:int, f2:long), r2:record(f5, r3:record(f3:float, f4))";
   * String STR_STORAGE = "[r1.f1]; [r2.r3.f4]; [r1.f2, r2.r3.f3]";
   */
  @Test
  // test stitch, r1.f1, r2.r3.f3
  public void testRead3() throws IOException, ParseException {
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
    System.out.println("read 3:" + RowValue.toString());
    // read 3:(1,1.3)
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

  @Test
  public void testRead4() throws IOException, ParseException {
    String projection3 = new String("r1, r2");
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
    System.out.println("read 4:" + RowValue.toString());
    // read 4:((1,1001L),(f5 row1 byte array,(1.3,r3 row1 byte array)))

    Assert.assertEquals(1, ((Tuple) RowValue.get(0)).get(0));
    Assert.assertEquals(1001L, ((Tuple) RowValue.get(0)).get(1));
    Assert.assertEquals("f5 row1 byte array", ((Tuple) RowValue.get(1)).get(0)
        .toString());

    Assert.assertEquals(1.3, ((Tuple) ((Tuple) RowValue.get(1)).get(1)).get(0));
    Assert.assertEquals("r3 row1 byte array",
        ((Tuple) ((Tuple) RowValue.get(1)).get(1)).get(1).toString());

    scanner.advance();

    scanner.getKey(key);
    Assert.assertEquals(key, new BytesWritable("k12".getBytes()));
    scanner.getValue(RowValue);
    Assert.assertEquals(2, ((Tuple) RowValue.get(0)).get(0));
    Assert.assertEquals(1002L, ((Tuple) RowValue.get(0)).get(1));
    Assert.assertEquals("f5 row2 byte array", ((Tuple) RowValue.get(1)).get(0)
        .toString());

    Assert.assertEquals(2.3, ((Tuple) ((Tuple) RowValue.get(1)).get(1)).get(0));
    Assert.assertEquals("r3 row2 byte array",
        ((Tuple) ((Tuple) RowValue.get(1)).get(1)).get(1).toString());

    reader.close();
  }

  @Test
  // test stitch, r2.f5.f3
  public void testRead5() throws IOException, ParseException {
    String projection3 = new String("r2.r3.f3");
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
    System.out.println("read 5:" + RowValue.toString());
    // read 5:(1.3)
    Assert.assertEquals(1.3, RowValue.get(0));

    scanner.advance();

    scanner.getKey(key);
    Assert.assertEquals(key, new BytesWritable("k12".getBytes()));
    scanner.getValue(RowValue);
    Assert.assertEquals(2.3, RowValue.get(0));

    reader.close();
  }

  /*
   * String STR_SCHEMA =
   * "r1:record(f1:int, f2:long), r2:record(f5, r3:record(f3:float, f4))";
   * String STR_STORAGE = "[r1.f1]; [r2.r3.f4]; [r1.f2, r2.r3.f3]";
   */
  @Test
  // test stitch,(r1.f2, r1.f1)
  public void testRead6() throws IOException, ParseException {
    String projection3 = new String("r1.f2,r1.f1");
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
    System.out.println("read 6:" + RowValue.toString());
    // read 6:(1001L,1)

    Assert.assertEquals(1001L, RowValue.get(0));
    Assert.assertEquals(1, RowValue.get(1));
    try {
      RowValue.get(2);
      Assert.fail("Should throw index out of bound exception ");
    } catch (Exception e) {
      e.printStackTrace();
    }

    scanner.advance();

    scanner.getKey(key);
    Assert.assertEquals(key, new BytesWritable("k12".getBytes()));
    scanner.getValue(RowValue);
    Assert.assertEquals(1002L, RowValue.get(0));
    Assert.assertEquals(2, RowValue.get(1));
    try {
      RowValue.get(2);
      Assert.fail("Should throw index out of bound exception ");
    } catch (Exception e) {
      e.printStackTrace();
    }
    reader.close();
  }

  @Test
  // test projection, negative, none-exist column name.
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

  @Test
  // test projection, negative, none-exist column name.
  public void testReadNegative2() throws IOException, ParseException {
    String projection4 = new String("NO");
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

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
public class TestSchema {
  // final static String STR_SCHEMA =
  // "s1:bool, s2:int, s3:long, s4:float, s5:string, s6:bytes, r1:record(f1:int, f2:long),  m1:map(string), c:collection(f13:double, f14:float, f15:bytes)";
  // final static String STR_STORAGE =
  // "[s1, s2]; [r1.f1]; [s3, s4]; [s5, s6]; [r1.f2]; [c.f13]";
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
    path = new Path(fs.getWorkingDirectory(), "TestSchema");
    fs = path.getFileSystem(conf);
    // drop any previous tables
    BasicTable.drop(path, conf);
  }

  /**
   * Return the name of the routine that called getCurrentMethodName
   * 
   */
  public String getCurrentMethodName() {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintWriter pw = new PrintWriter(baos);
    (new Throwable()).printStackTrace(pw);
    pw.flush();
    String stackTrace = baos.toString();
    pw.close();

    StringTokenizer tok = new StringTokenizer(stackTrace, "\n");
    tok.nextToken(); // 'java.lang.Throwable'
    tok.nextToken(); // 'at ...getCurrentMethodName'
    String l = tok.nextToken(); // 'at ...<caller to getCurrentRoutine>'
    // Parse line 3
    tok = new StringTokenizer(l.trim(), " <(");
    String t = tok.nextToken(); // 'at'
    t = tok.nextToken(); // '...<caller to getCurrentRoutine>'
    return t;
  }

  @AfterClass
  public static void tearDownOnce() throws IOException {
    BasicTable.drop(path, conf);
  }

  @Test
  public void testSimple() throws IOException, ParseException {
    String STR_SCHEMA = "s1:bool, s2:int, s3:long, s4:double, s5:string, s6:bytes";
    String STR_STORAGE = "[s1, s2]; [s3, s4]; [s5, s6]";

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

    // insert data in row 1
    int row = 0;
    tuple.set(0, true); // bool
    tuple.set(1, 1); // int
    tuple.set(2, 1001L); // long
    tuple.set(3, 1.1); // float
    tuple.set(4, "hello world 1"); // string
    tuple.set(5, new DataByteArray("hello byte 1")); // byte
    inserter.insert(new BytesWritable(String.format("k%d%d", part + 1, row + 1)
        .getBytes()), tuple);

    // insert data in row 2
    row++;
    tuple.set(0, false);
    tuple.set(1, 2); // int
    tuple.set(2, 1002L); // long
    tuple.set(3, 3.1); // float
    tuple.set(4, "hello world 2"); // string
    tuple.set(5, new DataByteArray("hello byte 2")); // byte
    inserter.insert(new BytesWritable(String.format("k%d%d", part + 1, row + 1)
        .getBytes()), tuple);

    // finish building table, closing out the inserter, writer, writer1
    inserter.close();
    writer1.finish();
    writer.close();

    // Starting read
    String projection = new String("s6,s5,s4,s3,s2,s1");
    BasicTable.Reader reader = new BasicTable.Reader(path, conf);
    reader.setProjection(projection);
    List<RangeSplit> splits = reader.rangeSplit(1);
    TableScanner scanner = reader.getScanner(splits.get(0), true);
    BytesWritable key = new BytesWritable();
    Tuple RowValue = TypesUtils.createTuple(scanner.getSchema());

    scanner.getKey(key);
    Assert.assertEquals(key, new BytesWritable("k11".getBytes()));
    scanner.getValue(RowValue);
    Assert.assertEquals(true, RowValue.get(5));
    Assert.assertEquals(1, RowValue.get(4));
    Assert.assertEquals(1.1, RowValue.get(2));
    Assert.assertEquals("hello world 1", RowValue.get(1));

    scanner.advance();
    scanner.getKey(key);
    Assert.assertEquals(key, new BytesWritable("k12".getBytes()));
    scanner.getValue(RowValue);
    Assert.assertEquals(false, RowValue.get(5));
    Assert.assertEquals(2, RowValue.get(4));
    Assert.assertEquals(3.1, RowValue.get(2));
    Assert.assertEquals("hello world 2", RowValue.get(1));

    // test stitch, I can re_use reader, split, but NOT scanner
    String projection2 = new String("s5, s1");
    reader.setProjection(projection2);
    scanner = reader.getScanner(splits.get(0), true);
    RowValue = TypesUtils.createTuple(scanner.getSchema());
    scanner.getKey(key);
    Assert.assertEquals(key, new BytesWritable("k11".getBytes()));
    scanner.getValue(RowValue);
    Assert.assertEquals("hello world 1", RowValue.get(0));
    Assert.assertEquals(true, RowValue.get(1));

    scanner.advance();

    scanner.getKey(key);
    Assert.assertEquals(key, new BytesWritable("k12".getBytes()));
    scanner.getValue(RowValue);
    Assert.assertEquals("hello world 2", RowValue.get(0));
    Assert.assertEquals(false, RowValue.get(1));

    reader.close();
  }

  @Test
  public void testRecord() throws IOException, ParseException {
    BasicTable.drop(path, conf);
    String STR_SCHEMA = "r1:record(f1:int, f2:long), r2:record(r3:record(f3:double, f4))";
    String STR_STORAGE = "[r1.f1]; [r2.r3.f4]; [r1.f2, r2.r3.f3]";

    path = new Path(getCurrentMethodName());
    // in case BasicTable exists
    BasicTable.drop(path, conf);
    System.out.println("in testRecord, get path: " + path.toString());
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
    tupRecord2.set(0, tupRecord3);
    tupRecord3.set(0, 1.3);
    tupRecord3.set(1, new DataByteArray("r3 row1 byte array"));
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
    tupRecord2.set(0, tupRecord3);
    tupRecord3.set(0, 2.3);
    tupRecord3.set(1, new DataByteArray("r3 row2 byte array"));
    tuple.set(1, tupRecord2);

    inserter.insert(new BytesWritable(String.format("k%d%d", part + 1, row + 1)
        .getBytes()), tuple);

    // finish building table, closing out the inserter, writer, writer1
    inserter.close();
    writer1.finish();
    writer.close();

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

    scanner.advance();
    scanner.getKey(key);
    Assert.assertEquals(key, new BytesWritable("k12".getBytes()));
    scanner.getValue(RowValue);
    Assert.assertEquals(2, RowValue.get(0));

    /*
     * String STR_SCHEMA =
     * "r1:record(f1:int, f2:long), r2:record(r3:record(f3:float, f4))"; String
     * STR_STORAGE = "[r1.f1]; [r2.r3.f4]; [r1.f2, r2.r3.f3]";
     */
    // Starting read , simple projection 1 , record of record level, FIXED,
    // PASS on advance
    String projection1 = new String("r2.r3.f4, r2.r3.f3");
    reader.setProjection(projection1);
    scanner = reader.getScanner(splits.get(0), true);
    RowValue = TypesUtils.createTuple(scanner.getSchema());

    scanner.getKey(key);
    Assert.assertEquals(key, new BytesWritable("k11".getBytes()));
    scanner.getValue(RowValue);
    Assert.assertEquals("r3 row1 byte array", RowValue.get(0).toString());
    Assert.assertEquals(1.3, RowValue.get(1));

    scanner.advance();
    scanner.getKey(key);
    Assert.assertEquals(key, new BytesWritable("k12".getBytes()));
    scanner.getValue(RowValue);
    Assert.assertEquals("r3 row2 byte array", RowValue.get(0).toString());
    Assert.assertEquals(2.3, RowValue.get(1));

    /*
     * String STR_SCHEMA =
     * "r1:record(f1:int, f2:long), r2:record(r3:record(f3:float, f4))"; String
     * STR_STORAGE = "[r1.f1]; [r2.r3.f4]; [r1.f2, r2.r3.f3]";
     */
    // test stitch, [r1.f1]; [r2.r3.f4]; [r1.f2, r2.r3.f3]
    String projection2 = new String("r1.f1, r2.r3.f3");
    reader.setProjection(projection2);
    scanner = reader.getScanner(splits.get(0), true);
    RowValue = TypesUtils.createTuple(scanner.getSchema());
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

    /*
     * String STR_SCHEMA =
     * "r1:record(f1:int, f2:long), r2:record(r3:record(f3:float, f4))"; String
     * STR_STORAGE = "[r1.f1]; [r2.r3.f4]; [r1.f2, r2.r3.f3]";
     */
    // test stitch, [r1.f1]; [r2.r3.f4]; [r1.f2, r2.r3.f3]
    String projection3 = new String("r1, r2");
    reader.setProjection(projection3);
    scanner = reader.getScanner(splits.get(0), true);
    RowValue = TypesUtils.createTuple(scanner.getSchema());
    scanner.getKey(key);
    Assert.assertEquals(key, new BytesWritable("k11".getBytes()));
    scanner.getValue(RowValue);

    Assert.assertEquals(1, ((Tuple) RowValue.get(0)).get(0));
    Assert.assertEquals(1001L, ((Tuple) RowValue.get(0)).get(1));
    Assert.assertEquals("1.3,#r3 row1 byte array", RowValue.get(1).toString());
    Assert.assertEquals(1.3, ((Tuple) ((Tuple) RowValue.get(1)).get(0)).get(0));
    Assert.assertEquals("r3 row1 byte array",
        ((Tuple) ((Tuple) RowValue.get(1)).get(0)).get(1).toString());

    scanner.advance();

    scanner.getKey(key);
    Assert.assertEquals(key, new BytesWritable("k12".getBytes()));
    scanner.getValue(RowValue);
    Assert.assertEquals(2, ((Tuple) RowValue.get(0)).get(0));
    Assert.assertEquals(1002L, ((Tuple) RowValue.get(0)).get(1));
    Assert.assertEquals("2.3,#r3 row2 byte array", RowValue.get(1).toString());
    Assert.assertEquals(2.3, ((Tuple) ((Tuple) RowValue.get(1)).get(0)).get(0));
    Assert.assertEquals("r3 row2 byte array",
        ((Tuple) ((Tuple) RowValue.get(1)).get(0)).get(1).toString());

    // test projection, negative, none-exist column name. FAILED, It crashes
    // here
    String projection4 = new String("r3");
    reader.setProjection(projection4);
    scanner = reader.getScanner(splits.get(0), true);
    RowValue = TypesUtils.createTuple(scanner.getSchema());
    scanner.getKey(key);
    Assert.assertEquals(key, new BytesWritable("k11".getBytes()));
    // scanner.getValue(RowValue); //FAILED, It crashes here

    reader.close();

  }

  @Test
  public void testMap() throws IOException, ParseException {
    String STR_SCHEMA = "m1:map(string),m2:map(map(int))";
    String STR_STORAGE = "[m1#{a}];[m2#{x|y}]; [m1#{b}, m2#{z}]";

    // String STR_SCHEMA = "m1:map(string)";
    // String STR_STORAGE = "[m1#{a}]";
    // Build Table and column groups
    path = new Path(getCurrentMethodName());
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
    int row = 0;
    inserter.insert(new BytesWritable(String.format("k%d%d", part + 1, row + 1)
        .getBytes()), tuple);

    // row 2
    row++;
    TypesUtils.resetTuple(tuple);
    m1.clear();
    m2.clear();
    m3.clear();
    m4.clear();
    // m1:map(string)
    m1.put("a", "A2");
    m1.put("b2", "B2");
    m1.put("c2", "C2");
    tuple.set(0, m1);

    // m2:map(map(int))
    m3.put("m321", 321);
    m3.put("m322", 322);
    m3.put("m323", 323);
    m2.put("z", m3);
    tuple.set(1, m2);
    inserter.insert(new BytesWritable(String.format("k%d%d", part + 1, row + 1)
        .getBytes()), tuple);

    // finish building table, closing out the inserter, writer, writer1
    inserter.close();
    writer1.finish();
    writer.close();

    // Starting read Simple read one map PASS
    String projection = new String("m1#{a}");
    BasicTable.Reader reader = new BasicTable.Reader(path, conf);
    reader.setProjection(projection);
    List<RangeSplit> splits = reader.rangeSplit(1);
    reader.close();
    reader = new BasicTable.Reader(path, conf);
    reader.setProjection(projection);
    TableScanner scanner = reader.getScanner(splits.get(0), true);
    BytesWritable key = new BytesWritable();
    Tuple RowValue = TypesUtils.createTuple(scanner.getSchema());

    scanner.getKey(key);
    Assert.assertEquals(key, new BytesWritable("k11".getBytes()));
    scanner.getValue(RowValue);
    Assert.assertEquals("{a=A}", RowValue.get(0).toString());

    // If row 1 has a==>A, and row 2 doesn't have key a, scanner advance
    // will still return a==>A
    scanner.advance();
    scanner.getKey(key);
    Assert.assertEquals(key, new BytesWritable("k12".getBytes()));
    scanner.getValue(RowValue);
    System.out.println(RowValue.get(0).toString());
    Assert.assertEquals("{a=A2}", RowValue.get(0).toString());

    // m1:map(string),m2:map(map(int))";
    // String STR_STORAGE = "[m1#{a}];[m2#{x|y}]; [m1#{b}, m2#{z}]";

    // test stitch, I can re_use reader, split, but NOT scanner
    String projection2 = new String("m1#{b}, m2#{x|z}");
    reader.setProjection(projection2);
    scanner = reader.getScanner(splits.get(0), true);
    RowValue = TypesUtils.createTuple(scanner.getSchema());
    scanner.getKey(key);
    Assert.assertEquals(key, new BytesWritable("k11".getBytes()));
    scanner.getValue(RowValue);
    // TODO Assert.assertEquals("{a=A}", RowValue.get(0).toString());
    Assert.assertEquals("B", ((Map) RowValue.get(0)).get("b"));
    Assert.assertEquals(321, ((Map) ((Map) RowValue.get(1)).get("x"))
        .get("m321"));
    Assert.assertEquals(null, ((Map) ((Map) RowValue.get(1)).get("z")));
    // m2's x key:
    /*
     * m3.put("m311", 311); m3.put("m321", 321); m3.put("m331", 331);
     */
    System.out.println(RowValue.get(1).toString());

    TypesUtils.resetTuple(RowValue);
    scanner.advance();

    scanner.getKey(key);
    Assert.assertEquals(key, new BytesWritable("k12".getBytes()));
    scanner.getValue(RowValue);
    Assert.assertEquals(null, ((Map) RowValue.get(0)).get("b"));
    Assert.assertEquals(null, ((Map) ((Map) RowValue.get(1)).get("x")));
    Assert.assertEquals(323, ((Map) ((Map) RowValue.get(1)).get("z"))
        .get("m323"));

    reader.close();

  }
}

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
public class TestMap {

  final static String STR_SCHEMA = "m1:map(string),m2:map(map(int))";
  final static String STR_STORAGE = "[m1#{a}];[m2#{x|y}]; [m1#{b}, m2#{z}]";
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
    path = new Path(fs.getWorkingDirectory(), "TestMap");
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

    // add data to row 1
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
  }

  @AfterClass
  public static void tearDownOnce() throws IOException {
    BasicTable.drop(path, conf);
  }

  // read one map
  @Test
  public void testRead1() throws IOException, ParseException {
    String projection = new String("m1#{a}");
    BasicTable.Reader reader = new BasicTable.Reader(path, conf);
    reader.setProjection(projection);
    List<RangeSplit> splits = reader.rangeSplit(1);
    TableScanner scanner = reader.getScanner(splits.get(0), true);
    BytesWritable key = new BytesWritable();
    Tuple RowValue = TypesUtils.createTuple(scanner.getSchema());

    scanner.getKey(key);
    Assert.assertEquals(key, new BytesWritable("k11".getBytes()));
    scanner.getValue(RowValue);
    System.out.println("read1 : " + RowValue.toString());
    Assert.assertEquals("{a=A}", RowValue.get(0).toString());

    scanner.advance();
    scanner.getKey(key);
    Assert.assertEquals(key, new BytesWritable("k12".getBytes()));
    scanner.getValue(RowValue);
    System.out.println(RowValue.get(0).toString());
    Assert.assertEquals("{a=A2}", RowValue.get(0).toString());

    reader.close();
  }

  // m1:map(string),m2:map(map(int))";
  // String STR_STORAGE = "[m1#{a}];[m2#{x|y}]; [m1#{b}, m2#{z}]";
  // read map of map, stitch
  @Test
  public void testRead2() throws IOException, ParseException {
    String projection2 = new String("m1#{b}, m2#{x|z}");
    BasicTable.Reader reader = new BasicTable.Reader(path, conf);
    reader.setProjection(projection2);
    List<RangeSplit> splits = reader.rangeSplit(1);
    TableScanner scanner = reader.getScanner(splits.get(0), true);
    BytesWritable key = new BytesWritable();
    Tuple RowValue = TypesUtils.createTuple(scanner.getSchema());
    scanner.getKey(key);
    Assert.assertEquals(key, new BytesWritable("k11".getBytes()));
    scanner.getValue(RowValue);
    System.out.println("map of map: " + RowValue.toString());
    // map of map: ([b#B],[z#,x#{m311=311, m321=321, m331=331}])
    Assert.assertEquals("B", ((Map) RowValue.get(0)).get("b"));
    Assert.assertEquals(321, ((Map) ((Map) RowValue.get(1)).get("x"))
        .get("m321"));
    Assert.assertEquals(311, ((Map) ((Map) RowValue.get(1)).get("x"))
        .get("m311"));
    Assert.assertEquals(331, ((Map) ((Map) RowValue.get(1)).get("x"))
        .get("m331"));
    Assert.assertEquals(null, ((Map) ((Map) RowValue.get(1)).get("x"))
        .get("m341"));
    Assert.assertEquals(null, ((Map) ((Map) RowValue.get(1)).get("z")));
    Assert.assertEquals(null, ((Map) ((Map) RowValue.get(0)).get("a")));
    Assert.assertEquals(null, ((Map) ((Map) RowValue.get(0)).get("c")));

    System.out.println("rowValue.get)1): " + RowValue.get(1).toString());
    // rowValue.get)1): {z=null, x={m311=311, m321=321, m331=331}}

    scanner.advance();

    scanner.getKey(key);
    Assert.assertEquals(key, new BytesWritable("k12".getBytes()));
    scanner.getValue(RowValue);
    Assert.assertEquals(null, ((Map) RowValue.get(0)).get("b"));
    Assert.assertEquals(null, ((Map) ((Map) RowValue.get(1)).get("x")));
    Assert.assertEquals(323, ((Map) ((Map) RowValue.get(1)).get("z"))
        .get("m323"));
    Assert.assertEquals(322, ((Map) ((Map) RowValue.get(1)).get("z"))
        .get("m322"));
    Assert.assertEquals(321, ((Map) ((Map) RowValue.get(1)).get("z"))
        .get("m321"));
    Assert.assertEquals(null, ((Map) ((Map) RowValue.get(0)).get("a")));
    Assert.assertEquals(null, ((Map) ((Map) RowValue.get(0)).get("b")));
    Assert.assertEquals(null, ((Map) ((Map) RowValue.get(1)).get("a")));

    reader.close();

  }

  // read one map negative non-exist column
  @Test
  public void testRead3() throws IOException, ParseException {
    String projection = new String("m5");
    BasicTable.Reader reader = new BasicTable.Reader(path, conf);
    reader.setProjection(projection);

    List<RangeSplit> splits = reader.rangeSplit(1);
    TableScanner scanner = reader.getScanner(splits.get(0), true);
    BytesWritable key = new BytesWritable();
    Tuple RowValue = TypesUtils.createTuple(scanner.getSchema());

    scanner.getKey(key);
    Assert.assertEquals(key, new BytesWritable("k11".getBytes()));
    scanner.getValue(RowValue);
    Assert.assertEquals(null, RowValue.get(0));
    Assert.assertEquals(1, RowValue.size());

    scanner.advance();
    scanner.getKey(key);
    Assert.assertEquals(key, new BytesWritable("k12".getBytes()));
    scanner.getValue(RowValue);
    Assert.assertEquals(null, RowValue.get(0));
    Assert.assertEquals(1, RowValue.size());
    reader.close();
  }

  // Doesn't exist key for all rows
  @Test
  public void testRead4() throws IOException, ParseException {
    String projection = new String("m1#{nonexist}");
    BasicTable.Reader reader = new BasicTable.Reader(path, conf);
    reader.setProjection(projection);
    List<RangeSplit> splits = reader.rangeSplit(1);
    TableScanner scanner = reader.getScanner(splits.get(0), true);
    BytesWritable key = new BytesWritable();
    Tuple RowValue = TypesUtils.createTuple(scanner.getSchema());

    scanner.getKey(key);
    Assert.assertEquals(key, new BytesWritable("k11".getBytes()));
    scanner.getValue(RowValue);
    System.out.println("read1 : " + RowValue.toString());
    Assert.assertEquals("{}", RowValue.get(0).toString());

    scanner.advance();
    scanner.getKey(key);
    Assert.assertEquals(key, new BytesWritable("k12".getBytes()));
    scanner.getValue(RowValue);
    System.out.println(RowValue.get(0).toString());
    Assert.assertEquals("{}", RowValue.get(0).toString());

    reader.close();
  }
}

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
import java.io.File;
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
import org.apache.hadoop.record.Record;
import org.apache.hadoop.zebra.io.BasicTable;
import org.apache.hadoop.zebra.io.TableInserter;
import org.apache.hadoop.zebra.io.TableScanner;
import org.apache.hadoop.zebra.io.BasicTable.Reader.RangeSplit;
import org.apache.hadoop.zebra.parser.ParseException;
import org.apache.hadoop.zebra.types.Projection;
import org.apache.hadoop.zebra.schema.Schema;
import org.apache.hadoop.zebra.types.TypesUtils;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.test.MiniCluster;
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
public class TestMapOfRecord {
  final static String STR_SCHEMA = "m1:map(string),m2:map(map(int)), m4:map(map(record(f1:int,f2:string)))";
  final static String STR_STORAGE = "[m1#{a}];[m2#{x|y}]; [m1#{b}, m2#{z}]; [m4#{a4}]; [m4#{b4|c4}]";

  private static Configuration conf;
  private static FileSystem fs;

  protected static ExecType execType = ExecType.MAPREDUCE;
  // private MiniCluster cluster;
  // protected PigServer pigServer;
  private static Path path;

  @BeforeClass
  public static void setUpOnce() throws IOException {

    conf = new Configuration();
    conf.setInt("table.output.tfile.minBlock.size", 64 * 1024);
    conf.setInt("tabaale.input.split.minSize", 64 * 1024);
    conf.set("table.output.tfile.compression", "none");

    RawLocalFileSystem rawLFS = new RawLocalFileSystem();
    fs = new LocalFileSystem(rawLFS);
    path = new Path(fs.getWorkingDirectory(), "TestMapOfRecord");
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

    Tuple record1;
    try {
      record1 = TypesUtils.createTuple(new Schema("f1:int, f2:string"));
    } catch (ParseException e) {
      e.printStackTrace();
      throw new IOException(e);
    } 

    Tuple record2;
    try {
      record2 = TypesUtils.createTuple(new Schema("f1:int, f2:string"));
    } catch (ParseException e) {
      e.printStackTrace();
      throw new IOException(e);
    }

    Tuple record3;
    try {
      record3 = TypesUtils.createTuple(new Schema("f1:int, f2:string"));
    } catch (ParseException e) {
      e.printStackTrace();
      throw new IOException(e);
    }

    // add data to row 1
    // m1:map(string)
    Map<String, String> m1 = new HashMap<String, String>();
    m1.put("a", "A");
    m1.put("b", "B");
    m1.put("c", "C");
    tuple.set(0, m1);

    // m2:map(map(int))
    HashMap<String, Map> m2 = new HashMap<String, Map>();
    Map<String, Integer> m31 = new HashMap<String, Integer>();
    m31.put("m311", 311);
    m31.put("m321", 321);
    m31.put("m331", 331);
    Map<String, Integer> m32 = new HashMap<String, Integer>();
    m32.put("m411", 411);
    m32.put("m421", 421);
    m32.put("m431", 431);
    m2.put("x", m31);
    m2.put("y", m32);
    tuple.set(1, m2);

    // m4:map(map(record(f1:int,f2:string)))
    record1.set(0, 11);
    record1.set(1, "record row 1.1");
    Map<String, Tuple> m51 = new HashMap<String, Tuple>();
    Map<String, Tuple> m52 = new HashMap<String, Tuple>();
    Map<String, Tuple> m53 = new HashMap<String, Tuple>();
    m51.put("ma4", (Tuple) record1);
    m52.put("ma41", (Tuple) record1);
    m53.put("ma43", (Tuple) record1);

    record2.set(0, 12);
    record2.set(1, "record row 1.2");
    m51.put("mb4", (Tuple) record2);
    m52.put("mb42", (Tuple) record2);
    m53.put("ma43", (Tuple) record2);
    System.out.println("record1-1: " + record1.toString());

    record3.set(0, 13);
    record3.set(1, "record row 1.3");
    System.out.println("record1-3: " + record1.toString());

    m51.put("mc4", (Tuple) record3);
    m52.put("mc42", (Tuple) record3);
    m53.put("ma43", (Tuple) record3);

    Map<String, Map> m4 = new HashMap<String, Map>();
    m4.put("a4", m51);
    m4.put("b4", m52);
    m4.put("c4", m53);
    m4.put("d4", m53);
    m4.put("ma43", m53);

    tuple.set(2, m4);

    int row = 0;
    inserter.insert(new BytesWritable(String.format("k%d%d", part + 1, row + 1)
        .getBytes()), tuple);

    // row 2
    row++;
    TypesUtils.resetTuple(tuple);
    TypesUtils.resetTuple(record1);
    TypesUtils.resetTuple(record2);
    TypesUtils.resetTuple(record3);
    m1.clear();
    m2.clear();
    m31.clear();
    m32.clear();
    m4.clear();
    m51.clear();
    m52.clear();
    m53.clear();
    // m1:map(string)
    m1.put("a", "A2");
    m1.put("b2", "B2");
    m1.put("c2", "C2");
    tuple.set(0, m1);

    // m2:map(map(int))
    m31.put("m321", 321);
    m31.put("m322", 322);
    m31.put("m323", 323);
    m2.put("z", m31);
    tuple.set(1, m2);

    // m4:map(map(record(f1:int,f2:string)))
    record1.set(0, 21);
    record1.set(1, "record row 2.1");
    m51.put("ma4", (Tuple) record1);
    m52.put("ma41", (Tuple) record1);
    m53.put("ma43", (Tuple) record1);

    record2.set(0, 22);
    record2.set(1, "record row 2.2");
    m51.put("mb4", (Tuple) record2);
    m52.put("mb42", (Tuple) record2);
    m53.put("ma43", (Tuple) record2);

    record3.set(0, 33);
    record3.set(1, "record row 3.3");
    m51.put("mc4", (Tuple) record3);
    m52.put("mc42", (Tuple) record3);
    m53.put("ma43", (Tuple) record3);

    m4.put("a4", m51);
    m4.put("b4", m52);

    m4.put("ma43", m53);

    tuple.set(2, m4);

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
  // read one map
  public void testReadSimpleMap() throws IOException, ParseException {
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
    Assert.assertEquals("{a=A}", RowValue.get(0).toString());

    scanner.advance();
    scanner.getKey(key);
    Assert.assertEquals(key, new BytesWritable("k12".getBytes()));
    scanner.getValue(RowValue);
    Assert.assertEquals("{a=A2}", RowValue.get(0).toString());

    reader.close();
  }

  @Test
  // m1:map(string),m2:map(map(int))";
  // String STR_STORAGE = "[m1#{a}];[m2#{x|y}]; [m1#{b}, m2#{z}]";
  // read map of map, stitch
  public void testReadMapOfMap() throws IOException, ParseException {
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

    System.out.println(RowValue.get(1).toString());

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

  // final static String STR_SCHEMA =
  // "m1:map(string),m2:map(map(int)), m4:map(map(record(f1:int,f2:string)))";
  // final static String STR_STORAGE =
  // "[m1#{a}];[m2#{x|y}]; [m1#{b}, m2#{z}]; [m4#{a4}]; [m4#{b4|c4}]";
  @Test
  // read map of map, stitch
  public void testReadMapOfRecord1() throws IOException, ParseException {
    String projection2 = new String("m1#{b}, m4#{a4|c4}");
    BasicTable.Reader reader = new BasicTable.Reader(path, conf);
    reader.setProjection(projection2);
    List<RangeSplit> splits = reader.rangeSplit(1);
    TableScanner scanner = reader.getScanner(splits.get(0), true);
    BytesWritable key = new BytesWritable();
    Tuple RowValue = TypesUtils.createTuple(scanner.getSchema());
    scanner.getKey(key);
    Assert.assertEquals(key, new BytesWritable("k11".getBytes()));
    scanner.getValue(RowValue);
    Assert.assertEquals("B", ((Map) RowValue.get(0)).get("b"));
    System.out.println(RowValue.get(1).toString());

    Assert.assertEquals(11, ((Tuple) ((Map) ((Map) RowValue.get(1)).get("a4"))
        .get("ma4")).get(0));
    Assert.assertEquals("record row 1.1", ((Tuple) ((Map) ((Map) RowValue
        .get(1)).get("a4")).get("ma4")).get(1));
    Assert.assertEquals(12, ((Tuple) ((Map) ((Map) RowValue.get(1)).get("a4"))
        .get("mb4")).get(0));
    Assert.assertEquals("record row 1.2", ((Tuple) ((Map) ((Map) RowValue
        .get(1)).get("a4")).get("mb4")).get(1));
    Assert.assertEquals(13, ((Tuple) ((Map) ((Map) RowValue.get(1)).get("a4"))
        .get("mc4")).get(0));
    Assert.assertEquals("record row 1.3", ((Tuple) ((Map) ((Map) RowValue
        .get(1)).get("a4")).get("mc4")).get(1));

    Assert.assertEquals(13, ((Tuple) ((Map) ((Map) RowValue.get(1)).get("c4"))
        .get("ma43")).get(0));
    Assert.assertEquals("record row 1.3", ((Tuple) ((Map) ((Map) RowValue
        .get(1)).get("c4")).get("ma43")).get(1));

    Assert.assertEquals(null, (((Map) ((Map) RowValue.get(1)).get("c4"))
        .get("mc4")));
    Assert.assertEquals(null, (((Map) ((Map) RowValue.get(1)).get("c4"))
        .get("mb4")));

    scanner.advance();

    scanner.getKey(key);
    Assert.assertEquals(key, new BytesWritable("k12".getBytes()));
    scanner.getValue(RowValue);
    System.out.println(RowValue.get(1).toString());
    Assert.assertEquals(21, ((Tuple) ((Map) ((Map) RowValue.get(1)).get("a4"))
        .get("ma4")).get(0));
    Assert.assertEquals("record row 2.1", ((Tuple) ((Map) ((Map) RowValue
        .get(1)).get("a4")).get("ma4")).get(1));
    Assert.assertEquals(22, ((Tuple) ((Map) ((Map) RowValue.get(1)).get("a4"))
        .get("mb4")).get(0));
    Assert.assertEquals("record row 2.2", ((Tuple) ((Map) ((Map) RowValue
        .get(1)).get("a4")).get("mb4")).get(1));
    Assert.assertEquals(33, ((Tuple) ((Map) ((Map) RowValue.get(1)).get("a4"))
        .get("mc4")).get(0));
    Assert.assertEquals("record row 3.3", ((Tuple) ((Map) ((Map) RowValue
        .get(1)).get("a4")).get("mc4")).get(1));

    reader.close();

  }
}

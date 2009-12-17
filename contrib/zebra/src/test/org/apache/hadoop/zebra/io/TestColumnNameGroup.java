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
public class TestColumnNameGroup {
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
    path = new Path(fs.getWorkingDirectory(), "TestColumnNameGroup");
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
  //  BasicTable.drop(path, conf);
  }

  @Test
  public void testSimple() throws IOException, ParseException {
    String STR_SCHEMA = "group:string, s6:bytes";
    String STR_STORAGE = "[group]; [s6]";

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
   
    tuple.set(0, "hello world 1"); // string
    tuple.set(1, new DataByteArray("hello byte 1")); // byte
    inserter.insert(new BytesWritable(String.format("k%d%d", part + 1, row + 1)
        .getBytes()), tuple);

    // insert data in row 2
    row++;
   
    tuple.set(0, "hello world 2"); // string
    tuple.set(1, new DataByteArray("hello byte 2")); // byte
    inserter.insert(new BytesWritable(String.format("k%d%d", part + 1, row + 1)
        .getBytes()), tuple);

    // finish building table, closing out the inserter, writer, writer1
    inserter.close();
    writer1.finish();
    writer.close();

    // Starting read
    String projection = new String("group");
    BasicTable.Reader reader = new BasicTable.Reader(path, conf);
    reader.setProjection(projection);
    List<RangeSplit> splits = reader.rangeSplit(1);
    TableScanner scanner = reader.getScanner(splits.get(0), true);
    BytesWritable key = new BytesWritable();
    Tuple RowValue = TypesUtils.createTuple(scanner.getSchema());

    scanner.getKey(key);
    Assert.assertEquals(key, new BytesWritable("k11".getBytes()));
    scanner.getValue(RowValue);
   
    Assert.assertEquals("hello world 1", RowValue.get(0));

    scanner.advance();
    scanner.getKey(key);
    Assert.assertEquals(key, new BytesWritable("k12".getBytes()));
    scanner.getValue(RowValue);
   
    Assert.assertEquals("hello world 2", RowValue.get(0));


    reader.close();
  }
}

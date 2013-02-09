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
import java.io.PrintStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
public class TestProjectionOnFullMap {
  final static String STR_SCHEMA = "f1:string, f2:map(string)";
  final static String STR_STORAGE = "[f1]; [f2]";
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
    path = new Path(fs.getWorkingDirectory(), "TestBasicTableMapSplits");
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

    tuple.set(0, "Kitty");

   
    Map<String, String> map = new HashMap<String, String>();
    map.put("a", "x");
    map.put("b", "y");
    map.put("c", "z");
    tuple.set(1, map);

    int row = 0;
    inserter.insert(new BytesWritable(String.format("k%d%d", part + 1, row + 1)
        .getBytes()), tuple);
    inserter.close();
    writer1.finish();

    writer.close();
  }

  @AfterClass
  public static void tearDownOnce() throws IOException {
  }

  public void testDescribse() throws IOException {

    BasicTable.dumpInfo(path.toString(), System.out, conf);

  }

  @Test
  public void test1() throws IOException, ParseException {
    String projection = new String("f2#{a}");
    BasicTable.Reader reader = new BasicTable.Reader(path, conf);
    reader.setProjection(projection);
    // long totalBytes = reader.getStatus().getSize();

    List<RangeSplit> splits = reader.rangeSplit(1);
    reader.close();
    reader = new BasicTable.Reader(path, conf);
    reader.setProjection(projection);
    TableScanner scanner = reader.getScanner(splits.get(0), true);
    BytesWritable key = new BytesWritable();
    Tuple value = TypesUtils.createTuple(scanner.getSchema());

    scanner.getKey(key);
    Assert.assertEquals(key, new BytesWritable("k11".getBytes()));
    scanner.getValue(value);

    Assert.assertEquals("x", ((Map)value.get(0)).get("a"));
    reader.close();
  }

}

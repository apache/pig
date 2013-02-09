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
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestBasicTableProjections {
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
    path = new Path(fs.getWorkingDirectory(), "TestBasicTableProjections");
    fs = path.getFileSystem(conf);
    // drop any previous tables
    BasicTable.drop(path, conf);

    BasicTable.Writer writer = new BasicTable.Writer(path, "a,b,c,d,e,f,g",
        "[a,b,c];[d,e,f,g]", conf);
    writer.finish();

    Schema schema = writer.getSchema();
    // String[] colNames = schema.getColumns();
    Tuple tuple = TypesUtils.createTuple(schema);

    // BytesWritable key;
    int parts = 2;
    for (int part = 0; part < parts; part++) {
      writer = new BasicTable.Writer(path, conf);
      TableInserter inserter = writer.getInserter("part" + part, true);
      TypesUtils.resetTuple(tuple);
      for (int row = 0; row < 2; row++) {
        try {
          for (int nx = 0; nx < tuple.size(); nx++)
            tuple.set(nx, new DataByteArray(String.format("%c%d%d", 'a' + nx, part + 1, row + 1).getBytes()));
        } catch (ExecException e) {
          e.printStackTrace();
        }
        inserter.insert(new BytesWritable(String.format("k%d%d", part + 1,
            row + 1).getBytes()), tuple);
      }
      inserter.close();
      writer.finish();
    }
    writer.close();
  }

  @AfterClass
  public static void tearDownOnce() throws IOException {
  }

  @Test
  public void test1() throws IOException, ParseException {
    String projection = new String("f,a");
    Configuration conf = new Configuration();
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
    Assert.assertEquals("f11", value.get(0).toString());
    Assert.assertEquals("a11", value.get(1).toString());
    try {
      value.get(2);
      Assert.fail("Failed to catch out of boundary exceptions.");
    } catch (IndexOutOfBoundsException e) {
      // no op, expecting out of bounds exceptions
    }

    scanner.advance();
    scanner.getKey(key);
    Assert.assertEquals(key, new BytesWritable("k12".getBytes()));
    TypesUtils.resetTuple(value);
    scanner.getValue(value);
    Assert.assertEquals("f12", value.get(0).toString());
    Assert.assertEquals("a12", value.get(1).toString());

    scanner.advance();
    scanner.getKey(key);
    Assert.assertEquals(key, new BytesWritable("k21".getBytes()));
    TypesUtils.resetTuple(value);
    scanner.getValue(value);
    Assert.assertEquals("f21", value.get(0).toString());
    Assert.assertEquals("a21", value.get(1).toString());

    scanner.advance();
    scanner.getKey(key);
    Assert.assertEquals(key, new BytesWritable("k22".getBytes()));
    TypesUtils.resetTuple(value);
    scanner.getValue(value);
    Assert.assertEquals("f22", value.get(0).toString());
    Assert.assertEquals("a22", value.get(1).toString());
  }

}

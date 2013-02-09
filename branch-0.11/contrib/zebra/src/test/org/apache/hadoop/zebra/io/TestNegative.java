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
public class TestNegative {
  private static Configuration conf;
  private static Path path;
  private static FileSystem fs;

  @BeforeClass
  public static void setUpOnce() throws IOException {

  }

  @AfterClass
  public static void tearDownOnce() throws IOException {
    BasicTable.drop(path, conf);
  }

  // Negative test case. For record split, we should not try to store same
  // record field on different column groups.
  @Test
  public void testWriteRecord5() throws IOException, ParseException {
    String STR_SCHEMA = "r1:record(f1:int, f2:long), r2:record(r3:record(f3:float, f4))";
    String STR_STORAGE = "[r1.f1]; [r2.r3]; [r1.f2, r2.r3.f3]";
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
    // Build Table and column groups
    BasicTable.Writer writer = null;
    try {
      writer = new BasicTable.Writer(path, STR_SCHEMA, STR_STORAGE, conf);
      Assert.fail("Should throw exception");
    } catch (Exception e) {
      System.out.println(e);
    }
  }

//Negative test case. For record split, we should not try to store same
  // record field on different column groups.
  @Test
  public void testWriteRecord6() throws IOException, ParseException {
    String STR_SCHEMA = "r1:record(f1:int, f2:long), r2:record(r3:record(f3:float, f4))";
    String STR_STORAGE = "[r1.f1]; [r1.f2, r2.r3.f3]; [r2.r3]";
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
    // Build Table and column groups
    BasicTable.Writer writer = null;
    try {
      writer = new BasicTable.Writer(path, STR_SCHEMA, STR_STORAGE, conf);
      Assert.fail("Should throw exception");
    } catch (Exception e) {
      System.out.println(e);
    }
  }
  
  // Negative test case. map storage syntax is wrong
  @Test
  public void testWriteMap1() throws IOException, ParseException {
    String STR_SCHEMA = " m2:map(map(map(string)))";
    String STR_STORAGE = "[m2#{k}#{j}]";

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
    // Build Table and column groups
    BasicTable.Writer writer = null;
    try {
      writer = new BasicTable.Writer(path, STR_SCHEMA, STR_STORAGE, conf);
      Assert.fail("Should throw exception");
    } catch (Exception e) {
      System.out.println(e);
    }
  }

  // Negative test case. map storage syntax is wrong
  @Test
  public void testWriteMap2() throws IOException, ParseException {
    String STR_SCHEMA = " m2:map(map(map(string)))";
    String STR_STORAGE = "[m2.{k}]";

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
    // Build Table and column groups
    BasicTable.Writer writer = null;
    try {
      writer = new BasicTable.Writer(path, STR_SCHEMA, STR_STORAGE, conf);
      Assert.fail("Should throw exception");
    } catch (Exception e) {
      System.out.println(e);
    }
  }

  // Negative test case. map storage syntax is wrong
  @Test
  public void testWriteMap3() throws IOException, ParseException {
    String STR_SCHEMA = " m2:map(map(map(string)))";
    String STR_STORAGE = "[m2{k}]";

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
    // Build Table and column groups
    BasicTable.Writer writer = null;
    try {
      writer = new BasicTable.Writer(path, STR_SCHEMA, STR_STORAGE, conf);
      Assert.fail("Should throw exception");
    } catch (Exception e) {
      System.out.println(e);
    }
  }

  // Negative test case. map storage syntax is wrong
  @Test
  public void testWriteMap4() throws IOException, ParseException {
    String STR_SCHEMA = " m2:map(map(map(string)))";
    String STR_STORAGE = "[m2#{k}";

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
    // Build Table and column groups
    BasicTable.Writer writer = null;
    try {
      writer = new BasicTable.Writer(path, STR_SCHEMA, STR_STORAGE, conf);
      Assert.fail("Should throw exception");
    } catch (Exception e) {
      System.out.println(e);
    }
  }

  // Negative test case. map schema syntax is wrong
  @Test
  public void testWriteMap5() throws IOException, ParseException {
    String STR_SCHEMA = " m2:map(map(map(string,string,string)))";
    String STR_STORAGE = "[m2#{k}]";

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
    // Build Table and column groups
    BasicTable.Writer writer = null;
    try {
      writer = new BasicTable.Writer(path, STR_SCHEMA, STR_STORAGE, conf);
      Assert.fail("Should throw exception");
    } catch (Exception e) {
      System.out.println(e);
    }
  }

  // Negative test case. map storage syntax is wrong.
  @Test
  public void testWriteMap6() throws IOException, ParseException {
    String STR_SCHEMA = " m2:map(map(map(string)))";
    String STR_STORAGE = "[m2#k#k1]";

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
    // Build Table and column groups
    BasicTable.Writer writer = null;
    try {
      writer = new BasicTable.Writer(path, STR_SCHEMA, STR_STORAGE, conf);
      Assert.fail("Should throw exception");
    } catch (Exception e) {
      System.out.println(e);
    }
  }

  // negative, map storage syntax is wrong
  @Test
  public void testWriteMap7() throws IOException, ParseException {
    String STR_SCHEMA = " m2:map(map(map(string)))";
    String STR_STORAGE = "[m2#k]";

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
    // Build Table and column groups
    BasicTable.Writer writer = null;
    try {
      writer = new BasicTable.Writer(path, STR_SCHEMA, STR_STORAGE, conf);
      Assert.fail("Should throw exception");
    } catch (Exception e) {
      System.out.println(e);
    }
  }

  // negative, should not take same field name
  @Test
  public void testWriteRecord1() throws IOException, ParseException {
    String STR_SCHEMA = " r1:record(f1,f2), r1:record(f1,f2)";
    String STR_STORAGE = "[r1]";

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
    // Build Table and column groups
    BasicTable.Writer writer = null;
    try {
      writer = new BasicTable.Writer(path, STR_SCHEMA, STR_STORAGE, conf);
      Assert.fail("Should throw exception");
    } catch (Exception e) {
      System.out.println(e);
    }
  }

  // negative, duplicate column storage
  @Test
  public void testWriteRecord2() throws IOException, ParseException {
    String STR_SCHEMA = " r1:record(f1,f2), r2:record(f1,f2)";
    String STR_STORAGE = "[r1,r1]";

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
    // Build Table and column groups
    BasicTable.Writer writer = null;
    try {
      writer = new BasicTable.Writer(path, STR_SCHEMA, STR_STORAGE, conf);
      Assert.fail("Should throw exception");
    } catch (Exception e) {
      System.out.println(e);
    }
  }

  // negative, duplicate column storage
  @Test
  public void testWriteRecord3() throws IOException, ParseException {
    String STR_SCHEMA = " r1:record(f1,f2), r2:record(f1,f2)";
    String STR_STORAGE = "[r1.f1, r2]; [r1.f1,r2]";

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
    // Build Table and column groups
    BasicTable.Writer writer = null;
    try {
      writer = new BasicTable.Writer(path, STR_SCHEMA, STR_STORAGE, conf);
      Assert.fail("Should throw exception");
    } catch (Exception e) {
      System.out.println(e);
    }
  }

  // negative, duplicate column storage
  @Test
  public void testWriteRecord4() throws IOException, ParseException {
    String STR_SCHEMA = " r1:record(f1,f2), r2:record(f1,f2)";
    String STR_STORAGE = "[r1.f1]; [r1.f1,r2]";

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
    // Build Table and column groups
    BasicTable.Writer writer = null;
    try {
      writer = new BasicTable.Writer(path, STR_SCHEMA, STR_STORAGE, conf);
      Assert.fail("Should throw exception");
    } catch (Exception e) {
      System.out.println(e);
    }
  }

  // negative, null column storage
  @Test
  public void testWriteNull5() throws IOException, ParseException {
    String STR_SCHEMA = " r1:record(f1,f2), r2:record(f1,f2)";

    String STR_STORAGE = null;

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
    // Build Table and column groups
    BasicTable.Writer writer = null;
    try {
      writer = new BasicTable.Writer(path, STR_SCHEMA, STR_STORAGE, conf);
      System.out.println("HERE HERE");
      Assert.fail("Should throw exception");
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  // empty column group
  @Test
  public void testWriteEmpty6() throws IOException, ParseException {
    String STR_SCHEMA = "f1:int, f2:string";

    String STR_STORAGE = "";

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
    // Build Table and column groups
    BasicTable.Writer writer = null;
    try {
      writer = new BasicTable.Writer(path, STR_SCHEMA, STR_STORAGE, conf);
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail("Should Not throw exception");
    }
    writer.finish();
    Schema schema = writer.getSchema();
    Tuple tuple = TypesUtils.createTuple(schema);
    BasicTable.Writer writer1 = new BasicTable.Writer(path, conf);
    int part = 0;
    TableInserter inserter = writer1.getInserter("part" + part, true);
    TypesUtils.resetTuple(tuple);

    // insert data in row 1
    int row = 0;
    tuple.set(0, 1);
    tuple.set(1, "hello1");

    inserter.insert(new BytesWritable(String.format("k%d%d", part + 1, row + 1)
        .getBytes()), tuple);

    // row 2
    row++;
    TypesUtils.resetTuple(tuple);

    tuple.set(0, 2);
    tuple.set(1, "hello2");

    inserter.insert(new BytesWritable(String.format("k%d%d", part + 1, row + 1)
        .getBytes()), tuple);

    // finish building table, closing out the inserter, writer, writer1
    inserter.close();
    writer1.finish();
    writer.close();

    String projection3 = new String("f1,f2");
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

    Assert.assertEquals(1, RowValue.get(0));
    Assert.assertEquals("hello1", RowValue.get(1));

    scanner.advance();

    scanner.getKey(key);
    Assert.assertEquals(key, new BytesWritable("k12".getBytes()));
    scanner.getValue(RowValue);
    Assert.assertEquals(2, RowValue.get(0));
    Assert.assertEquals("hello2", RowValue.get(1));

    reader.close();

  }

  // Positive test case. map storage , [m2] will storage everything besides k1
  @Test
  public void testMapWrite8() throws IOException, ParseException {
    String STR_SCHEMA = " m2:map(map(map(string))), m1:map(int)";
    String STR_STORAGE = "[m2#{k1}];[m2]";

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
    // Build Table and column groups
    BasicTable.Writer writer = null;
    try {
      writer = new BasicTable.Writer(path, STR_SCHEMA, STR_STORAGE, conf);
    } catch (Exception e) {
      System.out.println(e);
      Assert.fail("Should throw exception");
    }
  }

  // Negative test case.duplicate map storage
  @Test
  public void testMapWrite9() throws IOException, ParseException {
    String STR_SCHEMA = " m2:map(map(map(string))), m1:map(int)";
    String STR_STORAGE = "[m2#{k1}], [m2#{k1}]";

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
    // Build Table and column groups
    BasicTable.Writer writer = null;
    try {
      writer = new BasicTable.Writer(path, STR_SCHEMA, STR_STORAGE, conf);
      Assert.fail("Should throw exception");
    } catch (Exception e) {
      System.out.println(e);
    }
  }

  // Positive test case.duplicate map storage, TODO: why failed?

  public void xtestMapWrite10() throws IOException, ParseException {
    String STR_SCHEMA = " m2:map(map(map(string))), m1:map(int)";
    String STR_STORAGE = "[m2#{k1}]; [m2#{k2}]";

    Configuration conf = new Configuration();
    conf.setInt("table.output.tfile.minBlock.size", 64 * 1024);
    conf.setInt("table.input.split.minSize", 64 * 1024);
    conf.set("table.output.tfile.compression", "none");

    RawLocalFileSystem rawLFS = new RawLocalFileSystem();
    FileSystem fs = new LocalFileSystem(rawLFS);
    Path path = new Path(fs.getWorkingDirectory(), this.getClass()
        .getSimpleName());
    fs = path.getFileSystem(conf);
    // drop any previous tables
    BasicTable.drop(path, conf);
    // Build Table and column groups
    BasicTable.Writer writer = null;
    try {
      writer = new BasicTable.Writer(path, STR_SCHEMA, STR_STORAGE, conf);
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail("Should Not throw exception");
    }

  }

  // negative, schema, same field name, different type r1:record(f1:int,
  // f2:long).
  @Test
  public void testColumnField5() throws IOException, ParseException {
    String STR_SCHEMA = "r1:int, r1:float";
    String STR_STORAGE = "[r1]";

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
    // Build Table and column groups
    BasicTable.Writer writer = null;
    try {
      writer = new BasicTable.Writer(path, STR_SCHEMA, STR_STORAGE, conf);
      Assert.fail("Should throw exception");
    } catch (Exception e) {
      System.out.println(e);
    }
  }

}

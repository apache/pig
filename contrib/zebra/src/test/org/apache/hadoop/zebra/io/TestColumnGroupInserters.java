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
import java.util.SortedMap;
import java.util.TreeMap;

import junit.framework.Assert;
import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.zebra.io.ColumnGroup;
import org.apache.hadoop.zebra.io.TableInserter;
import org.apache.hadoop.zebra.parser.ParseException;
import org.apache.hadoop.zebra.schema.Schema;
import org.apache.hadoop.zebra.types.TypesUtils;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestColumnGroupInserters {
  final static String outputFile = "TestColumnGroupInserters";
  final static private Configuration conf = new Configuration();
  private static FileSystem fs;
  private static Path path;
  private static ColumnGroup.Writer writer;

  @BeforeClass
  public static void setUpOnce() throws IOException {
    // set default file system to local file system
    conf.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");

    // must set a conf here to the underlying FS, or it barks
    RawLocalFileSystem rawLFS = new RawLocalFileSystem();
    rawLFS.setConf(conf);
    fs = new LocalFileSystem(rawLFS);
    path = new Path(fs.getWorkingDirectory(), outputFile);
    System.out.println("output file: " + path);
  }

  @AfterClass
  public static void tearDownOnce() throws IOException {
    finish();
  }

  @Test
  public void testInsertNullValues() throws IOException, ParseException {
    fs.delete(path, true);
    System.out.println("testInsertNullValues");
    writer = new ColumnGroup.Writer(path, "abc, def", false, path.getName(), "pig", "gz",
        null, null, (short) -1, true, conf);
    TableInserter ins = writer.getInserter("part1", true);
    // Tuple row = TypesUtils.createTuple(writer.getSchema());
    // ins.insert(new BytesWritable("key".getBytes()), row);
    ins.close();
    close();
  }

  @Test
  public void testFailureInvalidSchema() throws IOException, ParseException {
    fs.delete(path, true);
    System.out.println("testFailureInvalidSchema");
    writer = new ColumnGroup.Writer(path, "abc, def", false, path.getName(), "pig", "gz",
        null, null, (short) -1, true, conf);
    TableInserter ins = writer.getInserter("part1", true);
    Tuple row = TypesUtils.createTuple(Schema.parse("xyz, ijk, def"));
    try {
      ins.insert(new BytesWritable("key".getBytes()), row);
      Assert.fail("Failed to catch diff schemas.");
    } catch (IOException e) {
      // noop, expecting exceptions
    } finally {
      ins.close();
      close();
    }
  }

  @Test
  public void testFailureGetInserterAfterWriterClosed() throws IOException,
    ParseException {
    fs.delete(path, true);
    System.out.println("testFailureGetInserterAfterWriterClosed");
    writer = new ColumnGroup.Writer(path, "abc, def", false, path.getName(), "pig", "gz",
        null, null, (short) -1, true, conf);
    try {
      writer.close();
      TableInserter ins = writer.getInserter("part1", true);
      Assert.fail("Failed to catch getInsertion after writer closure.");
    } catch (IOException e) {
      // noop, expecting exceptions
    } finally {
      close();
    }
  }

  @Test
  public void testFailureInsertAfterClose() throws IOException, ExecException,
    ParseException {
    fs.delete(path, true);
    System.out.println("testFailureInsertAfterClose");
    writer = new ColumnGroup.Writer(path, "abc:string, def:map(string)", false, path.getName(), "pig", "gz",
        null, null, (short) -1, true, conf);
    TableInserter ins = writer.getInserter("part1", true);

    Tuple row = TypesUtils.createTuple(writer.getSchema());
    row.set(0, new String("val1"));

    SortedMap<String, String> map = new TreeMap<String, String>();
    map.put("john", "boy");
    row.set(1, map);

    ins.insert(new BytesWritable("key".getBytes()), row);

    ins.close();
    writer.close();

    try {
      TableInserter ins2 = writer.getInserter("part2", true);
      Assert.fail("Failed to catch insertion after closure.");
    } catch (IOException e) {
      // noop, expecting exceptions
    } finally {
      close();
    }
  }

  @Test
  public void testFailureInsertXtraColumn() throws IOException, ExecException,
    ParseException {
    fs.delete(path, true);
    System.out.println("testFailureInsertXtraColumn");
    writer = new ColumnGroup.Writer(path, "abc ", false, path.getName(), "pig", "gz", null, null, (short) -1, true,
        conf);
    TableInserter ins = writer.getInserter("part1", true);

    try {
      Tuple row = TypesUtils.createTuple(writer.getSchema());
      row.set(0, new String("val1"));

      SortedMap<String, String> map = new TreeMap<String, String>();
      map.put("john", "boy");
      row.set(1, map);
      Assert
          .fail("Failed to catch insertion an extra column not defined in schema.");
    } catch (IndexOutOfBoundsException e) {
      // noop, expecting exceptions
    } finally {
      ins.close();
      close();
    }
  }

  @Test
  public void testInsertOneRow() throws IOException, ExecException,
    ParseException {
    fs.delete(path, true);
    System.out.println("testInsertOneRow");
    writer = new ColumnGroup.Writer(path, "abc:string, def:map(string)", false, path.getName(), "pig", "gz",
        null, null, (short) -1, true, conf);
    TableInserter ins = writer.getInserter("part1", true);

    Tuple row = TypesUtils.createTuple(writer.getSchema());
    row.set(0, new String("val1"));

    SortedMap<String, String> map = new TreeMap<String, String>();
    map.put("john", "boy");
    row.set(1, map);

    ins.insert(new BytesWritable("key".getBytes()), row);

    ins.close();
    close();
  }

  @Test
  public void testInsert2Rows() throws IOException, ExecException,
    ParseException {
    fs.delete(path, true);
    System.out.println("testInsert2Rows");
    writer = new ColumnGroup.Writer(path, "abc:string, def:map(string)", false, path.getName(), "pig", "gz",
        null, null, (short) -1, true, conf);
    TableInserter ins = writer.getInserter("part1", true);

    // row 1
    Tuple row = TypesUtils.createTuple(writer.getSchema());
    row.set(0, new String("val1"));

    SortedMap<String, String> map = new TreeMap<String, String>();
    map.put("john", "boy");
    row.set(1, map);

    ins.insert(new BytesWritable("key".getBytes()), row);

    // row 2
    TypesUtils.resetTuple(row);
    row.set(0, new String("val2"));
    map.put("joe", "boy");
    map.put("jane", "girl");
    // map should contain 3 k->v pairs
    row.set(1, map);

    ins.insert(new BytesWritable("key".getBytes()), row);
    ins.close();

    ins.close();
    close();
  }

  @Test
  public void testInsert2Inserters() throws IOException, ExecException,
    ParseException {
    fs.delete(path, true);
    System.out.println("testInsert2Inserters");
    writer = new ColumnGroup.Writer(path, "abc:string, def:map(string)", false, path.getName(), "pig", "gz",
        null, null, (short) -1, true, conf);
    TableInserter ins1 = writer.getInserter("part1", true);
    TableInserter ins2 = writer.getInserter("part2", true);

    // row 1
    Tuple row = TypesUtils.createTuple(writer.getSchema());
    row.set(0, new String("val1"));

    SortedMap<String, String> map = new TreeMap<String, String>();
    map.put("john", "boy");

    ins1.insert(new BytesWritable("key11".getBytes()), row);
    ins2.insert(new BytesWritable("key21".getBytes()), row);

    // row 2
    TypesUtils.resetTuple(row);
    row.set(0, new String("val2"));
    map.put("joe", "boy");
    map.put("jane", "girl");
    // map should contain 3 k->v pairs
    row.set(1, map);

    ins2.insert(new BytesWritable("key22".getBytes()), row);
    // ins2.close();
    ins1.insert(new BytesWritable("key12".getBytes()), row);

    ins1.close();
    ins2.close();
    close();
  }

  @Test
  public void testFailureOverlappingKeys() throws IOException, ExecException,
    ParseException {
    fs.delete(path, true);
    System.out.println("testFailureOverlappingKeys");
    writer = new ColumnGroup.Writer(path, "abc:string, def:map(string)", true, path.getName(), "pig", "gz",
        null, null, (short) -1, true, conf);
    TableInserter ins1 = writer.getInserter("part1", false);
    TableInserter ins2 = writer.getInserter("part2", false);

    // row 1

    Tuple row = TypesUtils.createTuple(writer.getSchema());
    row.set(0, new String("val1"));

    SortedMap<String, String> map = new TreeMap<String, String>();
    map.put("john", "boy");
    row.set(1, map);

    ins1.insert(new BytesWritable("key1".getBytes()), row);
    ins2.insert(new BytesWritable("key2".getBytes()), row);

    // row 2
    TypesUtils.resetTuple(row);
    row.set(0, new String("val2"));
    map.put("joe", "boy");
    map.put("jane", "girl");
    // map should contain 3 k->v pairs
    row.set(1, map);

    ins2.insert(new BytesWritable("key3".getBytes()), row);
    // ins2.close();
    ins1.insert(new BytesWritable("key4".getBytes()), row);
    try {
      ins1.close();
      ins2.close();
      close();
      Assert.fail("Failed to detect overlapping keys.");
    } catch (IOException e) {
      // noop, exceptions expected
    } finally {
      ColumnGroup.drop(path, conf);
    }
  }

  private static void finish() throws IOException {
    if (writer != null) {
      writer.finish();
    }
  }

  private static void close() throws IOException {
    if (writer != null) {
      writer.close();
      writer = null;
      ColumnGroup.drop(path, conf);
    }
  }
}

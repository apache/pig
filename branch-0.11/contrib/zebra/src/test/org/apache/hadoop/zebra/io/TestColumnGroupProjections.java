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
import java.util.ArrayList;
import java.util.List;

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
import org.apache.hadoop.zebra.io.TableScanner;
import org.apache.hadoop.zebra.parser.ParseException;
import org.apache.hadoop.zebra.schema.Schema;
import org.apache.hadoop.zebra.types.TypesUtils;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestColumnGroupProjections {
  final static String outputFile = "TestColumnGroupProjections";
  final static private Configuration conf = new Configuration();
  private static FileSystem fs;
  private static Path path;
  private static Schema schema;

  @BeforeClass
  public static void setUpOnce() throws IOException, ParseException {
    // set default file system to local file system
    conf.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");

    // must set a conf here to the underlying FS, or it barks
    RawLocalFileSystem rawLFS = new RawLocalFileSystem();
    rawLFS.setConf(conf);
    fs = new LocalFileSystem(rawLFS);
    path = new Path(fs.getWorkingDirectory(), outputFile);
    System.out.println("output file: " + path);
    
    if (fs.exists(path)) {
        ColumnGroup.drop(path, conf);
    }

    schema = new Schema("a:string,b:string,c:string,d:string,e:string,f:string,g:string");

    ColumnGroup.Writer writer = new ColumnGroup.Writer(path, schema, false, path.getName(),
        "pig", "gz", null, null, (short) -1, true, conf);
    TableInserter ins = writer.getInserter("part0", true);

    // row 1
    Tuple row = TypesUtils.createTuple(writer.getSchema());
    row.set(0, "a1");
    row.set(1, "b1");
    row.set(2, "c1");
    row.set(3, "d1");
    row.set(4, "e1");
    row.set(5, "f1");
    row.set(6, "g1");
    ins.insert(new BytesWritable("k1".getBytes()), row);

    // row 2
    TypesUtils.resetTuple(row);
    row.set(0, "a2");
    row.set(1, "b2");
    row.set(2, "c2");
    row.set(3, "d2");
    row.set(4, "e2");
    row.set(5, "f2");
    row.set(6, "g2");
    ins.insert(new BytesWritable("k2".getBytes()), row);
    ins.close();

    writer.close();
  }

  @AfterClass
  public static void tearDownOnce() throws IOException {
  }

  @Test
  // default projection (without any projection) should return every column
  public void testDefaultProjection() throws IOException, ExecException,
      ParseException {
    System.out.println("testDefaultProjection");
    // test without beginKey/endKey
    ColumnGroup.Reader reader = new ColumnGroup.Reader(path, conf);
    TableScanner scanner = reader.getScanner(null, false);

    defTest(reader, scanner);

    scanner.close();
    reader.close();
  }

  private void defTest(ColumnGroup.Reader reader, TableScanner scanner)
      throws IOException, ParseException {
    BytesWritable key = new BytesWritable();
    Tuple row = TypesUtils.createTuple(reader.getSchema());

    scanner.getKey(key);
    Assert.assertEquals(key, new BytesWritable("k1".getBytes()));
    scanner.getValue(row);
    Assert.assertEquals("a1", row.get(0));
    Assert.assertEquals("b1", row.get(1));
    Assert.assertEquals("g1", row.get(6));

    // move to next row
    scanner.advance();
    scanner.getKey(key);
    Assert.assertEquals(key, new BytesWritable("k2".getBytes()));
    TypesUtils.resetTuple(row);
    scanner.getValue(row);
    Assert.assertEquals("a2", row.get(0));
    Assert.assertEquals("b2", row.get(1));
    Assert.assertEquals("g2", row.get(6));
  }

  @Test
  // null projection should be same as default (fully projected) projection
  public void testNullProjection() throws IOException, ExecException,
      ParseException {
    System.out.println("testNullProjection");
    // test without beginKey/endKey
    ColumnGroup.Reader reader = new ColumnGroup.Reader(path, conf);
    reader.setProjection(null);
    TableScanner scanner = reader.getScanner(null, false);

    defTest(reader, scanner);

    scanner.close();
    reader.close();
  }

  @Test
  // empty projection should project no columns at all
  public void testEmptyProjection() throws Exception {
    System.out.println("testEmptyProjection");
    // test without beginKey/endKey
    ColumnGroup.Reader reader = new ColumnGroup.Reader(path, conf);
    reader.setProjection("");

    Tuple row = TypesUtils.createTuple(reader.getSchema());
    TableScanner scanner = reader.getScanner(null, false);
    BytesWritable key = new BytesWritable();

    scanner.getKey(key);
    Assert.assertEquals(key, new BytesWritable("k1".getBytes()));
    scanner.getValue(row);
    try {
      row.get(0);
      Assert.fail("Failed to catch out of boundary exceptions.");
    } catch (IndexOutOfBoundsException e) {
      // no op, expecting out of bounds exceptions
    }

    scanner.advance();
    scanner.getKey(key);
    Assert.assertEquals(key, new BytesWritable("k2".getBytes()));
    TypesUtils.resetTuple(row);
    scanner.getValue(row);
    // Assert.assertEquals("c2", row.get(0));

    scanner.close();
    reader.close();
  }

  @Test
  // a column name that does not exist in the column group
  public void testOneNonExistentProjection() throws Exception {
    System.out.println("testOneNonExistentProjection");
    // test without beginKey/endKey
    ColumnGroup.Reader reader = new ColumnGroup.Reader(path, conf);
    reader.setProjection("X");

    Tuple row = TypesUtils.createTuple(1);
    TableScanner scanner = reader.getScanner(null, false);
    BytesWritable key = new BytesWritable();

    scanner.getKey(key);
    Assert.assertEquals(key, new BytesWritable("k1".getBytes()));
    scanner.getValue(row);
    Assert.assertNull(row.get(0));
    try {
      row.get(1);
      Assert.fail("Failed to catch out of boundary exceptions.");
    } catch (IndexOutOfBoundsException e) {
      // no op, expecting out of bounds exceptions
    }

    scanner.advance();
    scanner.getKey(key);
    Assert.assertEquals(key, new BytesWritable("k2".getBytes()));
    TypesUtils.resetTuple(row);
    scanner.getValue(row);
    // Assert.assertEquals("c2", row.get(0));

    scanner.close();
    reader.close();
  }

  @Test
  // normal one column projection
  public void testOneColumnProjection() throws Exception {
    System.out.println("testOneColumnProjection");
    // test without beginKey/endKey
    ColumnGroup.Reader reader = new ColumnGroup.Reader(path, conf);
    reader.setProjection("c");

    Tuple row = TypesUtils.createTuple(reader.getSchema());
    TableScanner scanner = reader.getScanner(null, false);
    BytesWritable key = new BytesWritable();

    scanner.getKey(key);
    Assert.assertEquals(key, new BytesWritable("k1".getBytes()));
    scanner.getValue(row);
    Assert.assertEquals("c1", row.get(0));
    try {
      row.get(1);
      Assert.fail("Failed to catch 'out of boundary' exceptions.");
    } catch (IndexOutOfBoundsException e) {
      // no op, expecting out of bounds exceptions
    }

    scanner.advance();
    scanner.getKey(key);
    Assert.assertEquals(key, new BytesWritable("k2".getBytes()));
    TypesUtils.resetTuple(row);
    scanner.getValue(row);
    Assert.assertEquals("c2", row.get(0));

    scanner.close();
    reader.close();
  }

  @Test
  // normal one column plus a non-existent column projection
  public void testOnePlusNonProjection() throws Exception {
    System.out.println("testOnePlusNonProjection");
    ColumnGroup.Reader reader = new ColumnGroup.Reader(path, conf);
    reader.setProjection(",f");

    Tuple row = TypesUtils.createTuple(reader.getSchema());
    TableScanner scanner = reader.getScanner(null, false);
    BytesWritable key = new BytesWritable();

    scanner.getKey(key);
    Assert.assertEquals(key, new BytesWritable("k1".getBytes()));
    scanner.getValue(row);
    Assert.assertNull(row.get(0));
    Assert.assertEquals("f1", row.get(1));
    try {
      row.get(2);
      Assert.fail("Failed to catch 'out of boundary' exceptions.");
    } catch (IndexOutOfBoundsException e) {
      // no op, expecting out of bounds exceptions
    }

    scanner.advance();
    scanner.getKey(key);
    Assert.assertEquals(key, new BytesWritable("k2".getBytes()));
    TypesUtils.resetTuple(row);
    scanner.getValue(row);
    Assert.assertEquals("f2", row.get(1));

    scanner.close();
    reader.close();
  }

  @Test
  // two normal columns projected
  public void testTwoColumnsProjection() throws Exception {
    System.out.println("testTwoColumnsProjection");
    ColumnGroup.Reader reader = new ColumnGroup.Reader(path, conf);
    reader.setProjection("f,a");

    Tuple row = TypesUtils.createTuple(reader.getSchema());
    TableScanner scanner = reader.getScanner(null, false);
    BytesWritable key = new BytesWritable();

    scanner.getKey(key);
    Assert.assertEquals(key, new BytesWritable("k1".getBytes()));
    scanner.getValue(row);
    Assert.assertEquals("f1", row.get(0));
    Assert.assertEquals("a1", row.get(1));
    try {
      row.get(2);
      Assert.fail("Failed to catch 'out of boundary' exceptions.");
    } catch (IndexOutOfBoundsException e) {
      // no op, expecting out of bounds exceptions
    }

    scanner.advance();
    scanner.getKey(key);
    Assert.assertEquals(key, new BytesWritable("k2".getBytes()));
    TypesUtils.resetTuple(row);
    scanner.getValue(row);
    Assert.assertEquals("f2", row.get(0));
    Assert.assertEquals("a2", row.get(1));

    scanner.close();
    reader.close();
  }
}

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

import junit.framework.Assert;
import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.zebra.io.ColumnGroup;
import org.apache.hadoop.zebra.io.TableInserter;
import org.apache.hadoop.zebra.parser.ParseException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestColumnGroupOpen {
  final static String outputFile = "TestColumnGroupOpen";
  final private static Configuration conf = new Configuration();
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
  public void testNew() throws IOException, ParseException {
    System.out.println("testNew");
    writer = new ColumnGroup.Writer(path, "abc, def ", false, path.getName(), "pig", "gz",
        null, null, (short) -1, true, conf);
    // NOTE: don't call writer.close() here
    close();
  }

  @Test
  public void testFailureExistingSortedDiff() throws IOException,
    ParseException {
    System.out.println("testFailureExistingSortedDiff");
    try {
      writer = new ColumnGroup.Writer(path, "abc, def ", false, path.getName(), "pig", "gz",
          null, null, (short) -1, true, conf);
      finish();
      writer = new ColumnGroup.Writer(path, "abc, def", true, path.getName(), "pig", "gz",
          null, null, (short) -1, false, conf);
      Assert.fail("Failed to catch sorted flag alteration.");
    } catch (IOException e) {
      // noop, expecting exceptions
    } finally {
      close();
    }
  }

  @Test
  public void testExisting() throws IOException, ParseException {
    System.out.println("testExisting");
    writer = new ColumnGroup.Writer(path, " abc ,  def ", false, path.getName(), "pig", "gz",
        null, null, (short) -1, false, conf);
    writer.close();
    close();
  }

  @Test
  public void testFailurePathNotDir() throws IOException, ParseException {
    System.out.println("testFailurePathNotDir");
    try {
      // fs.delete(path, true);
      ColumnGroup.drop(path, conf);

      FSDataOutputStream in = fs.create(path);
      in.close();
      writer = new ColumnGroup.Writer(path, "   abc ,  def   ", false, path.getName(), "pig",
          "gz", null, null, (short) -1, false, conf);
      Assert.fail("Failed to catch path not a directory.");
    } catch (IOException e) {
      // noop, expecting exceptions
    } finally {
      close();
    }
  }

  @Test
  public void testFailureMetaFileExists() throws IOException, ParseException {
    System.out.println("testFailureMetaFileExists");
    try {
      fs.delete(path, true);
      FSDataOutputStream in = fs.create(new Path(path, ColumnGroup.META_FILE));
      in.close();
      writer = new ColumnGroup.Writer(path, "abc", false, path.getName(), "pig", "gz", null, null, (short) -1, false,
          conf);
      Assert.fail("Failed to catch meta file existence.");
    } catch (IOException e) {
      // noop, expecting exceptions
    } finally {
      close();
    }
  }

  @Test
  public void testFailureDiffSchema() throws IOException, ParseException {
    System.out.println("testFailureDiffSchema");
    try {
      writer = new ColumnGroup.Writer(path, "abc", false, path.getName(), "pig", "gz", null, null, (short) -1, false,
          conf);
      writer.finish();
      writer = new ColumnGroup.Writer(path, "efg", false, path.getName(), "pig", "gz", null, null, (short) -1, false,
          conf);
      Assert.fail("Failed to catch schema differences.");
    } catch (IOException e) {
      // noop, expecting exceptions
    } finally {
      close();
    }
  }

  @Test
  public void testMultiWriters() throws IOException, ParseException {
    System.out.println("testMultiWriters");
    ColumnGroup.Writer writer1 = null;
    ColumnGroup.Writer writer2 = null;
    ColumnGroup.Writer writer3 = null;
    try {
      writer1 = new ColumnGroup.Writer(path, "abc", false, path.getName(), "pig", "gz", null, null, (short) -1, true,
          conf);
      writer2 = new ColumnGroup.Writer(path, conf);
      writer3 = new ColumnGroup.Writer(path, conf);

      TableInserter ins1 = writer1.getInserter("part1", false);
      TableInserter ins2 = writer2.getInserter("part2", false);
      TableInserter ins3 = writer3.getInserter("part3", false);
      ins1.close();
      ins2.close();
      ins3.close();
      // }
      // catch (IOException e) {
      // // noop, expecting exceptions
      // throw e;
    } finally {
      if (writer1 != null) {
        writer1.finish();
      }
      if (writer2 != null) {
        writer2.finish();
      }
      if (writer3 != null) {
        writer3.finish();
      }
      close();
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
    }
    ColumnGroup.drop(path, conf);
  }
}

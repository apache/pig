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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

import junit.framework.Assert;
import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.zebra.tfile.RawComparable;
import org.apache.hadoop.zebra.io.BasicTable;
import org.apache.hadoop.zebra.io.BasicTableStatus;
import org.apache.hadoop.zebra.io.KeyDistribution;
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
import org.junit.BeforeClass;
import org.junit.Test;

public class TestBasicTable {
  public static Configuration conf;
  public static Random random;
  public static Path rootPath;
  public static FileSystem fs;

  @BeforeClass
  public static void setUpOnce() throws IOException {
    conf = new Configuration();
    conf.setInt("table.output.tfile.minBlock.size", 64 * 1024);
    conf.setInt("table.input.split.minSize", 64 * 1024);
    conf.set("table.output.tfile.compression", "none");
    random = new Random(System.nanoTime());
    rootPath = new Path(System.getProperty("test.build.data",
        "build/test/data/work-dir"));
    fs = rootPath.getFileSystem(conf);
  }

  @BeforeClass
  public static void tearDownOnce() throws IOException {
  }

  static BytesWritable makeRandomKey(int max) {
    return makeKey(random.nextInt(max));
  }

  static BytesWritable makeKey(int i) {
    return new BytesWritable(String.format("key%09d", i).getBytes());
  }

  static String makeString(String prefix, int max) {
    return String.format("%s%09d", prefix, random.nextInt(max));
  }

  public static int createBasicTable(int parts, int rows, String strSchema, String storage, String sortColumns,
      Path path, boolean properClose) throws IOException {
    if (fs.exists(path)) {
      BasicTable.drop(path, conf);
    }

    BasicTable.Writer writer = new BasicTable.Writer(path, strSchema, storage, sortColumns, null, conf);
    writer.finish();

    int total = 0;
    Schema schema = writer.getSchema();
    String colNames[] = schema.getColumns();
    Tuple tuple = TypesUtils.createTuple(schema);

    boolean sorted = writer.isSorted();
    for (int i = 0; i < parts; ++i) {
      writer = new BasicTable.Writer(path, conf);
      TableInserter inserter = writer.getInserter(
          String.format("part-%06d", i), true);
      if (rows > 0) {
        int actualRows = random.nextInt(rows) + rows / 2;
        for (int j = 0; j < actualRows; ++j, ++total) {
          BytesWritable key;
          if (!sorted) {
            key = makeRandomKey(rows * 10);
          } else {
            key = makeKey(total);
          }
          TypesUtils.resetTuple(tuple);
          for (int k = 0; k < tuple.size(); ++k) {
            try {
              tuple.set(k, new DataByteArray(makeString("col-" + colNames[k], rows * 10).getBytes()));
            } catch (ExecException e) {
              e.printStackTrace();
            }
          }
          inserter.insert(key, tuple);
        }
      }
      inserter.close();
    }

    if (properClose) {
      writer = new BasicTable.Writer(path, conf);
      writer.close();
      /* We can only test number of rows on sorted tables.*/
      if (sorted) {
        BasicTableStatus status = getStatus(path);
        Assert.assertEquals(total, status.getRows());
      }
    }

    return total;
  }

  static void rangeSplitBasicTable(int numSplits, int totalRows, String strProjection,
      Path path) throws IOException, ParseException {
    BasicTable.Reader reader = new BasicTable.Reader(path, conf);
    reader.setProjection(strProjection);
    long totalBytes = reader.getStatus().getSize();

    List<RangeSplit> splits = reader.rangeSplit(numSplits);
    reader.close();
    int total = 0;
    for (int i = 0; i < splits.size(); ++i) {
      reader = new BasicTable.Reader(path, conf);
      reader.setProjection(strProjection);
      total += doReadOnly(reader.getScanner(splits.get(i), true));
      totalBytes -= reader.getBlockDistribution(splits.get(i)).getLength();
    }
    Assert.assertEquals(total, totalRows);
    Assert.assertEquals(0L, totalBytes);
    // TODO: verify tuples contains the right projected values
  }

  static void doRangeSplit(int[] numSplits, int totalRows, String projection, Path path)
      throws IOException, ParseException {
    for (int i : numSplits) {
      if (i > 0) {
        rangeSplitBasicTable(i, totalRows, projection, path);
      }
    }
  }

  static void keySplitBasicTable(int numSplits, int totalRows, String strProjection,
      Path path) throws IOException, ParseException {
    BasicTable.Reader reader = new BasicTable.Reader(path, conf);
    reader.setProjection(strProjection);
    long totalBytes = reader.getStatus().getSize();
    BlockDistribution lastBd = new BlockDistribution();
    KeyDistribution keyDistri = reader.getKeyDistribution(numSplits * 10, 1, lastBd);
    Assert.assertEquals(totalBytes, keyDistri.length()+lastBd.getLength());
    reader.close();
    BytesWritable[] keys = null;
    if (keyDistri.size() >= numSplits) {
      keyDistri.resize(lastBd);
      Assert.assertEquals(totalBytes, keyDistri.length()+lastBd.getLength());
      RawComparable[] rawComparables = keyDistri.getKeys();
      keys = new BytesWritable[rawComparables.length];
      for (int i = 0; i < keys.length; ++i) {
        keys[i] = new BytesWritable();
        keys[i].setSize(rawComparables[i].size());
        System.arraycopy(rawComparables[i].buffer(),
            rawComparables[i].offset(), keys[i].get(), 0, rawComparables[i]
                .size());
      }
    } else {
      int targetSize = Math.min(totalRows / 10, numSplits);
      // revert to manually cooked up keys.
      Set<Integer> keySets = new TreeSet<Integer>();
      while (keySets.size() < targetSize) {
        keySets.add(random.nextInt(totalRows));
      }
      keys = new BytesWritable[targetSize];
      if (!keySets.isEmpty()) {
        int j = 0;
        for (int i : keySets.toArray(new Integer[keySets.size()])) {
          keys[j] = makeKey(i);
          ++j;
        }
      }
    }

    int total = 0;
    for (int i = 0; i < keys.length; ++i) {
      reader = new BasicTable.Reader(path, conf);
      reader.setProjection(strProjection);
      BytesWritable begin = (i == 0) ? null : keys[i - 1];
      BytesWritable end = (i == keys.length - 1) ? null : keys[i];
      total += doReadOnly(reader.getScanner(begin, end, true));
    }
    Assert.assertEquals(total, totalRows);
  }

  static void doKeySplit(int[] numSplits, int totalRows, String projection, Path path)
      throws IOException, ParseException {
    for (int i : numSplits) {
      if (i > 0) {
        keySplitBasicTable(i, totalRows, projection, path);
      }
    }
  }

  static BasicTableStatus getStatus(Path path) throws IOException {
    BasicTable.Reader reader = new BasicTable.Reader(path, conf);
    try {
      return reader.getStatus();
    } finally {
      reader.close();
    }
  }

  static void doReadWrite(Path path, int parts, int rows, String schema,
      String storage, String sortColumns, String projection, boolean properClose, boolean sorted)
      throws IOException, ParseException {
    int totalRows = createBasicTable(parts, rows, schema, storage, sortColumns, path,
        properClose);
    if (rows == 0) {
      Assert.assertEquals(rows, 0);
    }

    doRangeSplit(new int[] { 1, 2, parts / 2, parts, 2 * parts }, totalRows,
        projection, path);
    if (sorted) {
      doKeySplit(new int[] { 1, 2, parts / 2, parts, 2 * parts, 10 * parts },
          totalRows, projection, path);
    }
  }

  public void testMultiCGs() throws IOException, ParseException {
    Path path = new Path(rootPath, "TestBasicTableMultiCGs");
    doReadWrite(path, 2, 100, "SF_a,SF_b,SF_c,SF_d,SF_e", "[SF_a,SF_b,SF_c];[SF_d,SF_e]", null, "SF_f,SF_a,SF_c,SF_d", true, false);
  }

  public void testCornerCases() throws IOException, ParseException {
    Path path = new Path(rootPath, "TestBasicTableCornerCases");
    doReadWrite(path, 0, 0, "a, b, c", "", null, "a, d, c, f", false, false);
    doReadWrite(path, 0, 0, "a, b, c", "", null, "a, d, c, f", true, false);
    doReadWrite(path, 0, 0, "a, b, c", "", "a", "a, d, c, f", true, true);
    doReadWrite(path, 2, 0, "a, b, c", "", null, "a, d, c, f", false, false);
    doReadWrite(path, 2, 0, "a, b, c", "", null, "a, d, c, f", true, false);
    doReadWrite(path, 2, 0, "a, b, c", "", "a", "a, d, c, f", true, true);
  }

  static int doReadOnly(TableScanner scanner) throws IOException, ParseException {
    int total = 0;
    BytesWritable key = new BytesWritable();
    Tuple value = TypesUtils.createTuple(scanner.getSchema());
    for (; !scanner.atEnd(); scanner.advance()) {
      ++total;
      switch (random.nextInt() % 4) {
      case 0:
        scanner.getKey(key);
        break;
      case 1:
        scanner.getValue(value);
        break;
      case 2:
        scanner.getKey(key);
        scanner.getValue(value);
        break;
      default: // no-op.
      }
    }
    scanner.close();

    return total;
  }

  @Test
  public void testNullSplits() throws IOException, ParseException {
    Path path = new Path(rootPath, "TestBasicTableNullSplits");
    int totalRows = createBasicTable(2, 250, "a, b, c", "", "a", path, true);
    BasicTable.Reader reader = new BasicTable.Reader(path, conf);
    reader.setProjection("a,d,c,f");
    Assert.assertEquals(totalRows, doReadOnly(reader.getScanner(null, false)));
    Assert.assertEquals(totalRows, doReadOnly(reader.getScanner(null, null,
        false)));
    reader.close();
  }

  @Test
  public void testNegativeSplits() throws IOException, ParseException {
    Path path = new Path(rootPath, "TestNegativeSplits");
    int totalRows = createBasicTable(2, 250, "a, b, c", "", "", path, true);
    rangeSplitBasicTable(-1, totalRows, "a,d,c,f", path);
  }

  @Test
  public void testMetaBlocks() throws IOException, ParseException {
    Path path = new Path(rootPath, "TestBasicTableMetaBlocks");
    createBasicTable(3, 100, "a, b, c", "", null, path, false);
    BasicTable.Writer writer = new BasicTable.Writer(path, conf);
    BytesWritable meta1 = makeKey(1234);
    BytesWritable meta2 = makeKey(9876);
    DataOutputStream dos = writer.createMetaBlock("testMetaBlocks.meta1");
    try {
      meta1.write(dos);
    } finally {
      dos.close();
    }
    dos = writer.createMetaBlock("testMetaBlocks.meta2");
    try {
      meta2.write(dos);
    } finally {
      dos.close();
    }
    writer.close();

    BasicTable.Reader reader = new BasicTable.Reader(path, conf);
    reader.setProjection("a,d,c,f");
    BytesWritable tmp = new BytesWritable();
    DataInputStream dis = reader.getMetaBlock("testMetaBlocks.meta1");
    try {
      tmp.readFields(dis);
      Assert.assertTrue(tmp.compareTo(meta1) == 0);
    } finally {
      dis.close();
    }

    dis = reader.getMetaBlock("testMetaBlocks.meta2");
    try {
      tmp.readFields(dis);
      Assert.assertTrue(tmp.compareTo(meta2) == 0);
    } finally {
      dis.close();
    }
    reader.close();
  }

  @Test
  public void testNormalCases() throws IOException, ParseException {
    Path path = new Path(rootPath, "TestBasicTableNormal");
    doReadWrite(path, 2, 250, "a, b, c", "", null, "a, d, c, f", true, false);
    doReadWrite(path, 2, 250, "a, b, c", "", null, "a, d, c, f", true, false);
    doReadWrite(path, 2, 250, "a, b, c", "", "a", "a, d, c, f", true, true);
  }
}

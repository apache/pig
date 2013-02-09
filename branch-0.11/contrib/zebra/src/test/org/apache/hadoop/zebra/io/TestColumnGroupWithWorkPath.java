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
import java.util.HashSet;
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
import org.apache.hadoop.zebra.io.BasicTableStatus;
import org.apache.hadoop.zebra.io.ColumnGroup;
import org.apache.hadoop.zebra.io.KeyDistribution;
import org.apache.hadoop.zebra.io.TableInserter;
import org.apache.hadoop.zebra.io.TableScanner;
import org.apache.hadoop.zebra.io.ColumnGroup.Reader.CGRangeSplit;
import org.apache.hadoop.zebra.parser.ParseException;
import org.apache.hadoop.zebra.schema.Schema;
import org.apache.hadoop.zebra.types.TypesUtils;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Testing ColumnGroup APIs called as if in MapReduce Jobs
 */
public class TestColumnGroupWithWorkPath {
  static Configuration conf;
  static Random random;
  static Path rootPath;
  static FileSystem fs;

  @BeforeClass
  public static void setUpOnce() throws IOException {
    conf = new Configuration();
    conf.setInt("table.output.tfile.minBlock.size", 64 * 1024);
    conf.setInt("table.input.split.minSize", 64 * 1024);
    conf.set("table.output.tfile.compression", "none");
    conf.set("io.compression.codec.lzo.class", "no");
    random = new Random(System.nanoTime());
    rootPath = new Path(System.getProperty("test.build.data",
        "build/test/data/workdir3"));
    fs = rootPath.getFileSystem(conf);
  }

  @AfterClass
  public static void tearDownOnce() throws IOException {
  }

  BytesWritable makeRandomKey(int max) {
    return makeKey(random.nextInt(max));
  }

  static BytesWritable makeKey(int i) {
    return new BytesWritable(String.format("key%09d", i).getBytes());
  }

  String makeString(String prefix, int max) {
    return String.format("%s%09d", prefix, random.nextInt(max));
  }

  int createCG(int parts, int rows, String strSchema, Path path,
      boolean properClose, boolean sorted, int[] emptyTFiles)
      throws IOException, ParseException {
    if (fs.exists(path)) {
      ColumnGroup.drop(path, conf);
    }

    Set<Integer> emptyTFileSet = new HashSet<Integer>();
    if (emptyTFiles != null) {
      for (int i = 0; i < emptyTFiles.length; ++i) {
        emptyTFileSet.add(emptyTFiles[i]);
      }
    }

    ColumnGroup.Writer writer = new ColumnGroup.Writer(path, strSchema, sorted, path.getName(),
        "pig", "gz", "root", null, (short) Short.parseShort("755", 8), false, conf);

    writer.finish();

    int total = 0;
    Schema schema = new Schema(strSchema);
    String colNames[] = schema.getColumns();
    Tuple tuple = TypesUtils.createTuple(schema);
    int[] permutation = new int[parts];
    for (int i = 0; i < parts; ++i) {
      permutation[i] = i;
    }

    for (int i = parts - 1; i > 0; --i) {
      int targetIndex = random.nextInt(i + 1);
      int tmp = permutation[i];
      permutation[i] = permutation[targetIndex];
      permutation[targetIndex] = tmp;
    }

    for (int i = 0; i < parts; ++i) {
      Path workPath = new Path(path.getParent(), "_temporary");
      writer = new ColumnGroup.Writer(path, workPath, conf);
      TableInserter inserter = writer.getInserter(String.format("part-%06d",
          permutation[i]), true);
      if ((rows > 0) && !emptyTFileSet.contains(permutation[i])) {
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
              tuple.set(k, makeString("col-" + colNames[k], rows * 10));
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
      writer = new ColumnGroup.Writer(path, conf);
      writer.close();
      /* We can only test number of rows on sorted tables.*/
      if (sorted) {
        BasicTableStatus status = getStatus(path);
        Assert.assertEquals(total, status.getRows());
      }
    }

    return total;
  }

  static class DupKeyGen {
    int low, high;
    int current;
    boolean grow = true;
    int index = 0;
    int count = 0;

    DupKeyGen(int low, int high) {
      this.low = Math.max(10, low);
      this.high = Math.max(this.low * 2, high);
      current = this.low;
    }

    BytesWritable next() {
      if (count == 0) {
        count = nextCount();
        ++index;
      }
      --count;
      return makeKey(index);
    }

    int nextCount() {
      int ret = current;
      if ((grow && current > high) || (!grow && current < low)) {
        grow = !grow;
      }
      if (grow) {
        current *= 2;
      } else {
        current /= 2;
      }
      return ret;
    }
  }

  int createCGDupKeys(int parts, int rows, String strSchema, Path path)
      throws IOException, ParseException {
    if (fs.exists(path)) {
      ColumnGroup.drop(path, conf);
    }

    ColumnGroup.Writer writer = new ColumnGroup.Writer(path, strSchema, true, path.getName(),
        "pig", "gz", "root", null, (short) Short.parseShort("777", 8), false, conf);
    writer.finish();

    int total = 0;
    DupKeyGen keyGen = new DupKeyGen(10, rows * 3);
    Schema schema = new Schema(strSchema);
    String colNames[] = schema.getColumns();
    Tuple tuple = TypesUtils.createTuple(schema);
    int[] permutation = new int[parts];
    for (int i = 0; i < parts; ++i) {
      permutation[i] = i;
    }

    for (int i = parts - 1; i > 0; --i) {
      int targetIndex = random.nextInt(i + 1);
      int tmp = permutation[i];
      permutation[i] = permutation[targetIndex];
      permutation[targetIndex] = tmp;
    }

    for (int i = 0; i < parts; ++i) {
      writer = new ColumnGroup.Writer(path, conf);
      TableInserter inserter = writer.getInserter(String.format("part-%06d",
          permutation[i]), true);
      if (rows > 0) {
        int actualRows = random.nextInt(rows * 2 / 3) + rows * 2 / 3;
        for (int j = 0; j < actualRows; ++j, ++total) {
          BytesWritable key = keyGen.next();
          TypesUtils.resetTuple(tuple);
          for (int k = 0; k < tuple.size(); ++k) {
            try {
              tuple.set(k, makeString("col-" + colNames[k], rows * 10));
            } catch (ExecException e) {
              e.printStackTrace();
            }
          }
          inserter.insert(key, tuple);
        }
      }
      inserter.close();
    }

    writer = new ColumnGroup.Writer(path, conf);
    writer.close();
    BasicTableStatus status = getStatus(path);
    Assert.assertEquals(total, status.getRows());

    return total;
  }

  void rangeSplitCG(int numSplits, int totalRows, String strProjection,
      Path path) throws IOException, ParseException {
    ColumnGroup.Reader reader = new ColumnGroup.Reader(path, conf);
    reader.setProjection(strProjection);
    long totalBytes = reader.getStatus().getSize();

    List<CGRangeSplit> splits = reader.rangeSplit(numSplits);
    reader.close();
    int total = 0;
    for (int i = 0; i < splits.size(); ++i) {
      reader = new ColumnGroup.Reader(path, conf);
      reader.setProjection(strProjection);
      total += doReadOnly(reader.getScanner(splits.get(i), true));
      totalBytes -= reader.getBlockDistribution(splits.get(i)).getLength();
    }
    Assert.assertEquals(total, totalRows);
    Assert.assertEquals(totalBytes, 0L);
  }

  void doRangeSplit(int[] numSplits, int totalRows, String projection, Path path)
      throws IOException, ParseException {
    for (int i : numSplits) {
      if (i > 0) {
        rangeSplitCG(i, totalRows, projection, path);
      }
    }
  }

  void keySplitCG(int numSplits, int totalRows, String strProjection, Path path)
      throws IOException, ParseException {
    ColumnGroup.Reader reader = new ColumnGroup.Reader(path, conf);
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
      reader = new ColumnGroup.Reader(path, conf);
      reader.setProjection(strProjection);
      BytesWritable begin = (i == 0) ? null : keys[i - 1];
      BytesWritable end = (i == keys.length - 1) ? null : keys[i];
      total += doReadOnly(reader.getScanner(begin, end, true));
    }
    Assert.assertEquals(total, totalRows);
  }

  void doKeySplit(int[] numSplits, int totalRows, String projection, Path path)
      throws IOException, ParseException {
    for (int i : numSplits) {
      if (i > 0) {
        keySplitCG(i, totalRows, projection, path);
      }
    }
  }

  BasicTableStatus getStatus(Path path) throws IOException, ParseException {
    ColumnGroup.Reader reader = new ColumnGroup.Reader(path, conf);
    try {
      return reader.getStatus();
    } finally {
      reader.close();
    }
  }

  void doReadWrite(Path path, int parts, int rows, String schema,
      String projection, boolean properClose, boolean sorted, int[] emptyTFiles)
      throws IOException, ParseException {
    int totalRows = createCG(parts, rows, schema, path, properClose, sorted,
        emptyTFiles);
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

  int doReadOnly(TableScanner scanner) throws IOException, ParseException {
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
    Path path = new Path(rootPath, "TestColumnGroupNullSplits");
    int totalRows = createCG(2, 10, "a:string, b:string, c:string", path, true, true, null);
    ColumnGroup.Reader reader = new ColumnGroup.Reader(path, conf);
    reader.setProjection("a,d,c,f");
    Assert.assertEquals(totalRows, doReadOnly(reader.getScanner(null, false)));
    Assert.assertEquals(totalRows, doReadOnly(reader.getScanner(null, null,
        false)));
    reader.close();
  }

  @Test
  public void testNegativeSplits() throws IOException, ParseException {
    Path path = new Path(rootPath, "TestNegativeSplits");
    int totalRows = createCG(2, 100, "a:string, b:string, c:string", path, true, true, null);
    rangeSplitCG(-1, totalRows, "a,d,c,f", path);
  }

  @Test
  public void testEmptyCG() throws IOException, ParseException {
    Path path = new Path(rootPath, "TestColumnGroupEmptyCG");
    doReadWrite(path, 0, 0, "a:string, b:string, c:string", "a, d, c, f", true, false, null);
    doReadWrite(path, 0, 0, "a:string, b:string, c:string", "a, d, c, f", true, true, null);
  }

  @Test
  public void testEmptyTFiles() throws IOException, ParseException {
    Path path = new Path(rootPath, "TestColumnGroupEmptyTFile");
    doReadWrite(path, 2, 0, "a:string, b:string, c:string", "a, d, c, f", true, false, null);
    doReadWrite(path, 2, 0, "a:string, b:string, c:string", "a, d, c, f", true, true, null);    
  }

  public void testNormalCases() throws IOException, ParseException {
    Path path = new Path(rootPath, "TestColumnGroupNormal");
    doReadWrite(path, 2, 500, "a:string, b:string, c:string", "a, d, c, f", true, false, null);
    doReadWrite(path, 2, 500, "a:string, b:string, c:string", "a, d, c, f", true, true, null);
  }

  @Test
  public void testSomeEmptyTFiles() throws IOException, ParseException {
    Path path = new Path(rootPath, "TestColumnGroupSomeEmptyTFile");
		for (int[] emptyTFiles : new int[][] { { 1, 2 }}) {
      doReadWrite(path, 2, 250, "a:string, b:string, c:string", "a, d, c, f", true, false,
          emptyTFiles);
      doReadWrite(path, 2, 250, "a:string, b:string, c:string", "a, d, c, f", true, true,
          emptyTFiles);    
    }
  }

  int countRows(Path path, String projection) throws IOException,
    ParseException {
    ColumnGroup.Reader reader = new ColumnGroup.Reader(path, conf);
    if (projection != null) {
      reader.setProjection(projection);
    }
    int totalRows = 0;
    TableScanner scanner = reader.getScanner(null, true);
    for (; !scanner.atEnd(); scanner.advance()) {
      ++totalRows;
    }
    scanner.close();
    return totalRows;
  }

  @Test
  public void testProjection() throws IOException, ParseException {
    Path path = new Path(rootPath, "TestColumnGroupProjection");
    int totalRows = createCG(2, 250, "a:string, b:string, c:string", path, true, true, null);
    Assert.assertEquals(totalRows, countRows(path, null));
    Assert.assertEquals(totalRows, countRows(path, ""));
  }

  @Test
  public void testDuplicateKeys() throws IOException, ParseException {
    Path path = new Path(rootPath, "TestColumnGroupDuplicateKeys");
    int totalRows = createCGDupKeys(2, 250, "a:string, b:string, c:string", path);
    doKeySplit(new int[] { 1, 5 }, totalRows, "a, d, c, f",
        path);
  }

  @Test
  public void testSortedCGKeySplit() throws IOException, ParseException {
    conf.setInt("table.output.tfile.minBlock.size", 640 * 1024);
    Path path = new Path(rootPath, "TestSortedCGKeySplit");
    int totalRows = createCG(2, 250, "a:string, b:string, c:string", path, true, true, null);
    doKeySplit(new int[] { 1, 5 }, totalRows, "a, d, c, f",
        path);
  }
}

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.zebra.pig;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Iterator;
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
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.test.MiniCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * 
 * Test projections on complicated column types.
 * 
 */
public class TestCollection {

  final static String STR_SCHEMA = "c:collection(record(a:double, b:float, c:bytes))";
  final static String STR_STORAGE = "[c]";
  private static Configuration conf;
  private static FileSystem fs;

  protected static ExecType execType = ExecType.MAPREDUCE;
  private static MiniCluster cluster;
  protected static PigServer pigServer;
  private static Path path;

  @BeforeClass
  public static void setUpOnce() throws IOException {

    System.out.println("ONCE SETUP !! ---------");
    if (System.getProperty("hadoop.log.dir") == null) {
      String base = new File(".").getPath(); // getAbsolutePath();
      System
          .setProperty("hadoop.log.dir", new Path(base).toString() + "./logs");
    }

    if (execType == ExecType.MAPREDUCE) {
      cluster = MiniCluster.buildCluster();
      pigServer = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
    } else {
      pigServer = new PigServer(ExecType.LOCAL);
    }

    conf = new Configuration();
    FileSystem fs = cluster.getFileSystem();
    Path pathWorking = fs.getWorkingDirectory();
    // path = new Path(pathWorking, this.getClass().getSimpleName());
    path = fs.getWorkingDirectory();
    System.out.println("path =" + path);

    BasicTable.Writer writer = new BasicTable.Writer(path, STR_SCHEMA,
        STR_STORAGE, conf);
    /*
     * conf = new Configuration();
     * conf.setInt("table.output.tfile.minBlock.size", 64 * 1024);
     * conf.setInt("table.input.split.minSize", 64 * 1024);
     * conf.set("table.output.tfile.compression", "none");
     * 
     * RawLocalFileSystem rawLFS = new RawLocalFileSystem(); fs = new
     * LocalFileSystem(rawLFS); path = new Path(fs.getWorkingDirectory(),
     * this.getClass() .getSimpleName()); fs = path.getFileSystem(conf); // drop
     * any previous tables BasicTable.drop(path, conf); BasicTable.Writer writer
     * = new BasicTable.Writer(path, STR_SCHEMA, STR_STORAGE, false, conf);
     */
    /*
     * /* Schema schema = writer.getSchema(); Tuple tuple =
     * TypesUtils.createTuple(schema);
     * 
     * final int numsBatch = 10; final int numsInserters = 2; TableInserter[]
     * inserters = new TableInserter[numsInserters]; for (int i = 0; i <
     * numsInserters; i++) { inserters[i] = writer.getInserter("ins" + i,
     * false); }
     * 
     * for (int b = 0; b < numsBatch; b++) { for (int i = 0; i < numsInserters;
     * i++) { TypesUtils.resetTuple(tuple);
     * 
     * DataBag bagColl = TypesUtils.createBag(); Schema schColl =
     * schema.getColumn(0).getSchema(); Tuple tupColl1 =
     * TypesUtils.createTuple(schColl); Tuple tupColl2 =
     * TypesUtils.createTuple(schColl); byte[] abs1 = new byte[3]; byte[] abs2 =
     * new byte[4]; tupColl1.set(0, 3.1415926); tupColl1.set(1, 1.6); abs1[0] =
     * 11; abs1[1] = 12; abs1[2] = 13; tupColl1.set(2, new DataByteArray(abs1));
     * bagColl.add(tupColl1); tupColl2.set(0, 123.456789); tupColl2.set(1, 100);
     * abs2[0] = 21; abs2[1] = 22; abs2[2] = 23; abs2[3] = 24; tupColl2.set(2,
     * new DataByteArray(abs2)); bagColl.add(tupColl2); tuple.set(0, bagColl);
     * 
     * inserters[i].insert(new BytesWritable(("key" + i).getBytes()), tuple); }
     * } for (int i = 0; i < numsInserters; i++) { inserters[i].close(); }
     */

    writer.finish();

    Schema schema = writer.getSchema();
    Tuple tuple = TypesUtils.createTuple(schema);

    BasicTable.Writer writer1 = new BasicTable.Writer(path, conf);
    int part = 0;
    TableInserter inserter = writer1.getInserter("part" + part, true);
    TypesUtils.resetTuple(tuple);
    DataBag bagColl = TypesUtils.createBag();
    Schema schColl = schema.getColumn(0).getSchema().getColumn(0).getSchema();
    Tuple tupColl1 = TypesUtils.createTuple(schColl);
    Tuple tupColl2 = TypesUtils.createTuple(schColl);
    byte[] abs1 = new byte[3];
    byte[] abs2 = new byte[4];
    tupColl1.set(0, 3.1415926);
    tupColl1.set(1, 1.6);
    abs1[0] = 11;
    abs1[1] = 12;
    abs1[2] = 13;
    tupColl1.set(2, new DataByteArray(abs1));
    bagColl.add(tupColl1);
    tupColl2.set(0, 123.456789);
    tupColl2.set(1, 100);
    abs2[0] = 21;
    abs2[1] = 22;
    abs2[2] = 23;
    abs2[3] = 24;
    tupColl2.set(2, new DataByteArray(abs2));
    bagColl.add(tupColl2);
    tuple.set(0, bagColl);

    int row = 0;
    inserter.insert(new BytesWritable(String.format("k%d%d", part + 1, row + 1)
        .getBytes()), tuple);

    bagColl.clear();
    TypesUtils.resetTuple(tupColl1);
    TypesUtils.resetTuple(tupColl2);
    tupColl1.set(0, 7654.321);
    tupColl1.set(1, 0.0001);
    abs1[0] = 31;
    abs1[1] = 32;
    abs1[2] = 33;
    tupColl1.set(2, new DataByteArray(abs1));
    bagColl.add(tupColl1);
    tupColl2.set(0, 0.123456789);
    tupColl2.set(1, 0.3333);
    abs2[0] = 41;
    abs2[1] = 42;
    abs2[2] = 43;
    abs2[3] = 44;
    tupColl2.set(2, new DataByteArray(abs2));
    bagColl.add(tupColl2);
    tuple.set(0, bagColl);
    inserter.insert(new BytesWritable(String.format("k%d%d", part + 1, row + 1)
        .getBytes()), tuple);

    inserter.close();
    writer1.finish();

    writer.close();
  }

  @AfterClass
  public static void tearDownOnce() throws IOException {
    BasicTable.drop(path, conf);
  }

  @Test
  public void testRead1() throws IOException, ParseException {
    String query = "records = LOAD '" + path.toString()
        + "' USING org.apache.hadoop.zebra.pig.TableLoader('c');";
    System.out.println(query);
    pigServer.registerQuery(query);
    Iterator<Tuple> it = pigServer.openIterator("records");
    int row = 0;
    int inner = 0;
    while (it.hasNext()) {
      Tuple cur = it.next();
      row++;
      if (row == 1) {
        Iterator<Tuple> bag = ((DataBag) cur.get(0)).iterator();
        while (bag.hasNext()) {
          Tuple cur2 = bag.next();
          inner++;
          if (inner == 1) {
            Assert.assertEquals(3.1415926, cur2.get(0));
            Assert.assertEquals(1.6, cur2.get(1));
          }
          if (inner == 2) {
            Assert.assertEquals(123.456789, cur2.get(0));
            Assert.assertEquals(100, cur2.get(1));
          }
        }// inner while
      } // if count ==1
      if (row == 2) {
        Iterator<Tuple> bag = ((DataBag) cur.get(0)).iterator();
        while (bag.hasNext()) {
          Tuple cur2 = bag.next();
          inner++;
          if (inner == 1) {
            Assert.assertEquals(7654.321, cur2.get(0));
            Assert.assertEquals(0.0001, cur2.get(1));
            System.out.println("cur : " + cur2.toString());
            System.out.println("byte : " + cur2.get(2).toString());
          }
          if (inner == 2) {
            Assert.assertEquals(0.123456789, cur2.get(0));
            Assert.assertEquals(0.3333, cur2.get(1));
            System.out.println("byte : " + cur2.get(2).toString());
          }
        }// inner while
      }// if count ==2
      TypesUtils.resetTuple(cur);
    }
  }

  @Test
  // Negative none exist column, using IO layer impl
  public void testRead2() throws IOException, ParseException {
    String projection = new String("d");
    BasicTable.Reader reader = new BasicTable.Reader(path, conf);
    reader.setProjection(projection);
    List<RangeSplit> splits = reader.rangeSplit(1);
    TableScanner scanner = reader.getScanner(splits.get(0), true);
    BytesWritable key = new BytesWritable();
    Tuple RowValue = TypesUtils.createTuple(scanner.getSchema());

    scanner.getKey(key);
    // Assert.assertEquals(key, new BytesWritable("k11".getBytes()));
    scanner.getValue(RowValue);
    // RowValue is an emplty record
    Assert.assertEquals(false, RowValue.isNull());
    Assert.assertEquals(null, RowValue.get(0));
    Assert.assertEquals(1, RowValue.size());
    reader.close();
  }

  // Negative none exist column, TODO: failed, throw null pointer
  public void testReadNeg2() throws IOException, ParseException {
    String query = "records = LOAD '" + path.toString()
        + "' USING org.apache.hadoop.zebra.pig.TableLoader('d');";
    System.out.println(query);
    pigServer.registerQuery(query);
    // TODO: verify it returns a tuple with null value
  }

}
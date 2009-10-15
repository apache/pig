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

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.zebra.io.BasicTable;
import org.apache.hadoop.zebra.io.TableInserter;
import org.apache.hadoop.zebra.schema.Schema;
import org.apache.hadoop.zebra.types.TypesUtils;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.test.MiniCluster;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestCollectionTableLoader {
  protected static ExecType execType = ExecType.MAPREDUCE;
  private static MiniCluster cluster;
  protected static PigServer pigServer;
  private static Path pathTable;

  @BeforeClass
  public static void setUp() throws Exception {
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

    Configuration conf = new Configuration();
    FileSystem fs = cluster.getFileSystem();
    Path pathWorking = fs.getWorkingDirectory();
    pathTable = new Path(pathWorking, "TestCollectionTable");
    // drop any previous tables
    BasicTable.drop(pathTable, conf);

    BasicTable.Writer writer = new BasicTable.Writer(pathTable,
        "c:collection(a:double, b:float, c:bytes)", "[c]", false, conf);
    Schema schema = writer.getSchema();
    Tuple tuple = TypesUtils.createTuple(schema);

    final int numsBatch = 10;
    final int numsInserters = 2;
    TableInserter[] inserters = new TableInserter[numsInserters];
    for (int i = 0; i < numsInserters; i++) {
      inserters[i] = writer.getInserter("ins" + i, false);
    }

    for (int b = 0; b < numsBatch; b++) {
      for (int i = 0; i < numsInserters; i++) {
        TypesUtils.resetTuple(tuple);

        DataBag bagColl = TypesUtils.createBag();
        Schema schColl = schema.getColumn(0).getSchema();
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

        inserters[i].insert(new BytesWritable(("key" + i).getBytes()), tuple);
      }
    }
    for (int i = 0; i < numsInserters; i++) {
      inserters[i].close();
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    pigServer.shutdown();
  }

  @Test
  public void testReader() throws ExecException, IOException {
    String query = "records = LOAD '" + pathTable.toString()
        + "' USING org.apache.hadoop.zebra.pig.TableLoader('c');";
    System.out.println(query);
    pigServer.registerQuery(query);
    Iterator<Tuple> it = pigServer.openIterator("records");
    while (it.hasNext()) {
      Tuple cur = it.next();
      System.out.println(cur);
    }
  }
}
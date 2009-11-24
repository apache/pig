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
import java.util.List;

import junit.framework.Assert;
import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.zebra.io.BasicTable;
import org.apache.hadoop.zebra.io.TableInserter;
import org.apache.hadoop.zebra.io.TableScanner;
import org.apache.hadoop.zebra.io.BasicTable.Reader.RangeSplit;
import org.apache.hadoop.zebra.parser.ParseException;
import org.apache.hadoop.zebra.schema.Schema;
import org.apache.hadoop.zebra.types.TypesUtils;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.test.MiniCluster;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestBasicTableUnionLoader {
  protected static ExecType execType = ExecType.MAPREDUCE;
  private static MiniCluster cluster;
  protected static PigServer pigServer;
  private static Path pathWorking, pathTable1, pathTable2;
  private static Configuration conf;

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

    conf = new Configuration();
    FileSystem fs = cluster.getFileSystem();
    pathWorking = fs.getWorkingDirectory();

    /*
     * create 1st basic table;
     */
    pathTable1 = new Path(pathWorking, "TestBasicTableUnion" + "1");
    System.out.println("pathTable1 =" + pathTable1);

    BasicTable.Writer writer = new BasicTable.Writer(pathTable1,
        "a:string,b,c:string", "[a,b];[c]", conf);
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
        for (int k = 0; k < tuple.size(); ++k) {
          try {
            tuple.set(k, b + "_" + i + "" + k);
          } catch (ExecException e) {
            e.printStackTrace();
          }
        }
        inserters[i].insert(new BytesWritable(("key1" + i).getBytes()), tuple);
      }
    }
    for (int i = 0; i < numsInserters; i++) {
      inserters[i].close();
    }

    /*
     * create 2nd basic table;
     */
    pathTable2 = new Path(pathWorking, "TestBasicTableUnion" + "2");
    System.out.println("pathTable2 =" + pathTable2);

    writer = new BasicTable.Writer(pathTable2, "a:string,b,d:string",
        "[a,b];[d]", conf);
    schema = writer.getSchema();
    tuple = TypesUtils.createTuple(schema);

    inserters = new TableInserter[numsInserters];
    for (int i = 0; i < numsInserters; i++) {
      inserters[i] = writer.getInserter("ins" + i, false);
    }

    for (int b = 0; b < numsBatch; b++) {
      for (int i = 0; i < numsInserters; i++) {
        TypesUtils.resetTuple(tuple);
        for (int k = 0; k < tuple.size(); ++k) {
          try {
            tuple.set(k, b + "_" + i + "" + k);
          } catch (ExecException e) {
            e.printStackTrace();
          }
        }
        inserters[i].insert(new BytesWritable(("key2" + i).getBytes()), tuple);
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
    /*
     * remove hdfs prefix part like "hdfs://localhost.localdomain:42540" pig
     * will fill that in.
     */
    String str1 = pathTable1.toString().substring(
        pathTable1.toString().indexOf("/", 7), pathTable1.toString().length());
    String str2 = pathTable2.toString().substring(
        pathTable2.toString().indexOf("/", 7), pathTable2.toString().length());
    String query = "records = LOAD '" + str1 + "," + str2
        + "' USING org.apache.hadoop.zebra.pig.TableLoader('a, b, c, d');";
    System.out.println(query);
    pigServer.registerQuery(query);
    Iterator<Tuple> it = pigServer.openIterator("records");

    int cnt = 0;
    Tuple cur;
    while (it.hasNext()) {
      cur = it.next();
      System.out.println(cur);
      cnt++;
      if (cnt == 1) {
        Assert.assertEquals("0_00", cur.get(0));
        Assert.assertEquals("0_01", cur.get(1));
        Assert.assertEquals("0_02", cur.get(2));
        Assert.assertEquals(null, cur.get(3));
      }
      if (cnt == 21) {
        Assert.assertEquals("0_00", cur.get(0));
        Assert.assertEquals("0_01", cur.get(1));
        Assert.assertEquals(null, cur.get(2));
        Assert.assertEquals("0_02", cur.get(3));
      }
    }
    Assert.assertEquals(cnt, 40);
  }

}

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
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.data.Tuple;
import org.apache.pig.test.MiniCluster;
import org.junit.Test;
import org.junit.BeforeClass;
import org.junit.AfterClass;
import junit.framework.Assert;

/**
 * Note:
 * 
 * Make sure you add the build/pig-0.1.0-dev-core.jar to the Classpath of the
 * app/debug configuration, when run this from inside the Eclipse.
 * 
 */
public class TestTableLoaderPrune {
  static protected ExecType execType = ExecType.MAPREDUCE;
  static private MiniCluster cluster;
  static protected PigServer pigServer;
  static private Path pathTable;

  private static Configuration conf;
  private static FileSystem fs;
  private static String zebraJar;
  private static String whichCluster;

  @BeforeClass
  public static void setUp() throws Exception {
    if (System.getProperty("hadoop.log.dir") == null) {
      String base = new File(".").getPath(); // getAbsolutePath();
      System
          .setProperty("hadoop.log.dir", new Path(base).toString() + "./logs");
    }

    // if whichCluster is not defined, or defined something other than
    // "realCluster" or "miniCluster", set it to "miniCluster"
    if (System.getProperty("whichCluster") == null
        || ((!System.getProperty("whichCluster")
            .equalsIgnoreCase("realCluster")) && (!System.getProperty(
            "whichCluster").equalsIgnoreCase("miniCluster")))) {
      System.setProperty("whichCluster", "miniCluster");
      whichCluster = System.getProperty("whichCluster");
    } else {
      whichCluster = System.getProperty("whichCluster");
    }

    System.out.println("cluster: " + whichCluster);
    if (whichCluster.equalsIgnoreCase("realCluster")
        && System.getenv("HADOOP_HOME") == null) {
      System.out.println("Please set HADOOP_HOME");
      System.exit(0);
    }

    conf = new Configuration();

    if (whichCluster.equalsIgnoreCase("realCluster")
        && System.getenv("USER") == null) {
      System.out.println("Please set USER");
      System.exit(0);
    }
    zebraJar = System.getenv("HADOOP_HOME") + "/../jars/zebra.jar";
    File file = new File(zebraJar);
    if (!file.exists() && whichCluster.equalsIgnoreCase("realCulster")) {
      System.out.println("Please put zebra.jar at hadoop_home/../jars");
      System.exit(0);
    }

    if (whichCluster.equalsIgnoreCase("realCluster")) {
      pigServer = new PigServer(ExecType.MAPREDUCE, ConfigurationUtil
          .toProperties(conf));
      pigServer.registerJar(zebraJar);
      pathTable = new Path("/user/" + System.getenv("USER")
          + "/TestMapType");
      fs = pathTable.getFileSystem(conf);
    }

    if (whichCluster.equalsIgnoreCase("miniCluster")) {
      if (execType == ExecType.MAPREDUCE) {
        cluster = MiniCluster.buildCluster();
        pigServer = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        fs = cluster.getFileSystem();
        pathTable = new Path(fs.getWorkingDirectory() + "TesMapType");
        System.out.println("path1 =" + pathTable);
      } else {
        pigServer = new PigServer(ExecType.LOCAL);
      }
    }

    BasicTable.Writer writer = new BasicTable.Writer(pathTable,
        "a:string,b,c:string,d,e,f,g", "[a,b,c];[d,e,f,g]", conf);
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
        inserters[i].insert(new BytesWritable(("key" + i).getBytes()), tuple);
      }
    }
    for (int i = 0; i < numsInserters; i++) {
      inserters[i].close();
    }
    writer.close();
  }

  @AfterClass
  static public void tearDown() throws Exception {
    pigServer.shutdown();
  }

//  @Test
  public void testReader1() throws ExecException, IOException {
    String query = "records = LOAD '" + pathTable.toString()
        + "' USING org.apache.hadoop.zebra.pig.TableLoader('c, a');";
    
  //  String query = "records = LOAD '/homes/jing1234/grid/pig-table20/pig-table/contrib/zebra/src/keyGenerator/test3' USING org.apache.hadoop.zebra.pig.TableLoader();";
    

    System.out.println(query);
    pigServer.registerQuery(query);
    Iterator<Tuple> it = pigServer.openIterator("records");
    while (it.hasNext()) {
      Tuple cur = it.next();
      System.out.println(cur);
    }
  }


  @Test
  public void testReader2() throws ExecException, IOException {
    String query = "records = LOAD '" + pathTable.toString()
        + "' USING org.apache.hadoop.zebra.pig.TableLoader('a, aa, b, c');";
    System.out.println(query);
    
    String prune = "pruneR = foreach records generate $0, $2;";
    
    pigServer.registerQuery(query);
    pigServer.registerQuery(prune);  
    Iterator<Tuple> it = pigServer.openIterator("pruneR");
//    Iterator<Tuple> it = pigServer.openIterator("records");

    int cnt = 0;
    while (it.hasNext()) {
      Tuple cur = it.next();
      cnt++;
      if (cnt == 1) {
        Assert.assertEquals(2, cur.size());
        Assert.assertEquals("0_00", cur.get(0));
        Assert.assertEquals("0_01", cur.get(1));
      }
      System.out.println(cur);
    }
  }
}

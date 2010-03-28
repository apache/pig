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

package org.apache.hadoop.zebra;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configured;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.data.Tuple;

public class BaseTestCase extends Configured {
  protected static PigServer pigServer = null;
  protected static Configuration conf = null;
  protected static FileSystem fs = null;
  protected static String zebraJar = null;
  
  // by default, we use local model for testing
  protected static TestMode mode = TestMode.local;
  
  protected enum TestMode {
    local, cluster
  };
  
  /*
   * Initialized for the test case;
   */
  public static void init() throws Exception {
    if (conf == null) {
      conf = new Configuration();
      if (System.getProperty("mapred.job.queue.name") != null) {
        //conf.set("mapred.job.queue.name", "grideng");
        conf.set("mapred.job.queue.name", System.getProperty("mapred.job.queue.name"));
      }
    }
    
    if (System.getenv("hadoop.log.dir") == null) {
      String base = new File(".").getPath(); // getAbsolutePath();
      System.setProperty("hadoop.log.dir", new Path(base).toString() + "./logs");
    }
   
    String str = System.getenv("ZebraTestMode");
    if (str != null && str.equals("cluster")) {
      mode = TestMode.cluster;
    }

    if (mode == TestMode.cluster) {
      System.out.println(" get env hadoop home: " + System.getenv("HADOOP_HOME"));
      System.out.println(" get env user name: " + System.getenv("USER"));

      if (System.getenv("HADOOP_HOME") == null) {
        System.out.println("Please set HADOOP_HOME for cluster testing mode");
        System.exit(0);
      }

      if (System.getenv("USER") == null) {
        System.out.println("Please set USER for cluster testing mode");
        System.exit(0);
      }

      String zebraJar = System.getenv("HADOOP_HOME") + "/lib/zebra.jar";
      File file = new File(zebraJar);
      if (!file.exists()) {
        System.out.println("Please place zebra.jar at $HADOOP_HOME/lib");
        System.exit(0);
      }
      
      pigServer = new PigServer(ExecType.MAPREDUCE, ConfigurationUtil.toProperties(conf));
      pigServer.registerJar(zebraJar);
      
      Path path = new Path("/user/" + System.getenv("USER"));
      fs = path.getFileSystem(conf);
    } else {
      pigServer = new PigServer(ExecType.LOCAL);
      fs = LocalFileSystem.get(conf);
    }
  }
  
  protected static void removeDir(Path path) throws IOException {
    String command = null;
    
    if (mode == TestMode.cluster) {
      command = System.getenv("HADOOP_HOME") +"/bin/hadoop fs -rmr " + path.toString();
    } else{
      command = "rm -rf " + path.toString();
    }
    
    Runtime runtime = Runtime.getRuntime();
    Process proc = runtime.exec(command);
    
    try {
      proc.waitFor();
    } catch (InterruptedException e) {
      System.err.println(e);
    }
  } 
  
  protected static Path getTableFullPath(String tableName) {
    if (mode == TestMode.cluster) {
      return new Path("/user/" + System.getenv("USER") + "/" + tableName);
    } else {
      return new Path(fs.getWorkingDirectory().toString().split(":")[1] + "/" + tableName);    
    }
  }
  
  /**
   * Verify union output table with expected results
   * 
   */
  protected int verifyTable(HashMap<Integer, ArrayList<ArrayList<Object>>> resultTable,
      int keyColumn, int tblIdxCol, Iterator<Tuple> it) throws IOException {
    int numbRows = 0;
    int index = 0, rowIndex = -1, rowCount = -1, prevIndex = -1;
    Object value;
    boolean first = true;
    ArrayList<ArrayList<Object>> rows = null;
    
    while (it.hasNext()) {
      Tuple rowValues = it.next();
      
      if (first) {
        index = (Integer) rowValues.get(tblIdxCol);
        Assert.assertNotSame(prevIndex, index);
        rows = resultTable.get(index);
        rowIndex = 0;
        rowCount = rows.size();
        first = false;
      }
      value = rows.get(rowIndex++).get(keyColumn);
      Assert.assertEquals("Table comparison error for row : " + numbRows + " - no key found for : "
          + rowValues.get(keyColumn), value, rowValues.get(keyColumn));
      
      if (rowIndex == rowCount)
      {
        // current table is run out; start on a new table for next iteration
        first = true;
        prevIndex = index;
      }
      
      ++numbRows;
    }
    return numbRows;
  }
  
  public static void checkTableExists(boolean expected, String strDir) throws IOException {  
    File theDir = null; 
    boolean actual = false;

    theDir = new File(strDir.split(":")[0]);
    actual = fs.exists(new Path (theDir.toString()));
    
    System.out.println("the dir : "+ theDir.toString());
   
    if (actual != expected){
      Assert.fail("dir exists or not is different from what expected.");
    }
  }
}

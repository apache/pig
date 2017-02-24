/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the
 * NOTICE file distributed with this work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */
package org.apache.pig.piggybank.test.storage;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.test.MiniCluster;
import org.apache.pig.test.Util;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import junit.framework.Assert;
import junit.framework.TestCase;

public class TestMultiStorage extends TestCase {
  private static final String INPUT_FILE = "MultiStorageInput.txt";

  private PigServer pigServer;
  private PigServer pigServerLocal;

  private MiniCluster cluster = MiniCluster.buildCluster();

  public TestMultiStorage() throws ExecException, IOException {
    pigServer = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
    pigServerLocal = new PigServer(ExecType.LOCAL);
  }

  public static final PathFilter hiddenPathFilter = new PathFilter() {
    public boolean accept(Path p) {
      String name = p.getName();
      return !name.startsWith("_") && !name.startsWith(".");
    }
  };

  private void createFile() throws IOException {
    PrintWriter w = new PrintWriter(new FileWriter(INPUT_FILE));
    w.println("100\tapple\taaa1");
    w.println("200\torange\tbbb1");
    w.println("300\tstrawberry\tccc1");

    w.println("101\tapple\taaa2");
    w.println("201\torange\tbbb2");
    w.println("301\tstrawberry\tccc2");

    w.println("102\tapple\taaa3");
    w.println("202\torange\tbbb3");
    w.println("302\tstrawberry\tccc3");

    w.close();
    Util.deleteFile(cluster, INPUT_FILE);
    Util.copyFromLocalToCluster(cluster, INPUT_FILE, INPUT_FILE);
  }

  @Override
  @Before
  public void setUp() throws Exception {
    createFile();
    FileSystem fs = FileSystem.getLocal(new Configuration());
    Path localOut = new Path("local-out");
    Path dummy = new Path("dummy");
    if (fs.exists(localOut)) {
      fs.delete(localOut, true);
    }
    if (fs.exists(dummy)) {
      fs.delete(dummy, true);
    }
  }

  @Override
  @After
  public void tearDown() throws Exception {
    new File(INPUT_FILE).delete();
    Util.deleteFile(cluster, INPUT_FILE);
    cluster.shutDown();
  }

  enum Mode {
    local, cluster
  };

  @Test
  public void testMultiStorage() throws IOException {
    final String LOAD = "A = LOAD '" + INPUT_FILE + "' as (id, name, n);";
    final String MULTI_STORE_CLUSTER = "STORE A INTO 'mr-out' USING "
        + "org.apache.pig.piggybank.storage.MultiStorage('mr-out', '1');";
    final String MULTI_STORE_LOCAL = "STORE A INTO 'dummy' USING "
        + "org.apache.pig.piggybank.storage.MultiStorage('local-out', '1');";

    System.out.print("Testing in LOCAL mode: ...");
    //testMultiStorage(Mode.local, "local-out", LOAD, MULTI_STORE_LOCAL);
    System.out.println("Succeeded!");
    
    System.out.print("Testing in CLUSTER mode: ...");
    testMultiStorage( Mode.cluster, "mr-out", LOAD, MULTI_STORE_CLUSTER);
    System.out.println("Succeeded!");
    
    
  }

  /**
   * The actual method that run the test in local or cluster mode. 
   * 
   * @param pigServer
   * @param mode
   * @param queries
   * @throws IOException
   */
  private void testMultiStorage( Mode mode, String outPath,
      String... queries) throws IOException {
    PigServer pigServer = (Mode.local == mode) ? this.pigServerLocal : this.pigServer;
    pigServer.setBatchOn();
    for (String query : queries) {
      pigServer.registerQuery(query);
    }
    pigServer.executeBatch();
    verifyResults(mode, outPath);
  }

  /**
   * Test if records are split into directories corresponding to split field
   * values
   * 
   * @param mode
   * @throws IOException
   */
  private void verifyResults(Mode mode, String outPath) throws IOException {
    FileSystem fs = (Mode.local == mode ? FileSystem
        .getLocal(new Configuration()) : cluster.getFileSystem());
    Path output = new Path(outPath);
    Assert.assertTrue("Output dir does not exists!", fs.exists(output)
        && fs.getFileStatus(output).isDir());

    Path[] paths = FileUtil.stat2Paths(fs.listStatus(output, hiddenPathFilter));
    Assert.assertTrue("Split field dirs not found!", paths != null);

    for (Path path : paths) {
      String splitField = path.getName();
      Path[] files = FileUtil.stat2Paths(fs.listStatus(path, hiddenPathFilter));
      Assert.assertTrue("No files found for path: " + path.toUri().getPath(),
          files != null);
      for (Path filePath : files) {
        Assert.assertTrue("This shouldn't be a directory", fs.isFile(filePath));
        
        BufferedReader reader = new BufferedReader(new InputStreamReader(fs
                .open(filePath)));
        String line = "";
        int count = 0;
        while ((line = reader.readLine()) != null) {
          String[] fields = line.split("\\t");
          Assert.assertEquals(fields.length, 3);
          Assert.assertEquals("Unexpected field value in the output record",
                splitField, fields[1]);
          count++;
          System.out.println("field: " + fields[1]);
        }        
        reader.close();
        Assert.assertEquals(count, 3);
      }
    }
  }
}




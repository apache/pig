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
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecJob;
import org.apache.pig.test.MiniGenericCluster;
import org.apache.pig.test.Util;
import org.apache.pig.tools.pigstats.JobStats;
import org.apache.pig.tools.pigstats.OutputStats;
import org.apache.pig.tools.pigstats.PigStats;
import org.apache.pig.tools.pigstats.mapreduce.SimplePigStats;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestMultiStorage {
  private static final String INPUT_FILE = "MultiStorageInput.txt";

  private PigServer pigServer;
  private PigServer pigServerLocal;

  private static MiniGenericCluster cluster = MiniGenericCluster.buildCluster();

  public TestMultiStorage() throws Exception {
    pigServer = new PigServer(cluster.getExecType(), cluster.getProperties());
    pigServerLocal = new PigServer(Util.getLocalTestMode());
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

  @Before
  public void setUp() throws Exception {
    createFile();
    FileSystem fs = FileSystem.getLocal(new Configuration());
    Path localOut = new Path("local-out");
    if (fs.exists(localOut)) {
      fs.delete(localOut, true);
    }
  }

  @After
  public void tearDown() throws Exception {
    new File(INPUT_FILE).delete();
    Util.deleteFile(cluster, INPUT_FILE);
  }

  @AfterClass
  public static void shutdown() {
    cluster.shutDown();
  }

  enum Mode {
    local, cluster
  }

  @Test
  public void testMultiStorage() throws IOException {
    final String LOAD = "A = LOAD '" + INPUT_FILE + "' as (id, name, n);";
    final String MULTI_STORE_CLUSTER = "STORE A INTO 'mr-out' USING "
        + "org.apache.pig.piggybank.storage.MultiStorage('mr-out', '1');";
    final String MULTI_STORE_LOCAL = "STORE A INTO 'local-out' USING "
        + "org.apache.pig.piggybank.storage.MultiStorage('local-out', '1');";

    System.out.print("Testing in LOCAL mode: ...");
    testMultiStorage(Mode.local, "local-out", LOAD, MULTI_STORE_LOCAL);
    System.out.println("Succeeded!");

    System.out.print("Testing in CLUSTER mode: ...");
    testMultiStorage( Mode.cluster, "mr-out", LOAD, MULTI_STORE_CLUSTER);
    System.out.println("Succeeded!");
  }

  @Test
  public void testOutputStats() throws IOException {
    FileSystem fs = cluster.getFileSystem();

    pigServer.setBatchOn();
    pigServer.registerQuery("A = LOAD '" + INPUT_FILE + "' as (id, name, n);");
    pigServer.registerQuery("B = FILTER A BY name == 'apple';");
    pigServer.registerQuery("STORE A INTO 'out1' USING org.apache.pig.piggybank.storage.MultiStorage('out1', '1');"); //153 bytes
    pigServer.registerQuery("STORE B INTO 'out2' USING org.apache.pig.piggybank.storage.MultiStorage('out2', '1');"); // 45 bytes

    ExecJob job = pigServer.executeBatch().get(0);

    PigStats stats = job.getStatistics();
    PigStats.JobGraph jobGraph = stats.getJobGraph();
    JobStats jobStats = (JobStats) jobGraph.getSinks().get(0);
    Map<String, Long> multiStoreCounters = jobStats.getMultiStoreCounters();
    List<OutputStats> outputStats = SimplePigStats.get().getOutputStats();
    OutputStats outputStats1 = "out1".equals(outputStats.get(0).getName()) ? outputStats.get(0) : outputStats.get(1);
    OutputStats outputStats2 = "out2".equals(outputStats.get(0).getName()) ? outputStats.get(0) : outputStats.get(1);

    assertEquals(153 + 45, stats.getBytesWritten());
    assertEquals(2, outputStats.size()); // 2 split conditions
    assertEquals(153, outputStats1.getBytes());
    assertEquals(45, outputStats2.getBytes());
    assertEquals(9, outputStats1.getRecords());
    assertEquals(3, outputStats2.getRecords());
    assertEquals(3L, multiStoreCounters.get("Output records in _1_out2").longValue());
    assertEquals(9L, multiStoreCounters.get("Output records in _0_out1").longValue());

    fs.delete(new Path("out1"), true);
    fs.delete(new Path("out2"), true);
  }

    /**
   * The actual method that run the test in local or cluster mode.
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
   */
  private void verifyResults(Mode mode, String outPath) throws IOException {
    FileSystem fs = (Mode.local == mode ? FileSystem
        .getLocal(new Configuration()) : cluster.getFileSystem());
    Path output = new Path(outPath);
    assertTrue("Output dir does not exists!", fs.exists(output)
        && fs.getFileStatus(output).isDir());

    Path[] paths = FileUtil.stat2Paths(fs.listStatus(output, hiddenPathFilter));
    assertTrue("Split field dirs not found!", paths != null);

    for (Path path : paths) {
      String splitField = path.getName();
      Path[] files = FileUtil.stat2Paths(fs.listStatus(path, hiddenPathFilter));
      assertTrue("No files found for path: " + path.toUri().getPath(),
          files != null);
      for (Path filePath : files) {
        assertTrue("This shouldn't be a directory", fs.isFile(filePath));
        BufferedReader reader = new BufferedReader(new InputStreamReader(fs
                .open(filePath)));
        String line = "";
        int count = 0;
        while ((line = reader.readLine()) != null) {
          String[] fields = line.split("\\t");
          assertEquals(fields.length, 3);
          assertEquals("Unexpected field value in the output record",
                splitField, fields[1]);
          count++;
          System.out.println("field: " + fields[1]);
        }
        reader.close();
        assertEquals(count, 3);
      }
    }
  }
}




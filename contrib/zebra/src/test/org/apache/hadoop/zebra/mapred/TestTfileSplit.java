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
package org.apache.hadoop.zebra.mapred;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.zebra.mapred.TableInputFormat;
import org.apache.hadoop.zebra.io.BasicTable;
import org.apache.hadoop.zebra.io.TestBasicTable;
import org.apache.hadoop.zebra.parser.ParseException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestTfileSplit {
  private static Configuration conf;
  private static Path path;
  
  @BeforeClass
  public static void setUpOnce() throws IOException {
    TestBasicTable.setUpOnce();
    conf = TestBasicTable.conf;
    path = new Path(TestBasicTable.rootPath, "TfileSplitTest");
  }
  
  @AfterClass
  public static void tearDown() throws IOException {
    BasicTable.drop(path, conf);
  }
  
  /* In this test, we test creating input splits for the projection on non-existing column case.
   * The first non-deleted column group should be used for split. */ 
  @Test
  public void testTfileSplit1() 
          throws IOException, ParseException {
    BasicTable.drop(path, conf);
    TestBasicTable.createBasicTable(1, 100, "a, b, c, d, e, f", "[a, b]; [c, d]", null, path, true);    

    TableInputFormat inputFormat = new TableInputFormat();
    JobConf jobConf = new JobConf(conf);
    inputFormat.setInputPaths(jobConf, path);
    inputFormat.setMinSplitSize(jobConf, 100);
    inputFormat.setProjection(jobConf, "aa");
    InputSplit[] splits = inputFormat.getSplits(jobConf, 40);
    
    RowTableSplit split = (RowTableSplit) splits[0];
    String str = split.getSplit().toString();
    StringTokenizer tokens = new StringTokenizer(str, "\n");
    str = tokens.nextToken();
    tokens = new StringTokenizer(str, " ");
    tokens.nextToken();
    tokens.nextToken();
    String s = tokens.nextToken();
    s = s.substring(0, s.length()-1);
    int cgIndex = Integer.parseInt(s);
    Assert.assertEquals(cgIndex, 0); 
  }

  /* In this test, we test creating input splits when dropped column groups are around.
   * Here the projection involves all columns and only one valid column group is present.
   * As such, that column group should be used for split.*/
  @Test
  public void testTfileSplit2() 
          throws IOException, ParseException {    
    BasicTable.drop(path, conf);
    TestBasicTable.createBasicTable(1, 100, "a, b, c, d, e, f", "[a, b]; [c, d]", null, path, true);    
    BasicTable.dropColumnGroup(path, conf, "CG0");
    BasicTable.dropColumnGroup(path, conf, "CG2");
    
    TableInputFormat inputFormat = new TableInputFormat();
    JobConf jobConf = new JobConf(conf);
    inputFormat.setInputPaths(jobConf, path);
    inputFormat.setMinSplitSize(jobConf, 100);
    InputSplit[] splits = inputFormat.getSplits(jobConf, 40);
    
    RowTableSplit split = (RowTableSplit) splits[0];
    String str = split.getSplit().toString(); 
    StringTokenizer tokens = new StringTokenizer(str, "\n");
    str = tokens.nextToken();
    tokens = new StringTokenizer(str, " ");
    tokens.nextToken();
    tokens.nextToken();
    String s = tokens.nextToken();
    s = s.substring(0, s.length()-1);
    int cgIndex = Integer.parseInt(s);
    Assert.assertEquals(cgIndex, 1); 
  }
  
  /* In this test, we test creating input splits when there is no valid column group present.
   * Should return 0 splits. */
  @Test
  public void testTfileSplit3() 
          throws IOException, ParseException {    
    BasicTable.drop(path, conf);
    TestBasicTable.createBasicTable(1, 100, "a, b, c, d, e, f", "[a, b]; [c, d]", null, path, true);    
    BasicTable.dropColumnGroup(path, conf, "CG0");
    BasicTable.dropColumnGroup(path, conf, "CG1");
    BasicTable.dropColumnGroup(path, conf, "CG2");
    
    TableInputFormat inputFormat = new TableInputFormat();
    JobConf jobConf = new JobConf(conf);
    inputFormat.setInputPaths(jobConf, path);
    inputFormat.setMinSplitSize(jobConf, 100);
    InputSplit[] splits = inputFormat.getSplits(jobConf, 40);
    
    Assert.assertEquals(splits.length, 0);
  }
}

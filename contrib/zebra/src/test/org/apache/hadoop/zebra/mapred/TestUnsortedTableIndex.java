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
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.zebra.io.BasicTable;
import org.apache.hadoop.zebra.io.TestBasicTable;
import org.apache.hadoop.zebra.mapred.RowTableSplit;
import org.apache.hadoop.zebra.mapred.TableInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.zebra.parser.ParseException;
import org.apache.hadoop.zebra.types.TypesUtils;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.io.BytesWritable;
import org.apache.pig.data.Tuple;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestUnsortedTableIndex {
	private static Configuration conf;
	private static JobConf jobConf;
	private static Path path1, path2;

	@BeforeClass
	public static void setUpOnce() throws IOException {
		TestBasicTable.setUpOnce();
		conf = TestBasicTable.conf;
    jobConf = new JobConf(conf);
		path1 = new Path(TestBasicTable.rootPath, "TestUnsortedTableIndex1");
		path2 = new Path(TestBasicTable.rootPath, "TestUnsortedTableIndex2");
	}

	@AfterClass
	public static void tearDown() throws IOException {
		BasicTable.drop(path1, conf);
		BasicTable.drop(path2, conf);
	}

	@Test
	public void testUnsortedTableIndex() 
	throws IOException, ParseException, InterruptedException {
		BasicTable.drop(path1, conf);
		BasicTable.drop(path2, conf);
		int total1 = TestBasicTable.createBasicTable(1, 100, "a, b, c, d, e, f", "[a, b]; [c, d]", null, path1, true);    
		int total2 = TestBasicTable.createBasicTable(1, 100, "a, b, c, d, e, f", "[a, b]; [c, d]", null, path2, true);    

		TableInputFormat inputFormat = new TableInputFormat();
		TableInputFormat.setInputPaths(jobConf, path1, path2);
		TableInputFormat.setProjection(jobConf, "source_table");
		InputSplit[] splits = inputFormat.getSplits(jobConf, -1);
    Assert.assertEquals(splits.length, 2);
    for (int i = 0; i < 2; i++)
    {
	    int count = 0;
	  	RowTableSplit split = (RowTableSplit) splits[i];
	    TableRecordReader rr = (TableRecordReader) inputFormat.getRecordReader(split, jobConf, null);
	    Tuple t = TypesUtils.createTuple(1);
      BytesWritable key = new BytesWritable();
	    while (rr.next(key, t)) {
        int idx= (Integer) t.get(0);
        Assert.assertEquals(idx, i);
	      count++;
	    }
	    rr.close();
      if (i == 0)
  	    Assert.assertEquals(count, total1);
      else
  	    Assert.assertEquals(count, total2);
    }
	}
}

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
package org.apache.hadoop.zebra.mapreduce;

import java.io.IOException;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.zebra.io.BasicTable;
import org.apache.hadoop.zebra.io.TestBasicTable;
import org.apache.hadoop.zebra.mapreduce.RowTableSplit;
import org.apache.hadoop.zebra.mapreduce.TableInputFormat;
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
		Job job = new Job(conf);
		inputFormat.setInputPaths(job, path);
		inputFormat.setMinSplitSize(job, 100);
		inputFormat.setProjection(job, "aa");
		List<InputSplit> splits = inputFormat.getSplits(job);

		RowTableSplit split = (RowTableSplit) splits.get(0);
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
		Job job = new Job(conf);
		inputFormat.setInputPaths(job, path);
		inputFormat.setMinSplitSize(job, 100);
		List<InputSplit> splits = inputFormat.getSplits(job);

		RowTableSplit split = (RowTableSplit) splits.get( 0 );
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
		Job job = new Job(conf);
		inputFormat.setInputPaths(job, path);
		inputFormat.setMinSplitSize(job, 100);
		List<InputSplit> splits = inputFormat.getSplits(job);

		Assert.assertEquals(splits.size(), 0);
	}
	
	@Test
	public void testSortedSplitOrdering() throws IOException, ParseException {
		BasicTable.drop(path, conf);
		TestBasicTable.createBasicTable(1, 1000000, "a, b, c, d, e, f", "[a, e, d]", "a", path, true);    

		TableInputFormat inputFormat = new TableInputFormat();
		Job job = new Job(conf);
		inputFormat.setInputPaths(job, path);
		inputFormat.setMinSplitSize(job, 100);
		inputFormat.setProjection(job, "d");
		inputFormat.requireSortedTable( job, null );
		List<InputSplit> splits = inputFormat.getSplits(job);
		
		int index = 0;
		for( InputSplit is : splits ) {
			Assert.assertTrue( is instanceof SortedTableSplit );
			SortedTableSplit split = (SortedTableSplit)is;
			Assert.assertEquals( index++, split.getIndex() );
		}
	}
	
}

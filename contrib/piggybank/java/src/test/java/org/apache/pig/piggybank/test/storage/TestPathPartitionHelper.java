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
package org.apache.pig.piggybank.test.storage;

import java.io.File;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.piggybank.storage.partition.PathPartitionHelper;
import org.apache.pig.test.Util;
import org.junit.Test;
import org.apache.pig.backend.hadoop.executionengine.shims.HadoopShims;

/**
 * 
 * Tests the PathPartitionHelper can:<br/>
 * <ul>
 * <li>Filter path partitioned files based on an expression being true</li>
 * <li>Filter path partitioned files based on an expression being false</li>
 * <li>Filter path partitioned files with no expression</li>
 * </ul>
 * 
 */
public class TestPathPartitionHelper extends TestCase {

    private static Configuration conf = null;

    File baseDir;
    File partition1;
    File partition2;
    File partition3;

    @Test
    public void testListStatusPartitionFilterNotFound() throws Exception {

	PathPartitionHelper partitionHelper = new PathPartitionHelper();

	Job job = new Job(conf);
	job.setJobName("TestJob");
	job.setInputFormatClass(FileInputFormat.class);

	Configuration conf = job.getConfiguration();
	FileInputFormat.setInputPaths(job, new Path(baseDir.getAbsolutePath()));

	Iterator<Map.Entry<String, String>> iter = conf.iterator();
	while (iter.hasNext()) {
	    Map.Entry<String, String> entry = iter.next();
	    System.out.println(entry.getKey() + ": " + entry.getValue());
	}
	JobContext jobContext = HadoopShims.createJobContext(conf, job.getJobID());

	partitionHelper.setPartitionFilterExpression("year < '2010'",
		PigStorage.class, "1");
	partitionHelper.setPartitionKeys(baseDir.getAbsolutePath(), conf,
		PigStorage.class, "1");

	List<FileStatus> files = partitionHelper.listStatus(jobContext,
		PigStorage.class, "1");

	assertEquals(0, files.size());

    }

    @Test
    public void testListStatusPartitionFilterFound() throws Exception {

	PathPartitionHelper partitionHelper = new PathPartitionHelper();

	Job job = new Job(conf);
	job.setJobName("TestJob");
	job.setInputFormatClass(FileInputFormat.class);

	Configuration conf = job.getConfiguration();
	FileInputFormat.setInputPaths(job, new Path(baseDir.getAbsolutePath()));

	JobContext jobContext = HadoopShims.createJobContext(conf, job.getJobID());

	partitionHelper.setPartitionFilterExpression(
		"year<='2010' and month=='01' and day>='01'", PigStorage.class, "2");
	partitionHelper.setPartitionKeys(baseDir.getAbsolutePath(), conf,
		PigStorage.class, "2");

	List<FileStatus> files = partitionHelper.listStatus(jobContext,
		PigStorage.class, "2");

	assertNotNull(files);
	assertEquals(1, files.size());

    }

    @Test
    public void testListStatus() throws Exception {

	PathPartitionHelper partitionHelper = new PathPartitionHelper();

	Job job = new Job(conf);
	job.setJobName("TestJob");
	job.setInputFormatClass(FileInputFormat.class);

	Configuration conf = job.getConfiguration();
	FileInputFormat.setInputPaths(job, new Path(baseDir.getAbsolutePath()));

	JobContext jobContext = HadoopShims.createJobContext(conf, job.getJobID());

	partitionHelper.setPartitionKeys(baseDir.getAbsolutePath(), conf,
		PigStorage.class, "3");

	List<FileStatus> files = partitionHelper.listStatus(jobContext,
		PigStorage.class, "3");

	assertNotNull(files);
	assertEquals(1, files.size());

    }

    @Override
    protected void tearDown() throws Exception {

	Util.deleteDirectory(baseDir);

    }

    @Override
    protected void setUp() throws Exception {
    File oldConf = new File("build/classes/hadoop-site.xml");
    oldConf.delete();
	conf = new Configuration();

	baseDir = createDir(null,
		"testPathPartitioner-testGetKeys-" + System.currentTimeMillis());

	partition1 = createDir(baseDir, "year=2010");
	partition2 = createDir(partition1, "month=01");
	partition3 = createDir(partition2, "day=01");

	File file = new File(partition3, "testfile-"
		+ System.currentTimeMillis());
	file.createNewFile();

    }

    private File createDir(File parent, String name) {
	File file = new File(parent, name);
	file.mkdirs();
	return file;
    }

}

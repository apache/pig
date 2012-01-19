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
import java.util.Map;
import java.util.Set;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.pig.piggybank.storage.partition.PathPartitioner;
import org.apache.pig.test.Util;
import org.junit.Test;

/**
 * 
 * Tests that the PathPartitioner can:<br/>
 * <ul>
 *   <li>Read keys from a partitioned file path</li>
 *   <li>Read keys and values from a partitioned file path</li>
 * </ul>
 *
 */
public class TestPathPartitioner extends TestCase {

    private static Configuration conf = null;

    File baseDir;
    File partition1;
    File partition2;
    File partition3;

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

    @Test
    public void testGetKeyValues() throws Exception {
	PathPartitioner partitioner = new PathPartitioner();

	Map<String, String> map = partitioner
		.getPathPartitionKeyValues(partition3.getAbsolutePath());

	String[] keys = map.keySet().toArray(new String[] {});

	assertEquals("2010", map.get(keys[0]));
	assertEquals("01", map.get(keys[1]));
	assertEquals("01", map.get(keys[2]));

    }

    @Test
    public void testGetKeys() throws Exception {

	PathPartitioner pathPartitioner = new PathPartitioner();
	Set<String> keys = pathPartitioner.getPartitionKeys(
		baseDir.getAbsolutePath(), conf);

	assertNotNull(keys);
	assertEquals(3, keys.size());

	String[] keyArr = keys.toArray(new String[] {});

	assertEquals("year", keyArr[0]);
	assertEquals("month", keyArr[1]);
	assertEquals("day", keyArr[2]);

    }

    private File createDir(File parent, String name) {
	File file = new File(parent, name);
	file.mkdirs();
	return file;
    }

}

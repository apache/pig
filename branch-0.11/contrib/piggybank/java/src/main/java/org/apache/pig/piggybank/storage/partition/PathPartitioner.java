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
package org.apache.pig.piggybank.storage.partition;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * 
 * Its convenient sometimes to partition logs by date values or other e.g.
 * country, city etc.<br/>
 * A daydate partitioned hdfs directory might look something like:<br/>
 * 
 * <pre>
 * /logs/repo/mylog/
 * 					daydate=2010-01-01
 * 				    daydate=2010-01-02
 * </pre>
 * 
 * This class accepts a path like /logs/repo/mylog and return a map of the
 * partition keys
 */
public class PathPartitioner {

    /**
     * Note: this must be the path lowes in the Searches for the key=value pairs
     * in the path pointer by the location parameter.
     * 
     * @param location
     *            String root path in hdsf e.g. /user/hive/warehouse or
     *            /logs/repo
     * @param conf
     *            Configuration
     * @return Set of String. The order is maintained as per the directory tree.
     *         i.e. if /logs/repo/year=2010/month=2010 exists the first item in
     *         the set will be year and the second month.
     * @throws IOException
     */
    public Map<String, String> getPathPartitionKeyValues(String location)
	    throws IOException {

	// use LinkedHashSet because order is important here.
	Map<String, String> partitionKeys = new LinkedHashMap<String, String>();

	String[] pathSplit = location.split("/");

	for (String pathSplitItem : pathSplit) {
	    parseAndPutKeyValue(pathSplitItem, partitionKeys);
	}

	return partitionKeys;
    }

    /**
     * Searches for the key=value pairs in the path pointer by the location
     * parameter.
     * 
     * @param location
     *            String root path in hdsf e.g. /user/hive/warehouse or
     *            /logs/repo
     * @param conf
     *            Configuration
     * @return Set of String. The order is maintained as per the directory tree.
     *         i.e. if /logs/repo/year=2010/month=2010 exists the first item in
     *         the set will be year and the second month.
     * @throws IOException
     */
    public Set<String> getPartitionKeys(String location, Configuration conf)
	    throws IOException {

	// find the hive type partition key=value pairs from the path.
	// first parse the string alone.
	Path path = new Path(location);
	FileSystem fs = path.getFileSystem(conf);

	FileStatus[] fileStatusArr = null;

	// use LinkedHashSet because order is important here.
	Set<String> partitionKeys = new LinkedHashSet<String>();

	parseAndPutKeyValue(location, partitionKeys);

	while (!((fileStatusArr = fs.listStatus(path)) == null || fs
		.isFile(path))) {
	    for (FileStatus fileStatus : fileStatusArr) {

		path = fileStatus.getPath();

		// ignore hidden directories
		if (fileStatus.getPath().getName().startsWith("_")
			|| !fileStatus.isDir())
		    continue;

		parseAndPutKeyValue(path.getName(), partitionKeys);
		// at the first directory found stop the for loop after parsing
		// for key value pairs
		break;
	    }

	}

	return partitionKeys;
    }

    private final void parseAndPutKeyValue(String pathName,
	    Map<String, String> partitionKeys) {
	String[] keyValue = parsePathKeyValue(pathName);
	if (keyValue != null) {
	    partitionKeys.put(keyValue[0], keyValue[1]);
	}

    }

    private final void parseAndPutKeyValue(String pathName,
	    Set<String> partitionKeys) {
	String[] keyValue = parsePathKeyValue(pathName);
	if (keyValue != null) {
	    partitionKeys.add(keyValue[0]);
	}

    }

    /**
     * Will look for key=value pairs in the path for example:
     * /user/hive/warehouse/mylogs/year=2010/month=07
     * 
     * @param path
     * @return String[] [0]= key [1] = value
     */
    public String[] parsePathKeyValue(String path) {
	int slashIndex = path.lastIndexOf('/');
	String parsedPath = path;
	String[] keyValue = null;

	if (slashIndex > 0) {
	    parsedPath = path.substring(slashIndex);
	}

	if (parsedPath.contains("=")) {
	    String split[] = parsedPath.split("=");
	    if (split.length == 2) {
		keyValue = split;
	    }
	}

	return keyValue;
    }

}

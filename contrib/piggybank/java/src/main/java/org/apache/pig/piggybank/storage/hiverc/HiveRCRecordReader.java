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
package org.apache.pig.piggybank.storage.hiverc;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.RCFileRecordReader;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * This class delegates the work to the RCFileRecordReader<br/>
 */
public class HiveRCRecordReader extends
	RecordReader<LongWritable, BytesRefArrayWritable> {

    LongWritable key;
    BytesRefArrayWritable value;

    RCFileRecordReader<LongWritable, BytesRefArrayWritable> rcFileRecordReader;

    Path splitPath = null;

    @Override
    public void close() throws IOException {
	rcFileRecordReader.close();
    }

    @Override
    public LongWritable getCurrentKey() throws IOException,
	    InterruptedException {
	return key;
    }

    @Override
    public BytesRefArrayWritable getCurrentValue() throws IOException,
	    InterruptedException {
	return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
	return rcFileRecordReader.getProgress();
    }

    public Path getSplitPath() {
	return splitPath;
    }

    @SuppressWarnings("deprecation")
    @Override
    public void initialize(InputSplit split, TaskAttemptContext ctx)
	    throws IOException, InterruptedException {

	FileSplit fileSplit = (FileSplit) split;
	Configuration conf = ctx.getConfiguration();
	splitPath = fileSplit.getPath();

	rcFileRecordReader = new RCFileRecordReader<LongWritable, BytesRefArrayWritable>(
		conf, new org.apache.hadoop.mapred.FileSplit(splitPath,
			fileSplit.getStart(), fileSplit.getLength(),
			new org.apache.hadoop.mapred.JobConf(conf)));

	key = rcFileRecordReader.createKey();
	value = rcFileRecordReader.createValue();

    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
	return rcFileRecordReader.next(key, value);
    }

}

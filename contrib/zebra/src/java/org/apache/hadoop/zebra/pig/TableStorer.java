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

package org.apache.hadoop.zebra.pig;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Constructor;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.zebra.io.BasicTable;
import org.apache.hadoop.zebra.io.TableInserter;
import org.apache.hadoop.zebra.parser.ParseException;
import org.apache.pig.StoreConfig;
import org.apache.pig.StoreFunc;
import org.apache.pig.backend.hadoop.executionengine.util.MapRedUtil;
import org.apache.pig.data.Tuple;

public class TableStorer implements StoreFunc {
	private String schemaString;
	private String storageHintString;

	public TableStorer() {	  
	}

	public TableStorer(String schemaStr, String storageHintStr) throws ParseException, IOException {
		schemaString = schemaStr;
		storageHintString = storageHintStr;
	}
  
	@Override
	public void bindTo(OutputStream os) throws IOException {
		// no op
	}

	@Override
	public void finish() throws IOException {
		// no op
	}

	@Override
	public void putNext(Tuple f) throws IOException {
		// no op
	}

	@Override
	public Class getStorePreparationClass() throws IOException {
		return TableOutputFormat.class;
	}

	public String getSchemaString() {
		return schemaString;  
	}
  
	public String getStorageHintString() {
		return storageHintString;  
	}
  
	private static final Class[] emptyArray = new Class[] {};

	static public void main(String[] args) throws SecurityException, NoSuchMethodException {
		Constructor meth = TableOutputFormat.class.getDeclaredConstructor(emptyArray);
	}
}

/**
 * 
 * Table OutputFormat
 * 
 */
class TableOutputFormat implements OutputFormat<BytesWritable, Tuple> {
	@Override
	public void checkOutputSpecs(FileSystem ignored, JobConf job) throws IOException {
		StoreConfig storeConfig = MapRedUtil.getStoreConfig(job);
		String location = storeConfig.getLocation();
		TableStorer storeFunc = (TableStorer)MapRedUtil.getStoreFunc(job);   
		BasicTable.Writer writer = new BasicTable.Writer(new Path(location), 
				storeFunc.getSchemaString(), storeFunc.getStorageHintString(), false, job);
		writer.finish();
	}
    
	@Override
	public RecordWriter<BytesWritable, Tuple> getRecordWriter(FileSystem ignored,
			JobConf job, String name, Progressable progress) throws IOException {
		return new TableRecordWriter(name, job);
	}
}

/**
 * 
 * Table RecordWriter
 * 
 */
class TableRecordWriter implements RecordWriter<BytesWritable, Tuple> {
	final private BytesWritable KEY0 = new BytesWritable(new byte[0]); 
	private BasicTable.Writer writer;
	private TableInserter inserter;

	public TableRecordWriter(String name, JobConf conf) throws IOException {
		StoreConfig storeConfig = MapRedUtil.getStoreConfig(conf);
		String location = storeConfig.getLocation();

		// TODO: how to get? 1) column group splits; 2) flag of sorted-ness,
		// compression, etc.
		writer = new BasicTable.Writer(new Path(location), conf);
		inserter = writer.getInserter(name, false);
	}

	@Override
	public void close(Reporter reporter) throws IOException {
		inserter.close();
		writer.finish();
	}

	@Override
	public void write(BytesWritable key, Tuple value) throws IOException {
		if (key == null) {
			key = KEY0;
		}
		inserter.insert(key, value);
	}
}

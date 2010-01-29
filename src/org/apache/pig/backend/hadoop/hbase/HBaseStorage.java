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
package org.apache.pig.backend.hadoop.hbase;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

/**
 * A Hbase Loader
 */
public class HBaseStorage extends LoadFunc {

	private byte[][] m_cols;
	private HTable m_table;
	private Configuration m_conf=new Configuration();
	private RecordReader reader;
	private Scan scan=new Scan();
	
	private static final Log LOG = LogFactory.getLog(HBaseStorage.class);

	/**
	 * Constructor. Construct a HBase Table loader to load the cells of the
	 * provided columns.
	 * 
	 * @param columnList
	 *            columnlist that is a presented string delimited by space.
	 */
	public HBaseStorage(String columnList) {
		String[] colNames = columnList.split(" ");
		m_cols = new byte[colNames.length][];
		for (int i = 0; i < m_cols.length; i++) {
			m_cols[i] = Bytes.toBytes(colNames[i]);
			scan.addColumn(m_cols[i]);
		}		
	}


	@Override
	public Tuple getNext() throws IOException {
		try {
			if (reader.nextKeyValue()) {
				ImmutableBytesWritable rowKey = (ImmutableBytesWritable) reader
						.getCurrentKey();
				Result result = (Result) reader.getCurrentValue();
				Tuple tuple=TupleFactory.getInstance().newTuple(m_cols.length);
				for (int i=0;i<m_cols.length;++i){
					tuple.set(i, new DataByteArray(result.getValue(m_cols[i])));
				}
				return tuple;
			}
		} catch (InterruptedException e) {
			throw new IOException(e);
		}
		return null;
	}

	@Override
	public InputFormat getInputFormat() {
		TableInputFormat inputFormat = new TableInputFormat();
		inputFormat.setConf(m_conf);
		return inputFormat;
	}

	@Override
	public void prepareToRead(RecordReader reader, PigSplit split) {
		this.reader = reader;
	}

	@Override
    public void setLocation(String location, Job job) throws IOException {
        if (location.startsWith("hbase://")){
        	m_conf.set(TableInputFormat.INPUT_TABLE, location.substring(8));
        }else{
        	m_conf.set(TableInputFormat.INPUT_TABLE, location);
        }
        m_conf.set(TableInputFormat.SCAN, convertScanToString(scan));
    }

	@Override
	public String relativeToAbsolutePath(String location, Path curDir)
			throws IOException {
		return location;
	}
	
	private static String convertScanToString(Scan scan) {

		try {
			ByteArrayOutputStream out = new ByteArrayOutputStream();
			DataOutputStream dos = new DataOutputStream(out);
			scan.write(dos);
			return Base64.encodeBytes(out.toByteArray());
		} catch (IOException e) {
			LOG.error(e);
			return "";
		}

	}
}

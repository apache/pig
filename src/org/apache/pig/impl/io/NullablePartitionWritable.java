/*
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
package org.apache.pig.impl.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.pig.backend.hadoop.HDataType;

/**
 * NullablePartitionWritable is an adaptor class around PigNullableWritable that adds a partition
 * index to the class.
 */
public class NullablePartitionWritable extends PigNullableWritable{
	private int partitionIndex;
	private PigNullableWritable key;

	public NullablePartitionWritable() {

	}

	public NullablePartitionWritable(PigNullableWritable k) {
		setKey(k);
	}

	public void setKey(PigNullableWritable k) {
		key = k;
	}

	public PigNullableWritable getKey() {
		return key;
	}

	public void setPartition(int n) {
		partitionIndex = n;
	}

	public int getPartition() {
		return partitionIndex;
	}

  	@Override
    public int compareTo(Object o) {
		return key.compareTo(((NullablePartitionWritable)o).getKey());
	}

	@Override
    public void readFields(DataInput in) throws IOException {
		String c = in.readUTF();
		try {
			key = HDataType.getWritableComparable(c);
		} catch(Exception e) {
			throw new IOException(e);
		}
		key.readFields(in);
	}

	@Override
    public void write(DataOutput out) throws IOException {
		out.writeUTF(key.getClass().getName());
		key.write(out);
	}

	@Override
    public boolean isNull() {
		return key.isNull();
	}

	@Override
    public void setNull(boolean isNull) {
		key.setNull(isNull);
	}

	@Override
    public byte getIndex() {
		return key.getIndex();
	}

	@Override
    public void setIndex(byte index) {
		key.setIndex(index);
	}

	@Override
    public Object getValueAsPigType() {
		return key.getValueAsPigType();
	}

	@Override
    public int hashCode() {
		return key.hashCode();
	}

	@Override
    public String toString() {
		return "Partition: " + partitionIndex + " " + key.toString();
	}
}

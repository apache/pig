/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *	 http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.pig.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @deprecated Use new data types instead.  This type is not optimized for
 * performance.
 */
final public class DataAtom extends DataCharArrayUtf16 {
	
public DataAtom() { }

public DataAtom(String valIn) { setValue(valIn); }

public DataAtom(int valIn) { setValue(valIn); }

public DataAtom(long valIn) { setValue(valIn); }
	
public DataAtom(byte[] valIn) { setValue(valIn); }

public DataAtom(double valIn) { setValue(valIn); }

public void setValue(String valIn) { mVal = valIn; }
	
public void setValue(byte[] valIn) { mVal = new String(valIn); }

public void setValue(int valIn) { mVal = Integer.toString(valIn); }

public void setValue(long valIn) { mVal = Long.toString(valIn); }

public void setValue(double valIn) { mVal = Double.toString(valIn); }

public String strval() { return mVal; }

public Double numval() { return Double.parseDouble(mVal); }

@Override
public String toString()
{
	return mVal;
}

@Override
public boolean equals(Object other)
{
	return compareTo(other) == 0;
}

public int compareTo(Object other)
{
	if (!(other instanceof DataAtom)) return -1;
	DataAtom dOther = (DataAtom) other;

	return mVal.compareTo(dOther.mVal);
}

@Override
public void write(DataOutput out) throws IOException
{
	out.write(Datum.DataType.ATOM.getMarker());
	byte[] data;
	try {
		data = strval().getBytes("UTF-8");
	} catch (Exception e) {
		long size = strval().length();
		throw new RuntimeException("Error dealing with DataAtom of size " + size);
	}
	out.writeInt(data.length);
	out.write(data);
}

static DataAtom read(DataInput in) throws IOException
{
	int len = in.readInt();
	DataAtom ret = new DataAtom();
	byte[] data = new byte[len];
	in.readFully(data);
	ret.setValue(new String(data, "UTF-8"));
	return ret;
}

@Override
public int hashCode() { return mVal.hashCode(); }

}

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

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;

/**
 * Indicates a character array (string) datum with no encoding.
 */

public class DataCharArrayNone extends DataCharArray {

public DataCharArrayNone()
{
	super(DataCharArray.Encoding.NONE);
	mVal = null;
}

/**
 * Construct a new datum using this byte[].  Makes a copy of the byte[].
 */
public DataCharArrayNone(byte[] b)
{
	super(DataCharArray.Encoding.NONE);
	mVal = new byte[b.length];
	for (int i = 0; i < b.length; i++) mVal[i] = b[i];
}

public long size()
{
	if (mVal == null) return 0;
	else return mVal.length;
}

/**
 * Get the value of the datum, as a byte[]
 * @return byte[] value
 */
public final byte[] get() { return mVal; }

/**
 * Set the value of the datum.  Does not make a copy of the byte[], just
 * stores a reference to it.
 * @param val Value to be set.
 */
public final void set(byte[] val) { mVal = val; }

public String toString()
{
	return new String(mVal);
}

public int hashCode()
{
	int hash = 0;
	for (int i = 0; i < mVal.length; i++) {
		hash += mVal[0] * (31 ^ (mVal.length - i - 1));
	}
	return hash;
}

// Don't make this use compareTo.  These functions are used in things like hashs
// and we want them to be as fast as possible.
public boolean equals(Object other)
{
	if (!(other instanceof DataCharArrayNone)) return false;
	DataCharArrayNone o = (DataCharArrayNone)other;
	if (mVal.length != o.mVal.length) return false;
	boolean same = true;
	for (int i = 0; i < mVal.length && same; i++) {
		same &= mVal[i] == o.mVal[i];
	}
	return same;
}

public int compareTo(Object other)
{
	if (!(other instanceof Datum)) return -1;

	Datum od = (Datum)other;

	if (od.getType() != Datum.DataType.CHARARRAY) return crossTypeCompare(od);

	DataCharArray dco = (DataCharArray)od;

	if (dco.getEncoding() != getEncoding()) {
		return getEncoding().compareTo(dco.getEncoding());
	}

	DataCharArrayNone noneOther = (DataCharArrayNone)dco;

	int i;
	for (i = 0; i < mVal.length && i < noneOther.mVal.length; i++) {
		if (mVal[i] < noneOther.mVal[i]) return -1;
		else if (mVal[i] > noneOther.mVal[i]) return 1;
	}

	// Ran out of the other before we ran out of us.
	if (i < mVal.length) return 1;
	// Ran out of us before we ran out of the other.
	else if (i < noneOther.mVal.length) return -1;
	else return 0;
}

public void write(DataOutput out) throws IOException
{
	out.write(Datum.DataType.CHARARRAY.getMarker());
	out.write(ENC_NONE);
	out.writeInt(mVal.length);
	out.write(mVal);	
}

static DataCharArrayNone read(DataInput in) throws IOException
{
	// Assumes that the chararray and encoding indicators have already
	// been read in order to select his function.
	int len = in.readInt();
	DataCharArrayNone ret = new DataCharArrayNone();
	byte[] data = new byte[len];
	in.readFully(data);
	ret.mVal = data;
	return ret;
}
 
private byte[] mVal;

}


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
 * Indicates an character array (string) datum.
 */

public class DataCharArrayUtf16 extends DataCharArray {

public DataCharArrayUtf16()
{
	super(DataCharArray.Encoding.UTF16);
	mVal = null;
}

/**
 * Construct a new Datum using this string.  Makes a copy of the string.
 */
public DataCharArrayUtf16(String s)
{
	super(DataCharArray.Encoding.UTF16);
	mVal = new String(s);
}

/**
 * Construct a new Datum using these bytes.  Makes a new string out of the
 * byte[] passed in.
 */
public DataCharArrayUtf16(byte[] b)
{
	super(DataCharArray.Encoding.UTF16);
	mVal = new String(b);
}

public long size()
{
	if (mVal == null) return 0;
	else return mVal.length();
}

/**
 * Get the value of the datum, as a String
 * @return String value
 */
public final String get() { return mVal; }

/**
 * Set the value of the datum.  Does not make a copy of the string, just
 * stores a reference to it.
 * @param val Value to be set.
 */
public final void set(String val) { mVal = val; }

public String toString()
{
	return new String(mVal);
}

public int hashCode()
{
	return mVal.hashCode();
}

// Don't make this use compareTo.  These functions are used in things like hashs
// and we want them to be as fast as possible.
public boolean equals(Object other)
{
	if (!(other instanceof DataCharArrayUtf16)) return false;
	DataCharArrayUtf16 o = (DataCharArrayUtf16)other;
	return mVal.equals(o.mVal);
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

	DataCharArrayUtf16 utf16Other = (DataCharArrayUtf16)dco;

	return mVal.compareTo(utf16Other.mVal);
}

public void write(DataOutput out) throws IOException
{
	// I'm not sure if it's faster to do the in-memory translations from
	// utf16->utf8 and back or store double the bytes while leaving it
	// utf16.  But if we store it as utf16 it will be impossible to read
	// with a basic editor, and make debugging that much more painful.
	out.write(Datum.DataType.CHARARRAY.getMarker());
	out.write(ENC_UTF16);
	byte[] data;
	try {
		data = mVal.getBytes("UTF-8");
	} catch (Exception e) {
		long size = mVal.length();
		throw new RuntimeException("Error dealing with DataAtom of size " + size);
	}
	out.writeInt(data.length);
	out.write(data);	
}

static DataCharArrayUtf16 read(DataInput in) throws IOException
{
	// Assumes that the chararray and encoding indicators have already
	// been read in order to select his function.
	int len = in.readInt();
	DataCharArrayUtf16 ret = new DataCharArrayUtf16();
	byte[] data = new byte[len];
	in.readFully(data);
	ret.mVal = new String(data, "UTF-8");
	return ret;
}
 
// Protected so DataAtom can read it.
protected String mVal;

}


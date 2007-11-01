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
package org.apache.pig.data;

import java.io.DataOutput;
import java.io.DataInput;
import java.io.IOException;

/**
 * A marker class for a basic data unit.
 */
public interface Datum extends Comparable {

enum DataType {
	BAG((byte)0x7f, (byte)1), // DEL
	TUPLE((byte)0x7e, (byte)2), // ~
	MAP((byte)0x7d, (byte)3), // }
	INT((byte)0x7c, (byte)4), // |
	LONG((byte)0x7b, (byte)5), // {
	FLOAT((byte)0x7a, (byte)6), // z
	DOUBLE((byte)0x79, (byte)7), // y
	CHARARRAY((byte)0x78, (byte)8), // x
	UNKNOWN((byte)0x77, (byte)9), // w
	ATOM((byte)0x76, (byte)10); // v

	DataType(byte m, byte v) { marker = m; val = v; }

	private byte marker;
	private byte val;

	public byte getMarker() { return marker; }

	static DataType markerToType(byte marker) {
		switch (marker) {
		case 0x7f: return BAG;
		case 0x7e: return TUPLE;
		case 0x7d: return MAP;
		case 0x7c: return INT;
		case 0x7b: return LONG;
		case 0x7a: return FLOAT;
		case 0x79: return DOUBLE;
		case 0x78: return CHARARRAY;
		case 0x77: return UNKNOWN;
		case 0x76: return ATOM;
		default: throw new AssertionError("Unknown datatype marker " + marker);
		}
	}
};

enum DataDimension { ATOMIC, COMPLEX, UNKNOWN };

// Codes for writing out data.  Order is important here.
/*
public static final byte NULL      = 0x1;
public static final byte BAG       = 0x51;
public static final byte TUPLE     = 0x60;
public static final byte MAP       = 0x52;
public static final byte INT       = 0x53;
public static final byte LONG      = 0x54;
public static final byte FLOAT     = 0x55;
public static final byte DOUBLE    = 0x56;
public static final byte CHARARRAY = 0x57;
public static final byte UNKNOWN   = 0x58;
public static final byte RECORD_1  = 0x21;
public static final byte RECORD_2  = 0x31;
public static final byte RECORD_3  = 0x41;
public static final byte ATOM      = CHARARRAY;
*/


/**
 * Encode the integer so that the high bit is set on the last
 * byte.
 * @param DataOutput to write to
 * @param int value to write
 * @throws IOException
 */
/*
static void encodeInt(DataOutput os, int i) throws IOException 
{
	if (i >> 28 != 0) os.write((i >> 28) & 0x7f);
	if (i >> 21 != 0) os.write((i >> 21) & 0x7f);
	if (i >> 14 != 0) os.write((i >> 14) & 0x7f);
	if (i >> 7 != 0) os.write((i >> 7) & 0x7f);
	os.write((i & 0x7f) | (1 << 7));
}
*/

/**
 * Decode an integer to be read out of a file
 * @param is DataInput to read from
 * @return decoded integer
 */
/*
static int decodeInt(DataInput is) throws IOException
{
	int i = 0;
	int c;
	while (true) {
		c = is.readUnsignedByte();
		if (c == -1) break;
		i <<= 7;
		i += c & 0x7f;
		if ((c & 0x80) != 0) break;
	}
	return i;
}
*/

/**
 * Read a datum from a file. This is used for reading intermediate
 * results.  Public so it can be used in unit tests.
 * @param in DataInput source.
 * @return a new Datum.
 * @throws IOException if one of the extensions' constructors throws it.
 */
/*
public static Datum readDatum(DataInput in) throws IOException
{
	byte b = in.readByte();
	DataType t = DataType.markerToType(b);
	switch (t) {
	case BAG: return DataBag.read(in);
	case TUPLE: return Tuple.read(in);
	case MAP: return DataMap.read(in);
	case INT: return DataInteger.read(in);
	case LONG: return DataLong.read(in);
	case FLOAT: return DataFloat.read(in);
	case DOUBLE: return DataDouble.read(in);
	case CHARARRAY: return DataCharArray.read(in);
	case UNKNOWN: return DataUnknown.read(in);
	default:
		throw new AssertionError("Invalid data type indicator " + b + " while reading Datum from binary file");
	}
}
*/


	/*
	@Override
	public abstract boolean equals(Object o);
	*/

/**
 * Get the datatype of this datum.
 * @return type of this datum.
 */
DataType getType();
	
/**
 * Find the size of this datum.
 * @return size
 */
long size();

/**
 * Find out if this is an atomic or complex data type.  It can also be
 * unknown.
 * @return ATOMIC if this is int, long, float, double, or chararray,
 * COMPLEX if it is a tuple, bag, or map, and UNKNOWN if it's unknown.
 */
DataDimension getDimension();

/**
 * Find out if this datum is null.
 * @return true if this datum is null, false otherwise.
 */
boolean isNull();

/**
 * Set this datum as null or not null.
 * @param isnull if true, datum will be set to null
 */
void setNull(boolean isNull);
	
/**
 * Write contained data to an output.
 * @param out DataOutput to write to.
 * @throws IOException
 */
void write(DataOutput out) throws IOException;
	
}

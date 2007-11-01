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
public abstract class DatumImpl implements Datum {


/**
 * Read a datum from a file. This is used for reading intermediate
 * results.  Public so it can be used in unit tests.
 * @param in DataInput source.
 * @return a new Datum.
 * @throws IOException if one of the extensions' constructors throws it.
 */
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
	case ATOM: return DataAtom.read(in);
	default:
		throw new AssertionError("Invalid data type indicator " + b + " while reading Datum from binary file");
	}
}


/**
 * Find out if this datum is null.
 * @return true if this datum is null, false otherwise.
 */
public final boolean isNull() { return mNull; }

/**
 * Set this datum as null or not null.
 * @param isnull if true, datum will be set to null
 */
public final void setNull(boolean isNull) { mNull = isNull; }

/**
 * Handle intertype sorting, so that it is consistent.  It is assumed the two
 * objects are not of the same type.
 * @param other Datum to compare to.
 * @return a -1 if this object's datatype is less than other's datatype, +1
 * if this object's datatype is greater than other's datatype. 
 */
protected int crossTypeCompare(Datum other)
{
	return getType().compareTo(other.getType());
}
	
// Indicates whether this datum is null.
private boolean mNull = false;
	     
}

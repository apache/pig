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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;

import org.apache.hadoop.io.WritableComparable;

import org.apache.pig.impl.util.PigLogger;

/**
 * an ordered list of Data
 */
public class Tuple extends ComplexDatum implements WritableComparable
{

static String			  defaultDelimiter = "[,\t]";
static String			  NULL = "__PIG_NULL__";

public static final byte RECORD_1  = 0x21;
public static final byte RECORD_2  = 0x31;
public static final byte RECORD_3  = 0x41;

/**
 * Construct a tuple with no fields.
 */
public Tuple() { this(0); }

/**
 * Construct a tuple with a known number of fields.  The fields will be
 * pre-populated with nulls.
 * @param numFields Number of fields in the tuple.
 */
public Tuple(long numFields)
{
	this(numFields, true);
}

/**
 * Construct a tuple with a knwn number of fields.
 * @param numFields Number of fields in the tuple.
 * @param prepopulate If true, prepopulate with nulls, otherwise leave the
 * tuple empty.  Should be called with false only by Tuple.read()
 */
private Tuple(long numFields, boolean prepopulate)
{
	mFields = new ArrayList<Datum>((int)numFields);
	if (prepopulate) {
		for (int i = 0; i < numFields; i++) {
			mFields.add(null);
		}
	}
}

/**
 * Construct a tuple from an existing List.
 */
public Tuple(List<Datum> mFieldsIn)
{
	mFields = new ArrayList<Datum>(mFieldsIn.size());
	mFields.addAll(mFieldsIn);
}

/**
 * Construct a tuple from an existing tuple.  The fields are not copied,
 * only referenced.
 */
public Tuple(Tuple t)
{
	mFields = new ArrayList<Datum>(t.mFields.size());
	mFields.addAll(t.mFields);
}
	
/**
 * shortcut, if tuple only has one field
 */
public Tuple(Datum fieldIn)
{
	mFields = new ArrayList<Datum>(1);
	mFields.add(fieldIn);
}

/**
 * Creates a tuple from a delimited line of text.  For now creates elements as
 * DataAtoms.  This should change once we have expressions that can handle the
 * new types.
 * 
 * @param textLine
 *			the line containing mFields of data
 * @param delimiter
 *			a regular expression of the form specified by String.split(). If null, the default
 *			delimiter "[,\t]" will be used.
 */
public Tuple(String textLine, String delimiter)
{
	if (delimiter == null) {
		delimiter = defaultDelimiter;
	}
	String[] splitString = textLine.split(delimiter, -1);
	mFields = new ArrayList<Datum>(splitString.length);
	for (int i = 0; i < splitString.length; i++) {
		mFields.add(new DataAtom(splitString[i]));
	}
}

/**
 * Creates a tuple from a delimited line of text. This will invoke Tuple(textLine, null)
 * 
 * @param textLine
 *			the line containing mFields of data
 */
public Tuple(String textLine) { this(textLine, defaultDelimiter); }

public Tuple(Tuple[] otherTs)
{
	mFields = new ArrayList<Datum>(otherTs.length);
	for (int i = 0; i < otherTs.length; i++) {
			appendTuple(otherTs[i]);
	}
}

public DataType getType() { return Datum.DataType.TUPLE; }

public long size() { return mFields.size(); }

public void copyFrom(Tuple otherT)
{
	this.mFields = otherT.mFields;
}

/**
 * @deprecated Using size instead.
 */
public int arity() { return (int)size(); }

@Override
public String toString()
{
	StringBuffer sb = new StringBuffer();
	sb.append('(');
	for (Iterator<Datum> it = mFields.iterator(); it.hasNext();) {
		Datum d = it.next();
		if(d != null) {
			sb.append(d.toString());
		} else {
			sb.append(NULL);
		}
		if (it.hasNext())
			sb.append(", ");
	}
	sb.append(')');
	String s = sb.toString();
	return s;
}

public final void setField(long i, Datum val) throws IOException
{
	if (i >= mFields.size()) {
		throw new IOException("Column number out of range, tried to access " + i + " in a tuple of only " + mFields.size() + "columns");
	}
	mFields.set((int)i, val);
}

/**
 * Set a field with an int value. Push it into a DataAtom for the moment,
 * eventually we'll change this to a DataInteger.
 * @param val integer value to set field to.
 */
public final void setField(int i, int val) throws IOException
{
	setField(i, new DataAtom(val));
}

/**
 * Set a field with a double value. Push it into a DataAtom for the moment,
 * eventually we'll change this to a DataDouble.
 * @param val double value to set field to.
 */
public void setField(int i, double val) throws IOException
{
	setField(i, new DataAtom(val));
}

/**
 * Set a field with an string value. Push it into a DataAtom for the moment,
 * eventually we'll change this to a DataCharArrayUtf16.
 * @param val string value to set field to.
 */
public void setField(int i, String val) throws IOException
{
	setField(i, new DataAtom(val));
}

/**
 * Get a field from the tuple.
 * @param i Field to get
 * @return Field value, as Datum.
 */
public final Datum getField(long i)
{
	Datum d = null;
	if ((i >= mFields.size()) || ((d = mFields.get((int)i)) == null)) {
		d = new DataUnknown();
		d.setNull(true);
	}
	return d;
}

/**
 * @deprecated Use specific types instead of DataAtom.
 * Get field i, if it is an Atom or can be coerced into an Atom.
 * @param i field to get as an atom.
 * @return contents of the field. If its of type DataAtom then that will
 * be returned.  If it's of tuple of one field, then that field will be
 * returned.  If it's a bag of one element, then that element will be
 * returned.  If it's one of the new atomic types (int, etc.) it will push
 * that into a data atom and return that.
 * @throws IOException if the field isn't an atom and it can't figure out
 * how to do the coercion.
 */
public DataAtom getAtomField(int i) throws IOException
{
	Datum field = getField(i); // throws exception if field doesn't exist

	// This shouldn't actually ever happen anymore.
	if (field instanceof DataAtom) return (DataAtom) field;

	switch (field.getType()) {
	case INT: return new DataAtom(((DataInteger)field).get());
	case LONG: return new DataAtom(((DataLong)field).get());
	case FLOAT: return new DataAtom(((DataFloat)field).get());
	case DOUBLE: return new DataAtom(((DataDouble)field).get());
	case UNKNOWN: return new DataAtom(((DataUnknown)field).get());
	case CHARARRAY: 
		switch (((DataCharArray)field).getEncoding()) {
		case UTF16: 
			return new DataAtom(((DataCharArrayUtf16)field).get());
		case NONE: 
			return new DataAtom(((DataCharArrayNone)field).get());
		default: throw new AssertionError("Unknown encoding");
		}

	// Can't use getFieldAsAtomic for tuple and bag because these need to
	// recurse to getAtomField instead.
	case TUPLE: {
		Tuple t = (Tuple) field;
		if (t.size() == 1) {
			PigLogger.getLogger().warn("Asked for an atom field but found a tuple with one field.");
			return t.getAtomField(0);
		}
		break;
							   }

	case BAG: {
		DataBag b = (DataBag) field;
		if (b.bagOf() == Datum.DataType.TUPLE && b.size() == 1) {
			Tuple t = (Tuple)b.content().next();
			if (t.size() == 1) {
				PigLogger.getLogger().warn("Asked for an atom field but found a bag with a tuple with one field.");
				return t.getAtomField(0);
			}
		}
		break;
							 }

	default: break;
	}
	throw new IOException("Incompatible type for request getAtomField().");
}

/**
 * Get field i, if it is an Atomic type or can be coerced into an Atomic
 * type.
 * @param i field to get as an atomic type.
 * @return contents of the field. If its an atomic type it will be
 * returned as is.  If it is unknown, it will be converted to a char array
 * none and returned as that.  If it's of tuple of one field, then
 * that field will be returned.  If it's a bag of one element, then
 * that element will be returned.
 * @throws IOException if the field isn't an atom and it can't figure out
 * how to do the coercion.
 */
public AtomicDatum getFieldAsAtomic(int i) throws IOException
{
	Datum field = getField(i); // throws exception if field doesn't exist

	if (field.getDimension() == Datum.DataDimension.ATOMIC) {
		return (AtomicDatum)field;
	}

	switch (field.getType()) {
	case UNKNOWN:
		return new DataCharArrayNone(((DataUnknown)field).get());

	case TUPLE: {
		Tuple t = (Tuple) field;
		if (t.size() == 1) {
			PigLogger.getLogger().warn("Warning: Asked for an atom field but found a tuple with one field.");
			return t.getFieldAsAtomic(0);
		}
		break;
				}

	case BAG: {
		DataBag b = (DataBag) field;
		if (b.bagOf() == Datum.DataType.TUPLE && b.size() == 1) {
			Tuple t = (Tuple)b.content().next();
			if (t.size() == 1) {
				PigLogger.getLogger().warn("Warning: Asked for an atom field but found a bag with a tuple with one field.");
				return t.getFieldAsAtomic(0);
			}
		}
		break;
			  }

	default: break;
	}

	throw new IOException("Incompatible type for request getAtomField().");
}

/**
 * Attempt to fetch a field as a tuple.
 * @param i field number to get.
 * @return If the field is a tuple, return it.  If it's bag of one tuple,
 * return it.  Otherwise... 
 * @throws IOException if the field is neither a tuple nor a bag of one
 * tuple.
 */
public Tuple getTupleField(int i) throws IOException
{
	Datum field = getField(i); // throws exception if field doesn't exist

	if (field.getType() == Datum.DataType.TUPLE) {
		return (Tuple) field;
	} else if (field.getType() == Datum.DataType.BAG) {
		DataBag b = (DataBag) field;
		if (b.bagOf() == Datum.DataType.TUPLE && b.size() == 1) {
			return (Tuple)b.content().next();
		}
	}

	throw new IOException("Incompatible type for request getTupleField().");
}

/**
 * Attempt to fetch a field as a bag.
 * @param i field number to get.
 * @return If the field is a bag, return it. Otherwise... 
 * @throws IOException if the field is not a bag.
 */
public DataBag getBagField(int i) throws IOException
{
	Datum field = getField(i); // throws exception if field doesn't exist

	if (field.getType() == Datum.DataType.BAG) return (DataBag) field;

	throw new IOException("Incompatible type for request getBagField().");
}

public final void appendTuple(Tuple other)
{
	for (Iterator<Datum> it = other.mFields.iterator(); it.hasNext();) {
		mFields.add(it.next());
	}
}

public final void appendField(Datum newField) { mFields.add(newField); }

public String toDelimitedString(String delim) throws IOException
{
	StringBuffer buf = new StringBuffer();
	for (Iterator<Datum> it = mFields.iterator(); it.hasNext();) {
		Datum field = it.next();
		if (field.getDimension() == Datum.DataDimension.COMPLEX) {
			throw new IOException("Unable to convert non-flat tuple to string.");
		}

		buf.append(field.toString());
		if (it.hasNext()) buf.append(delim);
	}
	return buf.toString();
}

/*
	public boolean lessThan(Tuple other) {
		return (this.compareTo(other) < 0);
	}

	public boolean greaterThan(Tuple other) {
		return (this.compareTo(other) > 0);
	}
	
	*/

/**
 * See if two tuples are equal to each other.  Tuple equality is defined as being
 * of the same size, and for each field f1...fn in t1 and fields g1...gn in t2,
 * f1.equals(g1) ... fn.equals(gn) holds true.
 * Don't make this use compareTo.  These functions are used in things like hashs
 * and we want them to be as fast as possible.
 */
@Override
public boolean equals(Object other)
{
	if (!(other instanceof Tuple)) return false;

	Tuple t = (Tuple)other;

	long sz = size();

	if (t.size() != sz) return false;

	for (long i = 0; i < sz; i++) {
		if (!t.getField(i).equals(getField(i))) return false;
	}

	return true;
}

public int compareTo(Object other)
{
	if (!(other instanceof Datum)) return -1;

	Datum od = (Datum)other;

	if (od.getType() != Datum.DataType.TUPLE) return crossTypeCompare(od);

	Tuple t = (Tuple)od;

	long sz = size();
	long tsz = t.size();
	if (sz < tsz) return -1;
	else if (sz > tsz) return 1;

	for (long i = 0; i < sz; i++) {
		int c = mFields.get((int)i).compareTo(t.mFields.get((int)i));
		if (c != 0) return c;
	}
	return 0;
}

@Override
public int hashCode()
{
	int hash = 1;
	for (Iterator<Datum> it = mFields.iterator(); it.hasNext();) {
		Datum f = it.next();
		if (f == null) hash += 1;
		else hash = 31 * hash + f.hashCode();
	}
	return hash;
}

// WritableComparable methods:
   
@Override
public void write(DataOutput out) throws IOException
{
	out.write(Datum.DataType.TUPLE.getMarker());
	long n = size();
	out.writeLong(n);
	for (long i = 0; i < n; i++) {
		Datum d = getField((int)i);
		if (d != null){
			d.write(out);
		} else {
			throw new RuntimeException("Null field in tuple");
		}
	}
}

/**
 * This method is invoked when the beginning 'TUPLE' is still on the stream.
 * @param in DataInput to read from
 * @throws IOExcetion if the expected data isn't a tuple.
 */
public void readFields(DataInput in) throws IOException
{
	byte[] b = new byte[1];
	in.readFully(b);
	if (b[0] != Datum.DataType.TUPLE.getMarker())
		throw new IOException("Unexpected data while reading tuple from binary file");
	Tuple t = read(in);
	mFields = t.mFields;
}
	
//This method is invoked when the beginning 'TUPLE' has been read off the stream
public static Tuple read(DataInput in) throws IOException
{
	long size = in.readLong();

	// nuke the old contents of the tuple
	Tuple ret = new Tuple(size, false);

	for (int i = 0; i < size; i++) {
		ret.appendField(DatumImpl.readDatum(in));
	}
		
	return ret;
}
	
	/*
public static Datum readDatum(DataInput in) throws IOException
{
	byte[] b = new byte[1];
	in.readFully(b);
	switch (b[0]) {
	case TUPLE:
		return Tuple.read(in);
	case BAG:
		return DataBag.read(in);
	case MAP:
		return DataMap.read(in);
	case INT:
		return DataInt.read(in);
	case LONG:
		return DataLong.read(in);
	case FLOAT:
		return DataFloat.read(in);
	case DOUBLE:
		return DataDouble.read(in);
	case UNKNOWN:
		return DataUnknown.read(in);
	default:
		throw new AssertionError("Invalid data type indicator " + b[0] + " while reading Datum from binary file");
	}
}

	//  Encode the integer so that the high bit is set on the last
	// byte
	static void encodeInt(DataOutput os, int i) throws IOException {
		if (i >> 28 != 0)
			os.write((i >> 28) & 0x7f);
		if (i >> 21 != 0)
			os.write((i >> 21) & 0x7f);
		if (i >> 14 != 0)
			os.write((i >> 14) & 0x7f);
		if (i >> 7 != 0)
			os.write((i >> 7) & 0x7f);
		os.write((i & 0x7f) | (1 << 7));
	}

	static int decodeInt(DataInput is) throws IOException {
		int i = 0;
		int c;
		while (true) {
			c = is.readUnsignedByte();
			if (c == -1)
				break;
			i <<= 7;
			i += c & 0x7f;
			if ((c & 0x80) != 0)
				break;
		}
		return i;
	}
	*/

protected ArrayList<Datum> mFields;

}

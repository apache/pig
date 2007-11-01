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
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Iterator;

/**
 * A datum that contain a map.
 */

public class DataMap extends ComplexDatum {

public DataMap()
{
	mContent = new HashMap<AtomicDatum, Datum>();
}
	
public DataType getType() { return Datum.DataType.MAP; }

/**
 * @deprecated Use size() 
 * @return the cardinality of the data map
 */
public int cardinality() { return (int)size(); }

public long size() { return mContent.size(); }
	
/**
 * Adds the key value pair to the map.  Does not make a copy of the key or
 * value, simply stores a reference to them.
 * @param key
 * @param value
 */
public final void put(AtomicDatum key, Datum value) { mContent.put(key, value); }

/**
 * @deprecated
 * Add a string key with a Datum value to a map.  This is included for backward
 * compatibility only.
 * @param key
 * @param value
 */
public final void put(String key, Datum value)
{
	put(new DataAtom(key), value);
}
	/**
 * @deprecated
 * Add a string key/value pair to a map.  This is included for backward
 * compatibility only.
 * @param key
 * @param value
 */
public final void put(String key, String value)
{
	put(new DataAtom(key), new DataAtom(value));
}
	
/**
 * Fetch the value corresponding to a given key
 * @param key
 * @return Value, as a datum
 */
public final Datum get(AtomicDatum key)
{
	Datum d = mContent.get(key);
	if (d == null) {
		d = new DataUnknown();
		d.setNull(true);
		return d;
	} else {
		return d;
	}
}
	
@Override
public String toString()
{
	StringBuffer sb = new StringBuffer();
	sb.append('[');
	Iterator<Map.Entry<AtomicDatum, Datum> > i;
	for (i = mContent.entrySet().iterator(); i.hasNext(); ) {
		Map.Entry<AtomicDatum, Datum> e = i.next();
		sb.append(e.getKey().toString());
		sb.append('#');
		sb.append(e.getValue().toString());
		if (i.hasNext()) sb.append(", ");
	}
	sb.append(']');
	return sb.toString();
}

public int hashCode()
{
	int hash = 0;
	Iterator<Map.Entry<AtomicDatum, Datum> > i;
	for (i = mContent.entrySet().iterator(); i.hasNext(); ) {
		Map.Entry<AtomicDatum, Datum> e = i.next();
		hash += (e.getKey().hashCode() ^ e.getValue().hashCode());
	}
	return hash;
}

// Don't make this use compareTo.  These functions are used in things like hashs
// and we want them to be as fast as possible.
/*
public boolean equals(Object other)
{
	if (!(other instanceof DataMap)) return false;
	DataMap o = (DataMap)other;
	if (mContent.size() != o.mContent.size()) return false;

	Iterator<Map.Entry<AtomicDatum, Datum> > i;
	Iterator<Map.Entry<AtomicDatum, Datum> > j;
	for (i = mContent.entrySet().iterator(), j = o.mContent.entrySet().iterator();
			i.hasNext(); ) {
		Map.Entry<AtomicDatum, Datum> us = i.next();
		Map.Entry<AtomicDatum, Datum> them = j.next();
		if (!us.getKey().equals(them.getKey()) ||
				!us.getValue().equals(them.getValue())) return false;
	}

	return true;
}
*/

public boolean equals(Object other)
{
	if (!(other instanceof DataMap)) return false;
	DataMap o = (DataMap)other;
	if (mContent.size() != o.mContent.size()) return false;

	Iterator<Map.Entry<AtomicDatum, Datum> > i;
	for (i = mContent.entrySet().iterator(); i.hasNext(); ) {
		Map.Entry<AtomicDatum, Datum> us = i.next();
		Datum val = (Datum)o.get(us.getKey());
		if (val == null) return false;
		if (!val.equals(us.getValue())) return false;
	}

	return true;
}

public int compareTo(Object other)
{
	if (!(other instanceof Datum)) return -1;

	Datum od = (Datum)other;

	if (od.getType() != Datum.DataType.MAP) return crossTypeCompare(od);

	DataMap map = (DataMap)od;

	if (mContent.size() < map.mContent.size()) return -1;
	else if (mContent.size() > map.mContent.size()) return 1;

	Iterator<Map.Entry<AtomicDatum, Datum> > i;
	Iterator<Map.Entry<AtomicDatum, Datum> > j;
	for (i = mContent.entrySet().iterator(), j = map.mContent.entrySet().iterator();
			i.hasNext(); ) {
		Map.Entry<AtomicDatum, Datum> us = i.next();
		Map.Entry<AtomicDatum, Datum> them = j.next();
		int keyrc = us.getKey().compareTo(them.getKey());
		if (keyrc != 0) {
			return keyrc;
		} else {
			int valrc = us.getValue().compareTo(them.getValue());
			if (valrc != 0) return valrc;
		}
	}

	return 0;
}


public static DataMap read(DataInput in) throws IOException
{
	long size = in.readLong();
	DataMap ret = new DataMap();
	for (long i = 0; i < size; i++) {
		Datum key = DatumImpl.readDatum(in);
		if (key.getDimension() == Datum.DataDimension.COMPLEX) {
			throw new IOException("Maps only accept atomic and unknown data types as keys");
		}
		ret.put((AtomicDatum)key, DatumImpl.readDatum(in));
	}
	return ret;
}
	
@Override
public void write(DataOutput out) throws IOException
{
	out.write(Datum.DataType.MAP.getMarker());
	out.writeLong(size());
	for (Entry<AtomicDatum, Datum> e: mContent.entrySet()){
		AtomicDatum k = e.getKey();
		k.write(out);
		e.getValue().write(out);
 	}
}

private Map<AtomicDatum, Datum> mContent;

}

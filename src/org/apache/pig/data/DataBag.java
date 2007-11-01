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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.pig.impl.eval.EvalSpec;
import org.apache.pig.impl.eval.collector.DataCollector;


/**
 * A collection of Data values of a given type.  For performance reasons
 * types are not checked on add or read.
 */
public class DataBag extends ComplexDatum {
	
/**
 * Create an empty bag of the indicated type.
 * @param elementType what this will be a bag of.
 */
public DataBag(Datum.DataType elementType)
{
	mContent = new ArrayList<Datum>();
	mElementType = elementType;
}

/**
 * Create a bag based on a list.  The element type will be taken from the
 * first Datum in the list.
 * @param c list of data to place in the bag.  This list cannot be empty.
 * @throws IOException if the list is empty.
 */
public DataBag(List<Datum> c) throws IOException
{
	mContent = c;
	if (c.size() == 0) {
		throw new IOException("Attempt to instantiate empty bag with no type.");
	}
	mElementType = c.get(0).getType();
}

/**
 * Create a bag given a single datum.  The element type will be taken from
 * the datum.
 * @param t The datum to use.  The datum must not be java null (pig null
 * is ok).
 * @throws IOException if the datum is null.
 */
public DataBag(Datum t) throws IOException
{
	if (t == null) {
		throw new IOException("Attempt to instantiate empty bag with no type.");
	}
	mContent = new ArrayList<Datum>();
	mContent.add(t);
	mElementType = t.getType();
}

/**
 * @return BAG
 */
public DataType getType() { return Datum.DataType.BAG; }

/**
 * Find out what this is a bag of.
 * @return datatype
 */
public DataType bagOf() { return mElementType; }

public long size() { return mContent.size(); }

/**
 * @deprecated Use size() instead.
 */
public int cardinality() { return (int)size(); }

/**
 * Checks if the size of the bag is empty.
 */
public boolean isEmpty() { return mContent.size() == 0; }
	
public int compareTo(Object other)
{
	if (!(other instanceof Datum)) return -1;

	Datum od = (Datum)other;

	if (od.getType() != Datum.DataType.BAG) return crossTypeCompare(od);

	DataBag bag = (DataBag)od;

	Datum.DataType dt = bagOf();
	Datum.DataType dto = bag.bagOf();
	if (dt != dto) return dt.compareTo(dto);

	long sz = size();
	long tsz = bag.size();
	if (sz < tsz) return -1;
	else if (sz > tsz) return 1;

	Iterator<Datum> i = content();
	Iterator<Datum> j = bag.content();
	while (i.hasNext()) {
		Datum us = i.next();
		Datum them = j.next();
		int rc = us.compareTo(them);
		if (rc != 0) return rc;
	}

	return 0;
}
	
// Don't make this use compareTo.  These functions are used in things like hashs
// and we want them to be as fast as possible.
@Override
public boolean equals(Object other)
{
	if (!(other instanceof DataBag)) return false;

	DataBag bag = (DataBag)other;

	long sz = size();

	if (bagOf() != bag.bagOf()) return false;
	if (bag.size() != sz) return false;

	Iterator<Datum> i = content();
	Iterator<Datum> j = bag.content();
	while (i.hasNext()) {
		Datum us = i.next();
		Datum them = j.next();
		if (!us.equals(them)) return false;
	}

	return true;
}
	
public void sort()
{
	Collections.sort(mContent);
	mIsSorted = true;
}
	
public void sort(EvalSpec spec)
{
	Collections.sort(mContent, spec.getComparator());
	mIsSorted = true;
}
	
public void arrange(EvalSpec spec)
{
	sort(spec);
	mIsSorted = true;
}
	
public void distinct()
{
	// ARG!!!! We're sorting the whole thing and then doing a distinct.  Need to
	// change this to do distinct during sort.
	Collections.sort(mContent);
	mIsSorted = true;
		
	Tuple lastTup = null;
	for (Iterator<Datum> it = mContent.iterator(); it.hasNext(); ) {
		Tuple thisTup = (Tuple)it.next();
			
		if (lastTup == null) {
			lastTup = thisTup;
			continue;
		}
			
		if (thisTup.compareTo(lastTup) == 0) {
			it.remove();
		} else {
			lastTup = thisTup;
		}
	}
}

/**
 * Get an iterator to the contents of the bag.  The iterator is an
 * iterator of Datum.  If something else is expected the caller will have to
 * cast it.
 */
public Iterator<Datum> content() { return mContent.iterator(); }

/**
 * Add a datum to the bag.  The datatype of the datum should match the
 * result of bagOf(), but that will not be checked in the interest of
 * speed.  Would like this method to be final, but BigDataBag overrides it.
 */
public void add(Datum e)
{
	if (e != null) mContent.add(e);
}

/**
 * Add the contents of a bag to the bag.  The datatype of the data should match the
 * result of bagOf(), but that will not be checked in the interest of
 * speed.
 */
public final void addAll(DataBag b)
{
	Iterator<Datum> it = b.content();
	while (it.hasNext()) {
		add(it.next());
	}
}

/**
 * Remove a particular datum from the bag.  This operation will be slow
 * and should not be used much.
 */
public void remove(Datum d) { mContent.remove(d); }

/**
 * Returns the value of field i. Since there may be more than one tuple in the bag, this
 * function throws an exception if it is not the case that all tuples agree on this field
 */
/*
public DataAtom getField(int i) throws IOException
{
	DataAtom val = null;

	for (Iterator<Tuple> it = mContent(); it.hasNext();) {
		DataAtom currentVal = it.next().getAtomField(i);

		if (val == null) {
			val = currentVal;
		} else {
			if (!val.strval().equals(currentVal.strval()))
				throw new IOException("Cannot call getField on a databag unless all tuples agree.");
		}
	}

	if (val == null)
		throw new IOException("Cannot call getField on an empty databag.");

	return val;
}
*/

/**
 * Empty the bag of its contents.  It retains they type of bag it is.
 */
public void clear()
{
	mContent.clear();
	mIsSorted = false;
}
	
@Override
public void write(DataOutput out) throws IOException
{
	 out.write(Datum.DataType.BAG.getMarker());
	 // Now write out the element type, so the reader knows what kind of bag to
	 // instantiate.
	 out.write(mElementType.getMarker());
	 out.writeLong(size());
	 Iterator<Datum> it = content();
	 while (it.hasNext()) {
		 Datum item = it.next();
		 item.write(out);
	 }	
}
	
public static abstract class BagDelimiterTuple extends Tuple{}
public static class StartBag extends BagDelimiterTuple{}
	
public static class EndBag extends BagDelimiterTuple{}
	
public static final Tuple startBag = new StartBag();
public static final Tuple endBag = new EndBag();
	
static DataBag read(DataInput in) throws IOException
{
	DataType etype = Datum.DataType.markerToType(in.readByte());

	long size = in.readLong();
	DataBag ret = new DataBag(etype);
	// TODO
	//DataBag ret = BagFactory.getInstance().getNewBag();
		
	for (int i = 0; i < size; i++) {
		ret.add(DatumImpl.readDatum(in));
	}
	return ret;
}
	
public void markStale(boolean stale){}
	
@Override
public String toString() {
	StringBuffer sb = new StringBuffer();
	sb.append('{');
	Iterator<Datum> it = content();
	while ( it.hasNext() ) {
		Datum t = it.next();
		String s = t.toString();
		sb.append(s);
		if (it.hasNext())
			sb.append(", ");
	}
	sb.append('}');
	return sb.toString();
}
	
public boolean mIsSorted(){ return mIsSorted; }
	
protected boolean mIsSorted = false;
protected List<Datum> mContent;
protected Datum.DataType mElementType;

}

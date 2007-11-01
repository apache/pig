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
package org.apache.pig.test;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileInputStream;
import java.io.IOException;

import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;

import org.junit.Test;

import org.apache.pig.data.*;

/**
 * This class will exercise the data bag data type.
 * 
 * @author gates
 */
public class TestDataBag extends junit.framework.TestCase
{

public void testDefaultConstructor() throws Exception
{
	DataBag bag = new DataBag(Datum.DataType.INT);

	assertEquals("getType", Datum.DataType.BAG, bag.getType());
	assertFalse("is null", bag.isNull());
	assertTrue("bag of ints", bag.bagOf() == Datum.DataType.INT);

	assertEquals("Default constructor size before", 0, bag.size());
	DataInteger val = new DataInteger(42);

	bag.add(val);
	assertEquals("Default constructor size after", 1, bag.size());

	Iterator<Datum> i = bag.content();
	Datum d = i.next();

	assertTrue("should be an integer", d.getType() == Datum.DataType.INT);
	assertNotNull("get with entry in bag", d);
	assertEquals("value of val", 42, ((DataInteger)d).get());
}

public void testListConstructor() throws Exception
{
	List<Datum> list = new ArrayList<Datum>();
	list.add(new DataInteger(10));
	list.add(new DataInteger(11));
	list.add(new DataInteger(9));

	DataBag bag = new DataBag(list);

	assertEquals("list construct size", 3L, bag.size());

	Iterator<Datum> i = bag.content();
	Datum d = i.next();
	assertNotNull("get first entry in bag", d);
	assertTrue("should be an integer", d.getType() == Datum.DataType.INT);
	assertEquals("first value of val", 10, ((DataInteger)d).get());
	d = i.next();
	assertNotNull("get second entry in bag", d);
	assertTrue("should be an integer", d.getType() == Datum.DataType.INT);
	assertEquals("second value of val", 11, ((DataInteger)d).get());
	d = i.next();
	assertNotNull("get third entry in bag", d);
	assertTrue("should be an integer", d.getType() == Datum.DataType.INT);
	assertEquals("third value of val", 9, ((DataInteger)d).get());
	assertFalse("bag should be exhausted now", i.hasNext());

	bag.add(new DataInteger(4));
	i = bag.content();
	d = i.next();
	d = i.next();
	d = i.next();
	d = i.next();
	assertNotNull("get fourth entry in bag", d);
	assertTrue("should be an integer", d.getType() == Datum.DataType.INT);
	assertEquals("fourth value of val", 4, ((DataInteger)d).get());
	assertFalse("bag should be exhausted now", i.hasNext());
}


public void testBigBag() throws Exception
{
	DataBag bag = new DataBag(Datum.DataType.INT);

	for (int i = 0; i < 10000; i++) {
		bag.add(new DataInteger(i));
	}

	assertEquals("big size after loading", 10000, bag.size());

	Iterator<Datum> i = bag.content();
	for (int j = 0; j < 10000; j++) {
		assertTrue("should still have data", i.hasNext());
		Datum val = i.next();
		assertTrue("should be an integer", val.getType() == Datum.DataType.INT);
		assertEquals("value of val", j, ((DataInteger)val).get());
	}
	assertFalse("bag should be exhausted now", i.hasNext());
}

public void testToString() throws Exception
{
	DataBag bag = new DataBag(Datum.DataType.INT);

	bag.add(new DataInteger(1));
	bag.add(new DataInteger(1));
	bag.add(new DataInteger(3));

	assertEquals("toString", "{1, 1, 3}", bag.toString());
}

public void testEquals() throws Exception
{
	DataBag bag1 = new DataBag(Datum.DataType.INT);
	DataBag bag2 = new DataBag(Datum.DataType.INT);

	bag1.add(new DataInteger(3));
	bag2.add(new DataInteger(3));

	assertFalse("different object", bag1.equals(new String()));

	assertTrue("same data", bag1.equals(bag2));

	bag2 = new DataBag(Datum.DataType.INT);
	bag2.add(new DataInteger(4));
	assertFalse("different data", bag1.equals(bag2));

	bag2 = new DataBag(Datum.DataType.INT);
	bag2.add(new DataInteger(3));
	bag2.add(new DataInteger(3));
	assertFalse("different size", bag1.equals(bag2));

	bag2 = new DataBag(Datum.DataType.LONG);
	bag2.add(new DataLong(3));
	assertFalse("different type of bag", bag1.equals(bag2));
}

public void testCompareTo() throws Exception
{
	DataBag bag1 = new DataBag(Datum.DataType.INT);
	DataBag bag2 = new DataBag(Datum.DataType.INT);

	bag1.add(new DataInteger(3));
	bag2.add(new DataInteger(3));

	assertEquals("different object less than", -1, bag1.compareTo(new String()));

	Tuple t = new Tuple();
	assertTrue("less than tuple", bag1.compareTo(t) < 0);
	DataMap map = new DataMap();
	assertTrue("less than map", bag1.compareTo(map) < 0);
	DataLong l = new DataLong();
	assertTrue("less than long", bag1.compareTo(l) < 0);
	DataFloat f = new DataFloat();
	assertTrue("less than float", bag1.compareTo(f) < 0);
	DataDouble d = new DataDouble();
	assertTrue("less than double", bag1.compareTo(d) < 0);
	DataUnknown unk = new DataUnknown();
	assertTrue("less than unknown", bag1.compareTo(unk) < 0);
	DataCharArrayUtf16 utf16 = new DataCharArrayUtf16();
	assertTrue("less than utf16", bag1.compareTo(utf16) < 0);

	assertEquals("same data equal", 0,  bag1.compareTo(bag2));

	bag2 = new DataBag(Datum.DataType.INT);
	bag2.add(new DataInteger(2));
	assertEquals("greater than bag with lesser value", 1, bag1.compareTo(bag2));

	bag2 = new DataBag(Datum.DataType.INT);
	bag2.add(new DataInteger(4));
	assertEquals("less than bag with greater value", -1, bag1.compareTo(bag2));

	bag2 = new DataBag(Datum.DataType.INT);
	bag2.add(new DataInteger(3));
	bag2.add(new DataInteger(4));
	assertEquals("less than bigger bag", -1, bag1.compareTo(bag2));

	bag2 = new DataBag(Datum.DataType.INT);
	assertEquals("greater than smaller bag", 1, bag1.compareTo(bag2));

	bag2 = new DataBag(Datum.DataType.LONG);
	bag2.add(new DataLong(3));
	assertEquals("different type of bag", -1, bag1.compareTo(bag2));
}


public void testWriteReadUnknown() throws Exception
{
	DataBag before = new DataBag(Datum.DataType.UNKNOWN);

	String s = new String("zzz");
	before.add(new DataUnknown(s.getBytes()));
	s = new String("yyy");
	before.add(new DataUnknown(s.getBytes()));
	s = new String("xxx");
	before.add(new DataUnknown(s.getBytes()));

	File file = null;
	file = File.createTempFile("DataBagUnknown", "put");
	FileOutputStream fos = new FileOutputStream(file);
	DataOutput out = new DataOutputStream(fos);
	before.write(out);
	fos.close();

	FileInputStream fis = new FileInputStream(file);
	DataInput in = new DataInputStream(fis);
	Datum a = DatumImpl.readDatum(in);

	assertTrue("isa DataBag", a instanceof DataBag);

	DataBag after = (DataBag)a;

	assertTrue("bag of unknowns", after.bagOf() == Datum.DataType.UNKNOWN);
	assertEquals("after read, size", 3, after.size()); 

	Iterator<Datum> j = after.content();

	Datum valAfter = j.next();
	assertTrue("should be an unknown",
		valAfter.getType() == Datum.DataType.UNKNOWN);
	for (int i = 0; i < 3; i++) {
		assertEquals("value of valAfter", (byte)0x7a,
			((DataUnknown)valAfter).get()[i]);
	}

	valAfter = j.next();
	assertTrue("should be an unknown",
		valAfter.getType() == Datum.DataType.UNKNOWN);
	for (int i = 0; i < 3; i++) {
		assertEquals("value of valAfter", (byte)0x79,
			((DataUnknown)valAfter).get()[i]);
	}

	valAfter = j.next();
	assertTrue("should be an unknown",
		valAfter.getType() == Datum.DataType.UNKNOWN);
	for (int i = 0; i < 3; i++) {
		assertEquals("value of valAfter", (byte)0x78,
			((DataUnknown)valAfter).get()[i]);
	}

	assertFalse("should have read all values in bag", j.hasNext());
	
	file.delete();
}

public void testWriteReadInt() throws Exception
{
	DataBag before = new DataBag(Datum.DataType.INT);

	before.add(new DataInteger(99));
	before.add(new DataInteger(-98));
	before.add(new DataInteger(97));

	File file = null;
	file = File.createTempFile("DataBagInteger", "put");
	FileOutputStream fos = new FileOutputStream(file);
	DataOutput out = new DataOutputStream(fos);
	before.write(out);
	fos.close();

	FileInputStream fis = new FileInputStream(file);
	DataInput in = new DataInputStream(fis);
	Datum a = DatumImpl.readDatum(in);

	assertTrue("isa DataBag", a instanceof DataBag);

	DataBag after = (DataBag)a;

	assertTrue("bag of ints", after.bagOf() == Datum.DataType.INT);

	assertEquals("after read, size", 3, after.size()); 

	Iterator<Datum> j = after.content();

	Datum val = j.next();
	assertTrue("should be an integer", val.getType() == Datum.DataType.INT);
	assertEquals("value of valAfter", 99, ((DataInteger)val).get());

	val = j.next();
	assertTrue("should be an integer", val.getType() == Datum.DataType.INT);
	assertEquals("value of valAfter2", -98, ((DataInteger)val).get());

	val = j.next();
	assertTrue("should be an integer", val.getType() == Datum.DataType.INT);
	assertEquals("value of valAfter", 97, ((DataInteger)val).get());

	assertFalse("should have read all values in bag", j.hasNext());
		
	file.delete();
}

public void testWriteReadLong() throws Exception
{
	DataBag before = new DataBag(Datum.DataType.LONG);

	before.add(new DataLong(99000000000L));
	before.add(new DataLong(-98L));
	before.add(new DataLong(97L));

	File file = null;
	file = File.createTempFile("DataBagLong", "put");
	FileOutputStream fos = new FileOutputStream(file);
	DataOutput out = new DataOutputStream(fos);
	before.write(out);
	fos.close();

	FileInputStream fis = new FileInputStream(file);
	DataInput in = new DataInputStream(fis);
	Datum a = DatumImpl.readDatum(in);

	assertTrue("isa DataBag", a instanceof DataBag);

	DataBag after = (DataBag)a;

	assertTrue("bag of longs", after.bagOf() == Datum.DataType.LONG);
	assertEquals("after read, size", 3, after.size()); 

	Iterator<Datum> j = after.content();

	Datum val = j.next();
	assertTrue("should be a long", val.getType() == Datum.DataType.LONG);
	assertEquals("value of valAfter", 99000000000L, ((DataLong)val).get());

	val = j.next();
	assertTrue("should be a long", val.getType() == Datum.DataType.LONG);
	assertEquals("value of valAfter2", -98L, ((DataLong)val).get());

	val = j.next();
	assertTrue("should be a long", val.getType() == Datum.DataType.LONG);
	assertEquals("value of valAfter", 97L, ((DataLong)val).get());

	assertFalse("should have read all values in bag", j.hasNext());
		
	file.delete();
}

public void testWriteReadFloat() throws Exception
{
	DataBag before = new DataBag(Datum.DataType.FLOAT);

	before.add(new DataFloat(3.2e32f));
	before.add(new DataFloat(-9.929292e-29f));
	before.add(new DataFloat(97.0f));

	File file = null;
	file = File.createTempFile("DataBagFloat", "put");
	FileOutputStream fos = new FileOutputStream(file);
	DataOutput out = new DataOutputStream(fos);
	before.write(out);
	fos.close();

	FileInputStream fis = new FileInputStream(file);
	DataInput in = new DataInputStream(fis);
	Datum a = DatumImpl.readDatum(in);

	assertTrue("isa DataBag", a instanceof DataBag);

	DataBag after = (DataBag)a;

	assertTrue("bag of floats", after.bagOf() == Datum.DataType.FLOAT);
	assertEquals("after read, size", 3, after.size()); 

	Iterator<Datum> j = after.content();

	Datum val = j.next();
	assertTrue("should be a float", val.getType() == Datum.DataType.FLOAT);
	assertEquals("value of valAfter", 3.2e32f, ((DataFloat)val).get());

	val = j.next();
	assertTrue("should be a float", val.getType() == Datum.DataType.FLOAT);
	assertEquals("value of valAfter2", -9.929292e-29f, ((DataFloat)val).get());

	val = j.next();
	assertTrue("should be a float", val.getType() == Datum.DataType.FLOAT);
	assertEquals("value of valAfter", 97.0f, ((DataFloat)val).get());

	assertFalse("should have read all values in bag", j.hasNext());
		
	file.delete();
}

public void testWriteReadDouble() throws Exception
{
	DataBag before = new DataBag(Datum.DataType.DOUBLE);

	before.add(new DataDouble(3.2e132));
	before.add(new DataDouble(-9.929292e-129));
	before.add(new DataDouble(97.0));

	File file = null;
	file = File.createTempFile("DataBagDouble", "put");
	FileOutputStream fos = new FileOutputStream(file);
	DataOutput out = new DataOutputStream(fos);
	before.write(out);
	fos.close();

	FileInputStream fis = new FileInputStream(file);
	DataInput in = new DataInputStream(fis);
	Datum a = DatumImpl.readDatum(in);

	assertTrue("isa DataBag", a instanceof DataBag);

	DataBag after = (DataBag)a;

	assertTrue("bag of double", after.bagOf() == Datum.DataType.DOUBLE);
	assertEquals("after read, size", 3, after.size()); 

	Iterator<Datum> j = after.content();

	Datum val = j.next();
	assertTrue("should be a double", val.getType() == Datum.DataType.DOUBLE);
	assertEquals("value of valAfter", 3.2e132, ((DataDouble)val).get());

	val = j.next();
	assertTrue("should be a double", val.getType() == Datum.DataType.DOUBLE);
	assertEquals("value of valAfter2", -9.929292e-129, ((DataDouble)val).get());

	val = j.next();
	assertTrue("should be a double", val.getType() == Datum.DataType.DOUBLE);
	assertEquals("value of valAfter", 97.0, ((DataDouble)val).get());

	assertFalse("should have read all values in bag", j.hasNext());
		
	file.delete();
}

public void testWriteReadUtf16() throws Exception
{
	DataBag before = new DataBag(Datum.DataType.CHARARRAY);

	before.add(new DataCharArrayUtf16("zzz"));
	before.add(new DataCharArrayUtf16("yyy"));
	before.add(new DataCharArrayUtf16("xxx"));

	File file = null;
	file = File.createTempFile("DataBagUtf16", "put");
	FileOutputStream fos = new FileOutputStream(file);
	DataOutput out = new DataOutputStream(fos);
	before.write(out);
	fos.close();

	FileInputStream fis = new FileInputStream(file);
	DataInput in = new DataInputStream(fis);
	Datum a = DatumImpl.readDatum(in);

	assertTrue("isa DataBag", a instanceof DataBag);

	DataBag after = (DataBag)a;

	assertTrue("bag of chararray", after.bagOf() == Datum.DataType.CHARARRAY);
	assertEquals("after read, size", 3, after.size()); 

	Iterator<Datum> j = after.content();

	Datum val = j.next();
	assertTrue("should be a chararray", val.getType() == Datum.DataType.CHARARRAY);
	assertTrue("encoding should be utf16",
		((DataCharArray)val).getEncoding() == DataCharArray.Encoding.UTF16);
	assertEquals("value of valAfter", "zzz", ((DataCharArrayUtf16)val).get());

	val = j.next();
	assertTrue("should be a chararray", val.getType() == Datum.DataType.CHARARRAY);
	assertTrue("encoding should be utf16",
		((DataCharArray)val).getEncoding() == DataCharArray.Encoding.UTF16);
	assertEquals("value of valAfter2", "yyy", ((DataCharArrayUtf16)val).get());

	val = j.next();
	assertTrue("should be a chararray", val.getType() == Datum.DataType.CHARARRAY);
	assertTrue("encoding should be utf16",
		((DataCharArray)val).getEncoding() == DataCharArray.Encoding.UTF16);
	assertEquals("value of valAfter", "xxx", ((DataCharArrayUtf16)val).get());

	assertFalse("should have read all values in bag", j.hasNext());
		
	file.delete();
}

public void testWriteReadNone() throws Exception
{
	DataBag before = new DataBag(Datum.DataType.CHARARRAY);

	String s = new String("zzz");
	before.add(new DataCharArrayNone(s.getBytes()));
	s = new String("yyy");
	before.add(new DataCharArrayNone(s.getBytes()));
	s = new String("xxx");
	before.add(new DataCharArrayNone(s.getBytes()));

	File file = null;
	file = File.createTempFile("DataBagCharArrayNone", "put");
	FileOutputStream fos = new FileOutputStream(file);
	DataOutput out = new DataOutputStream(fos);
	before.write(out);
	fos.close();

	FileInputStream fis = new FileInputStream(file);
	DataInput in = new DataInputStream(fis);
	Datum a = DatumImpl.readDatum(in);

	assertTrue("isa DataBag", a instanceof DataBag);

	DataBag after = (DataBag)a;

	assertTrue("bag of chararray", after.bagOf() == Datum.DataType.CHARARRAY);
	assertEquals("after read, size", 3, after.size()); 

	Iterator<Datum> j = after.content();

	Datum valAfter = j.next();
	assertTrue("should be a chararray", valAfter.getType() == Datum.DataType.CHARARRAY);
	assertTrue("encoding should be none",
		((DataCharArray)valAfter).getEncoding() == DataCharArray.Encoding.NONE);
	for (int i = 0; i < 3; i++) {
		assertEquals("value of valAfter", (byte)0x7a,
			((DataCharArrayNone)valAfter).get()[i]);
	}

	valAfter = j.next();
	assertTrue("should be a chararray", valAfter.getType() == Datum.DataType.CHARARRAY);
	assertTrue("encoding should be none",
		((DataCharArray)valAfter).getEncoding() == DataCharArray.Encoding.NONE);
	for (int i = 0; i < 3; i++) {
		assertEquals("value of valAfter", (byte)0x79,
			((DataCharArrayNone)valAfter).get()[i]);
	}

	valAfter = j.next();
	assertTrue("should be a chararray", valAfter.getType() == Datum.DataType.CHARARRAY);
	assertTrue("encoding should be none",
		((DataCharArray)valAfter).getEncoding() == DataCharArray.Encoding.NONE);
	for (int i = 0; i < 3; i++) {
		assertEquals("value of valAfter", (byte)0x78,
			((DataCharArrayNone)valAfter).get()[i]);
	}

	assertFalse("should have read all values in bag", j.hasNext());
	
	file.delete();
}

public void testWriteReadMap() throws Exception
{
	DataBag before = new DataBag(Datum.DataType.MAP);

	DataMap map = new DataMap();

	DataInteger key = new DataInteger(1);
	Datum val = new DataInteger(99);
	map.put(key, val);

	before.add(map);

	File file = null;
	file = File.createTempFile("DataBagCharArrayNone", "put");
	FileOutputStream fos = new FileOutputStream(file);
	DataOutput out = new DataOutputStream(fos);
	before.write(out);
	fos.close();

	FileInputStream fis = new FileInputStream(file);
	DataInput in = new DataInputStream(fis);
	Datum a = DatumImpl.readDatum(in);

	assertTrue("isa DataBag", a instanceof DataBag);

	DataBag after = (DataBag)a;

	assertTrue("bag of maps", after.bagOf() == Datum.DataType.MAP);
	assertEquals("after read, size", 1, after.size()); 

	Iterator<Datum> j = after.content();

	Datum v = j.next();
	assertTrue("valAfter should be a map", v.getType() == Datum.DataType.MAP);
	DataMap valAfter = (DataMap)v;

	assertEquals("valAfter size", 1L, valAfter.size());

	DataInteger nosuch = new DataInteger(-1);
	Datum d = valAfter.get(nosuch);
	assertTrue("after read, no such key", d.isNull());

	Datum mapValAfter = valAfter.get(key);
	assertTrue("mapValAfter isa integer", mapValAfter instanceof DataInteger);
	assertEquals("value of valAfter", 99, ((DataInteger)mapValAfter).get());

	assertFalse("should have read all values in bag", j.hasNext());
	
	file.delete();
}

public void testWriteReadTuple() throws Exception
{
	DataBag before = new DataBag(Datum.DataType.TUPLE);

	Tuple t = new Tuple(1);
	t.setField(0, new DataInteger(1));
	before.add(t);

	File file = null;
	file = File.createTempFile("DataBagCharArrayNone", "put");
	FileOutputStream fos = new FileOutputStream(file);
	DataOutput out = new DataOutputStream(fos);
	before.write(out);
	fos.close();

	FileInputStream fis = new FileInputStream(file);
	DataInput in = new DataInputStream(fis);
	Datum a = DatumImpl.readDatum(in);

	assertTrue("isa DataBag", a instanceof DataBag);

	DataBag after = (DataBag)a;

	assertTrue("bag of tuples", after.bagOf() == Datum.DataType.TUPLE);
	assertEquals("after read, size", 1, after.size()); 

	Iterator<Datum> j = after.content();

	Datum v = j.next();
	assertTrue("valAfter should be a tuple",
		v.getType() == Datum.DataType.TUPLE);

	Tuple valAfter = (Tuple)v;

	assertEquals("valAfter size", 1L, valAfter.size());

	Datum tupleValAfter = valAfter.getField(0);
	assertTrue("tupleValAfter isa integer", tupleValAfter instanceof DataInteger);
	assertEquals("value of valAfter", 1, ((DataInteger)tupleValAfter).get());

	assertFalse("should have read all values in bag", j.hasNext());
	
	file.delete();
}

public void testWriteReadBag() throws Exception
{
	DataBag before = new DataBag(Datum.DataType.BAG);

	DataBag b = new DataBag(Datum.DataType.INT);
	b.add(new DataInteger(2));
	before.add(b);

	File file = null;
	file = File.createTempFile("DataBagCharArrayNone", "put");
	FileOutputStream fos = new FileOutputStream(file);
	DataOutput out = new DataOutputStream(fos);
	before.write(out);
	fos.close();

	FileInputStream fis = new FileInputStream(file);
	DataInput in = new DataInputStream(fis);
	Datum a = DatumImpl.readDatum(in);

	assertTrue("isa DataBag", a instanceof DataBag);

	DataBag after = (DataBag)a;

	assertTrue("bag of bags", after.bagOf() == Datum.DataType.BAG);
	assertEquals("after read, size", 1, after.size()); 

	Iterator<Datum> j = after.content();

	Datum v = j.next();
	assertTrue("valAfter should be a bag", v.getType() == Datum.DataType.BAG);
	DataBag valAfter = (DataBag)v;

	assertEquals("valAfter size", 1L, valAfter.size());

	Iterator<Datum> k = valAfter.content();
	Datum w = k.next();
	assertTrue("bagValAfter should be an integer",
		w.getType() == Datum.DataType.INT);
	DataInteger bagValAfter = (DataInteger)w;

	assertEquals("value of valAfter", 2, bagValAfter.get());

	assertFalse("should have read all values in inner bag", k.hasNext());
	assertFalse("should have read all values in bag", j.hasNext());
	
	file.delete();
}

}


 

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * tstributed with this work for adttional information
 * regartng copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * tstributed under the License is tstributed on an "AS IS" BASIS,
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
import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;

import org.junit.Test;

import org.apache.pig.data.*;

/**
 * This class will exercise the tuple data type.
 * 
 * @author gates
 */
public class TestTuple extends junit.framework.TestCase
{

public void testDefaultConstructor() throws Exception
{
	Tuple t = new Tuple();

	assertEquals("getType", Datum.DataType.TUPLE, t.getType());
	assertFalse("is null", t.isNull());

	assertEquals("default construct size", 0L, t.size());

	Datum d = t.getField(0);
	assertTrue("unknown field is null", d.isNull());

	DataInteger di = new DataInteger(3);
	t.appendField(new DataInteger(3));

	assertEquals("default constructor size after append", 1L, t.size());
	d = t.getField(0);
	assertTrue("default constructor after append", di.equals(d));

	t.setField(0, new DataInteger(4));
	assertEquals("default constructor size after set", 1L, t.size());
	d = t.getField(0);
	assertFalse("default constructor after set", di.equals(d));
}

public void testLongConstructor() throws Exception
{
	Tuple t = new Tuple(3);

	assertEquals("long construct size", 3L, t.size());

	Datum d = t.getField(2);
	assertTrue("unset field is null", d.isNull());

	d = t.getField(3);
	assertTrue("field past end is null", d.isNull());


	t.setField(0, new DataInteger(10));
	t.setField(1, new DataLong(11L));
	t.setField(2, new DataFloat(9.9f));

	assertEquals("long constructor size after set", 3L, t.size());
	d = t.getField(0);
	assertEquals("long constructor after set, col 0", 10,
		((DataInteger)d).get());
	d = t.getField(1);
	assertEquals("long constructor after set, col 1", 11L,
		((DataLong)d).get());
	d = t.getField(2);
	assertEquals("long constructor after set, col 2", 9.9f,
		((DataFloat)d).get());

	t.appendField(new DataInteger(4));
	assertEquals("long constructor size after append", 4L, t.size());
	d = t.getField(3);
	assertEquals("long constructor after append, col 3", 4,
		((DataInteger)d).get());
}

public void testListConstructor() throws Exception
{
	List<Datum> list = new ArrayList<Datum>();
	list.add(new DataInteger(10));
	list.add(new DataLong(11L));
	list.add(new DataFloat(9.9f));

	Tuple t = new Tuple(list);

	assertEquals("list construct size", 3L, t.size());

	Datum d = t.getField(2);
	assertFalse("set field is not null", d.isNull());

	d = t.getField(3);
	assertTrue("field past end is null", d.isNull());


	d = t.getField(0);
	assertEquals("list constructor after set, col 0", 10,
		((DataInteger)d).get());
	d = t.getField(1);
	assertEquals("list constructor after set, col 1", 11L,
		((DataLong)d).get());
	d = t.getField(2);
	assertEquals("list constructor after set, col 2", 9.9f,
		((DataFloat)d).get());

	t.appendField(new DataInteger(4));
	assertEquals("list constructor size after append", 4L, t.size());
	d = t.getField(3);
	assertEquals("list constructor after append, col 3", 4,
		((DataInteger)d).get());
}


public void testToString() throws Exception
{
	Tuple t = giveMeOneOfEach();

	assertEquals("toString",
		"(hello world, 1, 2, 3.1415, 4.23223, mary had a little lamb, jack be nimble, [aaa#1], (99), {8})", t.toString());
}

public void testHashCode() throws Exception
{
	Tuple t1 = new Tuple(2);
	String s = new String("hello world");
	t1.setField(0, new DataUnknown(s.getBytes()));
	t1.setField(1, new DataInteger(1));

	Tuple t2 = new Tuple();
	t2.appendField(new DataUnknown(s.getBytes()));
	t2.appendField(new DataInteger(1));

	assertEquals("same data", t1.hashCode(), t2.hashCode());

	Tuple t3 = new Tuple(3);
	t1.setField(0, new DataUnknown(s.getBytes()));
	t1.setField(1, new DataInteger(1));
	assertFalse("different size", t1.hashCode() == t3.hashCode()); 

	Tuple t4 = new Tuple(2);
	t1.setField(0, new DataUnknown(s.getBytes()));
	t1.setField(1, new DataInteger(2));
	assertFalse("same size, different data", t1.hashCode() == t3.hashCode()); 
}

public void testEquals() throws Exception
{
	Tuple t1 = new Tuple();
	Tuple t2 = new Tuple();

	t1.appendField(new DataInteger(3));
	t2.appendField(new DataInteger(3));

	assertFalse("different object", t1.equals(new String()));

	assertTrue("same data", t1.equals(t2));

	t2 = new Tuple();
	t2.appendField(new DataInteger(4));
	assertFalse("different data", t1.equals(t2));

	t2 = new Tuple();
	t2.appendField(new DataInteger(3));
	t2.appendField(new DataInteger(3));
	assertFalse("different size", t1.equals(t2));
}

public void testCompareTo() throws Exception
{
	Tuple t1 = new Tuple();
	Tuple t2 = new Tuple();

	t1.appendField(new DataInteger(3));
	t2.appendField(new DataInteger(3));

	assertEquals("different object less than", -1, t1.compareTo(new String()));

	DataBag bag = new DataBag(Datum.DataType.INT);
	assertTrue("greater than bag", t1.compareTo(bag) > 0);
	DataMap map = new DataMap();
	assertTrue("less than map", t1.compareTo(map) < 0);
	DataLong l = new DataLong();
	assertTrue("less than long", t1.compareTo(l) < 0);
	DataFloat f = new DataFloat();
	assertTrue("less than float", t1.compareTo(f) < 0);
	DataDouble d = new DataDouble();
	assertTrue("less than double", t1.compareTo(d) < 0);
	DataUnknown unk = new DataUnknown();
	assertTrue("less than unknown", t1.compareTo(unk) < 0);
	DataCharArrayUtf16 utf16 = new DataCharArrayUtf16();
	assertTrue("less than utf16", t1.compareTo(utf16) < 0);

	assertEquals("same data equal", 0,  t1.compareTo(t2));

	t2 = new Tuple();
	t2.appendField(new DataInteger(2));
	assertEquals("greater than tuple with lesser value", 1, t1.compareTo(t2));

	t2 = new Tuple();
	t2.appendField(new DataInteger(4));
	assertEquals("less than tuple with greater value", -1, t1.compareTo(t2));

	t2 = new Tuple();
	t2.appendField(new DataInteger(3));
	t2.appendField(new DataInteger(4));
	assertEquals("less than bigger tuple", -1, t1.compareTo(t2));

	t2 = new Tuple();
	assertEquals("greater than smaller tuple", 1, t1.compareTo(t2));
}


public void testWriteRead() throws Exception
{
	Tuple before = giveMeOneOfEach();

	File file = null;
	file = File.createTempFile("Tuple", "put");
	FileOutputStream fos = new FileOutputStream(file);
	DataOutput out = new DataOutputStream(fos);
	before.write(out);
	fos.close();

	FileInputStream fis = new FileInputStream(file);
	DataInput in = new DataInputStream(fis);
	Datum a = DatumImpl.readDatum(in);

	assertTrue("isa Tuple", a instanceof Tuple);

	Tuple after = (Tuple)a;
		
	String s = new String("hello world");
	Datum f1 = after.getField(0);
	assertTrue("isa Unknown", f1 instanceof DataUnknown);
	byte[] beforeBytes = s.getBytes();
	byte[] afterBytes = ((DataUnknown)f1).get();
	assertEquals("length", beforeBytes.length, afterBytes.length);
	boolean same = true;
	for (int i = 0; i < beforeBytes.length && same; i++) {
		same &= beforeBytes[i] == afterBytes[i];
	}
	assertTrue("after write/read unknown", same);

	Datum f2 = after.getField(1);
	assertTrue("isa Integer", f2 instanceof DataInteger);
	assertEquals("after read/write integer", 1, ((DataInteger)f2).get());

	Datum f3 = after.getField(2);
	assertTrue("isa Long", f3 instanceof DataLong);
	assertEquals("after read/write long", 2L, ((DataLong)f3).get());

	Datum f4 = after.getField(3);
	assertTrue("isa Float", f4 instanceof DataFloat);
	assertEquals("after read/write float", 3.1415f, ((DataFloat)f4).get());

	Datum f5 = after.getField(4);
	assertTrue("isa Double", f5 instanceof DataDouble);
	assertEquals("after read/write double", 4.23223, ((DataDouble)f5).get());

	s = new String("mary had a little lamb");
	Datum f6 = after.getField(5);
	assertTrue("isa None", f6 instanceof DataCharArrayNone);
	beforeBytes = s.getBytes();
	afterBytes = ((DataCharArrayNone)f6).get();
	assertEquals("length", beforeBytes.length, afterBytes.length);
	same = true;
	for (int i = 0; i < beforeBytes.length && same; i++) {
		same &= beforeBytes[i] == afterBytes[i];
	}
	assertTrue("after write/read none", same);

	Datum f7 = after.getField(6);
	assertTrue("isa Utf16", f7 instanceof DataCharArrayUtf16);
	assertEquals("after read/write utf16", "jack be nimble",
		((DataCharArrayUtf16)f7).get());

	Datum f8 = after.getField(7);
	assertTrue("isa map", f8 instanceof DataMap);
	assertEquals("after read/write size of map", 1L, f8.size());
	Datum val = ((DataMap)f8).get(new DataCharArrayUtf16("aaa"));
	assertFalse("val of map after read/write", val.isNull());
	assertTrue("val is a long after read/write", val instanceof DataLong);
	assertEquals("val is 1 after read/write", 1L, ((DataLong)val).get());

	Datum f9 = after.getField(8);
	assertTrue("isa tuple", f9 instanceof Tuple);
	assertEquals("after read/write size of tuple", 1L, f9.size());
	val = ((Tuple)f9).getField(0);
	assertFalse("val of tuple after read/write", val.isNull());
	assertTrue("val is an int after read/write", val instanceof DataInteger);
	assertEquals("val is 99 after read/write", 99, ((DataInteger)val).get());

	Datum f10 = after.getField(9);
	assertTrue("isa bag", f10 instanceof DataBag);
	assertEquals("after read/write size of bag", 1L, f10.size());
	Iterator<Datum> i = ((DataBag)f10).content();
	while (i.hasNext()) {
		DataInteger di = (DataInteger)i.next();
		assertEquals("after read/write bag element", 8, di.get());
	}

	file.delete();
}

private Tuple giveMeOneOfEach()
{
	// Create a tuple with one of each data type in it.
	Tuple t = new Tuple();
	String s = new String("hello world");
	t.appendField(new DataUnknown(s.getBytes()));
	t.appendField(new DataInteger(1));
	t.appendField(new DataLong(2L));
	t.appendField(new DataFloat(3.1415f));
	t.appendField(new DataDouble(4.23223));
	s = new String("mary had a little lamb");
	t.appendField(new DataCharArrayNone(s.getBytes()));
	t.appendField(new DataCharArrayUtf16("jack be nimble"));

	DataMap map = new DataMap();
	DataCharArrayUtf16 key = new DataCharArrayUtf16("aaa");
	Datum val = new DataLong(1L);
	map.put(key, val);
	t.appendField(map);

	Tuple tsub = new Tuple(new DataInteger(99));
	t.appendField(tsub);

	DataBag bag = new DataBag(Datum.DataType.INT);
	bag.add(new DataInteger(8));
	t.appendField(bag);

	return t;
}

}


 

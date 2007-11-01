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

import org.junit.Test;

import org.apache.pig.data.*;

/**
 * This class will exercise the data map data type.
 * 
 * @author gates
 */
public class TestDataMap extends junit.framework.TestCase
{

public void testDefaultConstructor() throws Exception
{
	DataMap map = new DataMap();

	assertEquals("getType", Datum.DataType.MAP, map.getType());
	assertFalse("is null", map.isNull());

	assertEquals("Default constructor size before", 0, map.size());
	String s = "hello world";
	DataUnknown key = new DataUnknown(s.getBytes());
	Datum d = map.get(key);
	assertTrue("get with no entries in map", d.isNull());
	DataInteger val = new DataInteger(42);

	map.put(key, val);
	assertEquals("Default constructor size after", 1, map.size());
	d = map.get(key);
	assertNotNull("get with entry in map", d);
	assertTrue("val isa integer", d instanceof DataInteger);
	assertEquals("value of val", 42, ((DataInteger)d).get());
}

public void testBigMap() throws Exception
{
	DataMap map = new DataMap();

	for (int i = 0; i < 10000; i++) {
		Integer ii = new Integer(i);
		DataCharArrayUtf16 key = new DataCharArrayUtf16(ii.toString());
		DataInteger val = new DataInteger(i);
		map.put(key, val);
	}

	assertEquals("big size after loading", 10000, map.size());
	DataCharArrayUtf16 key = new DataCharArrayUtf16("no such key");
	Datum d = map.get(key);
	assertTrue("get no such key", d.isNull());

	for (int i = 9999; i >= 0; i--) {
		Integer ii = new Integer(i);
		key = new DataCharArrayUtf16(ii.toString());
		Datum val = map.get(key);
		assertFalse("val should not be null", val.isNull());
		assertTrue("val isa integer", val instanceof DataInteger);
		assertEquals("value of val", i, ((DataInteger)val).get());
	}
}

public void testToString() throws Exception
{
	DataMap map = new DataMap();

	DataCharArrayUtf16 key = new DataCharArrayUtf16("aaa");
	Datum val = new DataLong(1L);
	map.put(key, val);
	key = new DataCharArrayUtf16("bbb");
	val = new DataFloat(2.0f);
	map.put(key, val);
	key = new DataCharArrayUtf16("ccc");
	val = new DataDouble(3.14159);
	map.put(key, val);

	assertEquals("toString", "[aaa#1, ccc#3.14159, bbb#2.0]", map.toString());
}

public void testHashCode() throws Exception
{
	DataMap map1 = new DataMap();
	DataCharArrayUtf16 key1 = new DataCharArrayUtf16("aaa");
	DataLong val1 = new DataLong(1L);
	map1.put(key1, val1);

	DataMap map2 = new DataMap();
	DataCharArrayUtf16 key2 = new DataCharArrayUtf16("aaa");
	DataLong val2 = new DataLong(1L);
	map2.put(key2, val2);

	assertEquals("same data", map1.hashCode(), map2.hashCode());

	DataMap map3 = new DataMap();
	DataCharArrayUtf16 key3 = new DataCharArrayUtf16("aaa");
	DataLong val3 = new DataLong(2L);
	map3.put(key3, val3);

	assertFalse("different data", map1.hashCode() == map3.hashCode()); 
}

public void testEquals() throws Exception
{
	DataMap map1 = new DataMap();
	DataMap map2 = new DataMap();

	map1.put(new DataInteger(3), new DataInteger(4));
	map2.put(new DataInteger(3), new DataInteger(4));

	assertFalse("different object", map1.equals(new String()));

	assertTrue("same data", map1.equals(map2));

	map2 = new DataMap();
	map2.put(new DataInteger(3), new DataInteger(5));
	assertFalse("different data", map1.equals(map2));

	map2 = new DataMap();
	map2.put(new DataInteger(3), new DataInteger(4));
	map2.put(new DataInteger(4), new DataInteger(4));
	assertFalse("different size", map1.equals(map2));
}

public void testCompareTo() throws Exception
{
	DataMap map1 = new DataMap();
	DataMap map2 = new DataMap();

	map1.put(new DataInteger(3), new DataInteger(4));
	map2.put(new DataInteger(3), new DataInteger(4));

	assertEquals("different object less than", -1, map1.compareTo(new String()));

	DataBag bag = new DataBag(Datum.DataType.INT);
	assertTrue("greater than bag", map1.compareTo(bag) > 0);
	Tuple t = new Tuple();
	assertTrue("greater than tuple", map1.compareTo(t) > 0);
	DataInteger i = new DataInteger();
	assertTrue("less than integer", map1.compareTo(i) < 0);
	DataLong l = new DataLong();
	assertTrue("less than long", map1.compareTo(l) < 0);
	DataFloat f = new DataFloat();
	assertTrue("less than float", map1.compareTo(f) < 0);
	DataDouble d = new DataDouble();
	assertTrue("less than double", map1.compareTo(d) < 0);
	DataUnknown unk = new DataUnknown();
	assertTrue("less than unknown", map1.compareTo(unk) < 0);
	DataCharArrayUtf16 utf16 = new DataCharArrayUtf16();
	assertTrue("less than utf16", map1.compareTo(utf16) < 0);

	assertEquals("same data equal", 0, map1.compareTo(map2));

	map2 = new DataMap();
	map2.put(new DataInteger(2), new DataInteger(4));
	assertTrue("greater than map with lesser key", map1.compareTo(map2) > 0);

	map2 = new DataMap();
	map2.put(new DataInteger(3), new DataInteger(3));
	assertTrue("greater than map with lesser value", map1.compareTo(map2) > 0);

	map2 = new DataMap();
	map2.put(new DataInteger(4), new DataInteger(4));
	assertTrue("less than map with greater key", map1.compareTo(map2) < 0);

	map2 = new DataMap();
	map2.put(new DataInteger(3), new DataInteger(5));
	assertTrue("less than map with greater value", map1.compareTo(map2) < 0);

	map2 = new DataMap();
	map2.put(new DataInteger(3), new DataInteger(5));
	map2.put(new DataInteger(4), new DataInteger(5));
	assertTrue("less than bigger map", map1.compareTo(map2) < 0);

	map2 = new DataMap();
	assertTrue("greater than smaller map", map1.compareTo(map2) > 0);
}

public void testWriteRead() throws Exception
{
	DataMap before = new DataMap();

	String s = new String("aaa");
	DataUnknown keyUnknown = new DataUnknown(s.getBytes());
	s = new String("zzz");
	Datum val = new DataUnknown(s.getBytes());
	before.put(keyUnknown, val);

	DataInteger keyInt = new DataInteger(1);
	val = new DataInteger(99);
	before.put(keyInt, val);

	DataLong keyLong = new DataLong(1L);
	val = new DataLong(99000000000L);
	before.put(keyLong, val);

	DataFloat keyFloat = new DataFloat(1.0f);
	val = new DataFloat(3.2e32f);
	before.put(keyFloat, val);

	DataDouble keyDouble = new DataDouble(1.0);
	val = new DataDouble(3.2e132);
	before.put(keyDouble, val);

	DataCharArrayUtf16 keyUtf16 = new DataCharArrayUtf16("aaa");
	val = new DataCharArrayUtf16("yyy");
	before.put(keyUtf16, val);

	s = new String("aaa");
	DataCharArrayNone keyNone = new DataCharArrayNone(s.getBytes());
	s = new String("xxx");
	val = new DataCharArrayNone(s.getBytes());
	before.put(keyNone, val);

	File file = null;
	file = File.createTempFile("DataMapUnknown", "put");
	FileOutputStream fos = new FileOutputStream(file);
	DataOutput out = new DataOutputStream(fos);
	before.write(out);
	fos.close();

	FileInputStream fis = new FileInputStream(file);
	DataInput in = new DataInputStream(fis);
	Datum a = DatumImpl.readDatum(in);

	assertTrue("isa DataMap", a instanceof DataMap);

	DataMap after = (DataMap)a;

	assertEquals("after read, size", 7, after.size()); 

	s = new String("no such key");
	DataUnknown nosuch = new DataUnknown(s.getBytes());
	Datum d = after.get(nosuch);
	assertTrue("after read, no such key", d.isNull());

	Datum valAfter = after.get(keyUnknown);
	assertTrue("valAfter isa integer", valAfter instanceof DataUnknown);
	for (int i = 0; i < 3; i++) {
		assertEquals("value of valAfter", (byte)0x7a,
			((DataUnknown)valAfter).get()[i]);
	}

	valAfter = after.get(keyInt);
	assertTrue("valAfter isa integer", valAfter instanceof DataInteger);
	assertEquals("value of valAfter", 99, ((DataInteger)valAfter).get());

	valAfter = after.get(keyLong);
	assertTrue("valAfter isa long", valAfter instanceof DataLong);
	assertEquals("value of valAfter", 99000000000L, ((DataLong)valAfter).get());

	valAfter = after.get(keyFloat);
	assertTrue("valAfter isa float", valAfter instanceof DataFloat);
	assertEquals("value of valAfter", 3.2e32f, ((DataFloat)valAfter).get());

	valAfter = after.get(keyDouble);
	assertTrue("valAfter isa double", valAfter instanceof DataDouble);
	assertEquals("value of valAfter", 3.2e132, ((DataDouble)valAfter).get());

	valAfter = after.get(keyUtf16);
	assertTrue("valAfter isa utf16", valAfter instanceof DataCharArrayUtf16);
	assertEquals("value of valAfter", "yyy", ((DataCharArrayUtf16)valAfter).get());

	valAfter = after.get(keyNone);
	assertTrue("valAfter isa none", valAfter instanceof DataCharArrayNone);
	for (int i = 0; i < 3; i++) {
		assertEquals("value of valAfter", (byte)0x78,
			((DataCharArrayNone)valAfter).get()[i]);
	}

	file.delete();
}

}


 

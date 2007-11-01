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

import org.junit.Test;

import org.apache.pig.data.*;

/**
 * This class will exercise the unknown data type.
 * 
 * @author gates
 */
public class TestDataUnknown extends junit.framework.TestCase
{

public void testDefaultConstructor() throws Exception
{
	DataUnknown unk = new DataUnknown();

	assertEquals("getType", Datum.DataType.UNKNOWN, unk.getType());
	assertFalse("is null", unk.isNull());

	assertEquals("Default constructor size before", 0L, unk.size());
	assertNull("Default constructor get before", unk.get());
	String s = "hello world";
	unk.set(s.getBytes());
	assertEquals("Default constructor size after", 11L, unk.size());
	String n = new String(unk.get());
	assertEquals("Default constructor get after", "hello world", n);
}

public void testByteConstructor() throws Exception
{
	String s = "hello world";
	DataUnknown unk = new DataUnknown(s.getBytes());
	assertEquals("Byte constructor size before", 11L, unk.size());
	String n = new String(unk.get());
	assertEquals("Byte constructor get before", "hello world", n);

	s = "goodbye world";
	unk.set(s.getBytes());
	assertEquals("Byte constructor after size", 13L, unk.size());
	n = new String(unk.get());
	assertEquals("Byte constructor after get", "goodbye world", n);
}

public void testToString() throws Exception
{
	String s = "hello world";
	DataUnknown unk = new DataUnknown(s.getBytes());
	assertEquals("toString", s, unk.toString());
}

public void testHashCode() throws Exception
{
	String s = "hello world";
	DataUnknown unk1 = new DataUnknown(s.getBytes());
	DataUnknown unk2 = new DataUnknown(s.getBytes());
	assertEquals("same data", unk1.hashCode(), unk2.hashCode());
	s = "goodbye world";
	DataUnknown unk3 = new DataUnknown(s.getBytes());
	assertFalse("different data", unk1.hashCode() == unk3.hashCode()); 
}

public void testEquals() throws Exception
{
	String s = "hello world";
	DataUnknown unk1 = new DataUnknown(s.getBytes());
	DataUnknown unk2 = new DataUnknown(s.getBytes());

	assertFalse("different object", unk1.equals(s));

	assertTrue("same data", unk1.equals(unk2));

	s = "goodbye world";
	DataUnknown unk3 = new DataUnknown(s.getBytes());
	assertFalse("different data", unk1.equals(unk3));
}

public void testCompareTo() throws Exception
{
	String s = "hello world";
	DataUnknown unk1 = new DataUnknown(s.getBytes());
	DataUnknown unk2 = new DataUnknown(s.getBytes());

	assertEquals("different object less than", -1, unk1.compareTo(s));

	DataBag bag = new DataBag(Datum.DataType.INT);
	assertTrue("greater than bag", unk1.compareTo(bag) > 0);
	Tuple t = new Tuple();
	assertTrue("greater than tuple", unk1.compareTo(t) > 0);
	DataMap map = new DataMap();
	assertTrue("greater than map", unk1.compareTo(map) > 0);
	DataInteger i = new DataInteger();
	assertTrue("greater than integer", unk1.compareTo(i) > 0);
	DataLong l = new DataLong();
	assertTrue("greater than long", unk1.compareTo(l) > 0);
	DataFloat f = new DataFloat();
	assertTrue("greater than float", unk1.compareTo(f) > 0);
	DataDouble d = new DataDouble();
	assertTrue("greater than double", unk1.compareTo(d) > 0);
	DataCharArrayUtf16 utf16 = new DataCharArrayUtf16();
	assertTrue("greater than char array", unk1.compareTo(utf16) > 0);

	assertEquals("same data equal", 0,  unk1.compareTo(unk2));

	s = "hello dollie";
	unk2 = new DataUnknown(s.getBytes());
	assertEquals("greater than unknown with greater lexigraphic value", 1, unk1.compareTo(unk2));

	s = "hello zylophone";
	unk2 = new DataUnknown(s.getBytes());
	assertEquals("less than unknown with lower lexigraphic value", -1, unk1.compareTo(unk2));

	s = "hello world, goodbye moon";
	unk2 = new DataUnknown(s.getBytes());
	assertEquals("less than longer unknown", -1, unk1.compareTo(unk2));

	s = "hello worl";
	unk2 = new DataUnknown(s.getBytes());
	assertEquals("greater than shorter unknown", 1, unk1.compareTo(unk2));
}


public void testWriteRead() throws Exception
{
	String s = "hello world";
	DataUnknown before = new DataUnknown(s.getBytes());
	File file = null;
	file = File.createTempFile("DataUnknown", "put");
	FileOutputStream fos = new FileOutputStream(file);
	DataOutput out = new DataOutputStream(fos);
	before.write(out);
	fos.close();

	FileInputStream fis = new FileInputStream(file);
	DataInput in = new DataInputStream(fis);
	Datum a = DatumImpl.readDatum(in);

	assertTrue("isa DataUnknown", a instanceof DataUnknown);

	DataUnknown after = (DataUnknown)a;
		
	byte[] beforeBytes = before.get();
	byte[] afterBytes = after.get();

	assertEquals("length", beforeBytes.length, afterBytes.length);
	boolean same = true;
	for (int i = 0; i < beforeBytes.length; i++) {
		same &= beforeBytes[i] == afterBytes[i];
	}
	assertTrue("byte values", same);
	file.delete();
}

}


 

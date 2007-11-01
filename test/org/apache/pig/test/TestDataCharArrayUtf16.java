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
 * This class will exercise the utf16nown data type.
 * 
 * @author gates
 */
public class TestDataCharArrayUtf16 extends junit.framework.TestCase
{

public void testDefaultConstructor() throws Exception
{
	DataCharArrayUtf16 utf16 = new DataCharArrayUtf16();

	assertEquals("getType", Datum.DataType.CHARARRAY, utf16.getType());
	assertFalse("is null", utf16.isNull());
	assertEquals("getEncoding", DataCharArray.Encoding.UTF16, utf16.getEncoding());

	assertEquals("Default constructor size before", 0L, utf16.size());
	assertNull("Default constructor get before", utf16.get());
	String s = "hello world";
	utf16.set(s);
	assertEquals("Default constructor size after", 11L, utf16.size());
	String n = utf16.get();
	assertEquals("Default constructor get after", "hello world", n);
}

public void testByteConstructor() throws Exception
{
	String s = "hello world";
	DataCharArrayUtf16 utf16 = new DataCharArrayUtf16(s.getBytes());
	assertEquals("Byte constructor size before", 11L, utf16.size());
	String n = utf16.get();
	assertEquals("Byte constructor get before", "hello world", n);

	s = "goodbye world";
	utf16.set(s);
	assertEquals("Byte constructor after size", 13L, utf16.size());
	n = utf16.get();
	assertEquals("Byte constructor after get", "goodbye world", n);
}

public void testStringConstructor() throws Exception
{
	String s = "hello world";
	DataCharArrayUtf16 utf16 = new DataCharArrayUtf16(s);
	assertEquals("String constructor size before", 11L, utf16.size());
	String n = utf16.get();
	assertEquals("String constructor get before", "hello world", n);
}

public void testToString() throws Exception
{
	String s = "hello world";
	DataCharArrayUtf16 utf16 = new DataCharArrayUtf16(s);
	assertEquals("toString", s, utf16.toString());
}

public void testHashCode() throws Exception
{
	String s = "hello world";
	DataCharArrayUtf16 utf161 = new DataCharArrayUtf16(s);
	DataCharArrayUtf16 utf162 = new DataCharArrayUtf16(s);
	assertEquals("same data", utf161.hashCode(), utf162.hashCode());
	s = "goodbye world";
	DataCharArrayUtf16 utf163 = new DataCharArrayUtf16(s);
	assertFalse("different data", utf161.hashCode() == utf163.hashCode()); 
}

public void testEquals() throws Exception
{
	DataCharArrayUtf16 utf16_1 = new DataCharArrayUtf16("hello world");
	DataCharArrayUtf16 utf16_2 = new DataCharArrayUtf16("hello world");

	String s = new String("hello world");

	assertFalse("different object", utf16_1.equals(s));

	assertTrue("same data", utf16_1.equals(utf16_2));

	DataCharArrayUtf16 utf16_3 = new DataCharArrayUtf16("goodbye world");
	assertFalse("different data", utf16_1.equals(utf16_3));
}

public void testCompareTo() throws Exception
{
	String s = "hello world";
	DataCharArrayUtf16 utf16_1 = new DataCharArrayUtf16(s);
	DataCharArrayUtf16 utf16_2 = new DataCharArrayUtf16(s);

	assertEquals("different object less than", -1, utf16_1.compareTo(s));

	DataBag bag = new DataBag(Datum.DataType.INT);
	assertTrue("greater than bag", utf16_1.compareTo(bag) > 0);
	Tuple t = new Tuple();
	assertTrue("greater than tuple", utf16_1.compareTo(t) > 0);
	DataMap map = new DataMap();
	assertTrue("greater than map", utf16_1.compareTo(map) > 0);
	DataInteger i = new DataInteger();
	assertTrue("greater than integer", utf16_1.compareTo(i) > 0);
	DataLong l = new DataLong();
	assertTrue("greater than long", utf16_1.compareTo(l) > 0);
	DataFloat f = new DataFloat();
	assertTrue("greater than float", utf16_1.compareTo(f) > 0);
	DataDouble d = new DataDouble();
	assertTrue("greater than double", utf16_1.compareTo(d) > 0);
	DataUnknown unk = new DataUnknown();
	assertTrue("less than unknown", utf16_1.compareTo(unk) < 0);
	DataCharArrayNone none = new DataCharArrayNone(s.getBytes());
	assertTrue("less than none", utf16_1.compareTo(none) < 0);

	assertEquals("same data equal", 0,  utf16_1.compareTo(utf16_2));

	s = "hello dollie";
	utf16_2 = new DataCharArrayUtf16(s);
	assertTrue("greater than unknown with greater lexigraphic value", utf16_1.compareTo(utf16_2) > 0);

	s = "hello zylophone";
	utf16_2 = new DataCharArrayUtf16(s);
	assertTrue("less than unknown with lower lexigraphic value", utf16_1.compareTo(utf16_2) < 0);

	s = "hello world, goodbye moon";
	utf16_2 = new DataCharArrayUtf16(s);
	assertTrue("less than longer unknown", utf16_1.compareTo(utf16_2) < 0);

	s = "hello worl";
	utf16_2 = new DataCharArrayUtf16(s);
	assertTrue("greater than shorter unknown", utf16_1.compareTo(utf16_2) > 0);
}


public void testWriteRead() throws Exception
{
	String s = "hello world";
	DataCharArrayUtf16 before = new DataCharArrayUtf16(s);
	File file = null;
	file = File.createTempFile("DataCharArrayUtf16", "put");
	FileOutputStream fos = new FileOutputStream(file);
	DataOutput out = new DataOutputStream(fos);
	before.write(out);
	fos.close();

	FileInputStream fis = new FileInputStream(file);
	DataInput in = new DataInputStream(fis);
	Datum a = DatumImpl.readDatum(in);

	assertTrue("isa DataCharArrayUtf16", a instanceof DataCharArrayUtf16);

	DataCharArrayUtf16 after = (DataCharArrayUtf16)a;
		
	assertEquals("after read/write", before.get(), after.get());

	file.delete();
}

}


 

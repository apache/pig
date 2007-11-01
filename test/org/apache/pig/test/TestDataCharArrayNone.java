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
 * This class will exercise the nonenown data type.
 * 
 * @author gates
 */
public class TestDataCharArrayNone extends junit.framework.TestCase
{

public void testDefaultConstructor() throws Exception
{
	DataCharArrayNone none = new DataCharArrayNone();

	assertEquals("getType", Datum.DataType.CHARARRAY, none.getType());
	assertFalse("is null", none.isNull());
	assertEquals("getEncoding", DataCharArray.Encoding.NONE, none.getEncoding());

	assertEquals("Default constructor size before", 0L, none.size());
	assertNull("Default constructor get before", none.get());
	String s = "hello world";
	none.set(s.getBytes());
	assertEquals("Default constructor size after", 11L, none.size());
	String n = new String(none.get());
	assertEquals("Default constructor get after", "hello world", n);
}

public void testByteConstructor() throws Exception
{
	String s = "hello world";
	DataCharArrayNone none = new DataCharArrayNone(s.getBytes());
	assertEquals("Byte constructor size before", 11L, none.size());
	String n = new String(none.get());
	assertEquals("Byte constructor get before", "hello world", n);

	s = "goodbye world";
	none.set(s.getBytes());
	assertEquals("Byte constructor after size", 13L, none.size());
	n = new String(none.get());
	assertEquals("Byte constructor after get", "goodbye world", n);
}

public void testToString() throws Exception
{
	String s = "hello world";
	DataCharArrayNone none = new DataCharArrayNone(s.getBytes());
	assertEquals("toString", s, none.toString());
}

public void testHashCode() throws Exception
{
	String s = "hello world";
	DataCharArrayNone none1 = new DataCharArrayNone(s.getBytes());
	DataCharArrayNone none2 = new DataCharArrayNone(s.getBytes());
	assertEquals("same data", none1.hashCode(), none2.hashCode());
	s = "goodbye world";
	DataCharArrayNone none3 = new DataCharArrayNone(s.getBytes());
	assertFalse("different data", none1.hashCode() == none3.hashCode()); 
}

public void testEquals() throws Exception
{
	String s = "hello world";
	DataCharArrayNone none1 = new DataCharArrayNone(s.getBytes());
	DataCharArrayNone none2 = new DataCharArrayNone(s.getBytes());

	assertFalse("different object", none1.equals(s));

	assertTrue("same data", none1.equals(none2));

	s = "goodbye world";
	DataCharArrayNone none3 = new DataCharArrayNone(s.getBytes());
	assertFalse("different data", none1.equals(none3));
}

public void testCompareTo() throws Exception
{
	String s = "hello world";
	DataCharArrayNone none1 = new DataCharArrayNone(s.getBytes());
	DataCharArrayNone none2 = new DataCharArrayNone(s.getBytes());

	assertEquals("different object less than", -1, none1.compareTo(s));

	DataBag bag = new DataBag(Datum.DataType.INT);
	assertTrue("greater than bag", none1.compareTo(bag) > 0);
	Tuple t = new Tuple();
	assertTrue("greater than tuple", none1.compareTo(t) > 0);
	DataMap map = new DataMap();
	assertTrue("greater than map", none1.compareTo(map) > 0);
	DataInteger i = new DataInteger();
	assertTrue("greater than integer", none1.compareTo(i) > 0);
	DataLong l = new DataLong();
	assertTrue("greater than long", none1.compareTo(l) > 0);
	DataFloat f = new DataFloat();
	assertTrue("greater than float", none1.compareTo(f) > 0);
	DataDouble d = new DataDouble();
	assertTrue("greater than double", none1.compareTo(d) > 0);
	DataUnknown unk = new DataUnknown();
	assertTrue("less than unknown", none1.compareTo(unk) < 0);
	DataCharArrayUtf16 utf16 = new DataCharArrayUtf16(s);
	assertTrue("greater than utf16", none1.compareTo(utf16) > 0);

	assertEquals("same data equal", 0,  none1.compareTo(none2));

	s = "hello dollie";
	none2 = new DataCharArrayNone(s.getBytes());
	assertEquals("greater than unknown with greater lexigraphic value", 1, none1.compareTo(none2));

	s = "hello zylophone";
	none2 = new DataCharArrayNone(s.getBytes());
	assertEquals("less than unknown with lower lexigraphic value", -1, none1.compareTo(none2));

	s = "hello world, goodbye moon";
	none2 = new DataCharArrayNone(s.getBytes());
	assertEquals("less than longer unknown", -1, none1.compareTo(none2));

	s = "hello worl";
	none2 = new DataCharArrayNone(s.getBytes());
	assertEquals("greater than shorter unknown", 1, none1.compareTo(none2));
}

public void testWriteRead() throws Exception
{
	String s = "hello world";
	DataCharArrayNone before = new DataCharArrayNone(s.getBytes());
	File file = null;
	file = File.createTempFile("DataCharArrayNone", "put");
	FileOutputStream fos = new FileOutputStream(file);
	DataOutput out = new DataOutputStream(fos);
	before.write(out);
	fos.close();

	FileInputStream fis = new FileInputStream(file);
	DataInput in = new DataInputStream(fis);
	Datum a = DatumImpl.readDatum(in);

	assertTrue("isa DataCharArrayNone", a instanceof DataCharArrayNone);

	DataCharArrayNone after = (DataCharArrayNone)a;
		
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


 

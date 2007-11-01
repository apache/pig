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
 * This class will exercise the dinown data type.
 * 
 * @author gates
 */
public class TestDataInteger extends junit.framework.TestCase
{

public void testDefaultConstructor() throws Exception
{
	DataInteger di = new DataInteger();

	assertEquals("getType", Datum.DataType.INT, di.getType());
	assertFalse("is null", di.isNull());

	assertEquals("Default constructor get before", 0, di.get());
	di.set(11);
	assertEquals("Default constructor get after", 11, di.get());
}

public void testValueConstructor() throws Exception
{
	DataInteger di = new DataInteger(3231132);
	assertEquals("Byte constructor get before", 3231132, di.get());

	di.set(-1);
	assertEquals("Byte constructor after get", -1, di.get());
}

public void testToString() throws Exception
{
	DataInteger di = new DataInteger(99);
	assertEquals("toString", "99", di.toString());
}

public void testHashCode() throws Exception
{
	DataInteger di1 = new DataInteger(8);
	DataInteger di2 = new DataInteger(8);
	assertEquals("same data", di1.hashCode(), di2.hashCode());
	DataInteger di3 = new DataInteger(9);
	assertFalse("different data", di1.hashCode() == di3.hashCode()); 
}

public void testEquals() throws Exception
{
	DataInteger di1 = new DataInteger(-1);
	DataInteger di2 = new DataInteger(-1);

	Integer ii = new Integer(-1);

	assertFalse("different object", di1.equals(ii));

	assertTrue("same data", di1.equals(di2));

	DataInteger di3 = new DataInteger(37);
	assertFalse("different data", di1.equals(di3));
}

public void testCompareTo() throws Exception
{
	DataInteger di1 = new DataInteger(3);
	DataInteger di2 = new DataInteger(3);

	assertEquals("different object less than", -1, di1.compareTo(new Integer(3)));

	DataBag bag = new DataBag(Datum.DataType.INT);
	assertTrue("greater than bag", di1.compareTo(bag) > 0);
	Tuple t = new Tuple();
	assertTrue("greater than tuple", di1.compareTo(t) > 0);
	DataMap map = new DataMap();
	assertTrue("greater than map", di1.compareTo(map) > 0);
	DataLong l = new DataLong(3);
	assertTrue("less than long", di1.compareTo(l) < 0);
	DataFloat f = new DataFloat();
	assertTrue("less than float", di1.compareTo(f) < 0);
	DataDouble d = new DataDouble();
	assertTrue("less than double", di1.compareTo(d) < 0);
	DataUnknown unk = new DataUnknown();
	assertTrue("less than unknown", di1.compareTo(unk) < 0);
	DataCharArrayUtf16 utf16 = new DataCharArrayUtf16("hello world");
	assertTrue("less than none", di1.compareTo(utf16) < 0);

	assertEquals("same data equal", 0,  di1.compareTo(di2));

	di2 = new DataInteger(5);
	assertEquals("less than int with greater value", -1, di1.compareTo(di2));

	di2 = new DataInteger(1);
	assertEquals("greater than int with lower value", 1, di1.compareTo(di2));
}


public void testWriteRead() throws Exception
{
	DataInteger before = new DataInteger(17);
	File file = null;
	file = File.createTempFile("DataInteger", "put");
	FileOutputStream fos = new FileOutputStream(file);
	DataOutput out = new DataOutputStream(fos);
	before.write(out);
	fos.close();

	FileInputStream fis = new FileInputStream(file);
	DataInput in = new DataInputStream(fis);
	Datum a = DatumImpl.readDatum(in);

	assertTrue("isa DataInteger", a instanceof DataInteger);

	DataInteger after = (DataInteger)a;
		
	assertEquals("after read/write", before.get(), after.get());
	file.delete();
}

}


 

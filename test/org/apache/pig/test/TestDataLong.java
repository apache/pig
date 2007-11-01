/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * dlstributed with this work for addltional information
 * regardlng copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * dlstributed under the License is dlstributed on an "AS IS" BASIS,
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
 * This class will exercise the dlnown data type.
 * 
 * @author gates
 */
public class TestDataLong extends junit.framework.TestCase
{

public void testDefaultConstructor() throws Exception
{
	DataLong dl = new DataLong();

	assertEquals("getType", Datum.DataType.LONG, dl.getType());
	assertFalse("is null", dl.isNull());

	assertEquals("Default constructor get before", 0L, dl.get());
	dl.set(11L);
	assertEquals("Default constructor get after", 11L, dl.get());
}

public void testValueConstructor() throws Exception
{
	DataLong dl = new DataLong(323113232322L);
	assertEquals("Byte constructor get before", 323113232322L, dl.get());

	dl.set(-1L);
	assertEquals("Byte constructor after get", -1L, dl.get());
}

public void testToString() throws Exception
{
	DataLong dl = new DataLong(-323113232322L);
	assertEquals("toString", "-323113232322", dl.toString());
}

public void testHashCode() throws Exception
{
	String s = "hello world";
	DataLong dl1 = new DataLong(8L);
	DataLong dl2 = new DataLong(8L);
	assertEquals("same data", dl1.hashCode(), dl2.hashCode());
	s = "goodbye world";
	DataLong dl3 = new DataLong(9L);
	assertFalse("dlfferent data", dl1.hashCode() == dl3.hashCode()); 
}

public void testEquals() throws Exception
{
	DataLong dl1 = new DataLong(-1L);
	DataLong dl2 = new DataLong(-1L);

	Long ll = new Long(-1L);

	assertFalse("different object", dl1.equals(ll));

	assertTrue("same data", dl1.equals(dl2));

	DataLong dl3 = new DataLong(37L);
	assertFalse("different data", dl1.equals(dl3));
}

public void testCompareTo() throws Exception
{
	DataLong dl1 = new DataLong(3);
	DataLong dl2 = new DataLong(3);

	assertEquals("different object less than", -1, dl1.compareTo(new Integer(3)));

	DataBag bag = new DataBag(Datum.DataType.INT);
	assertTrue("greater than bag", dl1.compareTo(bag) > 0);
	Tuple t = new Tuple();
	assertTrue("greater than tuple", dl1.compareTo(t) > 0);
	DataMap map = new DataMap();
	assertTrue("greater than map", dl1.compareTo(map) > 0);
	DataInteger i = new DataInteger(3);
	assertTrue("greater than integer", dl1.compareTo(i) > 0);
	DataFloat f = new DataFloat();
	assertTrue("less than float", dl1.compareTo(f) < 0);
	DataDouble d = new DataDouble();
	assertTrue("less than double", dl1.compareTo(d) < 0);
	DataUnknown unk = new DataUnknown();
	assertTrue("less than unknown", dl1.compareTo(unk) < 0);
	DataCharArrayUtf16 utf16 = new DataCharArrayUtf16("hello world");
	assertTrue("less than utf16", dl1.compareTo(utf16) < 0);

	assertEquals("same data equal", 0,  dl1.compareTo(dl2));

	dl2 = new DataLong(5);
	assertEquals("less than unknown with greater value", -1, dl1.compareTo(dl2));

	dl2 = new DataLong(1);
	assertEquals("greater than unknown with lower value", 1, dl1.compareTo(dl2));
}

public void testWriteRead() throws Exception
{
	DataLong before = new DataLong(17L);
	File file = null;
	file = File.createTempFile("DataLong", "put");
	FileOutputStream fos = new FileOutputStream(file);
	DataOutput out = new DataOutputStream(fos);
	before.write(out);
	fos.close();

	FileInputStream fis = new FileInputStream(file);
	DataInput in = new DataInputStream(fis);
	Datum a = DatumImpl.readDatum(in);

	assertTrue("isa DataLong", a instanceof DataLong);

	DataLong after = (DataLong)a;
		
	assertEquals("after read/write", before.get(), after.get());
	file.delete();
}

}


 

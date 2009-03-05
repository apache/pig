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

import java.util.Random;
import java.util.Map;
import java.io.IOException;

import org.apache.pig.builtin.PigStorage;
import org.apache.pig.test.utils.GenRandomData;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;

import org.junit.Test;

import junit.framework.TestCase;
import junit.framework.AssertionFailedError;

/**
 * Test class to test conversions from bytes to types
 * and vice versa
 * 
 */
public class TestConversions extends TestCase {

    PigStorage ps = new PigStorage();
	Random r = new Random();
	final int MAX = 10;
    
    @Test
    public  void testBytesToInteger() throws IOException
    {
        // valid ints
        String[] a = {"1", "-2345",  "1234567", "1.1", "-23.45", ""};
        Integer[] ia = {1, -2345, 1234567, 1, -23};
        
        for (int i = 0; i < ia.length; i++) {
            byte[] b = a[i].getBytes();
            assertEquals(ia[i], ps.bytesToInteger(b));
        }
        
        // invalid ints
        a = new String[]{"1234567890123456", "This is an int", ""};
        for (String s : a) {
            byte[] b = s.getBytes();
            Integer i = ps.bytesToInteger(b);
            assertEquals(null, i);            
        }        
    }
    
    @Test
    public  void testBytesToFloat() throws IOException
    {
        // valid floats
        String[] a = {"1", "-2.345",  "12.12334567", "1.02e-2",".23344",
		      "23.1234567897", "12312.33f", "002312.33F", "1.02e-2f", ""};

        Float[] f = {1f, -2.345f,  12.12334567f, 1.02e-2f,.23344f, 23.1234567f, // 23.1234567f is a truncation case
		     12312.33f, 2312.33f, 1.02e-2f }; 

        for (int j = 0; j < f.length; j++) {
            byte[] b = a[j].getBytes();            
            assertEquals(f[j], ps.bytesToFloat(b));
        }
        
        // invalid floats
        a = new String[]{"1a.1", "23.1234567a890123456",  "This is a float", ""};
        for (String s : a) {
            byte[] b = s.getBytes();
            Float fl = ps.bytesToFloat(b);
            assertEquals(null, fl);
            
        }        
    }
    
    @Test
    public  void testBytesToDouble() throws IOException
    {
        // valid doubles
        String[] a = {"1", "-2.345",  "12.12334567890123456", "1.02e12","-.23344", ""};
        Double[] d = {(double)1, -2.345,  12.12334567890123456, 1.02e12, -.23344};
        for (int j = 0; j < d.length; j++) {
            byte[] b = a[j].getBytes();            
            assertEquals(d[j], ps.bytesToDouble(b));
        }
        
        // invalid doubles
        a = new String[]{"-0x1.1", "-23a.45",  "This is a double", ""};
        for (String s : a) {
            byte[] b = s.getBytes();
            Double dl = ps.bytesToDouble(b);
            assertEquals(null, dl);
            
        }        
    }
    
    @Test
    public  void testBytesToLong() throws IOException
    {
        // valid Longs
        String[] a = {"1", "-2345",  "123456789012345678", "1.1", "-23.45",
		      "21345345l", "3422342L", ""};
        Long[] la = {1L, -2345L, 123456789012345678L, 1L, -23L, 
		     21345345L, 3422342L};
        
        for (int i = 0; i < la.length; i++) {
            byte[] b = a[i].getBytes();
            assertEquals(la[i], ps.bytesToLong(b));
        }
        
        // invalid longs
        a = new String[]{"This is a long", "1.0e1000", ""};
        for (String s : a) {
            byte[] b = s.getBytes();
            Long l = ps.bytesToLong(b);
            assertEquals(null, l);            
        }        
    }
    
    @Test
    public  void testBytesToChar() throws IOException
    {
        // valid Strings
        String[] a = {"1", "-2345",  "text", "hello\nworld", ""};
        
        for (String s : a) {
            byte[] b = s.getBytes();
            assertEquals(s, ps.bytesToCharArray(b));
        }                        
    }
    
    @Test
    public  void testBytesToTuple() throws IOException
    {
        for (int i = 0; i < MAX; i++) {
            Tuple t = GenRandomData.genRandSmallBagTextTuple(r, 1, 100);
            Tuple convertedTuple = ps.bytesToTuple(t.toString().getBytes());
            assertTrue(t.equals(convertedTuple));
        }
        
    }
    
    @Test
    public  void testBytesToBag() throws IOException
    {
        for (int i = 0; i < MAX; i++) {
            DataBag b = GenRandomData.genRandFullTupTextDataBag(r,5,100);
            DataBag convertedBag = ps.bytesToBag(b.toString().getBytes());
            assertTrue(b.equals(convertedBag));
        }
        
    }
        
    @Test
    public  void testBytesToMap() throws IOException
    {
        
        for (int i = 0; i < MAX; i++) {
            Map<Object, Object>  m = GenRandomData.genRandObjectMap(r,5);
            String expectedMapString = DataType.mapToString(m);
            Map<Object, Object> convertedMap = ps.bytesToMap(expectedMapString.getBytes());
            assertTrue(m.equals(convertedMap));
        }
        
    }

    @Test
    public void testIntegerToBytes() throws IOException {
        Integer i = r.nextInt();
        assertTrue(DataType.equalByteArrays(i.toString().getBytes(), ps.toBytes(i)));
    }
        
    @Test
    public void testLongToBytes() throws IOException {
        Long l = r.nextLong();
        assertTrue(DataType.equalByteArrays(l.toString().getBytes(), ps.toBytes(l)));
    }
        
    @Test
    public void testFloatToBytes() throws IOException {
        Float f = r.nextFloat();
        assertTrue(DataType.equalByteArrays(f.toString().getBytes(), ps.toBytes(f)));
    }
        
    @Test
    public void testDoubleToBytes() throws IOException {
        Double d = r.nextDouble();
        assertTrue(DataType.equalByteArrays(d.toString().getBytes(), ps.toBytes(d)));
    }
        
    @Test
    public void testCharArrayToBytes() throws IOException {
        String s = GenRandomData.genRandString(r);
        assertTrue(s.equals(new String(ps.toBytes(s))));
    }
        
    @Test
    public void testTupleToBytes() throws IOException {
        Tuple t = GenRandomData.genRandSmallBagTextTuple(r, 1, 100);
        //Tuple t = GenRandomData.genRandSmallTuple(r, 100);
        assertTrue(DataType.equalByteArrays(t.toString().getBytes(), ps.toBytes(t)));
    }
        
    @Test
    public void testBagToBytes() throws IOException {
        DataBag b = GenRandomData.genRandFullTupTextDataBag(r,5,100);
        assertTrue(DataType.equalByteArrays(b.toString().getBytes(), ps.toBytes(b)));
    }
        
    @Test
    public void testMapToBytes() throws IOException {
        Map<Object, Object>  m = GenRandomData.genRandObjectMap(r,5);
        assertTrue(DataType.equalByteArrays(DataType.mapToString(m).getBytes(), ps.toBytes(m)));
    }
}

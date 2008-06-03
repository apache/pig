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

import java.io.IOException;

import org.apache.pig.builtin.PigStorage;

import org.junit.Test;

import junit.framework.TestCase;

/**
 * Test class to test conversions from bytes to types
 * and vice versa
 * 
 */
public class TestConversions extends TestCase {

    PigStorage ps = new PigStorage();
    
    @Test
    public  void testBytesToInteger() throws IOException
    {
        // valid ints
        String[] a = {"1", "-2345",  "1234567"};
        Integer[] ia = {1, -2345, 1234567};
        
        for (int i = 0; i < ia.length; i++) {
            byte[] b = a[i].getBytes();
            assertEquals(ia[i], ps.bytesToInteger(b));
        }
        
        // invalid ints
        a = new String[]{"1.1", "-23.45",  "1234567890123456", "This is an int"};
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
        String[] a = {"1", "-2.345",  "12.12334567", "1.02e-2",".23344", "23.1234567897"};
        Float[] f = {1f, -2.345f,  12.12334567f, 1.02e-2f,.23344f, 23.1234567f}; // last case is a truncation case
        for (int j = 0; j < f.length; j++) {
            byte[] b = a[j].getBytes();            
            assertEquals(f[j], ps.bytesToFloat(b));
        }
        
        // invalid floats
        a = new String[]{"1a.1", "23.1234567a890123456",  "This is a float"};
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
        String[] a = {"1", "-2.345",  "12.12334567890123456", "1.02e12","-.23344"};
        Double[] d = {(double)1, -2.345,  12.12334567890123456, 1.02e12, -.23344};
        for (int j = 0; j < d.length; j++) {
            byte[] b = a[j].getBytes();            
            assertEquals(d[j], ps.bytesToDouble(b));
        }
        
        // invalid doubles
        a = new String[]{"-0x1.1", "-23a.45",  "This is a double"};
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
        String[] a = {"1", "-2345",  "123456789012345678"};
        Long[] la = {1L, -2345L, 123456789012345678L};
        
        for (int i = 0; i < la.length; i++) {
            byte[] b = a[i].getBytes();
            assertEquals(la[i], ps.bytesToLong(b));
        }
        
        // invalid longs
        a = new String[]{"1.1", "-23.45",  "This is a long"};
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
        String[] a = {"1", "-2345",  "text", "hello\nworld"};
        
        for (String s : a) {
            byte[] b = s.getBytes();
            assertEquals(s, ps.bytesToCharArray(b));
        }                        
    }
    
        
        
}

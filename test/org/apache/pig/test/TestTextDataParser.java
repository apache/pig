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
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.DefaultBagFactory;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Test;

public class TestTextDataParser extends junit.framework.TestCase {

    private final Log log = LogFactory.getLog(getClass());
    private TupleFactory tupleFactory = TupleFactory.getInstance();
    private BagFactory bagFactory = DefaultBagFactory.getInstance();
    PigStorage ps = new PigStorage();
    

    ResourceFieldSchema getTupleFieldSchema() throws IOException {
        ResourceFieldSchema stringfs = new ResourceFieldSchema();
        stringfs.setType(DataType.CHARARRAY);
        ResourceFieldSchema intfs = new ResourceFieldSchema();
        intfs.setType(DataType.INTEGER);
        
        ResourceSchema tupleSchema = new ResourceSchema();
        tupleSchema.setFields(new ResourceFieldSchema[]{intfs, stringfs});
        ResourceFieldSchema tuplefs = new ResourceFieldSchema();
        tuplefs.setSchema(tupleSchema);
        tuplefs.setType(DataType.TUPLE);
        
        return tuplefs;
    }
    
    public ResourceFieldSchema getBagFieldSchema() throws IOException{
        ResourceFieldSchema tuplefs = getTupleFieldSchema();
        
        ResourceSchema outBagSchema = new ResourceSchema();
        outBagSchema.setFields(new ResourceFieldSchema[]{tuplefs});
        ResourceFieldSchema outBagfs = new ResourceFieldSchema();
        outBagfs.setSchema(outBagSchema);
        outBagfs.setType(DataType.BAG);
        
        return outBagfs;
    }
    
    ResourceFieldSchema getLongFieldSchema() {
        ResourceFieldSchema longfs = new ResourceFieldSchema();
        longfs.setType(DataType.LONG);
        return longfs;
    }

    @Test
    public void testBoolean() throws Exception{
        String myBoolean = "true";
        Boolean b = ps.getLoadCaster().bytesToBoolean(myBoolean.getBytes());
        assertTrue(b.equals(Boolean.TRUE));
    }
    
    @Test
    public void testInteger() throws Exception{
        String myInteger = "1";
        Integer i = ps.getLoadCaster().bytesToInteger(myInteger.getBytes());
        assertTrue(i.equals(1));
    }

    @Test
    public void testLong() throws Exception{
        String myLong = "1";
        Long l = ps.getLoadCaster().bytesToLong(myLong.getBytes());
        assertTrue(l.equals(1l));
    }
    
    @Test
    public void testFloat() throws Exception{
        String myFloat = "0.1";
        Float f = ps.getLoadCaster().bytesToFloat(myFloat.getBytes());
        assertTrue(f.equals(0.1f));
    }
    
    @Test
    public void testDouble() throws Exception{
        String myDouble = "0.1";
        Double d = ps.getLoadCaster().bytesToDouble(myDouble.getBytes());
        assertTrue(d.equals(0.1));
    }
    
    @Test
    public void testString() throws Exception{
        String myString = "1a";
        String s = ps.getLoadCaster().bytesToCharArray(myString.getBytes());
        assertTrue(s.equals(myString));
    }
    

    //the value types of a map should always be a byte array
    //irrespective of the actual type
    @SuppressWarnings("unchecked")
    @Test
    public void testMapStringValueType() throws Exception{
        String myMap = "[key1#value1]";
        Map<String, Object> map = ps.getLoadCaster().bytesToMap(myMap.getBytes());
        String key = map.keySet().iterator().next();        
        Object v = map.get("key1");
        assertTrue(key.equals("key1"));
        assertTrue(v instanceof DataByteArray);
        String value = new String(((DataByteArray)v).get());
        assertTrue(value.equals("value1"));
    }
    
    //the value types of a map should always be a byte array
    //irrespective of the actual type
    @SuppressWarnings("unchecked")
    @Test
    public void testMapIntegerValueType() throws Exception{
        String myMap = "[key1#1]";
        Map<String, Object> map = ps.getLoadCaster().bytesToMap(myMap.getBytes());
        String key = map.keySet().iterator().next();        
        Object v = map.get("key1");
        assertTrue(key.equals("key1"));
        assertTrue(v instanceof DataByteArray);
        String value = new String(((DataByteArray)v).get());
        assertTrue(value.equals("1"));
    }

    //the value types of a map should always be a byte array
    //irrespective of the actual type
    @SuppressWarnings("unchecked")
    @Test
    public void testMapLongValueType() throws Exception{
        String myMap = "[key1#1l]";
        Map<String, Object> map = ps.getLoadCaster().bytesToMap(myMap.getBytes());
        String key = map.keySet().iterator().next();        
        Object v = map.get("key1");
        assertTrue(key.equals("key1"));
        assertTrue(v instanceof DataByteArray);
        String value = new String(((DataByteArray)v).get());
        assertTrue(value.equals("1l"));
    }

    //the value types of a map should always be a byte array
    //irrespective of the actual type
    @SuppressWarnings("unchecked")
    @Test
    public void testMapFloatValueType() throws Exception{
        String myMap = "[key1#0.1f]";
        Map<String, Object> map = ps.getLoadCaster().bytesToMap(myMap.getBytes());
        String key = map.keySet().iterator().next();        
        Object v = map.get("key1");
        assertTrue(key.equals("key1"));
        assertTrue(v instanceof DataByteArray);
        String value = new String(((DataByteArray)v).get());
        assertTrue(value.equals("0.1f"));
    }

    //the value types of a map should always be a byte array
    //irrespective of the actual type
    @SuppressWarnings("unchecked")
    @Test
    public void testMapDoubleValueType() throws Exception{
        String myMap = "[key1#0.1]";
        Map<String, Object> map = ps.getLoadCaster().bytesToMap(myMap.getBytes());
        String key = map.keySet().iterator().next();        
        Object v = map.get("key1");
        assertTrue(key.equals("key1"));
        assertTrue(v instanceof DataByteArray);
        String value = new String(((DataByteArray)v).get());
        assertTrue(value.equals("0.1"));
    }

    @Test
    public void testTuple() throws Exception{
        String myTuple = "(1,a)";
        Object o = ps.getLoadCaster().bytesToTuple(myTuple.getBytes(), getTupleFieldSchema());
        assertTrue(o instanceof Tuple);
        Tuple t = (Tuple)o;
        Tuple expectedTuple = tupleFactory.newTuple(2);
        expectedTuple.set(0, 1);
        expectedTuple.set(1, "a");
        assertTrue(t.equals(expectedTuple));
    }

    @Test
    public void testBag() throws Exception{
        String myBag = "{(1,a),(2,b)}";
        Object o = ps.getLoadCaster().bytesToBag(myBag.getBytes(), getBagFieldSchema());
        assertTrue(o instanceof DataBag);
        DataBag b = (DataBag)o;
        DataBag expectedBag = bagFactory.newDefaultBag();
        Tuple expectedTuple = tupleFactory.newTuple(2);
        expectedTuple.set(0, 1);
        expectedTuple.set(1, "a");
        expectedBag.add(expectedTuple);
        expectedTuple = tupleFactory.newTuple(2);
        expectedTuple.set(0, 2);
        expectedTuple.set(1, "b");
        expectedBag.add(expectedTuple);
        assertTrue(b.equals(expectedBag));
    }
    
    @Test
    public void testEmptyBag() throws Exception{
        String myBag = "{}";
        Object o = ps.getLoadCaster().bytesToBag(myBag.getBytes(), getBagFieldSchema());
        assertTrue(o instanceof DataBag);
        DataBag b = (DataBag)o;
        assertTrue(b.size()==0);
    }    
}

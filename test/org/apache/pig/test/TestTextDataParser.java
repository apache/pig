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

import java.io.ByteArrayInputStream;
import java.util.Map;

import org.junit.Test;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DefaultBagFactory;
import org.apache.pig.data.DefaultTupleFactory;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.data.parser.TextDataParser;
import org.apache.pig.data.parser.ParseException ;


public class TestTextDataParser extends junit.framework.TestCase {

    private final Log log = LogFactory.getLog(getClass());
    private TupleFactory tupleFactory = DefaultTupleFactory.getInstance();
    private BagFactory bagFactory = DefaultBagFactory.getInstance();
    

    @Test
    public void testInteger() throws Exception{
        String myInteger = "1";
        Integer i = (Integer)parseTextData(myInteger);
        assertTrue(i.equals(1));
    }

    @Test
    public void testLong() throws Exception{
        String myLong = "1l";
        Long l = (Long)parseTextData(myLong);
        assertTrue(l.equals(1l));
    }
    
    @Test
    public void testFloat() throws Exception{
        String myFloat = "0.1f";
        Float f = (Float)parseTextData(myFloat);
        assertTrue(f.equals(0.1f));
    }
    
    @Test
    public void testDouble() throws Exception{
        String myDouble = "0.1";
        Double d = (Double)parseTextData(myDouble);
        assertTrue(d.equals(0.1));
    }
    
    @Test
    public void testString() throws Exception{
        String myString = "1a";
        String s = (String)parseTextData(myString);
        assertTrue(s.equals(myString));
    }
    

    //the value types of a map should always be a byte array
    //irrespective of the actual type
    @SuppressWarnings("unchecked")
    @Test
    public void testMapStringValueType() throws Exception{
        String myMap = "[key1#value1]";
        Map<String, Object> map = (Map<String, Object>)parseTextData(myMap);
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
        Map<String, Object> map = (Map<String, Object>)parseTextData(myMap);
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
        Map<String, Object> map = (Map<String, Object>)parseTextData(myMap);
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
        Map<String, Object> map = (Map<String, Object>)parseTextData(myMap);
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
        Map<String, Object> map = (Map<String, Object>)parseTextData(myMap);
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
        Object o = parseTextData(myTuple);
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
        Object o = parseTextData(myBag);
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

    
    private Object parseTextData(String pigConstantAsString) throws ParseException {
        ByteArrayInputStream stream = new ByteArrayInputStream(pigConstantAsString.getBytes()) ;
        TextDataParser textDataParser = new TextDataParser(stream) ;
        return textDataParser.Datum();
    }
}

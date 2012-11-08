/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.pig.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.InternalMap;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Test;

/**
 * This class will exercise the basic Pig data model and members. It tests for proper behavior in
 * assigment and comparision, as well as function application.
 */
public class TestDataModel {

    @Test
    public void testDatum() throws Exception {
        // Datum is an empty class, but in the event we
        // ever add a method, test it here
    }

    @Test
    public void testTuple() throws Exception {
        TupleFactory tf = TupleFactory.getInstance();

        int arity = 5;
        int[] input1 = { 1, 2, 3, 4, 5 };
        String[] input2 = { "1", "2", "3", "4", "5" };
        String[] input3 = { "1", "2", "3", "4", "5", "6" };

        // validate construction and equality
        Tuple f1 = Util.loadFlatTuple(tf.newTuple(arity), input1);
        Tuple f2 = Util.loadFlatTuple(tf.newTuple(arity), input1);
        Tuple f3 = tf.newTuple(arity);
        assertEquals(arity, f1.size());
        assertEquals(f1, f2);

        // invalid equality
        f2 = Util.loadTuple(tf.newTuple(input3.length), input3);
        assertFalse(f1.equals(f2));

        // copy equality
        /*
         * f2.copyFrom(f1);
         * assertTrue(f1.equals(f2));
         */

        // append function and equality
        int[] input4 = { 1, 2, 3 };
        int[] input5 = { 4, 5 };
        /*
         * f1 = Util.loadFlatTuple(tf.newTuple(input4.length), input4);
         * f2 = Util.loadFlatTuple(tf.newTuple(input5.length), input5);
         * f3 = Util.loadFlatTuple(tf.newTuple(input1.length), input1);
         * f1.appendTuple(f2);
         * assertTrue(f3.equals(f1));
         */

        // arity then value comparision behavior
        f1 = Util.loadFlatTuple(tf.newTuple(input1.length), input1); // 1,2,3,4,5
        f2 = Util.loadFlatTuple(tf.newTuple(input4.length), input4); // 1,2,3
        assertTrue(f1.compareTo(f2) > 0);
        assertFalse(f1.compareTo(f2) < 0);

        int[] input6 = { 1, 2, 3, 4, 6 };
        f2 = Util.loadFlatTuple(tf.newTuple(input6.length), input6);
        assertTrue(f1.compareTo(f2) < 0);
        assertFalse(f1.compareTo(f2) > 0);

        // delimited export
        String expected = "1:2:3:4:5";
        f1 = Util.loadFlatTuple(tf.newTuple(input1.length), input1);
        assertTrue(expected.equals(f1.toDelimitedString(":")));

        // value read / write & marshalling
        PipedOutputStream pos = new PipedOutputStream();
        DataOutputStream dos = new DataOutputStream(pos);
        PipedInputStream pis = new PipedInputStream(pos);
        DataInputStream dis = new DataInputStream(pis);
        f1.write(dos);
        f1.write(dos);
        f2.readFields(dis);
        assertTrue(f1.equals(f2));
        f2.readFields(dis);
        assertTrue(f1.equals(f2));
    }

    @Test
    public void testNestTuple() throws Exception {
        TupleFactory tf = TupleFactory.getInstance();

        int[][] input1 = { { 1, 2, 3, 4, 5 }, { 1, 2, 3, 4, 5 }, { 1, 2, 3, 4, 5 },
                           { 1, 2, 3, 4, 5 }, { 1, 2, 3, 4, 5 } };
        int[][] input2 = { { 1, 2 }, { 1, 2 } };

        Tuple n1 = Util.loadNestTuple(tf.newTuple(input1.length), input1);
        Tuple n2 = tf.newTuple();

        n2 = Util.loadNestTuple(tf.newTuple(input2.length), input2);
    }

    @Test
    public void testReadWrite() throws Exception {
        // Create a tuple with every data type in it, and then read and
        // write it, both via DataReaderWriter and Tuple.readFields
        TupleFactory tf = TupleFactory.getInstance();

        Tuple t1 = giveMeOneOfEach();

        File file = File.createTempFile("Tuple", "put");
        FileOutputStream fos = new FileOutputStream(file);
        DataOutput out = new DataOutputStream(fos);
        t1.write(out);
        t1.write(out); // twice in a row on purpose
        fos.close();

        FileInputStream fis = new FileInputStream(file);
        DataInput in = new DataInputStream(fis);
        for (int i = 0; i < 2; i++) {
            Tuple after = tf.newTuple();
            after.readFields(in);

            Object o = after.get(0);
            assertTrue("isa Tuple", o instanceof Tuple);
            Tuple t3 = (Tuple)o;
            o = t3.get(0);
            assertTrue("isa Integer", o instanceof Integer);
            assertEquals(new Integer(3), (Integer)o);
            o = t3.get(1);
            assertTrue("isa Float", o instanceof Float);
            assertEquals(new Float(3.0), (Float)o);

            o = after.get(1);
            assertTrue("isa Bag", o instanceof DataBag);
            DataBag b = (DataBag)o;
            Iterator<Tuple> j = b.iterator();
            Tuple[] ts = new Tuple[2];
            assertTrue("first tuple in bag", j.hasNext());
            ts[0] = j.next();
            assertTrue("second tuple in bag", j.hasNext());
            ts[1] = j.next();
            o = ts[0].get(0);
            assertTrue("isa Integer", o instanceof Integer);
            assertEquals(new Integer(4), (Integer)o);
            o = ts[1].get(0);
            assertTrue("isa String", o instanceof String);
            assertEquals("mary had a little lamb", (String)o);

            o = after.get(2);
            assertTrue("isa Map", o instanceof Map);
            Map<String, Object> m = (Map<String, Object>)o;
            assertEquals("world", (String)m.get("hello"));
            assertEquals("all", (String)m.get("goodbye"));
            assertNull(m.get("fred"));

            o = after.get(3);
            assertTrue("isa Integer", o instanceof Integer);
            Integer ii = (Integer)o;
            assertEquals(new Integer(42), ii);

            o = after.get(4);
            assertTrue("isa Long", o instanceof Long);
            Long l = (Long)o;
            assertEquals(new Long(5000000000L), l);

            o = after.get(5);
            assertTrue("isa Float", o instanceof Float);
            Float f = (Float)o;
            assertEquals(new Float(3.141592654), f);

            o = after.get(6);
            assertTrue("isa Double", o instanceof Double);
            Double d = (Double)o;
            assertEquals(new Double(2.99792458e8), d);

            o = after.get(7);
            assertTrue("isa Boolean", o instanceof Boolean);
            Boolean bool = (Boolean)o;
            assertTrue(bool);

            o = after.get(8);
            assertTrue("isa DataByteArray", o instanceof DataByteArray);
            DataByteArray ba = (DataByteArray)o;
            assertEquals(new DataByteArray("hello"), ba);

            o = after.get(9);
            assertTrue("isa String", o instanceof String);
            String s = (String)o;
            assertEquals("goodbye", s);
        }

        file.delete();
    }

    @Test
    public void testReadWriteInternal() throws Exception {
        // Create a tuple with every internal data type in it, and then read and
        // write it, both via DataReaderWriter and Tuple.readFields
        TupleFactory tf = TupleFactory.getInstance();

        Tuple t1 = tf.newTuple(1);

        InternalMap map = new InternalMap(2);
        map.put(new Integer(1), new String("world"));
        map.put(new Long(3L), new String("all"));
        t1.set(0, map);

        File file = File.createTempFile("Tuple", "put");
        FileOutputStream fos = new FileOutputStream(file);
        DataOutput out = new DataOutputStream(fos);
        t1.write(out);
        fos.close();

        FileInputStream fis = new FileInputStream(file);
        DataInput in = new DataInputStream(fis);

        Tuple after = tf.newTuple();
        after.readFields(in);

        Object o = after.get(0);
        assertTrue("isa InternalMap", o instanceof InternalMap);

        InternalMap m = (InternalMap)o;
        assertEquals("world", (String)m.get(new Integer(1)));
        assertEquals("all", (String)m.get(new Long(3L)));
        assertNull(m.get("fred"));

        file.delete();
    }

    @Test
    public void testTupleToString() throws Exception {
        Tuple t = giveMeOneOfEach();

        assertEquals(
                "toString",
                "((3,3.0),{(4),(mary had a little lamb)},[hello#world,goodbye#all],42,5000000000,3.1415927,2.99792458E8,true,hello,goodbye,)",
                t.toString());
    }

    @Test
    public void testTupleHashCode() throws Exception {
        TupleFactory tf = TupleFactory.getInstance();

        Tuple t1 = tf.newTuple(2);
        t1.set(0, new DataByteArray("hello world"));
        t1.set(1, new Integer(1));

        Tuple t2 = tf.newTuple(2);
        t2.set(0, new DataByteArray("hello world"));
        t2.set(1, new Integer(1));

        assertEquals("same data", t1.hashCode(), t2.hashCode());

        Tuple t3 = tf.newTuple(3);
        t3.set(0, new DataByteArray("hello world"));
        t3.set(1, new Integer(1));
        t3.set(2, new Long(4));
        assertFalse("different size", t1.hashCode() == t3.hashCode());

        Tuple t4 = tf.newTuple(2);
        t4.set(0, new DataByteArray("hello world"));
        t4.set(1, new Integer(2));
        assertFalse("same size, different data", t1.hashCode() == t4.hashCode());

        // Make sure we can take the hash code of all the types.
        Tuple t5 = giveMeOneOfEach();
        t5.hashCode();
    }

    @Test
    public void testTupleEquals() throws Exception {
        TupleFactory tf = TupleFactory.getInstance();

        Tuple t1 = tf.newTuple();
        Tuple t2 = tf.newTuple();

        t1.append(new Integer(3));
        t2.append(new Integer(3));

        assertFalse("different object", t1.equals(new String()));

        assertTrue("same data", t1.equals(t2));

        t2 = tf.newTuple();
        t2.append(new Integer(4));
        assertFalse("different data", t1.equals(t2));

        t2 = tf.newTuple();
        t2.append(new Integer(3));
        t2.append(new Integer(3));
        assertFalse("different size", t1.equals(t2));
    }

    @Test
    public void testTupleCompareTo() throws Exception {
        TupleFactory tf = TupleFactory.getInstance();

        Tuple t1 = tf.newTuple();
        Tuple t2 = tf.newTuple();

        t1.append(new Integer(3));
        t2.append(new Integer(3));

        assertEquals("same data equal", 0, t1.compareTo(t2));

        t2 = tf.newTuple();
        t2.append(new Integer(2));
        assertTrue("greater than tuple with lesser value", 0 < t1.compareTo(t2));

        t2 = tf.newTuple();
        t2.append(new Integer(4));
        assertTrue("less than tuple with greater value", 0 > t1.compareTo(t2));

        t2 = tf.newTuple();
        t2.append(new Integer(3));
        t2.append(new Integer(4));
        assertTrue("less than bigger tuple", 0 > t1.compareTo(t2));

        t2 = tf.newTuple();
        assertTrue("greater than smaller tuple", 0 < t1.compareTo(t2));
    }

    @Test
    public void testMultiFieldTupleCompareTo() throws Exception {
        TupleFactory tf = TupleFactory.getInstance();

        Tuple t1 = tf.newTuple();
        Tuple t2 = tf.newTuple();

        t1.append(new DataByteArray("bbb"));
        t1.append(new DataByteArray("bbb"));
        t2.append(new DataByteArray("bbb"));
        t2.append(new DataByteArray("bbb"));

        assertEquals("same data equal", 0, t1.compareTo(t2));

        t2 = tf.newTuple();
        t2.append(new DataByteArray("aaa"));
        t2.append(new DataByteArray("aaa"));
        assertTrue("greater than tuple with lesser value", 0 < t1.compareTo(t2));

        t2 = tf.newTuple();
        t2.append(new DataByteArray("ddd"));
        t2.append(new DataByteArray("ddd"));
        assertTrue("less than tuple with greater value", 0 > t1.compareTo(t2));

        // First column same, second lesser
        t2 = tf.newTuple();
        t2.append(new DataByteArray("bbb"));
        t2.append(new DataByteArray("aaa"));
        assertTrue("greater than tuple with lesser value", 0 < t1.compareTo(t2));

        // First column same, second greater
        t2 = tf.newTuple();
        t2.append(new DataByteArray("bbb"));
        t2.append(new DataByteArray("ccc"));
        assertTrue("greater than tuple with lesser value", 0 > t1.compareTo(t2));

        // First column less, second same
        t2 = tf.newTuple();
        t2.append(new DataByteArray("aaa"));
        t2.append(new DataByteArray("bbb"));
        assertTrue("greater than tuple with lesser value", 0 < t1.compareTo(t2));

        // First column greater, second same
        t2 = tf.newTuple();
        t2.append(new DataByteArray("ccc"));
        t2.append(new DataByteArray("bbb"));
        assertTrue("greater than tuple with lesser value", 0 > t1.compareTo(t2));

        // First column less, second greater
        t2 = tf.newTuple();
        t2.append(new DataByteArray("aaa"));
        t2.append(new DataByteArray("ccc"));
        assertTrue("greater than tuple with lesser value", 0 < t1.compareTo(t2));

        // First column greater, second same
        t2 = tf.newTuple();
        t2.append(new DataByteArray("ccc"));
        t2.append(new DataByteArray("aaa"));
        assertTrue("greater than tuple with lesser value", 0 > t1.compareTo(t2));
    }

    @Test
    public void testByteArrayToString() throws Exception {
        DataByteArray ba = new DataByteArray("hello world");

        assertEquals("toString", "hello world", ba.toString());
    }

    @Test
    public void testByteArrayHashCode() throws Exception {
        DataByteArray ba1 = new DataByteArray("hello world");
        DataByteArray ba2 = new DataByteArray("hello world");
        DataByteArray ba3 = new DataByteArray("goodbye world");

        assertEquals("same data", ba1.hashCode(), ba2.hashCode());

        assertFalse("different data", ba1.hashCode() == ba3.hashCode());
    }

    @Test
    public void testByteArrayEquals() throws Exception {
        DataByteArray ba1 = new DataByteArray("hello world");
        DataByteArray ba2 = new DataByteArray("hello world");
        DataByteArray ba3 = new DataByteArray("goodbye world");

        assertTrue("same data", ba1.equals(ba2));

        assertFalse("different data", ba1.equals(ba3));
    }

    @Test
    public void testByteArrayCompareTo() throws Exception {
        DataByteArray ba1 = new DataByteArray("hello world");
        DataByteArray ba2 = new DataByteArray("hello world");
        DataByteArray ba3 = new DataByteArray("goodbye world");

        assertTrue("same data", ba1.compareTo(ba2) == 0);

        assertTrue("different length lexically lower value less than",
                ba3.compareTo(ba1) < 0);
        assertTrue("different length lexically higher value greater than",
                ba1.compareTo(ba3) > 0);

        ba2 = new DataByteArray("hello worlc");
        assertTrue("same length lexically lower value less than",
                ba2.compareTo(ba1) < 0);
        assertTrue("same length lexically higher value greater than",
                ba1.compareTo(ba2) > 0);

        ba2 = new DataByteArray("hello worlds");
        assertTrue("shorter lexically same value less than",
                ba1.compareTo(ba2) < 0);
        assertTrue("longer lexically same value greater than",
                ba2.compareTo(ba1) > 0);

    }

    @Test(expected = ExecException.class)
    public void testIntegerConversionErr() throws Exception {
        List list = new ArrayList();
        try {
            DataType.toInteger(list);
        } catch (ExecException ee) {
            assertEquals(1071, ee.getErrorCode());
            throw ee;
        }
    }

    @Test(expected = ExecException.class)
    public void testIntegerConversionErr1() throws Exception {
        DataByteArray ba = new DataByteArray("hello world");
        try {
            DataType.toInteger(ba);
        } catch (ExecException ee) {
            assertEquals(1074, ee.getErrorCode());
            throw ee;
        }
    }

    @Test(expected = ExecException.class)
    public void testTupleConversionErr() throws Exception {
        List list = new ArrayList();
        try {
            DataType.toTuple(list);
        } catch (ExecException ee) {
            assertEquals(1071, ee.getErrorCode());
            throw ee;
        }
    }

    @Test(expected = ExecException.class)
    public void testTupleConversionErr1() throws Exception {
        DataByteArray ba = new DataByteArray("hello world");
        try {
            DataType.toTuple(ba);
        } catch (ExecException ee) {
            assertEquals(1071, ee.getErrorCode());
            throw ee;
        }
    }

    @Test
    public void testByteArrayAppend() throws Exception {
        DataByteArray expected = new DataByteArray("hello world");
        DataByteArray db1 = new DataByteArray("hello ");
        DataByteArray db2 = new DataByteArray("world");
        db1.append(db2);
        assertEquals("appends as expected", db1, expected);
    }

    @Test
    public void testByteArrayAppendMore() throws Exception {
        DataByteArray expected = new DataByteArray("hello world!");
        DataByteArray db1 = new DataByteArray("hello ");
        DataByteArray db2 = new DataByteArray("world");
        DataByteArray db3 = new DataByteArray("!");
        db1.append(db2).append(db3);
        assertEquals("appends as expected", db1, expected);
    }

    @Test
    public void testByteArrayAppendBytes() throws Exception {
        DataByteArray expected = new DataByteArray("hello world");
        DataByteArray db1 = new DataByteArray("hello ");
        byte[] db2 = "world".getBytes();
        db1.append(db2);
        assertEquals("appends as expected", db1, expected);
    }

    @Test
    public void testByteArrayAppendString() throws Exception {
        DataByteArray expected = new DataByteArray("hello world");
        DataByteArray db1 = new DataByteArray("hello ");
        db1.append("world");
        assertEquals("appends as expected", db1, expected);
    }

    @Test(expected = ExecException.class)
    public void testMapConversionErr() throws Exception {
        List list = new ArrayList();
        try {
            DataType.toMap(list);
        } catch (ExecException ee) {
            assertEquals(1071, ee.getErrorCode());
            throw ee;
        }
    }

    @Test
    public void testMapConversion() throws Exception {
        Map<Integer, Float> map = new HashMap<Integer, Float>();
        DataType.toMap(map);
    }

    @Test(expected = ExecException.class)
    public void testDetermineFieldSchemaErr() throws Exception {
        List list = new ArrayList();
        try {
            DataType.determineFieldSchema(list);
            fail("Error expected.");
        } catch (ExecException ee) {
            assertEquals(1073, ee.getErrorCode());
            throw ee;
        }
    }

    private Tuple giveMeOneOfEach() throws Exception {
        TupleFactory tf = TupleFactory.getInstance();

        Tuple t1 = tf.newTuple(11);
        Tuple t2 = tf.newTuple(2);

        t2.set(0, new Integer(3));
        t2.set(1, new Float(3.0));

        DataBag bag = BagFactory.getInstance().newDefaultBag();
        bag.add(tf.newTuple(new Integer(4)));
        bag.add(tf.newTuple(new String("mary had a little lamb")));

        Map<String, Object> map = new LinkedHashMap<String, Object>(2);
        map.put(new String("hello"), new String("world"));
        map.put(new String("goodbye"), new String("all"));

        t1.set(0, t2);
        t1.set(1, bag);
        t1.set(2, map);
        t1.set(3, new Integer(42));
        t1.set(4, new Long(5000000000L));
        t1.set(5, new Float(3.141592654));
        t1.set(6, new Double(2.99792458e8));
        t1.set(7, new Boolean(true));
        t1.set(8, new DataByteArray("hello"));
        t1.set(9, new String("goodbye"));

        return t1;
    }
}


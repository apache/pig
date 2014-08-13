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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.joda.time.DateTime;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapred.JobConf;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigTupleDefaultRawComparator;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigTupleSortComparator;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DefaultDataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.io.NullableTuple;
import org.apache.pig.impl.util.ObjectSerializer;
import org.junit.Before;
import org.junit.Test;

public class TestPigTupleRawComparator {

    private TupleFactory tf = TupleFactory.getInstance();
    private PigTupleSortComparator comparator = new PigTupleSortComparator();
    private PigTupleDefaultRawComparator oldComparator = new PigTupleDefaultRawComparator();
    private List<Object> list;
    private NullableTuple prototype;
    private ByteArrayOutputStream baos1 = new ByteArrayOutputStream();
    private ByteArrayOutputStream baos2 = new ByteArrayOutputStream();
    private DataOutputStream dos1 = new DataOutputStream(baos1);
    private DataOutputStream dos2 = new DataOutputStream(baos2);
    private final static int TUPLE_NUMBER = (int) 1e3;
    private final static int TIMES = (int) 1e5;
    private final static int SEED = 123456789;

    @Before
    public void setUp() {
        JobConf jobConf = new JobConf();
        comparator.setConf(jobConf);
        oldComparator.setConf(jobConf);
        list = Arrays.<Object> asList(1f, 2, 3.0, 4l, (byte) 5, true,
                new DataByteArray(new byte[] { 0x10, 0x2a, 0x5e }), "hello world!",
                tf.newTuple(Arrays.<Object> asList(8.0, 9f, 10l, 11)), new DateTime(12L));
        prototype = new NullableTuple(tf.newTuple(list));
        baos1.reset();
        baos2.reset();
    }

    @Test
    public void testCompareEquals() throws IOException {
        NullableTuple t = new NullableTuple(tf.newTuple(list));
        int res = compareHelper(prototype, t, comparator);
        assertEquals(Math.signum(prototype.compareTo(t)), Math.signum(res), 0);
        assertTrue(res == 0);
    }

    @Test
    public void testCompareFloat() throws IOException {
        list.set(0, (Float) list.get(0) - 1);
        NullableTuple t = new NullableTuple(tf.newTuple(list));
        int res = compareHelper(prototype, t, comparator);
        assertEquals(Math.signum(prototype.compareTo(t)), Math.signum(res), 0);
        assertTrue(res > 0);
    }

    @Test
    public void testCompareInt() throws IOException {
        list.set(1, (Integer) list.get(1) + 1);
        NullableTuple t = new NullableTuple(tf.newTuple(list));
        int res = compareHelper(prototype, t, comparator);
        assertEquals(Math.signum(prototype.compareTo(t)), Math.signum(res), 0);
        assertTrue(res < 0);
    }

    @Test
    public void testCompareDouble() throws IOException {
        list.set(2, (Double) list.get(2) + 0.1);
        NullableTuple t = new NullableTuple(tf.newTuple(list));
        int res = compareHelper(prototype, t, comparator);
        assertEquals(Math.signum(prototype.compareTo(t)), Math.signum(res), 0);
        assertTrue(res < 0);
    }

    @Test
    public void testCompareByte() throws IOException {
        list.set(4, (Byte) list.get(4) + 1);
        NullableTuple t = new NullableTuple(tf.newTuple(list));
        int res = compareHelper(prototype, t, comparator);
        assertEquals(Math.signum(prototype.compareTo(t)), Math.signum(res), 0);
        assertTrue(res < 0);
    }

    @Test
    public void testCompareBoolean() throws IOException {
        list.set(5, false);
        NullableTuple t = new NullableTuple(tf.newTuple(list));
        int res = compareHelper(prototype, t, comparator);
        assertEquals(Math.signum(prototype.compareTo(t)), Math.signum(res), 0);
        assertTrue(res > 0);
    }

    @Test
    public void testCompareByteArray() throws IOException {
        list.set(6, new DataByteArray(new byte[] { 0x10, 0x1a }));
        NullableTuple t = new NullableTuple(tf.newTuple(list));
        int res = compareHelper(prototype, t, comparator);
        assertEquals(Math.signum(prototype.compareTo(t)), Math.signum(res), 0);
        
        list.set(6, new DataByteArray(new byte[] { 0x20 }));
        t = new NullableTuple(tf.newTuple(list));
        res = compareHelper(prototype, t, comparator);
        assertEquals(Math.signum(prototype.compareTo(t)), Math.signum(res), 0);
        
        // bytearray that will fit into BinInterSedes.TINYBYTEARRAY
        String largeTinyStr = appendChars("abc", 'x', 255 - 10);
        list.set(6, new DataByteArray(largeTinyStr));
        t = new NullableTuple(tf.newTuple(list));
        res = compareHelper(prototype, t, comparator);
        assertEquals(Math.signum(prototype.compareTo(t)), Math.signum(res), 0);    
        
        //longest bytearray that will fit into BinInterSedes.TINYBYTEARRAY
        largeTinyStr = appendChars("", 'x', 255);
        list.set(6, new DataByteArray(largeTinyStr));
        t = new NullableTuple(tf.newTuple(list));
        res = compareHelper(prototype, t, comparator);
        assertEquals(Math.signum(prototype.compareTo(t)), Math.signum(res), 0); 
        
        // bytearray that will fit into BinInterSedes.SMALLBYTEARRAY
        String largeSmallStr = appendChars("abc", 'x', 65535 - 100);
        list.set(6, new DataByteArray(largeSmallStr));
        t = new NullableTuple(tf.newTuple(list));
        res = compareHelper(prototype, t, comparator);
        assertEquals(Math.signum(prototype.compareTo(t)), Math.signum(res), 0);          

        // bytearray that will fit into BinInterSedes.BYTEARRAY
        String largeStr = appendChars("abc", 'x', 65535 + 10000);
        list.set(6, new DataByteArray(largeStr));
        t = new NullableTuple(tf.newTuple(list));
        res = compareHelper(prototype, t, comparator);
        assertEquals(Math.signum(prototype.compareTo(t)), Math.signum(res), 0);          
        
    }

   @Test
    public void testCompareCharArray() throws IOException {
        list.set(7, "hello world!");
        NullableTuple t = new NullableTuple(tf.newTuple(list));
        int res = compareHelper(prototype, t, comparator);
        assertEquals(Math.signum(prototype.compareTo(t)), Math.signum(res), 0);
        assertTrue(res == 0);
        
        list.set(7, "hello worlc!");
        t = new NullableTuple(tf.newTuple(list));
        res = compareHelper(prototype, t, comparator);
        assertEquals(Math.signum(prototype.compareTo(t)), Math.signum(res), 0);
        assertTrue(res > 0);
        
        // chararray that will fit into BinInterSedes.SMALLCHARARRAY
        String largeTinyString = appendChars("hello worlc!", 'x', 300);
        list.set(7, largeTinyString);
        t = new NullableTuple(tf.newTuple(list));
        res = compareHelper(prototype, t, comparator);
        assertEquals(Math.signum(prototype.compareTo(t)), Math.signum(res), 0);
        assertTrue(res > 0);
        
        list.set(7, "hello worlz!");
        t = new NullableTuple(tf.newTuple(list));
        res = compareHelper(prototype, t, comparator);
        assertEquals(Math.signum(prototype.compareTo(t)), Math.signum(res), 0);
        assertTrue(res < 0);
        list.set(7, "hello");
        t = new NullableTuple(tf.newTuple(list));
        res = compareHelper(prototype, t, comparator);
        assertEquals(Math.signum(prototype.compareTo(t)), Math.signum(res), 0);
        assertTrue(res > 0);
        list.set(7, "hello world!?");
        t = new NullableTuple(tf.newTuple(list));
        res = compareHelper(prototype, t, comparator);
        assertEquals(Math.signum(prototype.compareTo(t)), Math.signum(res), 0);
        assertTrue(res < 0);
    }

    private String appendChars(String str, char c, int rep) {
        StringBuilder sb = new StringBuilder(str.length() + rep);
        sb.append(str);
        for(int i=0; i<rep; i++){
            sb.append(c);
        }
        return sb.toString();
    }

    @Test
    public void compareInnerTuples() throws IOException {
        NullableTuple t = new NullableTuple(tf.newTuple(list));
        int res = compareHelper(prototype, t, comparator);
        assertEquals(Math.signum(prototype.compareTo(t)), Math.signum(res), 0);
        assertTrue(res == 0);
        
        list.set(8, tf.newTuple(Arrays.<Object> asList(8.0, 9f, 10l, 12)));
        t = new NullableTuple(tf.newTuple(list));
        res = compareHelper(prototype, t, comparator);
        assertEquals(Math.signum(prototype.compareTo(t)), Math.signum(res), 0);
        assertTrue(res < 0);
        
        list.set(8, tf.newTuple(Arrays.<Object> asList(8.0, 9f, 9l, 12)));
        t = new NullableTuple(tf.newTuple(list));
        res = compareHelper(prototype, t, comparator);
        assertEquals(Math.signum(prototype.compareTo(t)), Math.signum(res), 0);
        assertTrue(res > 0);
        
        list.set(8, tf.newTuple(Arrays.<Object> asList(7.0, 9f, 9l, 12)));
        t = new NullableTuple(tf.newTuple(list));
        res = compareHelper(prototype, t, comparator);
        assertEquals(Math.signum(prototype.compareTo(t)), Math.signum(res), 0);
        assertTrue(res > 0);
        
        //Tuple that will fit into BinInterSedes.TINYTUPLE
        Tuple tinyTuple = createLargeTuple(1, 200, tf);
        list.set(8, tinyTuple);
        t = new NullableTuple(tf.newTuple(list));
        res = compareHelper(prototype, t, comparator);
        assertEquals(Math.signum(prototype.compareTo(t)), Math.signum(res), 0);
        
        //Tuple that will fit into BinInterSedes.SMALLTUPLE
        Tuple smallTuple = createLargeTuple(1, 1000, tf);
        list.set(8, smallTuple);
        t = new NullableTuple(tf.newTuple(list));
        res = compareHelper(prototype, t, comparator);
        assertEquals(Math.signum(prototype.compareTo(t)), Math.signum(res), 0);
        
        // DataType.LONG < DataType.DOUBLE
        list.set(8, tf.newTuple(Arrays.<Object> asList(8l, 9f, 9l, 12)));
        t = new NullableTuple(tf.newTuple(list));
        res = compareHelper(prototype, t, comparator);
        assertEquals(Math.signum(prototype.compareTo(t)), Math.signum(res), 0);
        assertTrue(res > 0);
        
        // object after tuple
        list = new ArrayList<Object>(list);
        list.add(10);
        NullableTuple t1 = new NullableTuple(tf.newTuple(list));
        list.set(list.size() - 1, 11);
        NullableTuple t2 = new NullableTuple(tf.newTuple(list));
        res = compareHelper(t1, t2, comparator);
        assertEquals(Math.signum(t1.compareTo(t2)), Math.signum(res), 0);
        assertTrue(res < 0);
        
        // fancy tuple nesting
        list.set(list.size() - 1, tf.newTuple(list));
        t1 = new NullableTuple(tf.newTuple(list));
        list.set(list.size() - 1, 10);
        list.set(list.size() - 1, tf.newTuple(list));
        t2 = new NullableTuple(tf.newTuple(list));
        res = compareHelper(t1, t2, comparator);
        assertEquals(Math.signum(t1.compareTo(t2)), Math.signum(res), 0);
        assertTrue(res > 0);
    }

    private Tuple createLargeTuple(int num, int repetitions, TupleFactory tf) {
        ArrayList<Integer> ar = new ArrayList<Integer>(repetitions);
        for(int i=0; i<repetitions; i++){
            ar.add(i, num);
        }
        return tf.newTuple(ar); 
    }

    @Test
    public void testCompareDataBag() throws IOException {
        list = new ArrayList<Object>(list);
        list.add(new DefaultDataBag(Arrays.asList(tf.newTuple(Arrays.asList(0)))));
        NullableTuple t1 = new NullableTuple(tf.newTuple(list));
        list.set(list.size() - 1, new DefaultDataBag(Arrays.asList(tf.newTuple(Arrays.asList(1)))));
        NullableTuple t2 = new NullableTuple(tf.newTuple(list));
        int res = compareHelper(t1, t2, comparator);
        assertEquals(Math.signum(t1.compareTo(t2)), Math.signum(res), 0);
        assertTrue(res < 0);
        
        //bag that will fit into BinInterSedes.TINYBAG
        DataBag largeBag = createLargeBag(200, tf);
        t2 = new NullableTuple(tf.newTuple(largeBag));
        res = compareHelper(t1, t2, comparator);
        assertEquals(Math.signum(t1.compareTo(t2)), Math.signum(res), 0);

        //bag that will fit into BinInterSedes.SMALLBAG
        largeBag = createLargeBag(3000, tf);
        t2 = new NullableTuple(tf.newTuple(largeBag));
        res = compareHelper(t1, t2, comparator);
        assertEquals(Math.signum(t1.compareTo(t2)), Math.signum(res), 0);
    
    }

    private DataBag createLargeBag(int size, TupleFactory tf) {
        Tuple t = tf.newTuple(Arrays.asList(0));
        ArrayList<Tuple> tuplist = new ArrayList<Tuple>(size); 
        for(int i=0; i<size; i++){
            tuplist.add(t);
        }
        return new DefaultDataBag(tuplist);
    }

    @Test
    public void testCompareMap() throws IOException {
        list = new ArrayList<Object>(list);
        list.add(Collections.singletonMap("pig", "scalability"));
        NullableTuple t1 = new NullableTuple(tf.newTuple(list));
        list.set(list.size() - 1, Collections.singletonMap("pig", "scalability"));
        NullableTuple t2 = new NullableTuple(tf.newTuple(list));
        int res = compareHelper(t1, t2, comparator);
        assertEquals(Math.signum(t1.compareTo(t2)), Math.signum(res), 0);
        assertTrue(res == 0);
        list.set(list.size() - 1, Collections.singletonMap("pigg", "scalability"));
        t2 = new NullableTuple(tf.newTuple(list));
        res = compareHelper(t1, t2, comparator);
        assertEquals(Math.signum(t1.compareTo(t2)), Math.signum(res), 0);
        assertTrue(res < 0);
        list.set(list.size() - 1, Collections.singletonMap("pig", "Scalability"));
        t2 = new NullableTuple(tf.newTuple(list));
        res = compareHelper(t1, t2, comparator);
        assertEquals(Math.signum(t1.compareTo(t2)), Math.signum(res), 0);
        assertTrue(res > 0);
        list.set(list.size() - 1, Collections.singletonMap("pii", "scalability"));
        t2 = new NullableTuple(tf.newTuple(list));
        res = compareHelper(t1, t2, comparator);
        assertEquals(Math.signum(t1.compareTo(t2)), Math.signum(res), 0);
        assertTrue(res < 0);
        // object after map
        list.add(107);
        t1 = new NullableTuple(tf.newTuple(list));
        list.set(list.size() - 1, 108);
        t2 = new NullableTuple(tf.newTuple(list));
        res = compareHelper(t1, t2, comparator);
        assertEquals(Math.signum(t1.compareTo(t2)), Math.signum(res), 0);
        assertTrue(res < 0);
    }

    @Test
    public void testCompareDateTime() throws IOException {
        list.set(9, ((DateTime) list.get(9)).plus(1L));
        NullableTuple t = new NullableTuple(tf.newTuple(list));
        int res = compareHelper(prototype, t, comparator);
        assertEquals(Math.signum(prototype.compareTo(t)), Math.signum(res), 0);
        assertTrue(res < 0);
    }

    @Test
    public void testCompareDiffertTypes() throws IOException {
        // DataType.INTEGER < DataType.LONG
        list.set(3, 4);
        NullableTuple t = new NullableTuple(tf.newTuple(list));
        int res = compareHelper(prototype, t, comparator);
        assertEquals(Math.signum(prototype.compareTo(t)), Math.signum(res), 0);
        assertTrue(res > 0);
    }

    @Test
    public void testCompareDifferentSizes() throws IOException {
        list = new ArrayList<Object>(list);
        // this object should be never get into the comparison loop
        list.add(new DefaultDataBag());
        NullableTuple t = new NullableTuple(tf.newTuple(list));
        int res = compareHelper(prototype, t, comparator);
        assertEquals(Math.signum(prototype.compareTo(t)), Math.signum(res), 0);
        assertTrue(res < 0);
    }

    @Test
    public void testRandomTuples() throws IOException {
        Random rand = new Random(SEED);
        for (int i = 0; i < TUPLE_NUMBER; i++) {
            NullableTuple t = new NullableTuple(getRandomTuple(rand));
            int res = compareHelper(prototype, t, comparator);
            assertEquals(Math.signum(prototype.compareTo(t)), Math.signum(res), 0);
        }
    }

    @Test
    public void testSortOrder() throws IOException {
        // prototype < t but we use inverse sort order
        list.set(2, (Double) list.get(2) + 0.1);
        NullableTuple t = new NullableTuple(tf.newTuple(list));
        JobConf jobConf = new JobConf();
        jobConf.set("pig.sortOrder", ObjectSerializer.serialize(new boolean[] {false}));
        comparator.setConf(jobConf);
        int res = compareHelper(prototype, t, comparator);
        assertEquals(-1 * Math.signum(prototype.compareTo(t)), Math.signum(res), 0);
        assertTrue(res > 0);
        jobConf.set("pig.sortOrder", ObjectSerializer.serialize(new boolean[] {true,true,false,true,true,true,true,true,true}));
        comparator.setConf(jobConf);
        res = compareHelper(prototype, t, comparator);
        assertEquals(-1 * Math.signum(prototype.compareTo(t)), Math.signum(res), 0);
        assertTrue(res > 0);
    }

    private Tuple getRandomTuple(Random rand) throws IOException {
        int pos = rand.nextInt(list.size());
        Tuple t = tf.newTuple(list);
        switch (pos) {
        case 0:
            t.set(pos, rand.nextFloat());
            break;
        case 1:
            t.set(pos, rand.nextInt());
            break;
        case 2:
            t.set(pos, rand.nextDouble());
            break;
        case 3:
            t.set(pos, rand.nextLong());
            break;
        case 4:
            t.set(pos, (byte) rand.nextInt());
            break;
        case 5:
            t.set(pos, rand.nextBoolean());
            break;
        case 6:
            byte[] ba = new byte[3];
            rand.nextBytes(ba);
            t.set(pos, new DataByteArray(ba));
            break;
        case 7:
            int length = rand.nextInt(15);
            String s = randomString(length, rand);
            t.set(pos, s);
            break;
        case 8:
            length = rand.nextInt(6);
            t.set(pos, getRandomTuple(rand));
            break;
        case 9:
            t.set(pos, new DateTime(rand.nextLong()));
            break;
        default:
        }
        return t;
    }

    private int compareHelper(NullableTuple t1, NullableTuple t2, RawComparator comparator) throws IOException {
        t1.write(dos1);
        t2.write(dos2);
        byte[] b1 = baos1.toByteArray();
        byte[] b2 = baos2.toByteArray();
        baos1.reset();
        baos2.reset();
        return comparator.compare(b1, 0, b1.length, b2, 0, b2.length);
    }

    private static final String AB = "0123456789abcdefghijklmnopqrstuwxyz!?-_ ";

    private String randomString(int length, Random rand) {
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++)
            sb.append(AB.charAt(rand.nextInt(AB.length())));
        return sb.toString();
    }

    public static void main(String[] args) throws Exception {
        long before, after;
        Random rand = new Random(SEED);
        TestPigTupleRawComparator test = new TestPigTupleRawComparator();
        test.setUp();
        byte[][] toCompare1 = new byte[TUPLE_NUMBER][];
        byte[][] toCompare2 = new byte[TUPLE_NUMBER][];
        NullableTuple t;
        for (int i = 0; i < TUPLE_NUMBER; i++) {
            t = new NullableTuple(test.getRandomTuple(rand));
            t.write(test.dos1);
            toCompare1[i] = test.baos1.toByteArray();
        }
        for (int i = 0; i < TUPLE_NUMBER; i++) {
            t = new NullableTuple(test.getRandomTuple(rand));
            t.write(test.dos2);
            toCompare2[i] = test.baos2.toByteArray();
        }

        before = System.currentTimeMillis();
        for (int loop = 0; loop < TIMES; loop++) {
            for (int i = 0; i < TUPLE_NUMBER; i++) {
                test.comparator.compare(toCompare1[i], 0, toCompare1[i].length, toCompare2[i], 0, toCompare2[i].length);
            }
        }
        after = System.currentTimeMillis();
        System.out.println("Raw Version - elapsed time: " + Long.toString(after - before) + " milliseconds");

        before = System.currentTimeMillis();
        for (int loop = 0; loop < TIMES; loop++) {
            for (int i = 0; i < TUPLE_NUMBER; i++) {
                test.oldComparator.compare(toCompare1[i], 0, toCompare1[i].length, toCompare2[i], 0,
                        toCompare2[i].length);
            }
        }
        after = System.currentTimeMillis();
        System.out.println("Old Version - elapsed time: " + Long.toString(after - before) + " milliseconds");
    }
}

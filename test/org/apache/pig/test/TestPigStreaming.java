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
import java.util.ArrayList;
import java.util.TreeMap;
import java.util.List;
import java.util.Map;

import junit.framework.TestCase;

import org.apache.pig.builtin.PigStreaming;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DefaultBagFactory;
import org.apache.pig.data.DefaultTupleFactory;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;


@RunWith(JUnit4.class)
public class TestPigStreaming extends TestCase {
    PigStreaming ps = new PigStreaming();
    TupleFactory tf = TupleFactory.getInstance();

    @Test
    public void testSerialize__emptyTuple() throws IOException {
        Tuple t = tf.newTuple(0);
        byte[] expectedOutput = "".getBytes();
        byte[] output = ps.serialize(t);
        Assert.assertArrayEquals(expectedOutput, output);
    }
 
    @Test
    public void testSerialize__string() throws IOException {
        Tuple t = tf.newTuple(1);
        t.set(0, "1234");
        byte[] expectedOutput = "1234\n".getBytes();
        byte[] output = ps.serialize(t);
        Assert.assertArrayEquals(expectedOutput, output);
    }

    @Test
    public void testSerialize__emptyString() throws IOException {
        Tuple t = tf.newTuple(1);
        t.set(0, "");
        byte[] expectedOutput = "\n".getBytes();
        byte[] output = ps.serialize(t);
        Assert.assertArrayEquals(expectedOutput, output);
    }

    @Test
    public void testSerialize__nullString() throws IOException {
        Tuple t = tf.newTuple(1);
        t.set(0, null);
        byte[] expectedOutput = "\n".getBytes();
        byte[] output = ps.serialize(t);
        Assert.assertArrayEquals(expectedOutput, output);
    }

    @Test
    public void testSerialize__multifieldTuple() throws IOException {
        Tuple t = tf.newTuple(2);
        t.set(0, 1234);
        t.set(1, "sam");
        byte[] expectedOutput = "1234\tsam\n".getBytes();
        byte[] output = ps.serialize(t);
        Assert.assertArrayEquals(expectedOutput, output);
    }

    @Test
    public void testSerialize__multifieldTupleWithNull() throws IOException {
        Tuple t = tf.newTuple(3);
        t.set(0, 1234);
        t.set(1, "sam");
        t.set(2, null);
        byte[] expectedOutput = "1234\tsam\t\n".getBytes();
        byte[] output = ps.serialize(t);
        Assert.assertArrayEquals(expectedOutput, output);
    }

    @Test
    public void testSerialize__nestedTuple() throws IOException {
        Tuple t = tf.newTuple(2);
        Tuple nestedTuple = tf.newTuple(2);
        nestedTuple.set(0, "sam");
        nestedTuple.set(1, "oes");
        t.set(0, nestedTuple);
        t.set(1, "basil");
        byte[] expectedOutput = "(sam,oes)\tbasil\n".getBytes();
        byte[] output = ps.serialize(t);
        Assert.assertArrayEquals(expectedOutput, output);
    }

    @Test
    public void testSerialize__bag() throws IOException {
        Tuple t = tf.newTuple(1);
        Tuple t1 = tf.newTuple(2);
        Tuple t2 = tf.newTuple(2);
        List<Tuple> bagTuples = new ArrayList<Tuple>();
        bagTuples.add(t1);
        bagTuples.add(t2);
        t1.set(0, "A");
        t1.set(1, "B");
        t2.set(0, 1);
        t2.set(1, 2);
        DataBag b = DefaultBagFactory.getInstance().newDefaultBag(bagTuples);
        t.set(0,b);
        byte[] expectedOutput = "{(A,B),(1,2)}\n".getBytes();
        byte[] output = ps.serialize(t);
        Assert.assertArrayEquals(expectedOutput, output);
    }

    @Test
    public void testSerialize__map() throws IOException {
        Tuple t = tf.newTuple(1);
        Map<String, String> m = new TreeMap<String, String>();
        m.put("A", "B");
        m.put("C", "D");
        t.set(0,m);
        byte[] expectedOutput = "[A#B,C#D]\n".getBytes();
        byte[] output = ps.serialize(t);
        Assert.assertArrayEquals(expectedOutput, output);
    }

    @Test
    public void testSerialize__complex_map() throws IOException {
        Tuple t = tf.newTuple(1);
        Map<String, Object> inner_map = new TreeMap<String, Object>();
        inner_map.put("A", 1);
        inner_map.put("B", "E");
        
        Map<String, Object> outer_map = new TreeMap<String, Object>();
        outer_map.put("C", "F");
        outer_map.put("D", inner_map);
        
        t.set(0,outer_map);
        byte[] expectedOutput = "[C#F,D#[A#1,B#E]]\n".getBytes();
        byte[] output = ps.serialize(t);
        Assert.assertArrayEquals(expectedOutput, output);
    }

    @Test
    public void testSerialize__allDataTypes() throws IOException {
        Tuple t = tf.newTuple(8);
        t.set(0, null);
        t.set(1, true);
        t.set(2, 2);
        t.set(3, 3L);
        t.set(4, 4.0f);
        t.set(5, 5.0d);
        t.set(6, new DataByteArray("six"));
        t.set(7, "seven");
        byte[] expectedOutput = "\ttrue\t2\t3\t4.0\t5.0\tsix\tseven\n".getBytes();
        byte[] output = ps.serialize(t);
        Assert.assertArrayEquals(expectedOutput, output);
    }
    
    @Test
    public void testDeserialize__string() throws IOException {
        byte[] input = "1234".getBytes();
        Tuple out = ps.deserialize(input);
        Assert.assertEquals(tf.newTuple(new DataByteArray("1234")), out);
    }
    
    @Test
    public void testDeserialize__emptyString() throws IOException {
        byte[] input = "".getBytes();
        Tuple expectedOutput = tf.newTuple(1);
        expectedOutput.set(0, null);
      
        Tuple out = ps.deserialize(input);
        
        Assert.assertEquals(expectedOutput, out);
    }
}

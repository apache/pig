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
import static org.junit.Assert.assertTrue;

import org.apache.pig.builtin.Bloom;
import org.apache.pig.builtin.BuildBloom;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Test;

/**
 * This class unit tests the built in UDFs BuildBloom and Bloom.
 */
public class TestBloom {
    static class TestBuildBloom extends BuildBloom {
        TestBuildBloom(String numElements, String desiredFalsePositive) {
            super("jenkins", numElements, desiredFalsePositive);
        }

        int getSize() {
            return vSize;
        }

        int getNumHash() {
            return numHash;
        }
    }

    @Test
    public void testSizeCalc() throws Exception {
        TestBuildBloom tbb = new TestBuildBloom("1000", "0.01");
        assertEquals(9585, tbb.getSize());
        assertEquals(6, tbb.getNumHash());
        tbb = new TestBuildBloom("1000000", "0.01");
        assertEquals(9585058, tbb.getSize());
        assertEquals(6, tbb.getNumHash());
        tbb = new TestBuildBloom("1000", "0.0001");
        assertEquals(19170, tbb.getSize());
        assertEquals(13, tbb.getNumHash());
        tbb = new TestBuildBloom("1000000", "0.00001");
        assertEquals(23962645, tbb.getSize());
        assertEquals(16, tbb.getNumHash());
    }

    @Test(expected = RuntimeException.class)
    public void testBadHash() throws Exception {
        String size = "100";
        String numHash = "3";
        String hashFunc = "nosuchhash";
        try {
            BuildBloom bb = new BuildBloom(hashFunc, "fixed", size, numHash);
        } catch (RuntimeException re) {
            assertTrue(re.getMessage().contains("Unknown hash type"));
            throw re;
        }
    }

    @Test
    public void testFuncNames() throws Exception {
        String size = "100";
        String numHash = "3";
        String hashFunc = "JENKINS_HASH";
        BuildBloom bb = new BuildBloom(hashFunc, "fixed", size, numHash);
        assertEquals("org.apache.pig.builtin.BuildBloom$Initial",
                bb.getInitial());
        assertEquals("org.apache.pig.builtin.BuildBloom$Intermediate",
                bb.getIntermed());
        assertEquals("org.apache.pig.builtin.BuildBloom$Final",
                bb.getFinal());
    }

    @Test
    public void testMap() throws Exception {
        String size = "100";
        String numHash = "3";
        String hashFunc = "JENKINS";
        TupleFactory tf = TupleFactory.getInstance();
        BagFactory bf = BagFactory.getInstance();

        Tuple t = tf.newTuple(1);
        t.set(0, 1);
        DataBag b = bf.newDefaultBag();
        b.add(t);
        Tuple input = tf.newTuple(b);

        BuildBloom.Initial map =
                new BuildBloom.Initial(hashFunc, "fixed", size, numHash);
        t = map.exec(input);

        Bloom bloom = new Bloom("bla");
        bloom.setFilter((DataByteArray)t.get(0));

        // Test that everything we put in passes.
        Tuple t1 = tf.newTuple(1);
        t1.set(0, 1);
        assertTrue(bloom.exec(t1));

        // A few that don't pass
        for (int i = 100; i < 10; i++) {
            Tuple t2 = tf.newTuple(1);
            t2.set(0, i);
            assertFalse(bloom.exec(t2));
        }
    }

    @Test
    public void testCombiner() throws Exception {
        String size = "100";
        String numHash = "3";
        String hashFunc = "jenkins";
        TupleFactory tf = TupleFactory.getInstance();
        BagFactory bf = BagFactory.getInstance();

        DataBag combinerBag = bf.newDefaultBag();
        for (int j = 0; j < 3; j++) { // map loop
            Tuple t = tf.newTuple(1);
            t.set(0, 10 + j);
            DataBag mapBag = bf.newDefaultBag();
            mapBag.add(t);
            Tuple input = tf.newTuple(mapBag);
            BuildBloom.Initial map =
                    new BuildBloom.Initial(hashFunc, "fixed", size, numHash);
            combinerBag.add(map.exec(input));
        }
        Tuple t = tf.newTuple(1);
        t.set(0, combinerBag);
        BuildBloom.Intermediate combiner =
                new BuildBloom.Intermediate(hashFunc, "fixed", size, numHash);
        t = combiner.exec(t);

        Bloom bloom = new Bloom("bla");
        bloom.setFilter((DataByteArray)t.get(0));

        // Test that everything we put in passes.
        for (int j = 0; j < 3; j++) {
            Tuple t1 = tf.newTuple(1);
            t1.set(0, 10 + j);
            assertTrue(bloom.exec(t1));
        }

        // A few that don't pass
        for (int i = 100; i < 10; i++) {
            Tuple t2 = tf.newTuple(1);
            t2.set(0, i);
            assertFalse(bloom.exec(t2));
        }
    }

    @Test
    public void testSingleKey() throws Exception {
        String size = "100";
        String numHash = "3";
        String hashFunc = "MURMUR";
        TupleFactory tf = TupleFactory.getInstance();
        BagFactory bf = BagFactory.getInstance();

        DataBag reducerBag = bf.newDefaultBag();
        for (int i = 0; i < 3; i++) { // combiner loop
            DataBag combinerBag = bf.newDefaultBag();
            for (int j = 0; j < 3; j++) { // map loop
                Tuple t = tf.newTuple(1);
                t.set(0, i * 10 + j);
                DataBag mapBag = bf.newDefaultBag();
                mapBag.add(t);
                Tuple input = tf.newTuple(mapBag);
                BuildBloom.Initial map =
                        new BuildBloom.Initial(hashFunc, "fixed", size, numHash);
                combinerBag.add(map.exec(input));
            }
            Tuple t = tf.newTuple(1);
            t.set(0, combinerBag);
            BuildBloom.Intermediate combiner =
                    new BuildBloom.Intermediate(hashFunc, "fixed", size, numHash);
            reducerBag.add(combiner.exec(t));
        }

        Tuple t = tf.newTuple(1);
        t.set(0, reducerBag);
        BuildBloom.Final reducer =
                new BuildBloom.Final(hashFunc, "fixed", size, numHash);
        DataByteArray dba = reducer.exec(t);

        Bloom bloom = new Bloom("bla");
        bloom.setFilter(dba);

        // Test that everything we put in passes.
        for (int i = 0; i < 3; i++) {
            for (int j = 0; j < 3; j++) {
                Tuple t1 = tf.newTuple(1);
                t1.set(0, i * 10 + j);
                assertTrue(bloom.exec(t1));
            }
        }

        // A few that don't pass
        for (int i = 100; i < 10; i++) {
            Tuple t1 = tf.newTuple(1);
            t1.set(0, i);
            assertFalse(bloom.exec(t1));
        }
    }

    @Test
    public void testMultiKey() throws Exception {
        String numElements = "10";
        String falsePositive = "0.001";
        String hashFunc = "murmur";
        TupleFactory tf = TupleFactory.getInstance();
        BagFactory bf = BagFactory.getInstance();

        String[][] strs = {
                        { "fred", "joe", "bob" },
                        { "mary", "sally", "jane" },
                        { "fido", "spot", "fluffly" } };

        DataBag reducerBag = bf.newDefaultBag();
        for (int i = 0; i < 3; i++) { // combiner loop
            DataBag combinerBag = bf.newDefaultBag();
            for (int j = 0; j < 3; j++) { // map loop
                Tuple t = tf.newTuple(2);
                t.set(0, i * 10 + j);
                t.set(1, strs[i][j]);
                DataBag mapBag = bf.newDefaultBag();
                mapBag.add(t);
                Tuple input = tf.newTuple(mapBag);
                BuildBloom.Initial map =
                        new BuildBloom.Initial(hashFunc, numElements,
                                falsePositive);
                combinerBag.add(map.exec(input));
            }
            Tuple t = tf.newTuple(1);
            t.set(0, combinerBag);
            BuildBloom.Intermediate combiner =
                    new BuildBloom.Intermediate(hashFunc, numElements,
                            falsePositive);
            reducerBag.add(combiner.exec(t));
        }

        Tuple t = tf.newTuple(1);
        t.set(0, reducerBag);
        BuildBloom.Final reducer =
                new BuildBloom.Final(hashFunc, numElements, falsePositive);
        DataByteArray dba = reducer.exec(t);

        Bloom bloom = new Bloom("bla");
        bloom.setFilter(dba);

        // Test that everything we put in passes.
        for (int i = 0; i < 3; i++) {
            for (int j = 0; j < 3; j++) {
                Tuple t1 = tf.newTuple(2);
                t1.set(0, i * 10 + j);
                t1.set(1, strs[i][j]);
                assertTrue(bloom.exec(t1));
            }
        }

        // A few that don't pass
        for (int i = 100; i < 10; i++) {
            Tuple t1 = tf.newTuple(2);
            t1.set(0, i);
            t1.set(1, "ichabod");
            assertFalse(bloom.exec(t1));
        }
    }
}

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

import java.util.*;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;

import org.junit.Test;
import org.apache.pig.data.*;
import org.apache.pig.impl.util.Spillable;


/**
 * This class will exercise the basic Pig data model and members. It tests for proper behavior in
 * assigment and comparision, as well as function application.
 * 
 * @author dnm
 */
public class TestDataBag extends junit.framework.TestCase {

    private Random rand = new Random();

    private class TestMemoryManager {
        ArrayList<Spillable> mManagedObjects = new ArrayList<Spillable>();

        public void register(Spillable s) {
            mManagedObjects.add(s);
        }

        public void forceSpill() throws IOException {
            Iterator<Spillable> i = mManagedObjects.iterator();
            while (i.hasNext()) i.next().spill();
        }
    }

    // Need to override the regular bag factory so I can register with my local
    // memory manager.
    private class LocalBagFactory {
        TestMemoryManager mMemMgr;

        public LocalBagFactory(TestMemoryManager mgr) {
            mMemMgr = mgr;
        }

        public DataBag newDefaultBag() {
            DataBag bag = new DefaultDataBag();
            mMemMgr.register(bag);
            return bag;
        }

        public DataBag newSortedBag(Comparator<Tuple> comp) {
            DataBag bag = new SortedDataBag(comp);
            mMemMgr.register(bag);
            return bag;
        }

        public DataBag newDistinctBag() {
            DataBag bag = new DistinctDataBag();
            mMemMgr.register(bag);
            return bag;
        }
    }

    @Override
    protected void tearDown() throws Exception {
    	BagFactory.resetSelf();
        System.clearProperty("pig.data.bag.factory.name");
        System.clearProperty("pig.data.bag.factory.jar");
    }

    // Test reading and writing default from memory, no spills.
    @Test
    public void testDefaultInMemory() throws Exception {
        TestMemoryManager mgr = new TestMemoryManager();
        LocalBagFactory factory = new LocalBagFactory(mgr);
        DataBag b = factory.newDefaultBag();
        ArrayList<Tuple> rightAnswer = new ArrayList<Tuple>(10);

        // Write tuples into both
        for (int i = 0; i < 10; i++) {
            Tuple t = TupleFactory.getInstance().newTuple(new Integer(i));
            b.add(t);
            rightAnswer.add(t);
        }

        // Read tuples back, hopefully they come out in the same order.
        Iterator<Tuple> bIter = b.iterator();
        Iterator<Tuple> rIter = rightAnswer.iterator();

        while (rIter.hasNext()) {
            assertTrue("bag ran out of tuples before answer", bIter.hasNext());
            assertEquals("tuples should be the same", bIter.next(), rIter.next());
        }

        assertFalse("right answer ran out of tuples before the bag",
            bIter.hasNext());
    }

   // Test reading and writing default from file with one spill
    @Test
    public void testDefaultSingleSpill() throws Exception {
        TestMemoryManager mgr = new TestMemoryManager();
        LocalBagFactory factory = new LocalBagFactory(mgr);
        DataBag b = factory.newDefaultBag();
        ArrayList<Tuple> rightAnswer = new ArrayList<Tuple>(10);

        // Write tuples into both
        for (int i = 0; i < 10; i++) {
            Tuple t = TupleFactory.getInstance().newTuple(new Integer(i));
            b.add(t);
            rightAnswer.add(t);
        }
        mgr.forceSpill();

        // Read tuples back, hopefully they come out in the same order.
        Iterator<Tuple> bIter = b.iterator();
        Iterator<Tuple> rIter = rightAnswer.iterator();

        while (rIter.hasNext()) {
            assertTrue("bag ran out of tuples before answer", bIter.hasNext());
            assertEquals("tuples should be the same", bIter.next(), rIter.next());
        }

        assertFalse("right answer ran out of tuples before the bag",
            bIter.hasNext());
    }

   // Test reading and writing default from file with three spills
    @Test
    public void testDefaultTripleSpill() throws Exception {
        TestMemoryManager mgr = new TestMemoryManager();
        LocalBagFactory factory = new LocalBagFactory(mgr);
        DataBag b = factory.newDefaultBag();
        ArrayList<Tuple> rightAnswer = new ArrayList<Tuple>(30);

        // Write tuples into both
        for (int j = 0; j < 3; j++) {
            for (int i = 0; i < 10; i++) {
                Tuple t = TupleFactory.getInstance().newTuple(new Integer(i));
                b.add(t);
                rightAnswer.add(t);
            }
            mgr.forceSpill();
        }

        // Read tuples back, hopefully they come out in the same order.
        Iterator<Tuple> bIter = b.iterator();
        Iterator<Tuple> rIter = rightAnswer.iterator();

        while (rIter.hasNext()) {
            assertTrue("bag ran out of tuples before answer", bIter.hasNext());
            assertEquals("tuples should be the same", bIter.next(), rIter.next());
        }

        assertFalse("right answer ran out of tuples before the bag",
            bIter.hasNext());
    }

    @Test
    public void testTypedTupleSpill() throws Exception {
        TestMemoryManager mgr = new TestMemoryManager();
        LocalBagFactory factory = new LocalBagFactory(mgr);
        DataBag b = factory.newDefaultBag();
        ArrayList<Tuple> rightAnswer = new ArrayList<Tuple>(30);

        // Write tuples into both
        for (int j = 0; j < 3; j++) {
            for (int i = 0; i < 10; i++) {
                Tuple t = TupleFactory.getInstance().newTuple(new Integer(i));
                b.add(t);
                rightAnswer.add(t);
            }
            mgr.forceSpill();
        }

        // Read tuples back, hopefully they come out in the same order.
        Iterator<Tuple> bIter = b.iterator();
        Iterator<Tuple> rIter = rightAnswer.iterator();

        while (rIter.hasNext()) {
            assertTrue("bag ran out of tuples before answer", bIter.hasNext());
            assertEquals("tuples should be the same", bIter.next(), rIter.next());
        }

        assertFalse("right answer ran out of tuples before the bag",
            bIter.hasNext());
    }

    // Test reading with some in file, some in memory.
    @Test
    public void testDefaultInMemInFile() throws Exception {
        TestMemoryManager mgr = new TestMemoryManager();
        LocalBagFactory factory = new LocalBagFactory(mgr);
        DataBag b = factory.newDefaultBag();
        ArrayList<Tuple> rightAnswer = new ArrayList<Tuple>(20);

        // Write tuples into both
        for (int i = 0; i < 10; i++) {
            Tuple t = TupleFactory.getInstance().newTuple(new Integer(i));
            b.add(t);
            rightAnswer.add(t);
        }
        mgr.forceSpill();

        for (int i = 0; i < 10; i++) {
            Tuple t = TupleFactory.getInstance().newTuple(new Integer(i));
            b.add(t);
            rightAnswer.add(t);
        }

        // Read tuples back, hopefully they come out in the same order.
        Iterator<Tuple> bIter = b.iterator();
        Iterator<Tuple> rIter = rightAnswer.iterator();

        while (rIter.hasNext()) {
            assertTrue("bag ran out of tuples before answer", bIter.hasNext());
            assertEquals("tuples should be the same", bIter.next(), rIter.next());
        }

        assertFalse("right answer ran out of tuples before the bag",
            bIter.hasNext());
    }

   // Test reading with a spill happening in the middle of the read.
    @Test
    public void testDefaultSpillDuringRead() throws Exception {
        TestMemoryManager mgr = new TestMemoryManager();
        LocalBagFactory factory = new LocalBagFactory(mgr);
        DataBag b = factory.newDefaultBag();
        ArrayList<Tuple> rightAnswer = new ArrayList<Tuple>(20);

        // Write tuples into both
        for (int i = 0; i < 10; i++) {
            Tuple t = TupleFactory.getInstance().newTuple(new Integer(i));
            b.add(t);
            rightAnswer.add(t);
        }
        mgr.forceSpill();

        for (int i = 0; i < 10; i++) {
            Tuple t = TupleFactory.getInstance().newTuple(new Integer(i));
            b.add(t);
            rightAnswer.add(t);
        }

        // Read tuples back, hopefully they come out in the same order.
        Iterator<Tuple> bIter = b.iterator();
        Iterator<Tuple> rIter = rightAnswer.iterator();

        for (int i = 0; i < 15; i++) {
            assertTrue("bag ran out of tuples before answer", bIter.hasNext());
            assertEquals("tuples should be the same", bIter.next(), rIter.next());
        }

        mgr.forceSpill();

        while (rIter.hasNext()) {
            assertTrue("bag ran out of tuples before answer", bIter.hasNext());
            assertEquals("tuples should be the same", bIter.next(), rIter.next());
        }

        assertFalse("right answer ran out of tuples before the bag",
            bIter.hasNext());
    }

    // Test reading and writing sorted from memory, no spills.
    @Test
    public void testSortedInMemory() throws Exception {
        TestMemoryManager mgr = new TestMemoryManager();
        LocalBagFactory factory = new LocalBagFactory(mgr);
        DataBag b = factory.newSortedBag(null);
        PriorityQueue<Tuple> rightAnswer = new PriorityQueue<Tuple>(10);

        // Write tuples into both
        for (int i = 0; i < 10; i++) {
            Tuple t = TupleFactory.getInstance().newTuple(new Integer(rand.nextInt()));
            b.add(t);
            rightAnswer.add(t);
        }

        // Read tuples back, hopefully they come out in the same order.
        Iterator<Tuple> bIter = b.iterator();

        Tuple t;
        while ((t = rightAnswer.poll()) != null) {
            assertTrue("bag ran out of tuples before answer", bIter.hasNext());
            assertEquals("tuples should be the same", bIter.next(), t);
        }

        assertFalse("right answer ran out of tuples before the bag",
            bIter.hasNext());
    }

   // Test reading and writing default from file with one spill
    @Test
    public void testSortedSingleSpill() throws Exception {
        TestMemoryManager mgr = new TestMemoryManager();
        LocalBagFactory factory = new LocalBagFactory(mgr);
        DataBag b = factory.newSortedBag(null);
        PriorityQueue<Tuple> rightAnswer = new PriorityQueue<Tuple>(10);

        // Write tuples into both
        for (int i = 0; i < 10; i++) {
            Tuple t = TupleFactory.getInstance().newTuple(new Integer(rand.nextInt()));
            b.add(t);
            rightAnswer.add(t);
        }
        mgr.forceSpill();

        // Read tuples back, hopefully they come out in the same order.
        Iterator<Tuple> bIter = b.iterator();
        Tuple t;
        while ((t = rightAnswer.poll()) != null) {
            assertTrue("bag ran out of tuples before answer", bIter.hasNext());
            assertEquals("tuples should be the same", bIter.next(), t);
        }

        assertFalse("right answer ran out of tuples before the bag",
            bIter.hasNext());
    }

   // Test reading and writing default from file with three spills
    @Test
    public void testSortedTripleSpill() throws Exception {
        TestMemoryManager mgr = new TestMemoryManager();
        LocalBagFactory factory = new LocalBagFactory(mgr);
        DataBag b = factory.newSortedBag(null);
        PriorityQueue<Tuple> rightAnswer = new PriorityQueue<Tuple>(30);

        // Write tuples into both
        for (int j = 0; j < 3; j++) {
            for (int i = 0; i < 10; i++) {
                Tuple t = TupleFactory.getInstance().newTuple(new Integer(rand.nextInt()));
                b.add(t);
                rightAnswer.add(t);
            }
            mgr.forceSpill();
        }

        // Read tuples back, hopefully they come out in the same order.
        Iterator<Tuple> bIter = b.iterator();

        Tuple t;
        while ((t = rightAnswer.poll()) != null) {
            assertTrue("bag ran out of tuples before answer", bIter.hasNext());
            assertEquals("tuples should be the same", bIter.next(), t);
        }

        assertFalse("right answer ran out of tuples before the bag",
            bIter.hasNext());
    }

    // Test reading with some in file, some in memory.
    @Test
    public void testSortedInMemInFile() throws Exception {
        TestMemoryManager mgr = new TestMemoryManager();
        LocalBagFactory factory = new LocalBagFactory(mgr);
        DataBag b = factory.newSortedBag(null);
        PriorityQueue<Tuple> rightAnswer = new PriorityQueue<Tuple>(20);

        // Write tuples into both
        for (int i = 0; i < 10; i++) {
            Tuple t = TupleFactory.getInstance().newTuple(new Integer(rand.nextInt()));
            b.add(t);
            rightAnswer.add(t);
        }
        mgr.forceSpill();

        for (int i = 0; i < 10; i++) {
            Tuple t = TupleFactory.getInstance().newTuple(new Integer(rand.nextInt()));
            b.add(t);
            rightAnswer.add(t);
        }

        // Read tuples back, hopefully they come out in the same order.
        Iterator<Tuple> bIter = b.iterator();
        Tuple t;
        while ((t = rightAnswer.poll()) != null) {
            assertTrue("bag ran out of tuples before answer", bIter.hasNext());
            assertEquals("tuples should be the same", bIter.next(), t);
        }

        assertFalse("right answer ran out of tuples before the bag",
            bIter.hasNext());
    }

    // Test reading with a spill happening in the middle of the read.
    @Test
    public void testSortedSpillDuringRead() throws Exception {
        TestMemoryManager mgr = new TestMemoryManager();
        LocalBagFactory factory = new LocalBagFactory(mgr);
        DataBag b = factory.newSortedBag(null);
        PriorityQueue<Tuple> rightAnswer = new PriorityQueue<Tuple>(20);

        // Write tuples into both
        for (int i = 0; i < 10; i++) {
            Tuple t = TupleFactory.getInstance().newTuple(new Integer(rand.nextInt()));
            b.add(t);
            rightAnswer.add(t);
        }
        mgr.forceSpill();

        for (int i = 0; i < 10; i++) {
            Tuple t = TupleFactory.getInstance().newTuple(new Integer(rand.nextInt()));
            b.add(t);
            rightAnswer.add(t);
        }

        // Read tuples back, hopefully they come out in the same order.
        Iterator<Tuple> bIter = b.iterator();

        for (int i = 0; i < 15; i++) {
            assertTrue("bag ran out of tuples before answer", bIter.hasNext());
            assertEquals("tuples should be the same", bIter.next(), rightAnswer.poll());
        }

        mgr.forceSpill();

        Tuple t;
        while ((t = rightAnswer.poll()) != null) {
            assertTrue("bag ran out of tuples before answer", bIter.hasNext());
            assertEquals("tuples should be the same", bIter.next(), t);
        }

        assertFalse("right answer ran out of tuples before the bag",
            bIter.hasNext());
    }

    // Test reading with first spill happening in the middle of the read.
    @Test
    public void testSortedFirstSpillDuringRead() throws Exception {
        TestMemoryManager mgr = new TestMemoryManager();
        LocalBagFactory factory = new LocalBagFactory(mgr);
        DataBag b = factory.newSortedBag(null);
        PriorityQueue<Tuple> rightAnswer = new PriorityQueue<Tuple>(20);

        for (int i = 0; i < 10; i++) {
            Tuple t = TupleFactory.getInstance().newTuple(new Integer(rand.nextInt()));
            b.add(t);
            rightAnswer.add(t);
        }

        // Read tuples back, hopefully they come out in the same order.
        Iterator<Tuple> bIter = b.iterator();

        for (int i = 0; i < 5; i++) {
            assertTrue("bag ran out of tuples before answer", bIter.hasNext());
            assertEquals("tuples should be the same", bIter.next(), rightAnswer.poll());
        }

        mgr.forceSpill();

        Tuple t;
        while ((t = rightAnswer.poll()) != null) {
            assertTrue("bag ran out of tuples before answer", bIter.hasNext());
            assertEquals("tuples should be the same", bIter.next(), t);
        }

        assertFalse("right answer ran out of tuples before the bag",
            bIter.hasNext());
    }

   // Test reading and writing sorted file with so many spills it requires
   // premerge.
    @Test
    public void testSortedPreMerge() throws Exception {
        TestMemoryManager mgr = new TestMemoryManager();
        LocalBagFactory factory = new LocalBagFactory(mgr);
        DataBag b = factory.newSortedBag(null);
        PriorityQueue<Tuple> rightAnswer = new PriorityQueue<Tuple>(30);

        // Write tuples into both
        for (int j = 0; j < 373; j++) {
            for (int i = 0; i < 10; i++) {
                Tuple t = TupleFactory.getInstance().newTuple(new Integer(rand.nextInt()));
                b.add(t);
                rightAnswer.add(t);
            }
            mgr.forceSpill();
        }

        // Read tuples back, hopefully they come out in the same order.
        Iterator<Tuple> bIter = b.iterator();

        Tuple t;
        while ((t = rightAnswer.poll()) != null) {
            assertTrue("bag ran out of tuples before answer", bIter.hasNext());
            assertEquals("tuples should be the same", bIter.next(), t);
        }

        assertFalse("right answer ran out of tuples before the bag",
            bIter.hasNext());
    }

    // Test reading and writing distinct from memory, no spills.
    @Test
    public void testDistinctInMemory() throws Exception {
        TestMemoryManager mgr = new TestMemoryManager();
        LocalBagFactory factory = new LocalBagFactory(mgr);
        DataBag b = factory.newDistinctBag();
        TreeSet<Tuple> rightAnswer = new TreeSet<Tuple>();

        // Write tuples into both
        for (int i = 0; i < 50; i++) {
            Tuple t = TupleFactory.getInstance().newTuple(new Integer(rand.nextInt() % 5));
            b.add(t);
            rightAnswer.add(t);
        }

        // Read tuples back, hopefully they come out in the same order.
        Iterator<Tuple> bIter = b.iterator();
        Iterator<Tuple> rIter = rightAnswer.iterator();

        while (rIter.hasNext()) {
            assertTrue("bag ran out of tuples before answer", bIter.hasNext());
            assertEquals("tuples should be the same", bIter.next(), rIter.next());
        }

        assertFalse("right answer ran out of tuples before the bag",
            bIter.hasNext());
    }

   // Test reading and writing distinct from file with one spill
    @Test
    public void testDistinctSingleSpill() throws Exception {
        TestMemoryManager mgr = new TestMemoryManager();
        LocalBagFactory factory = new LocalBagFactory(mgr);
        DataBag b = factory.newDistinctBag();
        TreeSet<Tuple> rightAnswer = new TreeSet<Tuple>();

        // Write tuples into both
        for (int i = 0; i < 50; i++) {
            Tuple t = TupleFactory.getInstance().newTuple(new Integer(rand.nextInt() % 5));
            b.add(t);
            rightAnswer.add(t);
        }
        mgr.forceSpill();

        // Read tuples back, hopefully they come out in the same order.
        Iterator<Tuple> bIter = b.iterator();
        Iterator<Tuple> rIter = rightAnswer.iterator();

        while (rIter.hasNext()) {
            assertTrue("bag ran out of tuples before answer", bIter.hasNext());
            assertEquals("tuples should be the same", bIter.next(), rIter.next());
        }

        assertFalse("right answer ran out of tuples before the bag",
            bIter.hasNext());
    }

   // Test reading and writing distinct from file with three spills
    @Test
    public void testDistinctTripleSpill() throws Exception {
        TestMemoryManager mgr = new TestMemoryManager();
        LocalBagFactory factory = new LocalBagFactory(mgr);
        DataBag b = factory.newDistinctBag();
        TreeSet<Tuple> rightAnswer = new TreeSet<Tuple>();

        // Write tuples into both
        for (int j = 0; j < 3; j++) {
            for (int i = 0; i < 50; i++) {
                Tuple t = TupleFactory.getInstance().newTuple(new Integer(rand.nextInt() % 5));
                b.add(t);
                rightAnswer.add(t);
            }
            mgr.forceSpill();
        }
        
        assertEquals("Size of distinct data bag is incorrect", b.size(), rightAnswer.size());

        // Read tuples back, hopefully they come out in the same order.
        Iterator<Tuple> bIter = b.iterator();
        Iterator<Tuple> rIter = rightAnswer.iterator();

        while (rIter.hasNext()) {
            assertTrue("bag ran out of tuples before answer", bIter.hasNext());
            assertEquals("tuples should be the same", bIter.next(), rIter.next());
        }

        assertFalse("right answer ran out of tuples before the bag",
            bIter.hasNext());
    }

    // Test reading with some in file, some in memory.
    @Test
    public void testDistinctInMemInFile() throws Exception {
        TestMemoryManager mgr = new TestMemoryManager();
        LocalBagFactory factory = new LocalBagFactory(mgr);
        DataBag b = factory.newDistinctBag();
        TreeSet<Tuple> rightAnswer = new TreeSet<Tuple>();

        // Write tuples into both
        for (int i = 0; i < 50; i++) {
            Tuple t = TupleFactory.getInstance().newTuple(new Integer(rand.nextInt() % 5));
            b.add(t);
            rightAnswer.add(t);
        }
        mgr.forceSpill();

        for (int i = 0; i < 50; i++) {
            Tuple t = TupleFactory.getInstance().newTuple(new Integer(i));
            b.add(t);
            rightAnswer.add(t);
        }

        // Read tuples back, hopefully they come out in the same order.
        Iterator<Tuple> bIter = b.iterator();
        Iterator<Tuple> rIter = rightAnswer.iterator();

        while (rIter.hasNext()) {
            assertTrue("bag ran out of tuples before answer", bIter.hasNext());
            assertEquals("tuples should be the same", bIter.next(), rIter.next());
        }

        assertFalse("right answer ran out of tuples before the bag",
            bIter.hasNext());
    }

   // Test reading with a spill happening in the middle of the read.
    @Test
    public void testDistinctSpillDuringRead() throws Exception {
        TestMemoryManager mgr = new TestMemoryManager();
        LocalBagFactory factory = new LocalBagFactory(mgr);
        DataBag b = factory.newDistinctBag();
        TreeSet<Tuple> rightAnswer = new TreeSet<Tuple>();

        // Write tuples into both
        for (int i = 0; i < 50; i++) {
            Tuple t = TupleFactory.getInstance().newTuple(new Integer(rand.nextInt() % 5));
            b.add(t);
            rightAnswer.add(t);
        }
        mgr.forceSpill();

        for (int i = 0; i < 50; i++) {
            Tuple t = TupleFactory.getInstance().newTuple(new Integer(i));
            b.add(t);
            rightAnswer.add(t);
        }

        // Read tuples back, hopefully they come out in the same order.
        Iterator<Tuple> bIter = b.iterator();
        Iterator<Tuple> rIter = rightAnswer.iterator();

        for (int i = 0; i < 5; i++) {
            assertTrue("bag ran out of tuples before answer", bIter.hasNext());
            assertEquals("tuples should be the same", bIter.next(), rIter.next());
        }

        mgr.forceSpill();

        while (rIter.hasNext()) {
            assertTrue("bag ran out of tuples before answer", bIter.hasNext());
            assertEquals("tuples should be the same", bIter.next(), rIter.next());
        }

        assertFalse("right answer ran out of tuples before the bag",
            bIter.hasNext());
    }

   // Test reading and writing distinct from file with enough spills to
   // force a pre-merge
    @Test
    public void testDistinctPreMerge() throws Exception {
        TestMemoryManager mgr = new TestMemoryManager();
        LocalBagFactory factory = new LocalBagFactory(mgr);
        DataBag b = factory.newDistinctBag();
        TreeSet<Tuple> rightAnswer = new TreeSet<Tuple>();

        // Write tuples into both
        for (int j = 0; j < 321; j++) {
            for (int i = 0; i < 50; i++) {
                Tuple t = TupleFactory.getInstance().newTuple(new Integer(rand.nextInt() % 5));
                b.add(t);
                rightAnswer.add(t);
            }
            mgr.forceSpill();
        }

        // Read tuples back, hopefully they come out in the same order.
        Iterator<Tuple> bIter = b.iterator();
        Iterator<Tuple> rIter = rightAnswer.iterator();

        while (rIter.hasNext()) {
            assertTrue("bag ran out of tuples before answer", bIter.hasNext());
            assertEquals("tuples should be the same", bIter.next(), rIter.next());
        }

        assertFalse("right answer ran out of tuples before the bag",
            bIter.hasNext());
    }

    // Test the default bag factory.
    @Test
    public void testDefaultBagFactory() throws Exception {
        BagFactory f = BagFactory.getInstance();
       
        DataBag bag = f.newDefaultBag();
        DataBag sorted = f.newSortedBag(null);
        DataBag distinct = f.newDistinctBag();

        assertTrue("Expected a default bag", (bag instanceof DefaultDataBag));
        assertTrue("Expected a sorted bag", (sorted instanceof SortedDataBag));
        assertTrue("Expected a distinct bag", (distinct instanceof DistinctDataBag));         
    }

    @Test
    public void testProvidedBagFactory() throws Exception {
        // Test bogus factory name.
        BagFactory.resetSelf();
        System.setProperty("pig.data.bag.factory.name", "no such class");
        System.setProperty("pig.data.bag.factory.jar", "file:./pig.jar");
        boolean caughtIt = false;
        try {
            BagFactory f = BagFactory.getInstance();
        } catch (RuntimeException re) {
            assertEquals("Expected Unable to instantiate message",
                "Unable to instantiate bag factory no such class",
                re.getMessage());
            caughtIt = true;
        }
        assertTrue("Expected to catch exception", caughtIt);

        // Test factory that isn't a BagFactory
        BagFactory.resetSelf();
        System.setProperty("pig.data.bag.factory.name",
            "org.apache.pig.test.TestDataBag");
        System.setProperty("pig.data.bag.factory.jar",
            "file:./pig.jar");
        caughtIt = false;
        try {
            BagFactory f = BagFactory.getInstance();
        } catch (RuntimeException re) {
            assertEquals("Expected does not extend BagFactory message", 
                "Provided factory org.apache.pig.test.TestDataBag does not extend BagFactory!",
                re.getMessage());
            caughtIt = true;
        }
        assertTrue("Expected to catch exception", caughtIt);

        // Test that we can instantiate our test factory.
        BagFactory.resetSelf();
        System.setProperty("pig.data.bag.factory.name",
            "org.apache.pig.test.NonDefaultBagFactory");
        System.setProperty("pig.data.bag.factory.jar", "file:./pig.jar");
        BagFactory f = BagFactory.getInstance();
        DataBag b = f.newDefaultBag();
        b = f.newSortedBag(null);
        b = f.newDistinctBag();

        BagFactory.resetSelf();
    }
    
    @Test
    public void testNonSpillableDataBagEquals1() throws Exception {
        String[][] tupleContents = new String[][] {{"a", "b"},{"c", "d" }, { "e", "f"} };
        NonSpillableDataBag bg1 = new NonSpillableDataBag();
        for (int i = 0; i < tupleContents.length; i++) {
            bg1.add(Util.createTuple(tupleContents[i]));
        }
        NonSpillableDataBag bg2 = new NonSpillableDataBag();
        for (int i = 0; i < tupleContents.length; i++) {
            bg2.add(Util.createTuple(tupleContents[i]));
        }
        assertEquals(bg1, bg2);
    }
    
    @Test
    public void testNonSpillableDataBagEquals2() throws Exception {
        String[][] tupleContents = new String[][] {{"a", "b"},{"c", "d" }, { "e", "f"} };
        NonSpillableDataBag bg1 = new NonSpillableDataBag();
        for (int i = 0; i < tupleContents.length; i++) {
            bg1.add(Util.createTuple(tupleContents[i]));
        }
        tupleContents = new String[][] {{"c", "d" }, {"a", "b"},{ "e", "f"} };
        NonSpillableDataBag bg2 = new NonSpillableDataBag();
        for (int i = 0; i < tupleContents.length; i++) {
            bg2.add(Util.createTuple(tupleContents[i]));
        }
        assertEquals(bg1, bg2);
    }
    
    @Test
    public void testDefaultDataBagEquals1() throws Exception {
        String[][] tupleContents = new String[][] {{"a", "b"},{"c", "d" }, { "e", "f"} };
        TestMemoryManager mgr = new TestMemoryManager();
        LocalBagFactory factory = new LocalBagFactory(mgr);
        DataBag bg1 = factory.newDefaultBag();
        for (int i = 0; i < tupleContents.length; i++) {
            bg1.add(Util.createTuple(tupleContents[i]));
        }
        DataBag bg2 = factory.newDefaultBag();
        for (int i = 0; i < tupleContents.length; i++) {
            bg2.add(Util.createTuple(tupleContents[i]));
        }
        assertEquals(bg1, bg2);
    }
    
    @Test
    public void testDefaultDataBagEquals2() throws Exception {
        String[][] tupleContents = new String[][] {{"a", "b"},{"c", "d" }, { "e", "f"} };
        TestMemoryManager mgr = new TestMemoryManager();
        LocalBagFactory factory = new LocalBagFactory(mgr);
        DataBag bg1 = factory.newDefaultBag();
        for (int i = 0; i < tupleContents.length; i++) {
            bg1.add(Util.createTuple(tupleContents[i]));
        }
        tupleContents = new String[][] {{"c", "d" }, {"a", "b"},{ "e", "f"} };
        DataBag bg2 = factory.newDefaultBag();
        for (int i = 0; i < tupleContents.length; i++) {
            bg2.add(Util.createTuple(tupleContents[i]));
        }
        assertEquals(bg1, bg2);
    }
    
    public void testInternalCachedBag() throws Exception {    
    	// check adding empty tuple
    	DataBag bg0 = new InternalCachedBag();
    	bg0.add(TupleFactory.getInstance().newTuple());
    	bg0.add(TupleFactory.getInstance().newTuple());
    	assertEquals(bg0.size(), 2);
    	
    	// check equal of bags
    	DataBag bg1 = new InternalCachedBag(1, 0.5f);
    	assertEquals(bg1.size(), 0);
    	
    	String[][] tupleContents = new String[][] {{"a", "b"},{"c", "d" }, { "e", "f"} };
    	for (int i = 0; i < tupleContents.length; i++) {
            bg1.add(Util.createTuple(tupleContents[i]));
        }
    	
    	// check size, and isSorted(), isDistinct()
    	assertEquals(bg1.size(), 3);
    	assertFalse(bg1.isSorted());
    	assertFalse(bg1.isDistinct());
    	
    	tupleContents = new String[][] {{"c", "d" }, {"a", "b"},{ "e", "f"} };
    	DataBag bg2 = new InternalCachedBag(1, 0.5f);
        for (int i = 0; i < tupleContents.length; i++) {
             bg2.add(Util.createTuple(tupleContents[i]));
        }
        assertEquals(bg1, bg2);
        
        // check bag with data written to disk
        DataBag bg3 = new InternalCachedBag(1, 0.0f);
        tupleContents = new String[][] {{ "e", "f"}, {"c", "d" }, {"a", "b"}};
        for (int i = 0; i < tupleContents.length; i++) {
            bg3.add(Util.createTuple(tupleContents[i]));
        }
        assertEquals(bg1, bg3);
        
        // check iterator
        Iterator<Tuple> iter = bg3.iterator();
        DataBag bg4 = new InternalCachedBag(1, 0.0f);
        while(iter.hasNext()) {
        	bg4.add(iter.next());
        }
        assertEquals(bg3, bg4);
        
        // call iterator methods with irregular order
        iter = bg3.iterator();
        assertTrue(iter.hasNext());
        assertTrue(iter.hasNext());
        DataBag bg5 = new InternalCachedBag(1, 0.0f);
        bg5.add(iter.next());
        bg5.add(iter.next());
        assertTrue(iter.hasNext());
        bg5.add(iter.next());
        assertFalse(iter.hasNext());
        assertFalse(iter.hasNext());
        assertEquals(bg3, bg5);
        
        
        bg4.clear();
        assertEquals(bg4.size(), 0);        
    }
    
    public void testInternalSortedBag() throws Exception {    
    	
    	// check adding empty tuple
    	DataBag bg0 = new InternalSortedBag();
    	bg0.add(TupleFactory.getInstance().newTuple());
    	bg0.add(TupleFactory.getInstance().newTuple());
    	assertEquals(bg0.size(), 2);
    	
    	// check equal of bags
    	DataBag bg1 = new InternalSortedBag();
    	assertEquals(bg1.size(), 0);
    	
    	String[][] tupleContents = new String[][] {{ "e", "f"}, {"a", "b"}, {"c", "d" }};
    	for (int i = 0; i < tupleContents.length; i++) {
            bg1.add(Util.createTuple(tupleContents[i]));
        }
    	
    	// check size, and isSorted(), isDistinct()
    	assertEquals(bg1.size(), 3);
    	assertTrue(bg1.isSorted());
    	assertFalse(bg1.isDistinct());
    	
    	tupleContents = new String[][] {{"c", "d" }, {"a", "b"},{ "e", "f"} };
    	DataBag bg2 = new InternalSortedBag();
        for (int i = 0; i < tupleContents.length; i++) {
             bg2.add(Util.createTuple(tupleContents[i]));
        }
        assertEquals(bg1, bg2);
        
        Iterator<Tuple> iter = bg1.iterator();
        iter.next().equals(Util.createTuple(new String[] {"a", "b"}));
        iter.next().equals(Util.createTuple(new String[] {"c", "d"}));
        iter.next().equals(Util.createTuple(new String[] {"e", "f"}));
        
        // check bag with data written to disk
        DataBag bg3 = new InternalSortedBag(1, 0.0f, null);
        tupleContents = new String[][] {{ "e", "f"}, {"c", "d" }, {"a", "b"}};
        for (int i = 0; i < tupleContents.length; i++) {
            bg3.add(Util.createTuple(tupleContents[i]));
        }
        assertEquals(bg1, bg3);
        
        iter = bg3.iterator();
        iter.next().equals(Util.createTuple(new String[] {"a", "b"}));
        iter.next().equals(Util.createTuple(new String[] {"c", "d"}));
        iter.next().equals(Util.createTuple(new String[] {"e", "f"}));                
        
        // call iterator methods with irregular order
        iter = bg3.iterator();
        assertTrue(iter.hasNext());
        assertTrue(iter.hasNext());
        
        DataBag bg4 = new InternalSortedBag(1, 0.0f, null);
        bg4.add(iter.next());
        bg4.add(iter.next());
        assertTrue(iter.hasNext());
        bg4.add(iter.next());
        assertFalse(iter.hasNext());
        assertFalse(iter.hasNext());
        assertEquals(bg3, bg4);        
        
        // check clear
        bg3.clear();
        assertEquals(bg3.size(), 0);
        
        // test with all data spill out
        DataBag bg5 = new InternalSortedBag();        
        for(int j=0; j<3; j++) {
        	for (int i = 0; i < tupleContents.length; i++) {
        		bg5.add(Util.createTuple(tupleContents[i]));
        	}     
        	bg5.spill();
        }
        
        assertEquals(bg5.size(), 9);
        iter = bg5.iterator();
        for(int i=0; i<3; i++) {
        	iter.next().equals(Util.createTuple(new String[] {"a", "b"}));
        }
        for(int i=0; i<3; i++) {
        	iter.next().equals(Util.createTuple(new String[] {"c", "d"}));
        }
        for(int i=0; i<3; i++) {
        	iter.next().equals(Util.createTuple(new String[] {"e", "f"}));   
        }
        
        // test with most data spill out, with some data in memory
        // and merge of spill files
        DataBag bg6 = new InternalSortedBag();        
        for(int j=0; j<104; j++) {
        	for (int i = 0; i < tupleContents.length; i++) {
        		bg6.add(Util.createTuple(tupleContents[i]));
        	}        	
        	if (j != 103) {
        		bg6.spill();
        	}
        }
        
        assertEquals(bg6.size(), 104*3);
        iter = bg6.iterator();
        for(int i=0; i<104; i++) {
        	iter.next().equals(Util.createTuple(new String[] {"a", "b"}));
        }
        for(int i=0; i<104; i++) {
        	iter.next().equals(Util.createTuple(new String[] {"c", "d"}));
        }
        for(int i=0; i<104; i++) {
        	iter.next().equals(Util.createTuple(new String[] {"e", "f"}));   
        }
        
        // check two implementation of sorted bag can compare correctly
        DataBag bg7 = new SortedDataBag(null);        
        for(int j=0; j<104; j++) {
        	for (int i = 0; i < tupleContents.length; i++) {
        		bg7.add(Util.createTuple(tupleContents[i]));
        	}        	
        	if (j != 103) {
        		bg7.spill();
        	}
        }
        assertEquals(bg6, bg7);
    }
    
    public void testInternalDistinctBag() throws Exception {    
    	// check adding empty tuple
    	DataBag bg0 = new InternalDistinctBag();
    	bg0.add(TupleFactory.getInstance().newTuple());
    	bg0.add(TupleFactory.getInstance().newTuple());
    	assertEquals(bg0.size(), 1);
    	
    	// check equal of bags
    	DataBag bg1 = new InternalDistinctBag();
    	assertEquals(bg1.size(), 0);
    	
    	String[][] tupleContents = new String[][] {{ "e", "f"}, {"a", "b"}, {"e", "d" }, {"a", "b"}, {"e", "f"}};
    	for (int i = 0; i < tupleContents.length; i++) {
            bg1.add(Util.createTuple(tupleContents[i]));
        }
    	
    	// check size, and isSorted(), isDistinct()
    	assertEquals(bg1.size(), 3);
    	assertFalse(bg1.isSorted());
    	assertTrue(bg1.isDistinct());
    	
    	tupleContents = new String[][] {{"a", "b" }, {"e", "d"}, {"e", "d"}, { "e", "f"} };
    	DataBag bg2 = new InternalDistinctBag();
        for (int i = 0; i < tupleContents.length; i++) {
             bg2.add(Util.createTuple(tupleContents[i]));
        }
        assertEquals(bg1, bg2);
        
        Iterator<Tuple> iter = bg1.iterator();
        iter.next().equals(Util.createTuple(new String[] {"a", "b"}));
        iter.next().equals(Util.createTuple(new String[] {"c", "d"}));
        iter.next().equals(Util.createTuple(new String[] {"e", "f"}));
        
        // check bag with data written to disk
        DataBag bg3 = new InternalDistinctBag(1, 0.0f);
        tupleContents = new String[][] {{ "e", "f"}, {"a", "b"}, {"e", "d" }, {"a", "b"}, {"e", "f"}};
        for (int i = 0; i < tupleContents.length; i++) {
            bg3.add(Util.createTuple(tupleContents[i]));
        }
        assertEquals(bg2, bg3);
        assertEquals(bg3.size(), 3);
              
        
        // call iterator methods with irregular order
        iter = bg3.iterator();
        assertTrue(iter.hasNext());
        assertTrue(iter.hasNext());
        
        DataBag bg4 = new InternalDistinctBag(1, 0.0f);
        bg4.add(iter.next());
        bg4.add(iter.next());
        assertTrue(iter.hasNext());
        bg4.add(iter.next());
        assertFalse(iter.hasNext());
        assertFalse(iter.hasNext());
        assertEquals(bg3, bg4);        
        
        // check clear
        bg3.clear();
        assertEquals(bg3.size(), 0);
        
        // test with all data spill out
        DataBag bg5 = new InternalDistinctBag();        
        for(int j=0; j<3; j++) {
        	for (int i = 0; i < tupleContents.length; i++) {
        		bg5.add(Util.createTuple(tupleContents[i]));
        	}        
        	bg5.spill();
        }
        
        assertEquals(bg5.size(), 3);
    
        
        // test with most data spill out, with some data in memory
        // and merge of spill files
        DataBag bg6 = new InternalDistinctBag();        
        for(int j=0; j<104; j++) {
        	for (int i = 0; i < tupleContents.length; i++) {
        		bg6.add(Util.createTuple(tupleContents[i]));
        	}        	
        	if (j != 103) {
        		bg6.spill();
        	}
        }
        
        assertEquals(bg6.size(), 3);       
        
        // check two implementation of sorted bag can compare correctly
        DataBag bg7 = new DistinctDataBag();        
        for(int j=0; j<104; j++) {
        	for (int i = 0; i < tupleContents.length; i++) {
        		bg7.add(Util.createTuple(tupleContents[i]));
        	}        	
        	if (j != 103) {
        		bg7.spill();
        	}
        }
        assertEquals(bg6, bg7);
    }
    
    // See PIG-1231
    @Test
    public void testDataBagIterIdempotent() throws Exception {
        DataBag bg0 = new DefaultDataBag();
        processDataBag(bg0, true);
        
        DataBag bg1 = new DistinctDataBag();
        processDataBag(bg1, true);
        
        DataBag bg2 = new InternalDistinctBag();
        processDataBag(bg2, true);
        
        DataBag bg3 = new InternalSortedBag();
        processDataBag(bg3, true);
        
        DataBag bg4 = new SortedDataBag(null);
        processDataBag(bg4, true);
        
        DataBag bg5 = new InternalCachedBag(0, 0);
        processDataBag(bg5, false);
    }
    
    // See PIG-1285
    @Test
    public void testSerializeSingleTupleBag() throws Exception {
        Tuple t = Util.createTuple(new String[] {"foo", "bar", "baz"});
        DataBag stBag = new SingleTupleBag(t);
        PipedOutputStream pos = new PipedOutputStream();
        DataOutputStream dos = new DataOutputStream(pos);
        PipedInputStream pis = new PipedInputStream(pos);
        DataInputStream dis = new DataInputStream(pis);
        stBag.write(dos);
        DataBag dfBag = new DefaultDataBag();
        dfBag.readFields(dis);
        assertTrue(dfBag.equals(stBag));
    }
    
    // See PIG-2550
    static class MyCustomTuple extends DefaultTuple {
        private static final long serialVersionUID = 8156382697467819543L;
        public MyCustomTuple() {
            super();
        }
        public MyCustomTuple(Object t) {
            super();
            append(t);
        }
    }

    @Test
    public void testSpillCustomTuple() throws Exception {
        DataBag bag = new DefaultDataBag();
        Tuple t = new MyCustomTuple();
        t.append(1);
        t.append("hello");
        bag.add(t);
        bag.spill();
        Iterator<Tuple> iter = bag.iterator();
        Tuple t2 = iter.next();
        assertTrue(t2.equals(t));
    }
    
    void processDataBag(DataBag bg, boolean doSpill) {
        Tuple t = TupleFactory.getInstance().newTuple(new Integer(0));
        bg.add(t);
        if (doSpill)
            bg.spill();
        Iterator<Tuple> iter = bg.iterator();
        assertTrue(iter.hasNext());
        iter.next();
        assertFalse(iter.hasNext());
        assertFalse("hasNext should be idempotent", iter.hasNext());        
    }
}




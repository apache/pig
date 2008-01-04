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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.Iterator;
import java.util.Random;

import org.junit.Test;

import org.apache.pig.data.*;

/**
 * This class will exercise the basic Pig data model and members. It tests for proper behavior in
 * assigment and comparision, as well as function application.
 * 
 * @author dnm
 */
public class TestDataModel extends junit.framework.TestCase {

    @Test
    public void testDatum() throws Exception {
        // Datum is an empty class, but in the event we
        // ever add a method, test it here
    }

    @Test
    public void testDataAtom() throws Exception {
        // Make sure a DataAtom is still a Datum
        DataAtom da1 = new DataAtom("string");
        assertTrue(da1 instanceof Datum);

        // Test basic comparison functions
        DataAtom da2 = new DataAtom("string");
        assertTrue(da1.compareTo(da2) == 0);
        assertTrue(da1.equals(da2));
        assertTrue(da1.strval().equals(da2.toString()));

        // Make sure that case sensitivity is maintained
        da2 = new DataAtom("String");
        assertFalse(da1.compareTo(da2) == 0);
        assertFalse(da1.strval().equals(da2));
        assertFalse(da1.strval().equals(da2.toString()));

        // Test string/int/double comparison and storage
        da1 = new DataAtom(1);
        da2 = new DataAtom("1");
        assertTrue(da1.equals(da2));
        da2 = new DataAtom(1.0);
        assertTrue(da1.numval().equals(da2.numval()));
        da2 = new DataAtom("2");
        assertTrue(da1.compareTo(da2) < 0);
        assertFalse(da1.compareTo(da2) > 0);
    }

    @Test
    public void testTuple() throws Exception {
        int arity = 5;
        int[] input1 = { 1, 2, 3, 4, 5 };
        String[] input2 = { "1", "2", "3", "4", "5" };
        String[] input3 = { "1", "2", "3", "4", "5", "6" };

        // validate construction and equality
        Tuple f1 = Util.loadFlatTuple(new Tuple(arity), input1);
        Tuple f2 = Util.loadFlatTuple(new Tuple(arity), input1);
        Tuple f3 = new Tuple(arity);
        assertTrue(f1.arity() == arity);
        assertTrue(f1.equals(f2));

        // validate string vs. int construction and equality
        f2 = Util.loadTuple(new Tuple(arity), input2);
        assertTrue(f1.equals(f2));

        // invalid equality
        f2 = Util.loadTuple(new Tuple(input3.length), input3);
        assertFalse(f1.equals(f2));

        // copy equality
        f2.copyFrom(f1);
        assertTrue(f1.equals(f2));

        // append function and equality
        int[] input4 = { 1, 2, 3 };
        int[] input5 = { 4, 5 };
        f1 = Util.loadFlatTuple(new Tuple(input4.length), input4);
        f2 = Util.loadFlatTuple(new Tuple(input5.length), input5);
        f3 = Util.loadFlatTuple(new Tuple(input1.length), input1);
        f1.appendTuple(f2);
        assertTrue(f3.equals(f1));

        // arity then value comparision behavior
        f1 = Util.loadFlatTuple(new Tuple(input1.length), input1); // 1,2,3,4,5
        f2 = Util.loadFlatTuple(new Tuple(input4.length), input4); // 1,2,3
        assertTrue(f1.greaterThan(f2));
        assertFalse(f1.lessThan(f2));

        int[] input6 = { 1, 2, 3, 4, 6 };
        f2 = Util.loadFlatTuple(new Tuple(input6.length), input6);
        assertTrue(f1.lessThan(f2));
        assertFalse(f1.greaterThan(f2));

        // delimited export
        String expected = "1:2:3:4:5";
        f1 = Util.loadFlatTuple(new Tuple(input1.length), input1);
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
        int[][] input1 = { { 1, 2, 3, 4, 5 }, { 1, 2, 3, 4, 5 }, { 1, 2, 3, 4, 5 }, { 1, 2, 3, 4, 5 },
                { 1, 2, 3, 4, 5 } };
        int[][] input2 = { { 1, 2 }, { 1, 2 } };

        Tuple n1 = Util.loadNestTuple(new Tuple(input1.length), input1);
        Tuple n2 = new Tuple();

        // CompareTo is currently not implemented
        n2.copyFrom(n1);
        // assertTrue(n1.compareTo(n2) == 0);

        n2 = Util.loadNestTuple(new Tuple(input2.length), input2);
        // assertTrue(n1.compareTo(n2) == 1);

        // basic append test ..
        int n1Arity = n1.arity();
        int n2Arity = n2.arity();
        n1.appendTuple(n2);
        assertTrue(n1.arity() == n1Arity + n2Arity);
    }

    /*
    @Test
    public void testDataBag() throws Exception {
        int[] input1 = { 1, 2, 3, 4, 5 };
        int[] input2 = { 0, 2, 3, 4, 5 };

        // Check empty bag errors
        DataBag b = new DataBag();
        boolean caught = false;
        try {
            b.getField(0).strval().equals("1");
        } catch (IOException e) {
            caught = true;
        }
        assertTrue(caught);
        assertTrue(b.isEmpty());

        // Check field get for indentical rows
        Tuple f = Util.loadFlatTuple(new Tuple(input1.length), input1);
        for (int i = 0; i < 10; i++) {
            b.add(f);
        }
        assertTrue(b.getField(0).strval().equals("1"));
        assertTrue(b.cardinality() == 10);

        // Check field get for heterogenous rows
        f = Util.loadFlatTuple(new Tuple(input2.length), input2);
        b.add(f);
        caught = false;
        try {
            b.getField(0).strval().equals("1");
        } catch (IOException e) {
            caught = true;
        }   
        assertTrue(caught);

        // check that notifications are sent
         b.clear();
         DataBag.notifyInterval = 2;
         Tuple g = Util.loadFlatTuple(new Tuple(input1.length), input1);
         for (int i = 0; i < 10; i++) {
             b.add(g);
         }

         Iterator it = b.content();
         while (it.hasNext()) it.next();
         assert(b.numNotifies == 5);
    }

    @Test
    
    public void testBigDataBagInMemory() throws Exception{
    	testBigDataBag(5*1024*1024, 5000);
    }

    public void testBigDataBagOnDisk() throws Exception{
    	Runtime.getRuntime().gc();
    	testBigDataBag(Runtime.getRuntime().maxMemory() - 1*1024*1024, 1000000);
    }
    */

    private enum TestType {
    	PRE_SORT,
    	POST_SORT,
    	PRE_DISTINCT,
    	POST_DISTINCT,
    	NONE
    }
       
    
    /*
    private void testBigDataBag(long freeMemoryToMaintain, int numItems) throws Exception {
    	BigDataBag.FREE_MEMORY_TO_MAINTAIN = freeMemoryToMaintain;
        Random r = new Random();
   
    	for (TestType testType: TestType.values()){
    		BigDataBag bag = BagFactory.getInstance().getNewBigBag();

            assertTrue(bag.isEmpty());

            if (testType == TestType.PRE_SORT)
            	bag.sort();
            else if (testType == TestType.PRE_DISTINCT)
            	bag.distinct();
            
            //generate data and add it to the bag
            for(int i = 0; i < numItems; i++) {
                Tuple t = new Tuple(1);
                t.setField(0, r.nextInt(numItems));
                bag.add(t);
            }

            assertFalse(bag.isEmpty());

            if (testType == TestType.POST_SORT)
            	bag.sort();
            else if (testType == TestType.POST_DISTINCT)
            	bag.distinct();

            
            if (testType == TestType.NONE)
            	assertTrue(bag.cardinality() == numItems);
            checkContents(bag, numItems, testType);
            checkContents(bag, numItems, testType);

    	}
    }
     
    
    private void checkContents(DataBag bag, int numItems, TestType testType) throws Exception{
        String last = "";
        
        DataBag.notifyInterval = 100;
        
        Iterator<Tuple> it = bag.content();
        int count = 0;
        while(it.hasNext()) {
        	Tuple t = it.next();
        	String next = t.getAtomField(0).strval();
        	if (testType == TestType.POST_SORT || testType == TestType.PRE_SORT)
                assertTrue(last.compareTo(next)<=0);
        	else if (testType == TestType.POST_DISTINCT || testType == TestType.PRE_DISTINCT)
                assertTrue(last.compareTo(next)<0);
            last = next;
        	count++;
        }
        
        assertTrue(bag.cardinality() == count);
        
        if (testType != TestType.NONE)
        	assertTrue(bag.numNotifies >= count/DataBag.notifyInterval);
    }
    */

}

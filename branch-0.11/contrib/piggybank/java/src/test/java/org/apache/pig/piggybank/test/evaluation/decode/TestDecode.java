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
package org.apache.pig.piggybank.test.evaluation.decode;

import java.io.IOException;

import junit.framework.TestCase;

import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.piggybank.evaluation.decode.Bin;
import org.apache.pig.piggybank.evaluation.decode.BinCond;
import org.apache.pig.piggybank.evaluation.decode.Decode;
import org.junit.Test;

public class TestDecode extends TestCase {
    @Test
    public void testBin() throws Exception {
        Tuple t1 = TupleFactory.getInstance().newTuple(8);
        t1.set(0, new Integer(19));
        t1.set(1, "infant");
        t1.set(2, new Integer(5));
        t1.set(3, "young");
        t1.set(4, new Integer(20));
        t1.set(5, "normal");
        t1.set(6, new Integer(60));
        t1.set(7, "old");
        
        Tuple t2 = TupleFactory.getInstance().newTuple(8);
        t2.set(0, new Double(1.78));
        t2.set(1, "S");
        t2.set(2, new Double(1.70));
        t2.set(3, "M");
        t2.set(4, new Double(1.80));
        t2.set(5, "L");
        t2.set(6, new Double(1.90));
        t2.set(7, "XL");
        
        Tuple t3 = TupleFactory.getInstance().newTuple(8);
        t3.set(0, null);
        t3.set(1, "S");
        t3.set(2, new Double(1.70));
        t3.set(3, "M");
        t3.set(4, new Double(1.80));
        t3.set(5, "L");
        t3.set(6, new Double(1.90));
        t3.set(7, "XL");
        
        Tuple t4 = TupleFactory.getInstance().newTuple(8);
        t4.set(0, new Double(1.78));
        t4.set(1, null);
        t4.set(2, new Double(1.70));
        t3.set(3, "M");
        t3.set(4, new Double(1.80));
        t3.set(5, "L");
        t3.set(6, new Double(1.90));
        t4.set(7, "XL");

        Bin func = new Bin();
        String r = func.exec(t1);
        assertTrue(r.equals("young"));
        
        r = func.exec(t2);
        assertTrue(r.equals("M"));
        
        r = func.exec(t3);
        assertTrue(r==null);
        
        try {
            r = func.exec(t4);
            fail("Exception not triggered");
        } catch (IOException e) {
            assertTrue(e.getMessage().equals("Bin : Encounter null in the input"));
        }
    }
    @Test
    public void testBinCond() throws Exception {
        Tuple t1 = TupleFactory.getInstance().newTuple(7);
        t1.set(0, false);
        t1.set(1, "s&a");
        t1.set(2, false);
        t1.set(3, "a");
        t1.set(4, true);
        t1.set(5, "s");
        t1.set(6, "n");
        
        Tuple t2 = TupleFactory.getInstance().newTuple(7);
        t2.set(0, null);
        t2.set(1, "s&a");
        t2.set(2, false);
        t2.set(3, "a");
        t2.set(4, true);
        t2.set(5, "s");
        t2.set(6, "n");

        Tuple t3 = TupleFactory.getInstance().newTuple(7);
        t3.set(0, false);
        t3.set(1, "s&a");
        t3.set(2, null);
        t3.set(3, "a");
        t3.set(4, true);
        t3.set(5, "s");
        t3.set(6, "n");
        
        BinCond func = new BinCond();
        String r = func.exec(t1);
        assertTrue(r.equals("s"));
        r = func.exec(t2);
        assertTrue(r==null);
        try {
            r = func.exec(t3);
            fail("Exception not triggered");
        } catch (IOException e) {
            assertTrue(e.getMessage().equals("BinCond : Encounter null in the input"));
        }
    }
    @Test
    public void testDecode() throws Exception {
        Tuple t1 = TupleFactory.getInstance().newTuple(6);
        t1.set(0, new Integer(1));
        t1.set(1, 0);
        t1.set(2, "Sales");
        t1.set(3, 1);
        t1.set(4, "Engineering");
        t1.set(5, "Other");
        
        Tuple t2 = TupleFactory.getInstance().newTuple(6);
        t2.set(0, new Integer(3));
        t2.set(1, 0);
        t2.set(2, "Sales");
        t2.set(3, 1);
        t2.set(4, "Engineering");
        t2.set(5, "Other");

        Tuple t3 = TupleFactory.getInstance().newTuple(6);
        t3.set(0, null);
        t3.set(1, 0);
        t3.set(2, "Sales");
        t3.set(3, 1);
        t3.set(4, "Engineering");
        t3.set(5, "Other");

        Tuple t4 = TupleFactory.getInstance().newTuple(6);
        t4.set(0, new Integer(1));
        t4.set(1, null);
        t4.set(2, "Sales");
        t4.set(3, 1);
        t4.set(4, "Engineering");
        t4.set(5, "Other");
        
        Decode func = new Decode();
        String r = func.exec(t1);
        assertTrue(r.equals("Engineering"));        
        r = func.exec(t2);
        assertTrue(r.equals("Other"));
        r = func.exec(t3);
        assertTrue(r==null);
        try {
            r = func.exec(t4);
            fail("Exception not triggered");
        } catch (IOException e) {
            assertTrue(e.getMessage().equals("Decode : Encounter null in the input"));
        }
    }
}

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
package org.apache.pig.piggybank.evaluation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import org.junit.Before;
import org.junit.Test;

public class TestOver {

    @Test
    public void testSchema() throws Exception {
        // No type
        Over func = new Over();
        Schema in = Schema.generateNestedSchema(DataType.BAG, DataType.INTEGER);
        Schema out = func.outputSchema(in);
        assertEquals("{{NULL}}", out.toString());

        // chararray
        func = new Over("chararray");
        in = Schema.generateNestedSchema(DataType.BAG, DataType.INTEGER);
        out = func.outputSchema(in);
        assertEquals("{{chararray}}", out.toString());

        // int
        func = new Over("Int");
        in = Schema.generateNestedSchema(DataType.BAG, DataType.INTEGER);
        out = func.outputSchema(in);
        assertEquals("{{int}}", out.toString());

        // double
        func = new Over("DOUBLE");
        in = Schema.generateNestedSchema(DataType.BAG, DataType.INTEGER);
        out = func.outputSchema(in);
        assertEquals("{{double}}", out.toString());
    }

    @Test
    public void testBadInput() throws Exception {
        Over func = new Over();
        boolean caught = false;
        Tuple t = TupleFactory.getInstance().newTuple();
        t.append("Mary had a little lamb");
        t.append("count");
        t.append(0);
        t.append(0);
        try {
            func.exec(t);
        } catch (ExecException ee) {
            caught = true;
            assertEquals("Over expected a bag for arg 1 but received chararray",
                    ee.getMessage());
        }
        assertTrue(caught);

        func = new Over();
        DataBag inbag = BagFactory.getInstance().newDefaultBag();
        for (int i = 0; i < 10; i++) {
            t = TupleFactory.getInstance().newTuple(1);
            t.set(0, 1);
            inbag.add(t);
        }
        t = TupleFactory.getInstance().newTuple();
        t.append(inbag);
        caught = false;
        try {
            func.exec(t);
        } catch (ExecException ee) {
            caught = true;
            assertEquals("Over expected 2 or more inputs but received 1",
                    ee.getMessage());
        }
        assertTrue(caught);

        func = new Over();
        t.append(1);
        caught = false;
        try {
            func.exec(t);
        } catch (ExecException ee) {
            caught = true;
            assertEquals("Over expected a string for arg 2 but received int",
                    ee.getMessage());
        }
        assertTrue(caught);

        func = new Over();
        t.set(1, "count");
        t.append("fred");
        caught = false;
        try {
            func.exec(t);
        } catch (ExecException ee) {
            caught = true;
            assertEquals("Over expected an integer for arg 3 but received chararray",
                    ee.getMessage());
        }
        assertTrue(caught);

        func = new Over();
        t.set(2, -1);
        t.append("fred");
        caught = false;
        try {
            func.exec(t);
        } catch (ExecException ee) {
            caught = true;
            assertEquals("Over expected an integer for arg 4 but received chararray",
                    ee.getMessage());
        }
        assertTrue(caught);
    }
    
    @Test
    public void testBagFunc() throws Exception {
        Over func = new Over();
        DataBag inbag = BagFactory.getInstance().newDefaultBag();
        for (int i = 0; i < 10; i++) {
            Tuple t = TupleFactory.getInstance().newTuple(1);
            t.set(0, 1);
            inbag.add(t);
        }
        Tuple t = TupleFactory.getInstance().newTuple(2);
        t.set(0, inbag);
        t.set(1, "fred");
        boolean caught = false;
        try {
            DataBag outbag = func.exec(t);
        } catch (ExecException ee) {
            caught = true;
            assertEquals("Unknown aggregate fred", ee.getMessage());
        }
        assertTrue(caught);
    }


    @Test
    public void testCountNoWindow() throws Exception {
        Over func = new Over();
        DataBag inbag = BagFactory.getInstance().newDefaultBag();
        for (int i = 0; i < 10; i++) {
            Tuple t = TupleFactory.getInstance().newTuple(1);
            t.set(0, 1);
            inbag.add(t);
        }
        Tuple t = TupleFactory.getInstance().newTuple(4);
        t.set(0, inbag);
        t.set(1, "count");
        t.set(2, -1);
        t.set(3, -1);
        DataBag outbag = func.exec(t);
        assertEquals(10, outbag.size());
        for (Tuple to : outbag) {
            assertEquals(1, to.size());
            assertEquals(new Long(10), to.get(0));
        }
    }

    @Test
    public void testCountPrecedingUnboundedToCurrent() throws Exception {
        Over func = new Over();
        DataBag inbag = BagFactory.getInstance().newDefaultBag();
        for (int i = 0; i < 10; i++) {
            Tuple t = TupleFactory.getInstance().newTuple(1);
            t.set(0, 1);
            inbag.add(t);
        }
        Tuple t = TupleFactory.getInstance().newTuple(2);
        t.set(0, inbag);
        t.set(1, "count");
        DataBag outbag = func.exec(t);
        assertEquals(10, outbag.size());
        int cnt = 1;
        for (Tuple to : outbag) {
            assertEquals(1, to.size());
            assertEquals(new Long(cnt++), to.get(0));
        }
    }
    
    @Test
    public void testCountCurrentToUnboundedFollowing() throws Exception {
        Over func = new Over();
        DataBag inbag = BagFactory.getInstance().newDefaultBag();
        for (int i = 0; i < 10; i++) {
            Tuple t = TupleFactory.getInstance().newTuple(1);
            t.set(0, 1);
            inbag.add(t);
        }
        Tuple t = TupleFactory.getInstance().newTuple(4);
        t.set(0, inbag);
        t.set(1, "count");
        t.set(2, 0);
        t.set(3, -1);
        DataBag outbag = func.exec(t);
        assertEquals(10, outbag.size());
        int cnt = 10;
        for (Tuple to : outbag) {
            assertEquals(1, to.size());
            assertEquals(new Long(cnt--), to.get(0));
        }
    }

    @Test
    public void testThreeBeforeAndAfter() throws Exception {
        Over func = new Over();
        DataBag inbag = BagFactory.getInstance().newDefaultBag();
        for (int i = 0; i < 10; i++) {
            Tuple t = TupleFactory.getInstance().newTuple(1);
            t.set(0, 1);
            inbag.add(t);
        }
        Tuple t = TupleFactory.getInstance().newTuple(4);
        t.set(0, inbag);
        t.set(1, "sum(int)");
        t.set(2, 3);
        t.set(3, 3);
        DataBag outbag = func.exec(t);
        assertEquals(10, outbag.size());
        int sum = 1;
        for (Tuple to : outbag) {
            assertEquals(1, to.size());
            switch (sum++) {
                case 1:
                case 10:
                    assertEquals(new Long(4), to.get(0));
                    break;

                case 2:
                case 9:
                    assertEquals(new Long(5), to.get(0));
                    break;

                case 3:
                case 8:
                    assertEquals(new Long(6), to.get(0));
                    break;

                case 4:
                case 5:
                case 6:
                case 7:
                    assertEquals(new Long(7), to.get(0));
                    break;

                default:
                    // Huh?
                    throw new RuntimeException("We shouldn't be here, sum is "
                            + sum);
            }
        }
    }

    @Test
    public void testSumDouble() throws Exception {
        Over func = new Over();
        DataBag inbag = BagFactory.getInstance().newDefaultBag();
        for (int i = 0; i < 10; i++) {
            Tuple t = TupleFactory.getInstance().newTuple(1);
            t.set(0, 1.0);
            inbag.add(t);
        }
        Tuple t = TupleFactory.getInstance().newTuple(4);
        t.set(0, inbag);
        t.set(1, "sum(double)");
        t.set(2, -1);
        t.set(3, -1);
        DataBag outbag = func.exec(t);
        assertEquals(10, outbag.size());
        for (Tuple to : outbag) {
            assertEquals(1, to.size());
            assertEquals(new Double(10.0), to.get(0));
        }
    }

    @Test
    public void testSumByteArray() throws Exception {
        Over func = new Over();
        DataBag inbag = BagFactory.getInstance().newDefaultBag();
        for (int i = 0; i < 10; i++) {
            Tuple t = TupleFactory.getInstance().newTuple(1);
            t.set(0, new DataByteArray("1"));
            inbag.add(t);
        }
        Tuple t = TupleFactory.getInstance().newTuple(4);
        t.set(0, inbag);
        t.set(1, "sum(bytearray)");
        t.set(2, -1);
        t.set(3, -1);
        DataBag outbag = func.exec(t);
        assertEquals(10, outbag.size());
        for (Tuple to : outbag) {
            assertEquals(1, to.size());
            assertEquals(new Double(10.0), to.get(0));
        }
    }

    @Test
    public void testSumFloat() throws Exception {
        Over func = new Over();
        DataBag inbag = BagFactory.getInstance().newDefaultBag();
        for (int i = 0; i < 10; i++) {
            Tuple t = TupleFactory.getInstance().newTuple(1);
            t.set(0, 1.0f);
            inbag.add(t);
        }
        Tuple t = TupleFactory.getInstance().newTuple(4);
        t.set(0, inbag);
        t.set(1, "sum(float)");
        t.set(2, -1);
        t.set(3, -1);
        DataBag outbag = func.exec(t);
        assertEquals(10, outbag.size());
        for (Tuple to : outbag) {
            assertEquals(1, to.size());
            assertEquals(new Double(10.0), to.get(0));
        }
    }

    @Test
    public void testSumInt() throws Exception {
        Over func = new Over();
        DataBag inbag = BagFactory.getInstance().newDefaultBag();
        for (int i = 0; i < 10; i++) {
            Tuple t = TupleFactory.getInstance().newTuple(1);
            t.set(0, 1);
            inbag.add(t);
        }
        Tuple t = TupleFactory.getInstance().newTuple(4);
        t.set(0, inbag);
        t.set(1, "sum(int)");
        t.set(2, -1);
        t.set(3, -1);
        DataBag outbag = func.exec(t);
        assertEquals(10, outbag.size());
        for (Tuple to : outbag) {
            assertEquals(1, to.size());
            assertEquals(new Long(10), to.get(0));
        }
    }

    @Test
    public void testSumLong() throws Exception {
        Over func = new Over();
        DataBag inbag = BagFactory.getInstance().newDefaultBag();
        for (int i = 0; i < 10; i++) {
            Tuple t = TupleFactory.getInstance().newTuple(1);
            t.set(0, 1L);
            inbag.add(t);
        }
        Tuple t = TupleFactory.getInstance().newTuple(4);
        t.set(0, inbag);
        t.set(1, "sum(long)");
        t.set(2, -1);
        t.set(3, -1);
        DataBag outbag = func.exec(t);
        assertEquals(10, outbag.size());
        for (Tuple to : outbag) {
            assertEquals(1, to.size());
            assertEquals(new Long(10), to.get(0));
        }
    }

    @Test
    public void testAvgDouble() throws Exception {
        Over func = new Over();
        DataBag inbag = BagFactory.getInstance().newDefaultBag();
        for (int i = 0; i < 10; i++) {
            Tuple t = TupleFactory.getInstance().newTuple(1);
            t.set(0, (double)i);
            inbag.add(t);
        }
        Tuple t = TupleFactory.getInstance().newTuple(4);
        t.set(0, inbag);
        t.set(1, "avg(double)");
        t.set(2, -1);
        t.set(3, -1);
        DataBag outbag = func.exec(t);
        assertEquals(10, outbag.size());
        for (Tuple to : outbag) {
            assertEquals(1, to.size());
            assertEquals(new Double(4.5), to.get(0));
        }
    }

    @Test
    public void testAvgByteArray() throws Exception {
        Over func = new Over();
        DataBag inbag = BagFactory.getInstance().newDefaultBag();
        for (int i = 0; i < 10; i++) {
            Tuple t = TupleFactory.getInstance().newTuple(1);
            t.set(0, new DataByteArray("1"));
            inbag.add(t);
        }
        Tuple t = TupleFactory.getInstance().newTuple(4);
        t.set(0, inbag);
        t.set(1, "avg(bytearray)");
        t.set(2, -1);
        t.set(3, -1);
        DataBag outbag = func.exec(t);
        assertEquals(10, outbag.size());
        for (Tuple to : outbag) {
            assertEquals(1, to.size());
            assertEquals(new Double(1.0), to.get(0));
        }
    }

    @Test
    public void testAvgFloat() throws Exception {
        Over func = new Over();
        DataBag inbag = BagFactory.getInstance().newDefaultBag();
        for (int i = 0; i < 10; i++) {
            Tuple t = TupleFactory.getInstance().newTuple(1);
            t.set(0, (float)i);
            inbag.add(t);
        }
        Tuple t = TupleFactory.getInstance().newTuple(4);
        t.set(0, inbag);
        t.set(1, "avg(float)");
        t.set(2, -1);
        t.set(3, -1);
        DataBag outbag = func.exec(t);
        assertEquals(10, outbag.size());
        for (Tuple to : outbag) {
            assertEquals(1, to.size());
            assertEquals(new Double(4.5), to.get(0));
        }
    }

    @Test
    public void testAvgInt() throws Exception {
        Over func = new Over();
        DataBag inbag = BagFactory.getInstance().newDefaultBag();
        for (int i = 0; i < 10; i++) {
            Tuple t = TupleFactory.getInstance().newTuple(1);
            t.set(0, i);
            inbag.add(t);
        }
        Tuple t = TupleFactory.getInstance().newTuple(4);
        t.set(0, inbag);
        t.set(1, "avg(int)");
        t.set(2, -1);
        t.set(3, -1);
        DataBag outbag = func.exec(t);
        assertEquals(10, outbag.size());
        for (Tuple to : outbag) {
            assertEquals(1, to.size());
            assertEquals(new Double(4.5), to.get(0));
        }
    }

    @Test
    public void testAvgLong() throws Exception {
        Over func = new Over();
        DataBag inbag = BagFactory.getInstance().newDefaultBag();
        for (int i = 0; i < 10; i++) {
            Tuple t = TupleFactory.getInstance().newTuple(1);
            t.set(0, (long)i);
            inbag.add(t);
        }
        Tuple t = TupleFactory.getInstance().newTuple(4);
        t.set(0, inbag);
        t.set(1, "avg(long)");
        t.set(2, -1);
        t.set(3, -1);
        DataBag outbag = func.exec(t);
        assertEquals(10, outbag.size());
        for (Tuple to : outbag) {
            assertEquals(1, to.size());
            assertEquals(new Double(4.5), to.get(0));
        }
    }
    
    @Test
    public void testMinDouble() throws Exception {
        Over func = new Over();
        DataBag inbag = BagFactory.getInstance().newDefaultBag();
        for (int i = 0; i < 10; i++) {
            Tuple t = TupleFactory.getInstance().newTuple(1);
            t.set(0, (double)i);
            inbag.add(t);
        }
        Tuple t = TupleFactory.getInstance().newTuple(2);
        t.set(0, inbag);
        t.set(1, "min(double)");
        DataBag outbag = func.exec(t);
        assertEquals(10, outbag.size());
        for (Tuple to : outbag) {
            assertEquals(1, to.size());
            assertEquals(new Double(0.0), to.get(0));
        }
    }

    @Test
    public void testMinByteArray() throws Exception {
        Over func = new Over();
        DataBag inbag = BagFactory.getInstance().newDefaultBag();
        for (int i = 0; i < 10; i++) {
            Tuple t = TupleFactory.getInstance().newTuple(1);
            t.set(0, new DataByteArray(new Integer(i).toString()));
            inbag.add(t);
        }
        Tuple t = TupleFactory.getInstance().newTuple(2);
        t.set(0, inbag);
        t.set(1, "min(bytearray)");
        DataBag outbag = func.exec(t);
        assertEquals(10, outbag.size());
        for (Tuple to : outbag) {
            assertEquals(1, to.size());
            assertEquals(new Double(0.0), to.get(0));
        }
    }

    @Test
    public void testMinFloat() throws Exception {
        Over func = new Over();
        DataBag inbag = BagFactory.getInstance().newDefaultBag();
        for (int i = 0; i < 10; i++) {
            Tuple t = TupleFactory.getInstance().newTuple(1);
            t.set(0, (float)i);
            inbag.add(t);
        }
        Tuple t = TupleFactory.getInstance().newTuple(2);
        t.set(0, inbag);
        t.set(1, "min(float)");
        DataBag outbag = func.exec(t);
        assertEquals(10, outbag.size());
        for (Tuple to : outbag) {
            assertEquals(1, to.size());
            assertEquals(new Float(0.0), to.get(0));
        }
    }

    @Test
    public void testMinInt() throws Exception {
        Over func = new Over();
        DataBag inbag = BagFactory.getInstance().newDefaultBag();
        for (int i = 0; i < 10; i++) {
            Tuple t = TupleFactory.getInstance().newTuple(1);
            t.set(0, i);
            inbag.add(t);
        }
        Tuple t = TupleFactory.getInstance().newTuple(2);
        t.set(0, inbag);
        t.set(1, "min(int)");
        DataBag outbag = func.exec(t);
        assertEquals(10, outbag.size());
        for (Tuple to : outbag) {
            assertEquals(1, to.size());
            assertEquals(new Integer(0), to.get(0));
        }
    }

    @Test
    public void testMinLong() throws Exception {
        Over func = new Over();
        DataBag inbag = BagFactory.getInstance().newDefaultBag();
        for (int i = 0; i < 10; i++) {
            Tuple t = TupleFactory.getInstance().newTuple(1);
            t.set(0, (long)i);
            inbag.add(t);
        }
        Tuple t = TupleFactory.getInstance().newTuple(2);
        t.set(0, inbag);
        t.set(1, "min(long)");
        DataBag outbag = func.exec(t);
        assertEquals(10, outbag.size());
        for (Tuple to : outbag) {
            assertEquals(1, to.size());
            assertEquals(new Long(0), to.get(0));
        }
    }

    @Test
    public void testMinString() throws Exception {
        Over func = new Over();
        DataBag inbag = BagFactory.getInstance().newDefaultBag();
        for (int i = 0; i < 10; i++) {
            Tuple t = TupleFactory.getInstance().newTuple(1);
            t.set(0, new Integer(i).toString());
            inbag.add(t);
        }
        Tuple t = TupleFactory.getInstance().newTuple(2);
        t.set(0, inbag);
        t.set(1, "min(chararray)");
        DataBag outbag = func.exec(t);
        assertEquals(10, outbag.size());
        for (Tuple to : outbag) {
            assertEquals(1, to.size());
            assertEquals("0", to.get(0));
        }
    }

    @Test
    public void testMaxDouble() throws Exception {
        Over func = new Over();
        DataBag inbag = BagFactory.getInstance().newDefaultBag();
        for (int i = 0; i < 10; i++) {
            Tuple t = TupleFactory.getInstance().newTuple(1);
            t.set(0, (double)i);
            inbag.add(t);
        }
        Tuple t = TupleFactory.getInstance().newTuple(2);
        t.set(0, inbag);
        t.set(1, "max(double)");
        DataBag outbag = func.exec(t);
        assertEquals(10, outbag.size());
        int count = 0;
        for (Tuple to : outbag) {
            assertEquals(1, to.size());
            assertEquals(new Double(count++), to.get(0));
        }
    }

    @Test
    public void testMaxByteArray() throws Exception {
        Over func = new Over();
        DataBag inbag = BagFactory.getInstance().newDefaultBag();
        for (int i = 0; i < 10; i++) {
            Tuple t = TupleFactory.getInstance().newTuple(1);
            t.set(0, new DataByteArray(new Integer(i).toString()));
            inbag.add(t);
        }
        Tuple t = TupleFactory.getInstance().newTuple(2);
        t.set(0, inbag);
        t.set(1, "max(bytearray)");
        DataBag outbag = func.exec(t);
        assertEquals(10, outbag.size());
        int count = 0;
        for (Tuple to : outbag) {
            assertEquals(1, to.size());
            assertEquals(new Double(count++), to.get(0));
        }
    }

    @Test
    public void testMaxFloat() throws Exception {
        Over func = new Over();
        DataBag inbag = BagFactory.getInstance().newDefaultBag();
        for (int i = 0; i < 10; i++) {
            Tuple t = TupleFactory.getInstance().newTuple(1);
            t.set(0, (float)i);
            inbag.add(t);
        }
        Tuple t = TupleFactory.getInstance().newTuple(2);
        t.set(0, inbag);
        t.set(1, "max(float)");
        DataBag outbag = func.exec(t);
        assertEquals(10, outbag.size());
        int count = 0;
        for (Tuple to : outbag) {
            assertEquals(1, to.size());
            assertEquals(new Float(count++), to.get(0));
        }
    }

    @Test
    public void testMaxInt() throws Exception {
        Over func = new Over();
        DataBag inbag = BagFactory.getInstance().newDefaultBag();
        for (int i = 0; i < 10; i++) {
            Tuple t = TupleFactory.getInstance().newTuple(1);
            t.set(0, i);
            inbag.add(t);
        }
        Tuple t = TupleFactory.getInstance().newTuple(2);
        t.set(0, inbag);
        t.set(1, "max(int)");
        DataBag outbag = func.exec(t);
        assertEquals(10, outbag.size());
        int count = 0;
        for (Tuple to : outbag) {
            assertEquals(1, to.size());
            assertEquals(new Integer(count++), to.get(0));
        }
    }

    @Test
    public void testMaxLong() throws Exception {
        Over func = new Over();
        DataBag inbag = BagFactory.getInstance().newDefaultBag();
        for (int i = 0; i < 10; i++) {
            Tuple t = TupleFactory.getInstance().newTuple(1);
            t.set(0, (long)i);
            inbag.add(t);
        }
        Tuple t = TupleFactory.getInstance().newTuple(2);
        t.set(0, inbag);
        t.set(1, "max(long)");
        DataBag outbag = func.exec(t);
        assertEquals(10, outbag.size());
        int count = 0;
        for (Tuple to : outbag) {
            assertEquals(1, to.size());
            assertEquals(new Long(count++), to.get(0));
        }
    }

    @Test
    public void testMaxString() throws Exception {
        Over func = new Over();
        DataBag inbag = BagFactory.getInstance().newDefaultBag();
        for (int i = 0; i < 10; i++) {
            Tuple t = TupleFactory.getInstance().newTuple(1);
            t.set(0, new Integer(i).toString());
            inbag.add(t);
        }
        Tuple t = TupleFactory.getInstance().newTuple(4);
        t.set(0, inbag);
        t.set(1, "max(chararray)");
        t.set(2, -1);
        t.set(3, -1);
        DataBag outbag = func.exec(t);
        assertEquals(10, outbag.size());
        for (Tuple to : outbag) {
            assertEquals(1, to.size());
            assertEquals("9", to.get(0));
        }
    }

    @Test
    public void testRowNumber() throws Exception {
        Over func = new Over();
        DataBag inbag = BagFactory.getInstance().newDefaultBag();
        for (int i = 0; i < 10; i++) {
            Tuple t = TupleFactory.getInstance().newTuple(1);
            t.set(0, (double)i);
            inbag.add(t);
        }
        Tuple t = TupleFactory.getInstance().newTuple(4);
        t.set(0, inbag);
        t.set(1, "row_number");
        t.set(2, -1);
        t.set(3, -1);
        DataBag outbag = func.exec(t);
        assertEquals(10, outbag.size());
        int count = 1;
        for (Tuple to : outbag) {
            assertEquals(1, to.size());
            assertEquals(new Integer(count++), to.get(0));
        }
    }

    @Test
    public void testFirstValue() throws Exception {
        Over func = new Over();
        DataBag inbag = BagFactory.getInstance().newDefaultBag();
        for (int i = 0; i < 10; i++) {
            Tuple t = TupleFactory.getInstance().newTuple(1);
            t.set(0, (double)i);
            inbag.add(t);
        }
        Tuple t = TupleFactory.getInstance().newTuple(2);
        t.set(0, inbag);
        t.set(1, "first_value");
        DataBag outbag = func.exec(t);
        assertEquals(10, outbag.size());
        for (Tuple to : outbag) {
            assertEquals(1, to.size());
            assertEquals(new Double(0.0), to.get(0));
        }
    }

    @Test
    public void testLastValue() throws Exception {
        Over func = new Over();
        DataBag inbag = BagFactory.getInstance().newDefaultBag();
        for (int i = 0; i < 10; i++) {
            Tuple t = TupleFactory.getInstance().newTuple(1);
            t.set(0, i);
            inbag.add(t);
        }
        Tuple t = TupleFactory.getInstance().newTuple(2);
        t.set(0, inbag);
        t.set(1, "last_value");
        DataBag outbag = func.exec(t);
        assertEquals(10, outbag.size());
        int count = 0;
        for (Tuple to : outbag) {
            assertEquals(1, to.size());
            assertEquals(new Integer(count++), to.get(0));
        }
    }

    @Test
    public void testLeadDefaults() throws Exception {
        Over func = new Over();
        DataBag inbag = BagFactory.getInstance().newDefaultBag();
        for (int i = 0; i < 10; i++) {
            Tuple t = TupleFactory.getInstance().newTuple(1);
            t.set(0, i);
            inbag.add(t);
        }
        Tuple t = TupleFactory.getInstance().newTuple(4);
        t.set(0, inbag);
        t.set(1, "lead");
        t.set(2, -1);
        t.set(3, -1);
        DataBag outbag = func.exec(t);
        assertEquals(10, outbag.size());
        int count = 1;
        for (Tuple to : outbag) {
            assertEquals(1, to.size());
            if (count < 10) assertEquals(new Integer(count++), to.get(0));
            else assertNull(to.get(0));
        }
    }

    @Test
    public void testLeadWithRowsAheadNoDefault() throws Exception {
        Over func = new Over();
        DataBag inbag = BagFactory.getInstance().newDefaultBag();
        for (int i = 0; i < 10; i++) {
            Tuple t = TupleFactory.getInstance().newTuple(1);
            t.set(0, i);
            inbag.add(t);
        }
        Tuple t = TupleFactory.getInstance().newTuple(5);
        t.set(0, inbag);
        t.set(1, "lead");
        t.set(2, -1);
        t.set(3, -1);
        t.set(4, 3);
        DataBag outbag = func.exec(t);
        assertEquals(10, outbag.size());
        int count = 3;
        for (Tuple to : outbag) {
            assertEquals(1, to.size());
            if (count < 10) assertEquals(new Integer(count++), to.get(0));
            else assertNull(to.get(0));
        }
    }

    @Test
    public void testLeadWithRowsAheadDefault() throws Exception {
        Over func = new Over();
        DataBag inbag = BagFactory.getInstance().newDefaultBag();
        for (int i = 0; i < 10; i++) {
            Tuple t = TupleFactory.getInstance().newTuple(1);
            t.set(0, i);
            inbag.add(t);
        }
        Tuple t = TupleFactory.getInstance().newTuple(6);
        t.set(0, inbag);
        t.set(1, "lead");
        t.set(2, -1);
        t.set(3, -1);
        t.set(4, 3);
        t.set(5, 99);
        DataBag outbag = func.exec(t);
        assertEquals(10, outbag.size());
        int count = 3;
        for (Tuple to : outbag) {
            assertEquals(1, to.size());
            if (count < 10) assertEquals(new Integer(count++), to.get(0));
            else assertEquals(new Integer(99), to.get(0));
        }
    }

    @Test
    public void testLagDefaults() throws Exception {
        Over func = new Over();
        DataBag inbag = BagFactory.getInstance().newDefaultBag();
        for (int i = 0; i < 10; i++) {
            Tuple t = TupleFactory.getInstance().newTuple(1);
            t.set(0, i);
            inbag.add(t);
        }
        Tuple t = TupleFactory.getInstance().newTuple(4);
        t.set(0, inbag);
        t.set(1, "lag");
        t.set(2, -1);
        t.set(3, -1);
        DataBag outbag = func.exec(t);
        assertEquals(10, outbag.size());
        int count = -1;
        for (Tuple to : outbag) {
            assertEquals(1, to.size());
            try {
                if (count >= 0) assertEquals(new Integer(count), to.get(0));
                else assertNull(to.get(0));
            } finally {
                count++;
            }
        }
    }

    @Test
    public void testLagWithRowsBehindNoDefault() throws Exception {
        Over func = new Over();
        DataBag inbag = BagFactory.getInstance().newDefaultBag();
        for (int i = 0; i < 10; i++) {
            Tuple t = TupleFactory.getInstance().newTuple(1);
            t.set(0, i);
            inbag.add(t);
        }
        Tuple t = TupleFactory.getInstance().newTuple(5);
        t.set(0, inbag);
        t.set(1, "lag");
        t.set(2, -1);
        t.set(3, -1);
        t.set(4, 3);
        DataBag outbag = func.exec(t);
        assertEquals(10, outbag.size());
        int count = -3;
        for (Tuple to : outbag) {
            assertEquals(1, to.size());
            try {
                if (count >= 0) assertEquals(new Integer(count), to.get(0));
                else assertNull(to.get(0));
            } finally {
                count++;
            }
        }
    }

    @Test
    public void testLagWithRowsBehindDefault() throws Exception {
        Over func = new Over();
        DataBag inbag = BagFactory.getInstance().newDefaultBag();
        for (int i = 0; i < 10; i++) {
            Tuple t = TupleFactory.getInstance().newTuple(1);
            t.set(0, i);
            inbag.add(t);
        }
        Tuple t = TupleFactory.getInstance().newTuple(6);
        t.set(0, inbag);
        t.set(1, "lag");
        t.set(2, -1);
        t.set(3, -1);
        t.set(4, 3);
        t.set(5, 99);
        DataBag outbag = func.exec(t);
        assertEquals(10, outbag.size());
        int count = -3;
        for (Tuple to : outbag) {
            assertEquals(1, to.size());
            try {
                if (count >= 0) assertEquals(new Integer(count), to.get(0));
                else assertEquals(new Integer(99), to.get(0));
            } finally {
                count++;
            }
        }
    }

    @Test
    public void testRankNoArgs() throws Exception {
        Over func = new Over();
        DataBag inbag = BagFactory.getInstance().newDefaultBag();
        for (int i = 0; i < 10; i++) {
            Tuple t = TupleFactory.getInstance().newTuple(1);
            t.set(0, i);
            inbag.add(t);
        }
        Tuple t = TupleFactory.getInstance().newTuple(2);
        t.set(0, inbag);
        t.set(1, "rank");
        boolean caught = false;
        try {
            DataBag outbag = func.exec(t);
        } catch (ExecException ioe) {
            caught = true;
            assertTrue(ioe.getMessage().contains("Rank args must contain"));
        }
        assertTrue(caught);
    }

    @Test
    public void testRankBadArgs() throws Exception {
        Over func = new Over();
        DataBag inbag = BagFactory.getInstance().newDefaultBag();
        for (int i = 0; i < 10; i++) {
            Tuple t = TupleFactory.getInstance().newTuple(1);
            t.set(0, i);
            inbag.add(t);
        }
        Tuple t = TupleFactory.getInstance().newTuple(5);
        t.set(0, inbag);
        t.set(1, "rank");
        t.set(2, -1);
        t.set(3, -1);
        t.set(4, "fred");
        boolean caught = false;
        try {
            DataBag outbag = func.exec(t);
        } catch (ExecException ioe) {
            caught = true;
            assertTrue(ioe.getMessage().contains("Rank expected column number"));
        }
        assertTrue(caught);
    }

    @Test
    public void testRankSimple() throws Exception {
        Over func = new Over();
        DataBag inbag = BagFactory.getInstance().newDefaultBag();
        Random r = new Random();
        for (int i = 0; i < 10; i++) {
            Tuple t = TupleFactory.getInstance().newTuple(2);
            t.set(0, i);
            t.set(1, r.nextInt(100));
            inbag.add(t);
        }
        Tuple t = TupleFactory.getInstance().newTuple(5);
        t.set(0, inbag);
        t.set(1, "rank");
        t.set(2, -1);
        t.set(3, -1);
        t.set(4, 0);
        DataBag outbag = func.exec(t);
        assertEquals(10, outbag.size());
        int count = 1;
        for (Tuple to : outbag) {
            assertEquals(1, to.size());
            assertEquals(count++, to.get(0));
        }
    }

    @Test
    public void testRankWithRepeatValues() throws Exception {
        Over func = new Over();
        DataBag inbag = BagFactory.getInstance().newDefaultBag();
        Random r = new Random();
        Tuple t = TupleFactory.getInstance().newTuple(2);
        t.set(0, null);
        t.set(1, r.nextInt(100));
        inbag.add(t);
        t = TupleFactory.getInstance().newTuple(2);
        t.set(0, null);
        t.set(1, r.nextInt(100));
        inbag.add(t);
        t = TupleFactory.getInstance().newTuple(2);
        t.set(0, 2);
        t.set(1, r.nextInt(100));
        inbag.add(t);
        t = TupleFactory.getInstance().newTuple(2);
        t.set(0, 5);
        t.set(1, r.nextInt(100));
        inbag.add(t);
        t = TupleFactory.getInstance().newTuple(2);
        t.set(0, 5);
        t.set(1, r.nextInt(100));
        inbag.add(t);
        t = TupleFactory.getInstance().newTuple(2);
        t.set(0, 5);
        t.set(1, r.nextInt(100));
        inbag.add(t);
        t = TupleFactory.getInstance().newTuple(2);
        t.set(0, 7);
        t.set(1, r.nextInt(100));
        inbag.add(t);

        t = TupleFactory.getInstance().newTuple(5);
        t.set(0, inbag);
        t.set(1, "rank");
        t.set(2, -1);
        t.set(3, -1);
        t.set(4, 0);
        DataBag outbag = func.exec(t);
        assertEquals(7, outbag.size());
        Iterator<Tuple> iter = outbag.iterator();
        t = iter.next();
        assertEquals(1, t.get(0));
        t = iter.next();
        assertEquals(1, t.get(0));
        t = iter.next();
        assertEquals(3, t.get(0));
        t = iter.next();
        assertEquals(4, t.get(0));
        t = iter.next();
        assertEquals(4, t.get(0));
        t = iter.next();
        assertEquals(4, t.get(0));
        t = iter.next();
        assertEquals(7, t.get(0));
    }

   @Test
    public void testRankWithMultiKey() throws Exception {
        Over func = new Over();
        DataBag inbag = BagFactory.getInstance().newDefaultBag();
        Random r = new Random();
        Tuple t = TupleFactory.getInstance().newTuple(3);
        t.set(0, null);
        t.set(1, r.nextInt(100));
        t.set(2, "a");
        inbag.add(t);
        t = TupleFactory.getInstance().newTuple(3);
        t.set(0, null);
        t.set(1, r.nextInt(100));
        t.set(2, "b");
        inbag.add(t);
        t = TupleFactory.getInstance().newTuple(3);
        t.set(0, 2);
        t.set(1, r.nextInt(100));
        t.set(2, "b");
        inbag.add(t);
        t = TupleFactory.getInstance().newTuple(3);
        t.set(0, 5);
        t.set(1, r.nextInt(100));
        inbag.add(t);
        t.set(2, "b");
        t = TupleFactory.getInstance().newTuple(3);
        t.set(0, 5);
        t.set(1, r.nextInt(100));
        t.set(2, "c");
        inbag.add(t);
        t = TupleFactory.getInstance().newTuple(3);
        t.set(0, 5);
        t.set(1, r.nextInt(100));
        t.set(2, "c");
        inbag.add(t);
        t = TupleFactory.getInstance().newTuple(3);
        t.set(0, 7);
        t.set(1, r.nextInt(100));
        t.set(2, "z");
        inbag.add(t);

        t = TupleFactory.getInstance().newTuple(6);
        t.set(0, inbag);
        t.set(1, "rank");
        t.set(2, -1);
        t.set(3, -1);
        t.set(4, 0);
        t.set(5, 2);
        DataBag outbag = func.exec(t);
        assertEquals(7, outbag.size());
        Iterator<Tuple> iter = outbag.iterator();
        t = iter.next();
        assertEquals(1, t.get(0));
        t = iter.next();
        assertEquals(2, t.get(0));
        t = iter.next();
        assertEquals(3, t.get(0));
        t = iter.next();
        assertEquals(4, t.get(0));
        t = iter.next();
        assertEquals(5, t.get(0));
        t = iter.next();
        assertEquals(5, t.get(0));
        t = iter.next();
        assertEquals(7, t.get(0));
    }

    @Test
    public void testDenseRankSimple() throws Exception {
        Over func = new Over();
        DataBag inbag = BagFactory.getInstance().newDefaultBag();
        Random r = new Random();
        for (int i = 0; i < 10; i++) {
            Tuple t = TupleFactory.getInstance().newTuple(2);
            t.set(0, i);
            t.set(1, r.nextInt(100));
            inbag.add(t);
        }
        Tuple t = TupleFactory.getInstance().newTuple(5);
        t.set(0, inbag);
        t.set(1, "dense_rank");
        t.set(2, -1);
        t.set(3, -1);
        t.set(4, 0);
        DataBag outbag = func.exec(t);
        assertEquals(10, outbag.size());
        int count = 1;
        for (Tuple to : outbag) {
            assertEquals(1, to.size());
            assertEquals(count++, to.get(0));
        }
    }

    @Test
    public void testDenseRankWithRepeatValues() throws Exception {
        Over func = new Over();
        DataBag inbag = BagFactory.getInstance().newDefaultBag();
        Random r = new Random();
        Tuple t = TupleFactory.getInstance().newTuple(2);
        t.set(0, null);
        t.set(1, r.nextInt(100));
        inbag.add(t);
        t = TupleFactory.getInstance().newTuple(2);
        t.set(0, null);
        t.set(1, r.nextInt(100));
        inbag.add(t);
        t = TupleFactory.getInstance().newTuple(2);
        t.set(0, 2);
        t.set(1, r.nextInt(100));
        inbag.add(t);
        t = TupleFactory.getInstance().newTuple(2);
        t.set(0, 5);
        t.set(1, r.nextInt(100));
        inbag.add(t);
        t = TupleFactory.getInstance().newTuple(2);
        t.set(0, 5);
        t.set(1, r.nextInt(100));
        inbag.add(t);
        t = TupleFactory.getInstance().newTuple(2);
        t.set(0, 5);
        t.set(1, r.nextInt(100));
        inbag.add(t);
        t = TupleFactory.getInstance().newTuple(2);
        t.set(0, 7);
        t.set(1, r.nextInt(100));
        inbag.add(t);

        t = TupleFactory.getInstance().newTuple(5);
        t.set(0, inbag);
        t.set(1, "dense_rank");
        t.set(2, -1);
        t.set(3, -1);
        t.set(4, 0);
        DataBag outbag = func.exec(t);
        assertEquals(7, outbag.size());
        Iterator<Tuple> iter = outbag.iterator();
        t = iter.next();
        assertEquals(1, t.get(0));
        t = iter.next();
        assertEquals(1, t.get(0));
        t = iter.next();
        assertEquals(2, t.get(0));
        t = iter.next();
        assertEquals(3, t.get(0));
        t = iter.next();
        assertEquals(3, t.get(0));
        t = iter.next();
        assertEquals(3, t.get(0));
        t = iter.next();
        assertEquals(4, t.get(0));
    }

   @Test
    public void testDenseRankWithMultiKey() throws Exception {
        Over func = new Over();
        DataBag inbag = BagFactory.getInstance().newDefaultBag();
        Random r = new Random();
        Tuple t = TupleFactory.getInstance().newTuple(3);
        t.set(0, null);
        t.set(1, r.nextInt(100));
        t.set(2, "a");
        inbag.add(t);
        t = TupleFactory.getInstance().newTuple(3);
        t.set(0, null);
        t.set(1, r.nextInt(100));
        t.set(2, "b");
        inbag.add(t);
        t = TupleFactory.getInstance().newTuple(3);
        t.set(0, 2);
        t.set(1, r.nextInt(100));
        t.set(2, "b");
        inbag.add(t);
        t = TupleFactory.getInstance().newTuple(3);
        t.set(0, 5);
        t.set(1, r.nextInt(100));
        inbag.add(t);
        t.set(2, "b");
        t = TupleFactory.getInstance().newTuple(3);
        t.set(0, 5);
        t.set(1, r.nextInt(100));
        t.set(2, "c");
        inbag.add(t);
        t = TupleFactory.getInstance().newTuple(3);
        t.set(0, 5);
        t.set(1, r.nextInt(100));
        t.set(2, "c");
        inbag.add(t);
        t = TupleFactory.getInstance().newTuple(3);
        t.set(0, 7);
        t.set(1, r.nextInt(100));
        t.set(2, "z");
        inbag.add(t);

        t = TupleFactory.getInstance().newTuple(6);
        t.set(0, inbag);
        t.set(1, "dense_rank");
        t.set(2, -1);
        t.set(3, -1);
        t.set(4, 0);
        t.set(5, 2);
        DataBag outbag = func.exec(t);
        assertEquals(7, outbag.size());
        Iterator<Tuple> iter = outbag.iterator();
        t = iter.next();
        assertEquals(1, t.get(0));
        t = iter.next();
        assertEquals(2, t.get(0));
        t = iter.next();
        assertEquals(3, t.get(0));
        t = iter.next();
        assertEquals(4, t.get(0));
        t = iter.next();
        assertEquals(5, t.get(0));
        t = iter.next();
        assertEquals(5, t.get(0));
        t = iter.next();
        assertEquals(6, t.get(0));
    }

    @Test
    public void testNtileNoArgs() throws Exception {
        Over func = new Over();
        DataBag inbag = BagFactory.getInstance().newDefaultBag();
        for (int i = 0; i < 10; i++) {
            Tuple t = TupleFactory.getInstance().newTuple(1);
            t.set(0, i);
            inbag.add(t);
        }
        Tuple t = TupleFactory.getInstance().newTuple(2);
        t.set(0, inbag);
        t.set(1, "ntile");
        boolean caught = false;
        try {
            DataBag outbag = func.exec(t);
        } catch (ExecException ioe) {
            caught = true;
            assertTrue(ioe.getMessage().contains("Ntile args must contain"));
        }
        assertTrue(caught);
    }

    @Test
    public void testNtileBadArgs() throws Exception {
        Over func = new Over();
        DataBag inbag = BagFactory.getInstance().newDefaultBag();
        for (int i = 0; i < 10; i++) {
            Tuple t = TupleFactory.getInstance().newTuple(1);
            t.set(0, i);
            inbag.add(t);
        }
        Tuple t = TupleFactory.getInstance().newTuple(5);
        t.set(0, inbag);
        t.set(1, "ntile");
        t.set(2, -1);
        t.set(3, -1);
        t.set(4, "fred");
        boolean caught = false;
        try {
            DataBag outbag = func.exec(t);
        } catch (ExecException ioe) {
            caught = true;
            assertTrue(ioe.getMessage().contains("Ntile expected integer"));
        }
        assertTrue(caught);
    }

    @Test
    public void testNtileFour() throws Exception {
        Over func = new Over();
        DataBag inbag = BagFactory.getInstance().newDefaultBag();
        Random r = new Random();
        for (int i = 0; i < 10; i++) {
            Tuple t = TupleFactory.getInstance().newTuple(2);
            t.set(0, i);
            t.set(1, r.nextInt(100));
            inbag.add(t);
        }
        Tuple t = TupleFactory.getInstance().newTuple(5);
        t.set(0, inbag);
        t.set(1, "ntile");
        t.set(2, -1);
        t.set(3, -1);
        t.set(4, 4);
        DataBag outbag = func.exec(t);
        assertEquals(10, outbag.size());
        int count = 0;
        for (Tuple to : outbag) {
            assertEquals(1, to.size());
            if (count < 3) assertEquals(1, to.get(0));
            else if (count < 5) assertEquals(2, to.get(0));
            else if (count < 8) assertEquals(3, to.get(0));
            else assertEquals(4, to.get(0));
            count++;
        }
    }

    @Test
    public void testNtileTen() throws Exception {
        Over func = new Over();
        DataBag inbag = BagFactory.getInstance().newDefaultBag();
        Random r = new Random();
        for (int i = 0; i < 10; i++) {
            Tuple t = TupleFactory.getInstance().newTuple(2);
            t.set(0, i);
            t.set(1, r.nextInt(100));
            inbag.add(t);
        }
        Tuple t = TupleFactory.getInstance().newTuple(5);
        t.set(0, inbag);
        t.set(1, "ntile");
        t.set(2, -1);
        t.set(3, -1);
        t.set(4, 10);
        DataBag outbag = func.exec(t);
        assertEquals(10, outbag.size());
        int count = 1;
        for (Tuple to : outbag) {
            assertEquals(count, to.get(0));
            count++;
        }
    }

    @Test
    public void testNtileHundred() throws Exception {
        Over func = new Over();
        DataBag inbag = BagFactory.getInstance().newDefaultBag();
        Random r = new Random();
        for (int i = 0; i < 10; i++) {
            Tuple t = TupleFactory.getInstance().newTuple(2);
            t.set(0, i);
            t.set(1, r.nextInt(100));
            inbag.add(t);
        }
        Tuple t = TupleFactory.getInstance().newTuple(5);
        t.set(0, inbag);
        t.set(1, "ntile");
        t.set(2, -1);
        t.set(3, -1);
        t.set(4, 100);
        DataBag outbag = func.exec(t);
        assertEquals(10, outbag.size());
        int count = 1;
        for (Tuple to : outbag) {
            assertEquals(count, to.get(0));
            count++;
        }
    }

    @Test
    public void testPercentRankSimple() throws Exception {
        Over func = new Over();
        DataBag inbag = BagFactory.getInstance().newDefaultBag();
        Random r = new Random();
        for (int i = 0; i < 10; i++) {
            Tuple t = TupleFactory.getInstance().newTuple(2);
            t.set(0, i);
            t.set(1, r.nextInt(100));
            inbag.add(t);
        }
        Tuple t = TupleFactory.getInstance().newTuple(5);
        t.set(0, inbag);
        t.set(1, "percent_rank");
        t.set(2, -1);
        t.set(3, -1);
        t.set(4, 0);
        DataBag outbag = func.exec(t);
        assertEquals(10, outbag.size());
        int count = 0;
        for (Tuple to : outbag) {
            assertEquals(1, to.size());
            assertEquals(count/9.0, to.get(0));
            count++;
        }
    }

    @Test
    public void testPercentRankWithRepeatValues() throws Exception {
        Over func = new Over();
        DataBag inbag = BagFactory.getInstance().newDefaultBag();
        Random r = new Random();
        Tuple t = TupleFactory.getInstance().newTuple(2);
        t.set(0, null);
        t.set(1, r.nextInt(100));
        inbag.add(t);
        t = TupleFactory.getInstance().newTuple(2);
        t.set(0, null);
        t.set(1, r.nextInt(100));
        inbag.add(t);
        t = TupleFactory.getInstance().newTuple(2);
        t.set(0, 2);
        t.set(1, r.nextInt(100));
        inbag.add(t);
        t = TupleFactory.getInstance().newTuple(2);
        t.set(0, 5);
        t.set(1, r.nextInt(100));
        inbag.add(t);
        t = TupleFactory.getInstance().newTuple(2);
        t.set(0, 5);
        t.set(1, r.nextInt(100));
        inbag.add(t);
        t = TupleFactory.getInstance().newTuple(2);
        t.set(0, 5);
        t.set(1, r.nextInt(100));
        inbag.add(t);
        t = TupleFactory.getInstance().newTuple(2);
        t.set(0, 7);
        t.set(1, r.nextInt(100));
        inbag.add(t);

        t = TupleFactory.getInstance().newTuple(5);
        t.set(0, inbag);
        t.set(1, "percent_rank");
        t.set(2, -1);
        t.set(3, -1);
        t.set(4, 0);
        DataBag outbag = func.exec(t);
        assertEquals(7, outbag.size());
        Iterator<Tuple> iter = outbag.iterator();
        t = iter.next();
        assertEquals(0.0, t.get(0));
        t = iter.next();
        assertEquals(0.0, t.get(0));
        t = iter.next();
        assertEquals(0.3333333333333333, t.get(0));
        t = iter.next();
        assertEquals(0.5, t.get(0));
        t = iter.next();
        assertEquals(0.5, t.get(0));
        t = iter.next();
        assertEquals(0.5, t.get(0));
        t = iter.next();
        assertEquals(1.0, t.get(0));
    }

    @Test
    public void testCumeDistSimple() throws Exception {
        Over func = new Over();
        DataBag inbag = BagFactory.getInstance().newDefaultBag();
        Random r = new Random();
        for (int i = 0; i < 10; i++) {
            Tuple t = TupleFactory.getInstance().newTuple(2);
            t.set(0, i);
            t.set(1, r.nextInt(100));
            inbag.add(t);
        }
        Tuple t = TupleFactory.getInstance().newTuple(5);
        t.set(0, inbag);
        t.set(1, "cume_dist");
        t.set(2, -1);
        t.set(3, -1);
        t.set(4, 0);
        DataBag outbag = func.exec(t);
        assertEquals(10, outbag.size());
        int count = 1;
        for (Tuple to : outbag) {
            assertEquals(1, to.size());
            assertEquals(count/10.0, to.get(0));
            count++;
        }
    }

    @Test
    public void testCumeDistWithRepeatValues() throws Exception {
        Over func = new Over();
        DataBag inbag = BagFactory.getInstance().newDefaultBag();
        Random r = new Random();
        Tuple t = TupleFactory.getInstance().newTuple(2);
        t.set(0, null);
        t.set(1, r.nextInt(100));
        inbag.add(t);
        t = TupleFactory.getInstance().newTuple(2);
        t.set(0, null);
        t.set(1, r.nextInt(100));
        inbag.add(t);
        t = TupleFactory.getInstance().newTuple(2);
        t.set(0, 2);
        t.set(1, r.nextInt(100));
        inbag.add(t);
        t = TupleFactory.getInstance().newTuple(2);
        t.set(0, 5);
        t.set(1, r.nextInt(100));
        inbag.add(t);
        t = TupleFactory.getInstance().newTuple(2);
        t.set(0, 5);
        t.set(1, r.nextInt(100));
        inbag.add(t);
        t = TupleFactory.getInstance().newTuple(2);
        t.set(0, 5);
        t.set(1, r.nextInt(100));
        inbag.add(t);
        t = TupleFactory.getInstance().newTuple(2);
        t.set(0, 7);
        t.set(1, r.nextInt(100));
        inbag.add(t);

        t = TupleFactory.getInstance().newTuple(5);
        t.set(0, inbag);
        t.set(1, "cume_dist");
        t.set(2, -1);
        t.set(3, -1);
        t.set(4, 0);
        DataBag outbag = func.exec(t);
        assertEquals(7, outbag.size());
        int count = 1;
        for (Tuple to : outbag) {
            assertEquals(1, to.size());
            assertEquals(count/7.0, to.get(0));
            count++;
        }
        /*
        Iterator<Tuple> iter = outbag.iterator();
        t = iter.next();
        assertEquals(0.14285714285714285, t.get(0));
        t = iter.next();
        assertEquals(0.14285714285714285, t.get(0));
        t = iter.next();
        assertEquals(0.42857142857142855, t.get(0));
        t = iter.next();
        assertEquals(0.5714285714285714, t.get(0));
        t = iter.next();
        assertEquals(0.5714285714285714, t.get(0));
        t = iter.next();
        assertEquals(0.5714285714285714, t.get(0));
        t = iter.next();
        assertEquals(1.0, t.get(0));
        */
    }
}

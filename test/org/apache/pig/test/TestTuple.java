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
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.BinInterSedes;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DefaultBagFactory;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.util.TupleFormat;
import org.junit.Test;

public class TestTuple {
    private static final TupleFactory mTupleFactory = TupleFactory.getInstance();
    private static final Random random = new Random(100L);
    private static final BinInterSedes bis = new BinInterSedes();

    @Test
    public void testTupleFormat() {

        try {
            Tuple tuple = mTupleFactory.newTuple(7);
            tuple.set(0, 12);
            Map<String, String> map = new HashMap<String, String>();
            map.put("pig", "scalability");
            tuple.set(1, map);
            tuple.set(2, null);
            tuple.set(3, 12L);
            tuple.set(4, 1.2F);

            Tuple innerTuple = mTupleFactory.newTuple(1);
            innerTuple.set(0, "innerTuple");
            tuple.set(5, innerTuple);

            DataBag bag = BagFactory.getInstance().newDefaultBag();
            bag.add(innerTuple);
            tuple.set(6, bag);

            assertEquals(
                    "(12,[pig#scalability],,12,1.2,(innerTuple),{(innerTuple)})",
                    TupleFormat.format(tuple));
        } catch (ExecException e) {
            e.printStackTrace();
            fail();
        }

    }

    @Test
    public void testEmptyTupleSize() {
        Tuple t = mTupleFactory.newTuple();
        long size = t.getMemorySize();
        assertEquals("tuple size",size, 96);
    }
    
    @Test
    public void testEmptyBagSize() {
        DataBag bag = DefaultBagFactory.getInstance().newDefaultBag();
        long size = bag.getMemorySize();
        assertEquals("bag size",size, 124);
    }
    
    @Test
    // See PIG-1443
    public void testTupleSizeWithString() {
        Tuple t = Util.createTuple(new String[] {"1234567", "bar"});
        long size = t.getMemorySize();
        assertEquals("tuple size",size, 200);
    }
    
    @Test
    public void testTupleSizeWithByteArrays() {
        Tuple t = mTupleFactory.newTuple();
        t.append(new DataByteArray("1234567"));
        t.append(new DataByteArray("bar"));
        long size = t.getMemorySize();
        assertEquals("tuple size",size, 168);
    }

    @Test
    public void testTupleSizeWithDoubles() {
        Tuple t = mTupleFactory.newTuple();
        t.append(new Double(0.1));
        t.append(new Double(2000.10001));
        long size = t.getMemorySize();
        assertEquals("tuple size",size, 128);
    }

    @Test
    public void testTupleSizeWithFloats() {
        Tuple t = mTupleFactory.newTuple();
        t.append(new Float(0.1F));
        t.append(new Float(2000.10001F));
        long size = t.getMemorySize();
        assertEquals("tuple size",size, 128);
    }
    
    @Test
    public void testTupleSizeWithLongs() {
        Tuple t = mTupleFactory.newTuple();
        t.append(new Long(100));
        t.append(new Long(2000));
        long size = t.getMemorySize();
        assertEquals("tuple size",size, 128);
    }
    
    @Test
    public void testTupleSizeWithBooleans() {
        Tuple t = mTupleFactory.newTuple();
        t.append(new Boolean(true));
        t.append(new Boolean(false));
        long size = t.getMemorySize();
        assertEquals("tuple size",size, 128);
    }    
    
    @Test
    public void testTupleIterator() {
        Tuple t = mTupleFactory.newTuple();
        Random r = new Random();
        for (int i = 0; i < 1000; i++) {
            t.append(r.nextLong());
        }
        for (int i = 0; i < 1000; i++) {
            t.append(r.nextInt());
        }
        int i = 0;
        for (Object o : t) {
            try {
                assertEquals("Element " + i, t.get(i++), o);
            } catch (ExecException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Test
    public void testToDelimitedString() {
        Tuple t = mTupleFactory.newTuple();
        t.append(new Integer(1));
        t.append(new Long(2));
        t.append(new Float(1.1f));
        t.append(new Double(2.3));
        t.append("howdy howdy howdy");
        t.append(null);
        t.append("woah there");
        t.append(new Double(2000000.3000000001));
        t.append(new Float(1000000000.1000001f));
        t.append(new Long(2001010101));
        t.append(new Integer(100010101));
        try {
            String expected = "1,2,1.1,2.3,howdy howdy howdy,,woah there,2000000.3,1.0E9,2001010101,100010101";
            assertEquals(expected, t.toDelimitedString(","));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testTupleSerDe() throws Exception {
        for (int i = 0; i < 1000; i++) {
            Tuple t = mTupleFactory.newTuple(i);
            for (int j = 0; j < i; j++) {
                t.set(j,Integer.valueOf(random.nextInt()));
            }
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutput out = new DataOutputStream(baos);
            bis.writeDatum(out, t);

            DataInput in = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
            Tuple t2 = (Tuple) bis.readDatum(in);

            assertEquals(t, t2);
        }
    }

}

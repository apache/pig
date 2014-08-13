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

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

public class TestStitch {

    @Test
    public void testSchema() throws Exception {
        Schema s = new Schema();
        Schema in = new Schema();
        s.add(new FieldSchema("x", DataType.CHARARRAY));
        s.add(new FieldSchema("y", DataType.INTEGER));
        in.add(new FieldSchema("A", s, DataType.BAG));
        FieldSchema fs = new FieldSchema("Over",
                new Schema(Schema.generateNestedSchema(DataType.BAG,
                        DataType.NULL)), DataType.BAG);
        in.add(fs);
        Stitch func = new Stitch();
        Schema out = func.outputSchema(in);
        assertEquals("{stitched: {x: chararray,y: int,{NULL}}}", out.toString());
    }

    @Test
    public void testSchema2() throws Exception {
        Schema t = new Schema();
        Schema b = new Schema();
        t.add(new FieldSchema("x", DataType.CHARARRAY));
        t.add(new FieldSchema("y", DataType.INTEGER));
        b.add(new FieldSchema("A", t, DataType.TUPLE));
        Schema in = new Schema(new FieldSchema("", b, DataType.BAG));
        FieldSchema fs = new FieldSchema("Over",
                new Schema(Schema.generateNestedSchema(DataType.BAG,
                        DataType.NULL)), DataType.BAG);
        in.add(fs);
        Stitch func = new Stitch();
        Schema out = func.outputSchema(in);
        assertEquals("{stitched: {x: chararray,y: int,{NULL}}}", out.toString());
    }


    @Test
    public void testNoInput() throws Exception {
        Stitch func = new Stitch();
        assertNull(func.exec(null));
        Tuple t = TupleFactory.getInstance().newTuple();
        assertNull(func.exec(t));
    }

    @Test
    public void testBadInput() throws Exception {
        Stitch func = new Stitch();
        boolean caught = false;
        Tuple t = TupleFactory.getInstance().newTuple();
        t.append("Mary had a little lamb");
        try {
            func.exec(t);
        } catch (ExecException ee) {
            caught = true;
        }
        assertTrue(caught);

        DataBag b = BagFactory.getInstance().newDefaultBag();
        b.add(t);
        t = TupleFactory.getInstance().newTuple();
        t.append(b);
        t.append("its fleece was white as snow");
        caught = false;
         try {
            func.exec(t);
        } catch (ExecException ee) {
            caught = true;
        }
        assertTrue(caught);
    }

    @Test
    public void testSingleInput() throws Exception {
        Stitch func = new Stitch();
        Tuple t = TupleFactory.getInstance().newTuple();
        t.append(new Integer(1));
        DataBag b = BagFactory.getInstance().newDefaultBag();
        b.add(t);
        t = TupleFactory.getInstance().newTuple();
        t.append(b);
        DataBag out = func.exec(t);
        assertEquals(1, out.size());
        assertEquals(new Integer(1), out.iterator().next().get(0));
    }
    
    @Test
    public void testDoubleInput() throws Exception {
        Stitch func = new Stitch();
        DataBag b1 = BagFactory.getInstance().newDefaultBag();
        Tuple t = TupleFactory.getInstance().newTuple();
        t.append("a");
        t.append("b");
        b1.add(t);
        t = TupleFactory.getInstance().newTuple();
        t.append("c");
        t.append("d");
        b1.add(t);
        
        DataBag b2 = BagFactory.getInstance().newDefaultBag();
        t = TupleFactory.getInstance().newTuple();
        t.append("1");
        t.append("2");
        b2.add(t);
        t = TupleFactory.getInstance().newTuple();
        t.append("3");
        t.append("4");
        b2.add(t);

        t = TupleFactory.getInstance().newTuple();
        t.append(b1);
        t.append(b2);
        DataBag out = func.exec(t);
        assertEquals(2, out.size());
        Iterator<Tuple> iter = out.iterator();
        t = iter.next();
        assertEquals(4, t.size());
        assertEquals("a", t.get(0));
        assertEquals("b", t.get(1));
        assertEquals("1", t.get(2));
        assertEquals("2", t.get(3));
        t = iter.next();
        assertEquals(4, t.size());
        assertEquals("c", t.get(0));
        assertEquals("d", t.get(1));
        assertEquals("3", t.get(2));
        assertEquals("4", t.get(3));
    }

    @Test
    public void testSecondShort() throws Exception {
        Stitch func = new Stitch();
        DataBag b1 = BagFactory.getInstance().newDefaultBag();
        Tuple t = TupleFactory.getInstance().newTuple();
        t.append("a");
        t.append("b");
        b1.add(t);
        t = TupleFactory.getInstance().newTuple();
        t.append("c");
        t.append("d");
        b1.add(t);
        
        DataBag b2 = BagFactory.getInstance().newDefaultBag();
        t = TupleFactory.getInstance().newTuple();
        t.append("1");
        t.append("2");
        b2.add(t);

        t = TupleFactory.getInstance().newTuple();
        t.append(b1);
        t.append(b2);
        DataBag out = func.exec(t);
        assertEquals(2, out.size());
        Iterator<Tuple> iter = out.iterator();
        t = iter.next();
        assertEquals(4, t.size());
        assertEquals("a", t.get(0));
        assertEquals("b", t.get(1));
        assertEquals("1", t.get(2));
        assertEquals("2", t.get(3));
        t = iter.next();
        assertEquals(2, t.size());
        assertEquals("c", t.get(0));
        assertEquals("d", t.get(1));
    }

    @Test
    public void testFirstShort() throws Exception {
        Stitch func = new Stitch();
        DataBag b1 = BagFactory.getInstance().newDefaultBag();
        Tuple t = TupleFactory.getInstance().newTuple();
        t.append("a");
        t.append("b");
        b1.add(t);
        
        DataBag b2 = BagFactory.getInstance().newDefaultBag();
        t = TupleFactory.getInstance().newTuple();
        t.append("1");
        t.append("2");
        b2.add(t);
        t = TupleFactory.getInstance().newTuple();
        t.append("3");
        t.append("4");
        b2.add(t);

        t = TupleFactory.getInstance().newTuple();
        t.append(b1);
        t.append(b2);
        DataBag out = func.exec(t);
        assertEquals(1, out.size());
        Iterator<Tuple> iter = out.iterator();
        t = iter.next();
        assertEquals(4, t.size());
        assertEquals("a", t.get(0));
        assertEquals("b", t.get(1));
        assertEquals("1", t.get(2));
        assertEquals("2", t.get(3));
    }

          
}

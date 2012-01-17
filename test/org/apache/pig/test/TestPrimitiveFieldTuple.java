package org.apache.pig.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.pig.data.InterSedes;
import org.apache.pig.data.InterSedesFactory;
import org.apache.pig.data.PDoubleTuple;
import org.apache.pig.data.PFloatTuple;
import org.apache.pig.data.PIntTuple;
import org.apache.pig.data.PLongTuple;
import org.apache.pig.data.PStringTuple;
import org.apache.pig.data.PrimitiveFieldTuple;
import org.apache.pig.data.Tuple;
import org.junit.Test;

public class TestPrimitiveFieldTuple {
    private static final InterSedes sedes = InterSedesFactory.getInterSedesInstance();


    @Test
    public void testPIntTuple() throws IOException, InstantiationException, IllegalAccessException {
        PrimitiveFieldTuple intTuple = new PIntTuple();
        intTuple.append(12);
        assertEquals(12, intTuple.get(0));
        intTuple.set(0, 13);
        assertEquals(13, intTuple.get(0));
        // appending one more field should fail.
        try {
            intTuple.append(14);
            fail();
        } catch (RuntimeException e) {
            //expected
        }
        checkSerialization(intTuple);
        intTuple.set(0, null);
        checkSerialization(intTuple);


    }

    @Test
    public void testPStringTuple() throws IOException, InstantiationException, IllegalAccessException {
        PrimitiveFieldTuple tuple = new PStringTuple();
        tuple.append("foo");
        assertEquals("foo", tuple.get(0));
        tuple.set(0, "bar");
        assertEquals("bar", tuple.get(0));
        // appending one more field should fail.
        try {
            tuple.append("baz");
            fail();
        } catch (RuntimeException e) {
            //expected
        }
        checkSerialization(tuple);
    }

    @Test
    public void testPLongTuple() throws IOException, InstantiationException, IllegalAccessException {
        PrimitiveFieldTuple tuple = new PLongTuple();
        tuple.append(12);
        assertEquals(12L, tuple.get(0));
        tuple.set(0, 13.0);
        assertEquals(13L, tuple.get(0));
        // appending one more field should fail.
        try {
            tuple.append(14.0);
            fail();
        } catch (RuntimeException e) {
            //expected
        }
        checkSerialization(tuple);
    }

    @Test
    public void testPDoubleTuple() throws IOException, InstantiationException, IllegalAccessException {
        PrimitiveFieldTuple tuple = new PDoubleTuple();
        tuple.append(12.0);
        assertEquals(12.0, tuple.get(0));
        tuple.set(0, 13.0);
        assertEquals(13.0, tuple.get(0));
        // appending one more field should fail.
        try {
            tuple.append(14.0);
            fail();
        } catch (RuntimeException e) {
            //expected
        }
        checkSerialization(tuple);
    }

    @Test
    public void testPFloatTuple() throws IOException, InstantiationException, IllegalAccessException {
        PrimitiveFieldTuple tuple = new PFloatTuple();
        tuple.append(12.0f);
        assertEquals(12.0f, tuple.get(0));
        tuple.set(0, 13.0f);
        assertEquals(13.0f, tuple.get(0));
        // appending one more field should fail.
        try {
            tuple.append(14.0);
            fail();
        } catch (RuntimeException e) {
            //expected
        }
        checkSerialization(tuple);
    }


    @SuppressWarnings("unchecked")
    private void checkSerialization(Tuple from) throws IOException, InstantiationException, IllegalAccessException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        from.write(dos);
        byte[] bytes = baos.toByteArray();
        Tuple to = (Tuple) sedes.readDatum(new DataInputStream(new ByteArrayInputStream(bytes)));
        assertEquals(from, to);
        assertEquals(0, PrimitiveFieldTuple.getComparatorClass().newInstance().compare(from, to));
    }
}

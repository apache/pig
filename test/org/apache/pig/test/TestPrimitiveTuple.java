package org.apache.pig.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataType;
import org.apache.pig.data.InterSedes;
import org.apache.pig.data.InterSedesFactory;
import org.apache.pig.data.PrimitiveTuple;
import org.apache.pig.data.Tuple;
import org.junit.Test;

public class TestPrimitiveTuple {
    private static final InterSedes sedes = InterSedesFactory.getInterSedesInstance();

    private static final int INT = 54667;
    private static final long LONG = 12L;
    private static final Float FLOAT = 1.1f;
    private static final double DOUBLE = 12.1d;

    @Test
    public void testValidFields() throws ExecException {
        Tuple t = new PrimitiveTuple(DataType.INTEGER, DataType.LONG, DataType.FLOAT, DataType.DOUBLE);
        t.set(0, INT);
        assertEquals(INT, t.get(0));
        t.set(1, LONG);
        assertEquals(LONG, t.get(1));
        t.set(2, FLOAT);
        assertEquals(FLOAT, t.get(2));
        t.set(3, DOUBLE);
        assertEquals(DOUBLE, t.get(3));
        // make sure we didn't squash anything
        assertEquals(INT, t.get(0));
        assertEquals(LONG, t.get(1));
        assertEquals(FLOAT, t.get(2));
    }

    @Test
    public void testInvalidFields() {
        Tuple t = new PrimitiveTuple(DataType.INTEGER, DataType.LONG, DataType.FLOAT, DataType.DOUBLE);
        try {
            t.set(0, LONG);
            fail("Setting LONG when INT is expected should fail");
        } catch (ExecException e) {
            // expected.
        }
        try {
            t.set(5, INT);
            fail("Setting an out-of-bounds field should fail");
        } catch (ExecException e) {
            //expected
        }
    }
    @Test
    public void testSerialization() throws IOException {
        PrimitiveTuple t = new PrimitiveTuple(DataType.INTEGER, DataType.LONG, DataType.FLOAT, DataType.DOUBLE);
        t.setInt(0, 1);
        t.setLong(1, 2L);
        t.setFloat(2, 0.3f);
        t.setDouble(3, 0.2d);

        // check that native read / write methods work
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        t.write(dos);
        PrimitiveTuple t2 = (PrimitiveTuple) sedes.readDatum(new DataInputStream(new ByteArrayInputStream(baos.toByteArray())));
        assertEquals(t, t2);

        // check that BinInterSedes works too
        InterSedes sedes = InterSedesFactory.getInterSedesInstance();
        ByteArrayOutputStream baos2 = new ByteArrayOutputStream();
        DataOutputStream dos2 = new DataOutputStream(baos2);
        t.write(dos2);
        Tuple t3 = (Tuple) sedes.readDatum(new DataInputStream(new ByteArrayInputStream(baos2.toByteArray())));
        assertEquals(t, t3);

    }
}

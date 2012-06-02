package org.apache.pig.data;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.lang.reflect.Method;
import java.util.Random;

import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.data.utils.MethodHelper;
import org.apache.pig.impl.util.Utils;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

public class TestTupleSemantics {
    @Test
    public void testDefaultTupleFactory() throws Exception {
        testFactory(TupleFactory.getInstance());
    }

    //@Test
    public void testSchemaTupleFactoryNotAppendable() throws Exception {
        Schema s = Utils.getSchemaFromString("int, long, float, double, boolean, bytearray, chararray, (int, long, float, double, boolean, bytearray, chararray)");
        int id = SchemaTupleClassGenerator.generateAndAddToJar(s, false);
        testFactory(TupleFactory.getInstanceForSchemaId(id));

        s = Utils.getSchemaFromString("(long,(long),(long),(long),(long,(long),(long),(long,(long,(long),(long),(long)))))");
        id = SchemaTupleClassGenerator.generateAndAddToJar(s, false);
        testFactory(TupleFactory.getInstanceForSchemaId(id));
    }

    //@Test
    public void testSchemaTupleFactoryAppendable() throws Exception {
        Schema s = Utils.getSchemaFromString("int, long, float, double, boolean, bytearray, chararray, (int, long, float, double, boolean, bytearray, chararray)");
        int id = SchemaTupleClassGenerator.generateAndAddToJar(s, true);

        testFactory(TupleFactory.getInstanceForSchemaId(id));
        s = Utils.getSchemaFromString("(long,(long),(long),(long),(long,(long),(long),(long,(long,(long),(long),(long)))))");
        id = SchemaTupleClassGenerator.generateAndAddToJar(s, true);
        testFactory(TupleFactory.getInstanceForSchemaId(id));
    }


    public void testFactory(TupleFactory tf) throws Exception {
        testNewTuple(tf);
        testNewTupleFromList(tf);
        testNewTupleFromListNoCopy(tf);
        testNewTupleFromObject(tf);

        testComparison(tf);
        testRawComparison(tf);
        testAppend(tf);
        testSerDe(tf);
        testGetType(tf);
        testSetGet(tf);
        testIsNull(tf);
        testSize(tf);
    }

    public boolean isNotImplemented(Class clazz, Class objClass, String name, Class... params) throws Exception {
        Method m = objClass.getMethod(name, params);
        return MethodHelper.isNotImplementedAnnotationPresent(m, clazz);
    }

    public void testNewTuple(TupleFactory tf) throws Exception {
        if (isNotImplemented(tf.getClass(), TupleFactory.class, "newTuple")) {
            System.out.println("Skipping test testNewTuple for TupleFactory class: " + tf.getClass());
            return;
        }
    }

    public void testNewTupleFromList(TupleFactory tf) throws Exception {
        if (isNotImplemented(tf.getClass(), TupleFactory.class, "newTuple", List.class)) {
            System.out.println("Skipping test testNuewTupleFromListNoCopy for TupleFactory class: " + tf.getClass());
            return;
        }
    }

    public void testNewTupleFromListNoCopy(TupleFactory tf) throws Exception {
        if (isNotImplemented(tf.getClass(), TupleFactory.class, "newTupleNoCopy", List.class)) {
            System.out.println("Skipping test testNuewTupleFromListNoCopy for TupleFactory class: " + tf.getClass());
            return;
        }
    }

    public void testNewTupleFromObject(TupleFactory tf) throws Exception {
        if (isNotImplemented(tf.getClass(), TupleFactory.class, "newTuple", Object.class)) {
            System.out.println("Skipping test testNewTupleFromObject for TupleFactory class: " + tf.getClass());
            return;
        }
    }

    public void testComparison(TupleFactory tf) {
    }

    public void testRawComparison(TupleFactory tf) {
    }

    public void testAppend(TupleFactory tf) throws Exception {
        if (isNotImplemented(tf.getClass(), Tuple.class, "append", Object.class)) {
            System.out.println("Skipping test testAppend for TupleFactory class: " + tf.getClass());
            return;
        }
    }

    public void testSerDe(TupleFactory tf) {
    }

    public void testGetType(TupleFactory tf) {
    }

    public void testIsNull(TupleFactory tf) {
    }

    public void testSize(TupleFactory tf) throws Exception {
        if (isNotImplemented(tf.getClass(), TupleFactory.class, "newTuple", Integer.TYPE)) {
            System.out.println("Skipping test testSize for TupleFactory class: " + tf.getClass());
            return;
        }
        Tuple t = tf.newTuple(10);
        assertEquals(10, t.size());

        for (int i = 0; i < 10; i++) {
            t.append(i);
        }

        assertEquals(20, t.size());

        for (int i = 0; i < 10; i++) {
            assertNull(t.get(i));
        }

        for (int i = 10; i < 20; i++) {
            assertEquals(i - 10, ((Integer)t.get(i)).intValue());
        }

        for (int i = 0; i < 20; i++) {
            t.set(i, i);
        }

        for (int i = 0; i < 20; i++) {
            assertEquals(i, ((Integer)t.get(i)).intValue());
        }
    }

    public void testSetGet(TupleFactory tf) throws Exception {
        Tuple t = tf.newTuple();

        Random r = new Random(100L);
        for (int j = 0; j < 10; j++) {
            for (int i = 0; i < 100; i++) {
                t.append(new Integer(r.nextInt()));
            }
            t.append(null);
        }

        for (int j = 0; j < 10; j++) {
            for (int i = 0; i < 100; i++) {
                t.append(new Long(r.nextLong()));
            }
            t.append(null);
        }

        for (int j = 0; j < 10; j++) {
            for (int i = 0; i < 100; i++) {
                t.append(new Float(r.nextFloat()));
            }
            t.append(null);
        }

        for (int j = 0; j < 10; j++) {
            for (int i = 0; i < 100; i++) {
                t.append(new Double(r.nextDouble()));
            }
            t.append(null);
        }

        r = new Random(100L);
        int index = 0;
        for (int j = 0; j < 10; j++) {
            for (int i = 0; i < 100; i++) {
                assertEquals(r.nextInt(), ((Integer)t.get(index++)).intValue());
            }
            assertNull(t.get(index++));
        }

        for (int j = 0; j < 10; j++) {
            for (int i = 0; i < 100; i++) {
                assertEquals(r.nextLong(), ((Long)t.get(index++)).longValue());
            }
            assertNull(t.get(index++));
        }

        for (int j = 0; j < 10; j++) {
            for (int i = 0; i < 100; i++) {
                assertEquals(r.nextFloat(), ((Float)t.get(index++)).floatValue(), 0.001);
            }
            assertNull(t.get(index++));
        }

        for (int j = 0; j < 10; j++) {
            for (int i = 0; i < 100; i++) {
                assertEquals(r.nextDouble(), ((Double)t.get(index++)).doubleValue(), 0.001);
            }
            assertNull(t.get(index++));
        }
    }
}

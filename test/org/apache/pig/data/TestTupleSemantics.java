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

import com.google.common.collect.Lists;

public class TestTupleSemantics {
    @Test
    public void testDefaultTupleFactory() throws Exception {
        testFactory(TupleFactory.getInstance());
    }

    @Test
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
        testNewTupleFromObject(tf);

        testComparison(tf);
        testRawComparison(tf);
        testSerDe(tf);
        testGetType(tf);
        testSetGet(tf);
        testIsNull(tf);
        testSizeWithAppend(tf);
    }

    public boolean isNotImplemented(Class clazz, Class objClass, String name, Class... params) throws Exception {
        Method m = objClass.getMethod(name, params);
        boolean retVal = MethodHelper.isNotImplementedAnnotationPresent(m, clazz);
        if (retVal) {
            StackTraceElement[] ste = Thread.currentThread().getStackTrace();
            System.out.println("Skipping test " + ste[ste.length - 1].getMethodName() + " for " + objClass + " class: " + clazz);
        }
        return retVal;
    }

    public void testNewTuple(TupleFactory tf) throws Exception {
        if (isNotImplemented(tf.getClass(), TupleFactory.class, "newTuple")) {
            return;
        }
        //Also the mere fact that they can be successfully created is an implicit test
        Tuple t1 = tf.newTuple();
        Tuple t2 = tf.newTuple();
        assertEquals("Two brand new tuples should be equal", t1, t2);
    }

    public void testNewTupleFromList(TupleFactory tf) throws Exception {
        if (isNotImplemented(tf.getClass(), TupleFactory.class, "newTuple", List.class)) {
            return;
        }
        List<Object> lis = Lists.newArrayList((Object)1, 2, 3, 4, 5, 6);
        Tuple t1 = tf.newTuple(lis);
        Tuple t2 = tf.newTuple(lis);
        for (int i = 0; i < lis.size(); i++) {
            assertEquals("Elements should be equal", lis.get(i), t1.get(i));
            assertEquals("Elements should be equal", lis.get(i), t2.get(i));
        }
        assertEquals("Tuples from same list should be equal", t1, t2);
        assertEquals("Tuples should have same size as creating list", lis.size(), t1.size());
        int oldSize1 = t1.size();
        lis.add(7);
        assertEquals("newTuple(List) should guarantee that changing the list used after creation won't affect the Tuple", oldSize1, t1.size());
    }

    // We omit an explicit test for newTupleNoCopy because some Tuple implementations will neither copy all the elements
    // or take ownership of the list (ie anything using primitives underneath)

    public void testNewTupleFromObject(TupleFactory tf) throws Exception {
        if (isNotImplemented(tf.getClass(), TupleFactory.class, "newTuple", Integer.TYPE)) {
            return;
        }
        if (isNotImplemented(tf.getClass(), TupleFactory.class, "newTuple", Object.class)) {
            return;
        }
        Tuple t = tf.newTuple(100);
        List<Object> lis = Lists.newArrayList();
        for (int i = 0; i < 100; i++) {
            t.set(i, tf.newTuple(new Integer(i)));
            lis.add(new Integer(i));
        }
        for (int i = 0; i < 100; i++) {
            assertEquals("newTuple(Object) should have created the proper integer object", new Integer(i), lis.get(i));
            assertEquals("newTuple(Object) should have created the proper integer object", ((Tuple)t.get(i)).get(0), lis.get(i));
        }

    }

    public void testComparison(TupleFactory tf) {
    }

    public void testRawComparison(TupleFactory tf) {
    }

    public void testAppend(TupleFactory tf) throws Exception {
        if (isNotImplemented(tf.getClass(), Tuple.class, "append", Object.class)) {
            return;
        }

        Tuple t = tf.newTuple();
        for (int i = 0; i < 10; i++) {
            for (int j = 0; j < 10; j++) {
                t.append(null);
                for (int k = 0; k < 10; k++) {
                    t.append(new Integer(i + j + k));
                }
                t.append(null);
            }
        }

        int elem = 0;
        for (int i = 0; i < 10; i++) {
            for (int j = 0; j < 10; j++) {
                assertNull(t.get(elem++));
                for (int k = 0; k < 10; k++) {
                    assertEquals(new Integer(i + j + k), t.get(elem++));
                }
                assertNull(t.get(elem++));
            }
        }
    }

    public void testSize(TupleFactory tf) throws Exception {
        if (isNotImplemented(tf.getClass(), TupleFactory.class, "newTuple", Integer.TYPE)) {
            return;
        }
        Tuple t = tf.newTuple(10);
        assertEquals("A tuple instantiated with size 10 should have size 10", 10, t.size());
        for (int i = 0; i < 10; i++) {
            assertNull(t.get(i));
        }
    }

    public void testSerDe(TupleFactory tf) {
    }

    public void testGetType(TupleFactory tf) {
    }

    public void testIsNull(TupleFactory tf) {
    }

    public void testSizeWithAppend(TupleFactory tf) throws Exception {
        if (isNotImplemented(tf.getClass(), TupleFactory.class, "newTuple", Integer.TYPE)) {
            return;
        }
        if (isNotImplemented(tf.newTuple().getClass(), Tuple.class, "append", Object.class)) {
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
        if (isNotImplemented(tf.newTuple().getClass(), Tuple.class, "append", Object.class)) {
            return;
        }
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

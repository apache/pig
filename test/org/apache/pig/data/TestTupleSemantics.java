package org.apache.pig.data;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

public class TestTupleSemantics {
    @Test
    public void testDefaultTupleFactory() {
        testFactory(TupleFactory.getInstance());
    }

    @Test
    public void testSchemaTupleFactoryNotAppendable() {
        Schema s = ...;
        int id = generateAndAddToJar(s, false);
        testFactory(TupleFactory.getInstanceForSchemaId(id));
        s = ...;
        id = generateAndAddToJar(s, false);
        testFactory(TupleFactory.getInstanceForSchemaId(id));
    }

    public void testFactory(TupleFactory tf) {
        testComparison(tf);
        testRawComparison(tf);
        testAppend(tf);
        testSerDe(tf);
        testGetType(tf);
        testGet(tf);
        testSet(tf);
        testNull(tf);
        //TODO should we test for consistent printing?
    }

    public void test
}

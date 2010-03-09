package org.apache.pig.piggybank.test.evaluation.string;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.piggybank.evaluation.string.UcFirst;
import org.junit.Test;

public class TestUcFirst {

    private static final EvalFunc<String> ucf_ = new UcFirst();
    private static Tuple testTuple_ = TupleFactory.getInstance().newTuple(1);
    
    @Test
    public void testUcFirst() throws IOException {
        testTuple_.set(0,null);
        assertNull("null is null", ucf_.exec(testTuple_));
        
        testTuple_.set(0, "");
        assertEquals("empty string", "", ucf_.exec(testTuple_));
        
        testTuple_.set(0, "foo");
        assertEquals("lowercase string", "Foo", ucf_.exec(testTuple_));
        
        testTuple_.set(0, "Foo");
        assertEquals("uppercase string", "Foo", ucf_.exec(testTuple_));
    }

}

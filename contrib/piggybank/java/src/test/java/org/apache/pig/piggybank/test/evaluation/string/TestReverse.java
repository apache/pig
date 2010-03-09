package org.apache.pig.piggybank.test.evaluation.string;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.piggybank.evaluation.string.Reverse;
import org.junit.Test;

public class TestReverse {
    private static final EvalFunc<String> rev_ = new Reverse();
    private static Tuple testTuple_ = TupleFactory.getInstance().newTuple(1);
    
    @Test
    public void testLength() throws IOException {
        testTuple_.set(0,"fos");
        assertEquals("regular length should match", "sof", rev_.exec(testTuple_));
    }
}

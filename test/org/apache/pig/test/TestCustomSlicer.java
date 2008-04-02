package org.apache.pig.test;

import static org.apache.pig.PigServer.ExecType.MAPREDUCE;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Iterator;

import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataAtom;
import org.apache.pig.data.Tuple;
import org.junit.Test;
import junit.framework.TestCase;

public class TestCustomSlicer extends TestCase{
    /**
     * Uses RangeSlicer in place of pig's default Slicer to generate a few
     * values and count them.
     */
    @Test
    public void testUseRangeSlicer() throws ExecException, IOException {
        PigServer pig = new PigServer(MAPREDUCE);
        int numvals = 50;
        String query = "vals = foreach (group (load '"
                + numvals
                + "'using org.apache.pig.test.RangeSlicer()) all) generate COUNT($1);";
        pig.registerQuery(query);
        Iterator<Tuple> it = pig.openIterator("vals");
        Tuple cur = it.next();
        DataAtom val = cur.getAtomField(0);
        assertEquals(numvals, (int) val.longVal());
    }
}

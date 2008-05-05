package org.apache.pig.test;

import java.io.IOException;
import java.util.Iterator;

import org.apache.pig.PigServer.ExecType;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataAtom;
import org.apache.pig.data.Tuple;
import org.junit.Test;

public class TestCustomSlicer extends PigExecTestCase {
    /**
     * Uses RangeSlicer in place of pig's default Slicer to generate a few
     * values and count them.
     */
    @Test
    public void testUseRangeSlicer() throws ExecException, IOException {
        // FIXME : this should be tested in all modes
        if(execType == ExecType.LOCAL)
            return;
        int numvals = 50;
        String query = "vals = foreach (group (load '"
                + numvals
                + "'using org.apache.pig.test.RangeSlicer()) all) generate COUNT($1);";
        pigServer.registerQuery(query);
        Iterator<Tuple> it = pigServer.openIterator("vals");
        Tuple cur = it.next();
        DataAtom val = cur.getAtomField(0);
        assertEquals(numvals, (int) val.longVal());
    }
}

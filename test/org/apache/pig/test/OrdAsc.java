package org.apache.pig.test;

import org.apache.pig.data.Tuple;
import org.apache.pig.ComparisonFunc;

public class OrdAsc extends ComparisonFunc {
    // this is a simple example - more complex comparison will require
    //   breakout of the individual values. I suggest you'll have
    //   to convert "catch(IOException e) to RuntimeException('msg', e)"
    public int compare(Tuple t1, Tuple t2) {
        return t1.compareTo(t2);
    }
}

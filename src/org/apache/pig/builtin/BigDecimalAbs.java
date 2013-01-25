package org.apache.pig.builtin;

import java.io.IOException;
import java.math.BigDecimal;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

@OutputSchema("x:bigdecimal")
public class BigDecimalAbs extends EvalFunc<BigDecimal> {
    @Override
    public BigDecimal exec(Tuple input) throws IOException {
        return ((BigDecimal)input.get(0)).abs();
    }
}

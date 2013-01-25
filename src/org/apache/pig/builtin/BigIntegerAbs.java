package org.apache.pig.builtin;

import java.io.IOException;
import java.math.BigInteger;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

@OutputSchema("x:biginteger")
public class BigIntegerAbs extends EvalFunc<BigInteger> {
    @Override
    public BigInteger exec(Tuple input) throws IOException {
        return ((BigInteger)input.get(0)).abs();
    }
}

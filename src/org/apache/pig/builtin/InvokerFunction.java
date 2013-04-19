package org.apache.pig.builtin;

import java.io.IOException;

import org.apache.pig.data.Tuple;

public interface InvokerFunction {
    public Object eval(Tuple input) throws IOException;
}

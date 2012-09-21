package org.apache.pig.builtin;

import java.util.ArrayList;
import java.util.List;
import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * Pig UDF to test input <code>tuple.get(0)</code> against <code>tuple.get(1)</code>
 * to determine if the first argument starts with the string in the second.
 */
public class STARTSWITH extends EvalFunc<Boolean> {
    @Override
    public Boolean exec(Tuple tuple) {
        if (tuple == null || tuple.size() != 2) {
            return null;
        }
        String argument = null;
        String testAgainst = null;
        try {
            argument = (String) tuple.get(0);
            testAgainst = (String) tuple.get(1);
            return argument.startsWith(testAgainst);
        } catch (ExecException exe) {
          System.err.println("UDF STARTSWITH threw ExecException while processing '" +
            argument + "' while attempting to locate prefix '" + testAgainst + "'");
          return null;
        }
    }

    @Override
    public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
        List<FuncSpec> funcList = new ArrayList<FuncSpec>();
        Schema s = new Schema();
        s.add(new Schema.FieldSchema(null, DataType.CHARARRAY));
        s.add(new Schema.FieldSchema(null, DataType.CHARARRAY));
        funcList.add(new FuncSpec(this.getClass().getName(), s));
        return funcList;
    }
}

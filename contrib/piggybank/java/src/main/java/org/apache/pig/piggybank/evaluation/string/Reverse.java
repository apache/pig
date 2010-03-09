package org.apache.pig.piggybank.evaluation.string;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/*
 * reverses a string.
 */
public class Reverse extends EvalFunc<String> {
    @Override
    public String exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0) {
            return null;
        }
        try {
            String str = (String) input.get(0);
            if (str == null) return null;
            if (str.length() == 0) return str;
            char[] chars = str.toCharArray();
            int lastIndex = chars.length-1;
            for (int i=0; i<=lastIndex/2; i++) {
                char c = chars[i];
                chars[i] = chars[lastIndex-i];
                chars[lastIndex-i] = c;
            }
            return new String(chars);
        } catch (ExecException e) {
            log.warn("Error reading input: " + e.getMessage());
            return null;
        }
    }

    @Override
    public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(null, DataType.CHARARRAY));
    }
}

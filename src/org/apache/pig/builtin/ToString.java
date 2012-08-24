package org.apache.pig.builtin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * 
 * <p>ToString converts the DateTime object of the ISO or the customized string.</p>
 *
 */
public class ToString extends EvalFunc<String> {

    public String exec(Tuple input) throws IOException {
        if (input == null) {
            return null;
        }
        if (input.size() == 1) {
            return DataType.toDateTime(input.get(0)).toString();
        } else if (input.size() == 2) {
            DateTimeFormatter dtf = DateTimeFormat.forPattern(DataType.toString(input.get(1)));
            return DataType.toDateTime(input.get(0)).toString(dtf);
        } else {
            return null;
        }
    }

    @Override
    public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase(), input), DataType.CHARARRAY));
    }

    @Override
    public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
        List<FuncSpec> funcList = new ArrayList<FuncSpec>();
        Schema s = new Schema();
        s.add(new Schema.FieldSchema(null, DataType.DATETIME));
        s.add(new Schema.FieldSchema(null, DataType.CHARARRAY));
        funcList.add(new FuncSpec(this.getClass().getName(), s));
        return funcList;
    }
    
}

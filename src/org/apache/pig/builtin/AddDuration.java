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
import org.joda.time.DateTime;
import org.joda.time.Period;

/**
 * <p>AddDuration returns the result of a DateTime object plus a Duration object</p>
 *
 * <ul>
 * <li>Jodatime: http://joda-time.sourceforge.net/</li>
 * <li>ISO8601 Duration Format: http://en.wikipedia.org/wiki/ISO_8601#Durations</li>
 * </ul>
 * <br />
 * <pre>
 * Example usage:
 *
 * ISOin = LOAD 'test.tsv' USING PigStorage('\t') AS (dt:datetime, dr:chararray);
 *
 * DESCRIBE ISOin;
 * ISOin: {dt: datetime,dr: chararray}
 *
 * DUMP ISOin;
 *
 * (2009-01-07T01:07:01.000Z,PT1S)
 * (2008-02-06T02:06:02.000Z,PT1M)
 * (2007-03-05T03:05:03.000Z,P1D)
 * ...
 *
 * dtadd = FOREACH ISOin GENERATE AddDuration(dt, dr) AS dt1;
 *
 * DESCRIBE dtadd;
 * dtadd: {dt1: datetime}
 *
 * DUMP dtadd;
 *
 * (2009-01-07T01:07:02.000Z)
 * (2008-02-06T02:07:02.000Z)
 * (2007-03-06T03:05:03.000Z)
 *
 * </pre>
 */
public class AddDuration extends EvalFunc<DateTime> {

    @Override
    public DateTime exec(Tuple input) throws IOException {
        if (input == null || input.size() < 2) {
            return null;
        }
        
        return ((DateTime) input.get(0)).plus(new Period((String) input.get(1)));
    }
    
    @Override
    public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase(), input), DataType.DATETIME));
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

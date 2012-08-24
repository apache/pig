package org.apache.pig.builtin;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * This method should never be used directly, use {@link ToDate}.
 */
public class ToDate3ARGS extends EvalFunc<DateTime> {

    public DateTime exec(Tuple input) throws IOException {
        DateTimeFormatter dtf = DateTimeFormat.forPattern(DataType
                .toString(input.get(1)));
        DateTimeZone dtz = DateTimeZone.forOffsetMillis(DateTimeZone.forID(
                DataType.toString(input.get(2))).getOffset(null));
        return dtf.withZone(dtz).parseDateTime(DataType.toString(input.get(0)));
    }

}

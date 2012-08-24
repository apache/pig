package org.apache.pig.builtin;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

/**
 * This method should never be used directly, use {@link ToDate}.
 */
public class ToDateISO extends EvalFunc<DateTime> {

    public DateTime exec(Tuple input) throws IOException {
        String dtStr = DataType.toString(input.get(0));
        DateTimeZone dtz = ToDate.extractDateTimeZone(dtStr);
        if (dtz == null) {
            return new DateTime(dtStr);
        } else {
            return new DateTime(dtStr, dtz);
        }
    }

}

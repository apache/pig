package org.apache.pig.builtin;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * This method should never be used directly, use {@link ToDate}.
 */
public class ToDate2ARGS extends EvalFunc<DateTime> {

    public DateTime exec(Tuple input) throws IOException {
        String dtStr = DataType.toString(input.get(0));
        //DateTimeZone dtz = extractDateTimeZone(dtStr);
        //The timezone in the customized format is not predictable
        DateTimeFormatter dtf = DateTimeFormat.forPattern(DataType
                .toString(input.get(1)));
        //if (dtz == null) {
            return dtf.parseDateTime(dtStr);
        //} else {
        //    return dtf.withZone(dtz).parseDateTime(dtStr);
        //}
    }

}

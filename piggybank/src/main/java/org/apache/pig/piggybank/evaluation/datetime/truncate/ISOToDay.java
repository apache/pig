/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.pig.piggybank.evaluation.datetime.truncate;

import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.joda.time.DateTime;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * ISOToDay truncates an ISO8601 datetime string to the precision of the day field
 * <ul>
 * <li>Jodatime: http://joda-time.sourceforge.net/</li>
 * <li>ISO8601 Date Format: http://en.wikipedia.org/wiki/ISO_8601</li>
 * </ul>
 * <br />
 * <pre>
 * Example usage:
 * 
 * REGISTER /Users/me/commiter/piggybank/java/piggybank.jar ;
 * REGISTER /Users/me/commiter/piggybank/java/lib/joda-time-1.6.jar ;
 *
 * DEFINE ISOToYear org.apache.pig.piggybank.evaluation.datetime.truncate.ISOToYear();
 * DEFINE ISOToMonth org.apache.pig.piggybank.evaluation.datetime.truncate.ISOToMonth();
 * DEFINE ISOToWeek org.apache.pig.piggybank.evaluation.datetime.truncate.ISOToWeek();
 * DEFINE ISOToDay org.apache.pig.piggybank.evaluation.datetime.truncate.ISOToDay();
 * DEFINE ISOToHour org.apache.pig.piggybank.evaluation.datetime.truncate.ISOToHour();
 * DEFINE ISOToMinute org.apache.pig.piggybank.evaluation.datetime.truncate.ISOToMinute();
 * DEFINE ISOToSecond org.apache.pig.piggybank.evaluation.datetime.truncate.ISOToSecond();
 *
 * ISOin = LOAD 'test.tsv' USING PigStorage('\t') AS (dt:chararray, dt2:chararray);
 *
 * DESCRIBE ISOin;
 * ISOin: {dt: chararray,dt2: chararray}
 *
 * DUMP ISOin;
 *
 * (2009-01-07T01:07:01.000Z,2008-02-01T00:00:00.000Z)
 * (2008-02-06T02:06:02.000Z,2008-02-01T00:00:00.000Z)
 * (2007-03-05T03:05:03.000Z,2008-02-01T00:00:00.000Z)
 * ...
 * 
 * truncated = FOREACH ISOin GENERATE ISOToYear(dt) AS year,
 *     ISOToMonth(dt) as month,
 *     ISOToWeek(dt) as week,
 *     ISOToDay(dt) AS day,
 *        ISOToHour(dt) AS hour,
 *        ISOToMinute(dt) AS min,
 *        ISOToSecond(dt) as sec;
 *
 * DESCRIBE truncated;
 * truncated: {year: chararray,month: chararray,week: chararray,day: chararray,hour: chararray,min: chararray,sec: chararray}
 *
 * DUMP truncated;
 * (2009-01-01T00:00:00.000Z,2009-01-01T00:00:00.000Z,2009-01-05T00:00:00.000Z,2009-01-07T00:00:00.000Z,2009-01-07T01:00:00.000Z,2009-01-07T01:07:00.000Z,2009-01-07T01:07:01.000Z)
 * (2008-01-01T00:00:00.000Z,2008-02-01T00:00:00.000Z,2008-02-04T00:00:00.000Z,2008-02-06T00:00:00.000Z,2008-02-06T02:00:00.000Z,2008-02-06T02:06:00.000Z,2008-02-06T02:06:02.000Z)
 * (2007-01-01T00:00:00.000Z,2007-03-01T00:00:00.000Z,2007-03-05T00:00:00.000Z,2007-03-05T00:00:00.000Z,2007-03-05T03:00:00.000Z,2007-03-05T03:05:00.000Z,2007-03-05T03:05:03.000Z)
 * </pre>
 */

public class ISOToDay extends EvalFunc<String> {
    
    @Override
    public String exec(Tuple input) throws IOException {
        if (input == null || input.size() < 1) {
            return null;
        }

        DateTime dt = ISOHelper.parseDateTime(input);
            
        // Set the the hour, minute, second and milliseconds to 0
        DateTime result = dt.hourOfDay().setCopy(0).minuteOfHour().setCopy(0).secondOfMinute().setCopy(0).millisOfSecond().setCopy(0);

        return result.toString();
    }

    @Override
    public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase(), input), DataType.CHARARRAY));
    }

    @Override
    public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
        List<FuncSpec> funcList = new ArrayList<FuncSpec>();
        funcList.add(new FuncSpec(this.getClass().getName(), new Schema(new Schema.FieldSchema(null, DataType.CHARARRAY))));

        return funcList;
    }
}

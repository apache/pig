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

/**
 * <p>MilliSecondsBetween returns the number of milliseconds between two DateTime objects</p>
 *
 * <ul>
 * <li>Jodatime: http://joda-time.sourceforge.net/</li>
 * <li>ISO8601 Date Format: http://en.wikipedia.org/wiki/ISO_8601</li>
 * </ul>
 * <br />
 * <pre>
 * Example usage:
 *
 * ISOin = LOAD 'test.tsv' USING PigStorage('\t') AS (datetime, dt2:datetime);
 *
 * DESCRIBE ISOin;
 * ISOin: {dt: datetime,dt2: datetime}
 *
 * DUMP ISOin;
 *
 * (2009-01-07T01:07:01.000Z,2008-02-01T00:00:00.000Z)
 * (2008-02-06T02:06:02.000Z,2008-02-01T00:00:00.000Z)
 * (2007-03-05T03:05:03.000Z,2008-02-01T00:00:00.000Z)
 * ...
 *
 * diffs = FOREACH ISOin GENERATE YearsBetween(dt, dt2) AS years,
 * MonthsBetween(dt, dt2) AS months,
 * WeeksBetween(dt, dt2) AS weeks,
 * DaysBetween(dt, dt2) AS days,
 * HoursBetween(dt, dt2) AS hours,
 * MinutesBetween(dt, dt2) AS mins,
 * SecondsBetween(dt, dt2) AS secs;
 * MilliSecondsBetween(dt, dt2) AS millis;
 *
 * DESCRIBE diffs;
 * diffs: {years: long,months: long,weeks: long,days: long,hours: long,mins: long,secs: long,millis: long}
 *
 * DUMP diffs;
 *
 * (0L,11L,48L,341L,8185L,491107L,29466421L,29466421000L)
 * (0L,0L,0L,5L,122L,7326L,439562L,439562000L)
 * (0L,-10L,-47L,-332L,-7988L,-479334L,-28760097L,-28760097000L)
 *
 * </pre>
 */
public class MilliSecondsBetween extends EvalFunc<Long> {

    @Override
    public Long exec(Tuple input) throws IOException
    {
        if (input == null || input.size() < 2) {
            return null;
        }

        DateTime startDate = (DateTime) input.get(0);
        DateTime endDate = (DateTime) input.get(1);

        // Larger date first
        // Subtraction may overflow
        return startDate.getMillis() - endDate.getMillis();
    }

    @Override
    public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase(), input), DataType.LONG));
    }

    @Override
    public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
        List<FuncSpec> funcList = new ArrayList<FuncSpec>();
        Schema s = new Schema();
        s.add(new Schema.FieldSchema(null, DataType.DATETIME));
        s.add(new Schema.FieldSchema(null, DataType.DATETIME));
        funcList.add(new FuncSpec(this.getClass().getName(), s));
        return funcList;
    }
}

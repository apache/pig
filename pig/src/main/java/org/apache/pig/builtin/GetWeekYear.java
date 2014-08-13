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
 * GetMonth extracts the week year from a DateTime object.
 * <ul>
 * <li>Jodatime: http://joda-time.sourceforge.net/</li>
 * <li>ISO8601 Date Format: http://en.wikipedia.org/wiki/ISO_8601</li>
 * </ul>
 * <br />
 * <pre>
 * Example usage:
 * 
 * ISOin = LOAD 'test.tsv' USING PigStorage('\t') AS (dt:datetime);
 *
 * DESCRIBE ISOin;
 * ISOin: {dt: datetime}
 *
 * DUMP ISOin;
 *
 * (2009-01-07T01:07:01.071Z)
 * (2008-02-06T02:06:02.062Z)
 * (2007-03-05T03:05:03.053Z)
 * ...
 * 
 * truncated = FOREACH ISO in GENERATE GetYear(dt) AS year,
 *     GetMonth(dt) as month,
 *     GetWeek(dt) as week,
 *     GetWeekYear(dt) as weekyear,
 *     GetDay(dt) AS day,
 *     GetHour(dt) AS hour,
 *     GetMinute(dt) AS min,
 *     GetSecond(dt) AS sec;
 *     GetMillSecond(dt) AS milli;
 *
 * DESCRIBE truncated;
 * truncated: {year: int,month: int,week: int,weekyear: int,day: int,hour: int,min: int,sec: int,milli: int}
 *
 * DUMP truncated;
 * (2009,1,2,2009,7,1,7,1,71)
 * (2008,2,6,2008,6,2,6,2,62)
 * (2007,3,10,2007,5,3,5,3,53)
 * </pre>
 */
public class GetWeekYear extends EvalFunc<Integer> {

    @Override
    public Integer exec(Tuple input) throws IOException {
        if (input == null || input.size() < 1 || input.get(0) == null) {
            return null;
        }

        return ((DateTime) input.get(0)).getWeekyear();
    }

    @Override
    public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase(), input), DataType.INTEGER));
    }

    @Override
    public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
        List<FuncSpec> funcList = new ArrayList<FuncSpec>();
        funcList.add(new FuncSpec(this.getClass().getName(), new Schema(new Schema.FieldSchema(null, DataType.DATETIME))));

        return funcList;
    }
}

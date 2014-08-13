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
package org.apache.pig.piggybank.evaluation.datetime.convert;

import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * CustomFormatToISO converts arbitrary date formats to ISO format.
 * <ul>
 * <li>Jodatime: http://joda-time.sourceforge.net/</li>
 * <li>ISO8601 Date Format: http://en.wikipedia.org/wiki/ISO_8601</li>
 * <li>Jodatime custom date formats: http://joda-time.sourceforge.net/api-release/org/joda/time/format/DateTimeFormat.html</li>
 * </ul>
 * <br />
 * <pre>
 * Example usage:
 *
 * REGISTER /Users/me/commiter/piggybank/java/piggybank.jar ;
 * REGISTER /Users/me/commiter/piggybank/java/lib/joda-time-1.6.jar ;
 *
 * DEFINE CustomFormatToISO org.apache.pig.piggybank.evaluation.datetime.convert.CustomFormatToISO();
 * CustomIn = LOAD 'test3.tsv' USING PigStorage('\t') AS (dt:chararray);
 *
 * DESCRIBE CustomIn;
 * CustomIn: {dt: chararray}
 *
 * DUMP CustomIn;
 *
 * (10-1-2010)
 *
 * toISO = FOREACH CustomIn GENERATE CustomFormatToISO(dt, "MM-dd-YYYY") AS ISOTime:chararray;
 *
 * DESCRIBE toISO;
 * toISO: {ISOTime: chararray}
 *
 * DUMP toISO;
 * (2010-10-01T00:00:00.000Z)
 * ...
 * </pre>
 */

public class CustomFormatToISO extends EvalFunc<String> {

    @Override
    public String exec(Tuple input) throws IOException
    {
        if (input == null || input.size() < 2) {
            return null;
        }

        // Set the time to default or the output is in UTC
        DateTimeZone.setDefault(DateTimeZone.UTC);

        String date = input.get(0).toString();
        String format = input.get(1).toString();

        // See http://joda-time.sourceforge.net/api-release/org/joda/time/format/DateTimeFormat.html
        DateTimeFormatter parser = DateTimeFormat.forPattern(format);
        DateTime result;
        try {
            result = parser.parseDateTime(date);
        } catch(Exception e) {
            return null;
        }

        return result.toString();
    }

    @Override
    public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase(), input), DataType.CHARARRAY));
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

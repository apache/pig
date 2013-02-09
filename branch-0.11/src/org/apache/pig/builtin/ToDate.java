/**
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * 
 * <p>ToDate converts the ISO or the customized string or the Unix timestamp to the DateTime object.</p>
 * <p>ToDate is overloaded.</p>
 * 
 * <dl>
 * <dt><b>Syntax:</b></dt>
 * <dd><code>DateTime ToDate(Long millis)</code>.</dd>
 * <dt><b>Input:</b></dt>
 * <dd><code>the milliseconds</code>.</dd>
 * <dt><b>Output:</b></dt>
 * <dd><code>the DateTime object</code>.</dd>
 * </dl>
 *
 * <dl>
 * <dt><b>Syntax:</b></dt>
 * <dd><code>DateTime ToDate(String dtStr)</code>.</dd>
 * <dt><b>Input:</b></dt>
 * <dd><code>the ISO format date time string</code>.</dd>
 * <dt><b>Output:</b></dt>
 * <dd><code>the DateTime object</code>.</dd>
 * </dl>
 * 
 * <dl>
 * <dt><b>Syntax:</b></dt>
 * <dd><code>DateTime ToDate(String dtStr, String format)</code>.</dd>
 * <dt><b>Input:</b></dt>
 * <dd><code>dtStr: the string that represents a date time</code>.</dd>
 * <dd><code>format: the format string</code>.</dd>
 * <dt><b>Output:</b></dt>
 * <dd><code>the DateTime object</code>.</dd>
 * </dl>
 *
 * <dl>
 * <dt><b>Syntax:</b></dt>
 * <dd><code>DateTime ToDate(String dtStr, String format, String timezone)</code>.</dd>
 * <dt><b>Input:</b></dt>
 * <dd><code>dtStr: the string that represents a date time</code>.</dd>
 * <dd><code>format: the format string</code>.</dd>
 * <dd><code>timezone: the timezone string</code>.</dd>
 * <dt><b>Output:</b></dt>
 * <dd><code>the DateTime object</code>.</dd>
 * </dl>
 */
public class ToDate extends EvalFunc<DateTime> {

    public DateTime exec(Tuple input) throws IOException {
        return new DateTime(DataType.toLong(input.get(0)));
    }

    @Override
    public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass()
                .getName().toLowerCase(), input), DataType.DATETIME));
    }

    @Override
    public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
        List<FuncSpec> funcList = new ArrayList<FuncSpec>();
        Schema s = new Schema();
        s.add(new Schema.FieldSchema(null, DataType.LONG));
        funcList.add(new FuncSpec(this.getClass().getName(), s));
        s = new Schema();
        s.add(new Schema.FieldSchema(null, DataType.CHARARRAY));
        funcList.add(new FuncSpec(ToDateISO.class.getName(), s));
        s = new Schema();
        s.add(new Schema.FieldSchema(null, DataType.CHARARRAY));
        s.add(new Schema.FieldSchema(null, DataType.CHARARRAY));
        funcList.add(new FuncSpec(ToDate2ARGS.class.getName(), s));
        s = new Schema();
        s.add(new Schema.FieldSchema(null, DataType.CHARARRAY));
        s.add(new Schema.FieldSchema(null, DataType.CHARARRAY));
        s.add(new Schema.FieldSchema(null, DataType.CHARARRAY));
        funcList.add(new FuncSpec(ToDate3ARGS.class.getName(), s));
        return funcList;
    }
    
    public static DateTimeZone extractDateTimeZone(String dtStr) {
        Pattern pattern = Pattern.compile("(Z|((\\+|-)\\d{2}(:?\\d{2})?))$");
        Matcher matcher = pattern.matcher(dtStr);
        if (matcher.find()) {
            String dtzStr = matcher.group();
            if (dtzStr.equals("Z")) {
                return DateTimeZone.forOffsetMillis(DateTimeZone.UTC.getOffset(null));
            } else {
                return DateTimeZone.forOffsetMillis(DateTimeZone.forID(dtzStr).getOffset(null));
            }
        } else {
            return null;
        }
    }
}

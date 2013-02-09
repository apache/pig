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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * <p>UnixToISO converts Unix Time Long datetimes to ISO8601 datetime strings</p>
 *
 * <ul>
 * <li>Jodatime: http://joda-time.sourceforge.net/</li>
 * <li>ISO8601 Date Format: http://en.wikipedia.org/wiki/ISO_8601</li>
 * <li>Unix Time: http://en.wikipedia.org/wiki/Unix_time</li>
 * </ul>
 * <br />
 * <pre>
 * Example usage:
 *
 * REGISTER /Users/me/commiter/piggybank/java/piggybank.jar ;
 * REGISTER /Users/me/commiter/piggybank/java/lib/joda-time-1.6.jar ;
 * 
 * DEFINE UnixToISO org.apache.pig.piggybank.evaluation.datetime.convert.UnixToISO();
 *
 * UnixIn = LOAD 'test2.tsv' USING PigStorage('\t') AS (dt:long);
 *
 * DESCRIBE UnixIn;
 * UnixIn: {dt: long}
 *
 * DUMP UnixIn;
 *
 * (1231290421000L)
 * (1233885962000L)
 * (1236222303000L)
 *
 * toISO = FOREACH UnixIn GENERATE UnixToISO(dt) AS ISOTime:chararray;
 *
 * DESCRIBE toISO;
 * toISO: {ISOTime: chararray}
 *
 * DUMP toISO;
 *
 * (2009-01-07T01:07:01.000Z)
 * (2009-02-06T02:06:02.000Z)
 * (2009-03-05T03:05:03.000Z)
 * ...
 * </pre>
 */

public class UnixToISO extends EvalFunc<String> {

    @Override
    public String exec(Tuple input) throws IOException
    {
        if (input == null || input.size() < 1) {
            return null;
        }
        
        // Set the time to default or the output is in UTC
        DateTimeZone.setDefault(DateTimeZone.UTC);

        DateTime result = new DateTime(DataType.toLong(input.get(0)));

        return result.toString();
    }

	@Override
	public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase(), input), DataType.CHARARRAY));
	}

    @Override
    public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
        List<FuncSpec> funcList = new ArrayList<FuncSpec>();
        funcList.add(new FuncSpec(this.getClass().getName(), new Schema(new Schema.FieldSchema(null, DataType.LONG))));

        return funcList;
    }
}

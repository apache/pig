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
 * <p>ISOToUnix converts ISO8601 datetime strings to Unix Time Longs</p>
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
 * DEFINE ISOToUnix org.apache.pig.piggybank.evaluation.datetime.convert.ISOToUnix();
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
 * toUnix = FOREACH ISOin GENERATE ISOToUnix(dt) AS unixTime:long;
 *
 * DESCRIBE toUnix;
 * toUnix: {unixTime: long}
 *
 * DUMP toUnix;
 *
 * (1231290421000L)
 * (1202263562000L)
 * (1173063903000L)
 * ...
 *</pre>
 */

public class ISOToUnix extends EvalFunc<Long> {

    @Override
    public Long exec(Tuple input) throws IOException
    {
        if (input == null || input.size() < 1 || input.get(0) == null) {
            return null;
        }
        
        DateTime result = new DateTime(input.get(0).toString());

        return result.getMillis();
    }

	@Override
	public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase(), input), DataType.LONG));
	}

    @Override
    public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
        List<FuncSpec> funcList = new ArrayList<FuncSpec>();
        funcList.add(new FuncSpec(this.getClass().getName(), new Schema(new Schema.FieldSchema(null, DataType.CHARARRAY))));

        return funcList;
    }
}

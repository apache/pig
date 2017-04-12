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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.*;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.*;

/**
 * Search and find all matched characters in a string with a given
 * regular expression.
 *
 * Example:
 *
 * a = LOAD 'mydata' AS (name:chararray);
 * b = FOREACH A GENERATE REGEX_SEARCH(name, 'regEx');
 *
 * input tuple: the first field is a string on which performs regular expression matching;
 * the second field is the regular expression;
 */

public class REGEX_SEARCH extends EvalFunc<DataBag> {
	private static BagFactory bagFactory = BagFactory.getInstance();
	private static TupleFactory tupleFactory = TupleFactory.getInstance();

    public REGEX_SEARCH() {}

	@Override
	public DataBag exec(Tuple input) throws IOException {

		if (input == null || input.size() < 1) {
			return null;
		}
		if (input.get(0)==null)
            return null;

		try {
			if (!input.get(1).equals(mExpression)) {
                try {
                    mExpression = (String)input.get(1);
                    mPattern = Pattern.compile(mExpression);
                } catch (Exception e) {
                    String msg = "StringSearchAll : Mal-Formed Regular expression : "+input.get(1);
                    throw new IOException(msg);
                }
             }
        } catch (NullPointerException e) {
            String msg = "StringSearchAll : Regular expression is null";
            throw new IOException(msg);
        }
        Matcher m = mPattern.matcher((String)input.get(0));
        if (!m.find()) {
            return null;
        }

        Tuple tuple0 = tupleFactory.newTuple(1);
        tuple0.set(0, m.group(1));
        DataBag dataBag = bagFactory.newDefaultBag();
        dataBag.add(tuple0);
        while (m.find()) {
            Tuple tuple = tupleFactory.newTuple(1);
            tuple.set(0, m.group(1));
            dataBag.add(tuple);
        }
        return dataBag;
    }

    String mExpression = null;
    Pattern mPattern = null;
    @Override
    public Schema outputSchema(Schema input) {
        try {
            return new Schema(Utils.getSchemaFromString("{(match:chararray)}"));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean allowCompileTimeCalculation() {
        return true;
    }

}

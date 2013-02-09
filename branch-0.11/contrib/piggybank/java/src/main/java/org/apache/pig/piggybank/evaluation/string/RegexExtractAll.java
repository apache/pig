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
package org.apache.pig.piggybank.evaluation.string;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * <dl>
 * <dt><b>Syntax:</b></dt>
 * <dd><code>String RegexExtractAll(String expression, String regex)</code>.</dd>
 * <dt><b>Input:</b></dt>
 * <dd><code>expression</code>-<code>source string</code>.</dd>
 * <dd><code>regex</code>-<code>regular expression</code>.</dd>
 * <dt><b>Output:</b></dt>
 * <dd><code>A tuple of matched strings</code>.</dd>
 * </dl>
 */

/**
 * @deprecated Use {@link org.apache.pig.builtin.REGEX_EXTRACT_ALL}
 */
@Deprecated 

public class RegexExtractAll extends EvalFunc<Tuple> {

    private static TupleFactory tupleFactory = TupleFactory.getInstance();

    @Override
    public Tuple exec(Tuple input) throws IOException {
        if (input.size()!=2) {
            String msg = "RegexExtractAll : Only 2 parameters are allowed.";
            throw new IOException(msg);
        }

        if (input.get(0)==null)
            return null;
        try {
            if (!input.get(1).equals(mExpression)) {
                try {
                    mExpression = (String)input.get(1);
                    mPattern = Pattern.compile(mExpression);
                } catch (Exception e) {
                    String msg = "RegexExtractAll : Mal-Formed Regular expression : "+input.get(1);
                    throw new IOException(msg);
                }
            }
        } catch (NullPointerException e) {
            String msg = "RegexExtractAll : Regular expression is null";
            throw new IOException(msg);
        }

        Matcher m = mPattern.matcher((String)input.get(0));
        if (!m.matches()) {
            return null;
        }
        Tuple result = tupleFactory.newTuple(m.groupCount());
        for (int i = 0; i< m.groupCount(); i++) {
            result.set(i, m.group(i+1));
        }
        return result;
    }

    String mExpression = null;
    Pattern mPattern = null; 
    @Override
    public Schema outputSchema(Schema input) {
        try {
            return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase(), input), 
                    DataType.TUPLE));
        } catch (Exception e) {
            return null;
        }
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


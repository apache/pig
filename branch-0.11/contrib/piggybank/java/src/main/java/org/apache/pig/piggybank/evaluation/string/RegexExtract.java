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
import org.apache.pig.PigWarning;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
* <dl>
* <dt><b>Syntax:</b></dt>
* <dd><code>String RegexExtract(String expression, String regex, int match_index)</code>.</dd>
* <dt><b>Input:</b></dt>
* <dd><code>expression</code>-<code>source string</code>.</dd>
* <dd><code>regex</code>-<code>regular expression</code>.</dd>
* <dd><code>match_index</code>-<code>index of the group to extract</code>.</dd>
* <dt><b>Output:</b></dt>
* <dd><code>extracted group, if fail, return null</code>.</dd>
* </dl>
*/

/**
 * @deprecated Use {@link org.apache.pig.builtin.REGEX_EXTRACT}
 */
@Deprecated 

public class RegexExtract extends EvalFunc<String> {
    String mExpression = null;
    Pattern mPattern = null; 
    @Override
    public Schema outputSchema(Schema input) {
      try {
          return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase(), input), DataType.CHARARRAY));
      } catch (Exception e) {
        return null;
      }
    }

    public String exec(Tuple input) throws IOException {
        if (input.size()!=3) {
            String msg = "RegexExtract : Only 3 parameters are allowed.";
            throw new IOException(msg);
        }
        if (input.get(0)==null)
            return null;
        try {
            if (!input.get(1).equals(mExpression))
            {
                try
                {
                    mExpression = (String)input.get(1);
                    mPattern = Pattern.compile(mExpression);
                } catch (Exception e)
                {
                    String msg = "RegexExtract : Mal-Formed Regular expression : "+input.get(1);
                    throw new IOException(msg);
                }
            }
        } catch (NullPointerException e) {
            String msg = "RegexExtract : Regular expression is null";
            throw new IOException(msg);
        }
        int mIndex = (Integer)input.get(2);
        
        Matcher m = mPattern.matcher((String)input.get(0));
        if (m.find()&&m.groupCount()>=mIndex)
        {
            return m.group(mIndex);
        }
        warn("RegexExtract : Cannot extract group for input "+input.get(0), PigWarning.UDF_WARNING_1);
        return null;
    }
    @Override
    public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
        List<FuncSpec> funcList = new ArrayList<FuncSpec>();
        Schema s = new Schema();
        s.add(new Schema.FieldSchema(null, DataType.CHARARRAY));
        s.add(new Schema.FieldSchema(null, DataType.CHARARRAY));
        s.add(new Schema.FieldSchema(null, DataType.INTEGER));
        funcList.add(new FuncSpec(this.getClass().getName(), s));
        return funcList;
    } 
}

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
* <dd><code>int RegexMatch(String expression, String regex)</code>.</dd>
* <dt><b>Output:</b></dt>
* <dd><code>return 1 if expression contains regex, 0 otherwise</code>.</dd>
* </dl>
*/

public class RegexMatch extends EvalFunc<Integer> {
    String mExpression = null;
    Pattern mPattern = null; 
    @Override
    public Schema outputSchema(Schema input) {
      try {
          return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase(), input), DataType.INTEGER));
      } catch (Exception e) {
        return null;
      }
    }
    
    public Integer exec(Tuple input) throws IOException {
        if (input.size()!=2) {
            String msg = "RegexMatch : Only 2 parameters are allowed.";
            throw new IOException(msg);
        }
        if (input.get(0)==null)
            return null;
        try {
            if (!input.get(1).equals(mExpression))
            {
                mExpression = (String)input.get(1);
                try
                {
                    mPattern = Pattern.compile(mExpression);
                }
                catch (Exception e)
                {
                    String msg = "RegexMatch : Mal-Formed Regular Expression "+input.get(1);
                    throw new IOException(msg);
                } 
            }
        } catch (NullPointerException e) {
            String msg = "RegexMatch : Regular Expression is null ";
            throw new IOException(msg);
        }

        if (mPattern.matcher((String)input.get(0)).matches())
            return 1;
        return 0;
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

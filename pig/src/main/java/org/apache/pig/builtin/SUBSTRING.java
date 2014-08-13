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
import java.util.List;
import java.util.ArrayList;

import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.PigWarning;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.FrontendException;

/**
 * SUBSTRING implements eval function to get a part of a string.
 * Example:<code>
 *      A = load 'mydata' as (name);
 *      B = foreach A generate SUBSTRING(name, 10, 12);
 *      </code>
 * First argument is the string to take a substring of.<br>
 * Second argument is the index of the first character of substring.<br>
 * Third argument is the index of the last character of substring.<br>
 * if the last argument is past the end of the string, substring of (beginIndex, length(str)) is returned.
 */
public class SUBSTRING extends EvalFunc<String> {

    /**
     * Method invoked on every tuple during foreach evaluation
     * @param input tuple; first column is assumed to have the column to convert
     * @exception java.io.IOException
     */
    @Override
    public String exec(Tuple input) throws IOException {
        if (input == null || input.size() < 3) {
            warn("invalid number of arguments to SUBSTRING", PigWarning.UDF_WARNING_1);
            return null;
        }
        try {
            String source = (String)input.get(0);
            Integer beginindex = (Integer)input.get(1);
            Integer endindex = (Integer)input.get(2);
            return source.substring(beginindex, Math.min(source.length(), endindex));
        } catch (NullPointerException npe) {
            warn(npe.toString(), PigWarning.UDF_WARNING_2);
            return null;
        } catch (StringIndexOutOfBoundsException npe) {
            warn(npe.toString(), PigWarning.UDF_WARNING_3);
            return null;
        } catch (ClassCastException e) {
            warn(e.toString(), PigWarning.UDF_WARNING_4);
            return null;
        }
    }

    @Override
    public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(null, DataType.CHARARRAY));
    }

    /* (non-Javadoc)
     * @see org.apache.pig.EvalFunc#getArgToFuncMapping()
     */
    @Override
    public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
        List<FuncSpec> funcList = new ArrayList<FuncSpec>();
        Schema s = new Schema();
        s.add(new Schema.FieldSchema(null, DataType.CHARARRAY));
        s.add(new Schema.FieldSchema(null, DataType.INTEGER));
        s.add(new Schema.FieldSchema(null, DataType.INTEGER));
        funcList.add(new FuncSpec(this.getClass().getName(), s));
        return funcList;
    }
}

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

import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.PigWarning;
import org.apache.pig.data.*;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.PatternSyntaxException;

/**
 * Wrapper around Java's String.split<br>
 * input tuple: first column is assumed to have a string to split;<br>
 * the optional second column is assumed to have the delimiter or regex to split on;<br>
 * if not provided, it's assumed to be '\s' (space)<br>
 * the optional third column may provide a limit to the number of results.<br>
 * If limit is not provided, 0 is assumed, as per Java's split().
 */

public class STRSPLITTOBAG extends EvalFunc<DataBag> {

    private final static BagFactory bagFactory = BagFactory.getInstance();
    private final static TupleFactory tupleFactory = TupleFactory.getInstance();

    /**
     * Wrapper around Java's String.split
     *
     * @param input tuple; first column is assumed to have a string to split;
     *              the optional second column is assumed to have the delimiter or regex to split on;<br>
     *              if not provided, it's assumed to be '\s' (space)
     *              the optional third column may provide a limit to the number of results.<br>
     *              If limit is not provided, 0 is assumed, as per Java's split().
     * @throws java.io.IOException
     */
    @Override
    public DataBag exec(Tuple input) throws IOException {
        if (input == null || input.size() < 1) {
            return null;
        }
        try {
            String source = (String) input.get(0);
            String delim = (input.size() > 1) ? (String) input.get(1) : "\\s";
            int length = (input.size() > 2) ? (Integer) input.get(2) : 0;
            if (source == null || delim == null) {
                return null;
            }

            String[] splits = source.split(delim, length);
            DataBag dataBag = bagFactory.newDefaultBag();
            for (String eachSplit : splits) {
                Tuple tuple = tupleFactory.newTuple(1);
                tuple.set(0, eachSplit);
                dataBag.add(tuple);
            }
            return dataBag;
        } catch (ClassCastException e) {
            warn("class cast exception at " + e.getStackTrace()[0], PigWarning.UDF_WARNING_1);
        } catch (PatternSyntaxException e) {
            warn(e.getMessage(), PigWarning.UDF_WARNING_1);
        }
        // this only happens if the try block did not complete normally
        return null;
    }

    @Override
    public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(null, DataType.BAG));
    }

    @Override
    public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
        List<FuncSpec> funcList = new ArrayList<FuncSpec>();
        Schema s = new Schema(new Schema.FieldSchema(null, DataType.CHARARRAY));

        Schema s1 = new Schema();
        s1.add(new Schema.FieldSchema(null, DataType.CHARARRAY));
        s1.add(new Schema.FieldSchema(null, DataType.CHARARRAY));

        Schema s2 = new Schema();
        s2.add(new Schema.FieldSchema(null, DataType.CHARARRAY));
        s2.add(new Schema.FieldSchema(null, DataType.CHARARRAY));
        s2.add(new Schema.FieldSchema(null, DataType.INTEGER));

        funcList.add(new FuncSpec(this.getClass().getName(), s));
        funcList.add(new FuncSpec(this.getClass().getName(), s1));
        funcList.add(new FuncSpec(this.getClass().getName(), s2));
        return funcList;
    }

    @Override
    public boolean allowCompileTimeCalculation() {
        return true;
    }
}

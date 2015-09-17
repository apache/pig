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
import java.util.Map;

import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.PigWarning;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * 
 * REPLACE_MULTI implements eval function to replace all occurrences of search
 * keys with replacement values. Search - Replacement values are specified in
 * Map Example:<code>
 *      input_data = LOAD 'input_data' as (name); -- name = 'Hello World!'
 *      replaced_name = FOREACH input_data GENERATE REPLACE_MULTI ( name, [ ' '#'_', '!'#'', 'e'#'a', 'o'#'oo' ] ); -- replaced_name = Halloo_Woorld
 *      </code>
 * 
 * The first argument is the source string on which REPLACE_MULTI operation is
 * performed. The second argument is a map having search key - replacement value
 * pairs.
 *
 */

public class REPLACE_MULTI extends EvalFunc<String> {

    /**
     * Method invoked on every tuple during FOREACH evaluation. If source string
     * or search replacement map is empty or null, source string is returned.
     * 
     * @param input
     *            tuple; First field value is the source string and second field
     *            is a map having search - replacement values.
     * @exception java.io.IOException
     */
    @Override
    public String exec(Tuple input) throws IOException {
        try {

            if (input == null || input.size() != 2)
                return null;

            String source = (String) input.get(0);

            if (input.get(1) == null)
                return source;

            Map<String, String> searchReplacementMap = (Map<String, String>) input
                    .get(1);

            if (source == null || source.trim().equalsIgnoreCase("")
                    || searchReplacementMap.isEmpty())
                return source;

            for (Map.Entry<String, String> entry : searchReplacementMap
                    .entrySet()) {
                source = source.replaceAll(entry.getKey(), entry.getValue());
            }

            return source;

        } catch (Exception e) {
            warn("Failed to process input; error - " + e.getMessage(),
                    PigWarning.UDF_WARNING_1);
            return null;
        }
    }

    @Override
    public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(null, DataType.CHARARRAY));
    }

    @Override
    public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
        List<FuncSpec> funcList = new ArrayList<FuncSpec>();
        Schema s = new Schema();
        s.add(new Schema.FieldSchema(null, DataType.CHARARRAY));
        s.add(new Schema.FieldSchema(null, DataType.MAP));
        funcList.add(new FuncSpec(this.getClass().getName(), s));
        return funcList;
    }

}
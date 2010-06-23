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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * string.INSTR implements eval function to search for the last occurrence of a string
 * Returns null on error
 * Example:
 * <code>
 *      A = load 'mydata' as (name);
 *      B = foreach A generate LASTINDEXOF(name, ",");
 * </code>
 */
public class LAST_INDEX_OF extends EvalFunc<Integer> {
    private static final Log log = LogFactory.getLog(LAST_INDEX_OF.class);

    /**
     * Finds the last location of a substring in a given string.
     * @param input tuple:<ol>
     * <li>the string to process
     * <li>the substring to find
     * </ol>
     * @exception java.io.IOException
     * @return last location of substring, or null in case of processing errors.
     */
    public Integer exec(Tuple input) throws IOException {
        if (input == null || input.size() < 2)
            return null;

        try {
            String str = (String)input.get(0);
            String search = (String)input.get(1);
            return str.lastIndexOf(search);
        } catch(Exception e) {
            log.warn("Failed to process input; error - " + e.getMessage());
            return null;
        }
    }

    @Override
    public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(null, DataType.INTEGER));
    }

}
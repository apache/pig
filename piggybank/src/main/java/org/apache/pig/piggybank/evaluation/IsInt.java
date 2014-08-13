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

package org.apache.pig.piggybank.evaluation;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.PigWarning;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * This UDF is used to check whether the String input is an Integer.
 * Note this function checks for Integer range −2,147,483,648 to 2,147,483,647.
 * If range is not important, use IsNumeric instead if you would like to check if a String is numeric. 
 * Also IsNumeric performs slightly better compared to this function.
 */

public class IsInt extends EvalFunc<Boolean> {
    @Override
    public Boolean exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0) return false;
        try {
            String str = (String)input.get(0);
            if (str == null || str.length() == 0) return false;
            Integer.parseInt(str);
        } catch (NumberFormatException nfe) {
            return false;
        } catch (ClassCastException e) {
            warn("Unable to cast input "+input.get(0)+" of class "+
                    input.get(0).getClass()+" to String", PigWarning.UDF_WARNING_1);
            return false;
        }

        return true;
    }
    
    @Override
    public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(null, DataType.BOOLEAN)); 
    }
}

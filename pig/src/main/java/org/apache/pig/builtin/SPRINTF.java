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

import java.util.Formatter;

import org.apache.pig.EvalFunc;
import org.apache.pig.PigException;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import org.apache.pig.backend.executionengine.ExecException;

/**
 * Formatted strings using java.util.Formatter
 *
 * See http://docs.oracle.com/javase/7/docs/api/java/util/Formatter.html
 *
 * ex:
 *     SPRINTF('%2$10s %1$-17s %2$,10d %2$8x %3$10.3f %4$1TFT%<tT%<tz',
 *         city, pop_2011, (float)(pop_2011/69.0f), (long)(pop_2011 * 1000000L));
 *
 *     -'   8244910 New York           8,244,910   7dceae 119491.453 2231-04-09T23:46:40-0500'
 */
public class SPRINTF extends EvalFunc<String> {

    @Override
    public String exec(Tuple input) throws IOException {
        StringBuilder sb        = new StringBuilder();
        Formatter     formatter = new Formatter(sb);
        try{
            if (input == null || input.size() == 0) return null;
            if (input.get(0) == null){ return null; }

            String   fmt  = String.valueOf(input.get(0));
            Object[] args = new Object[input.size()-1];
            for (int i = 1; i < input.size(); i++) {
                args[i-1] =  input.get(i);
                if (args[i-1] == null){ return null; }
            }

            formatter.format(fmt, args);
            return sb.toString();
        } catch (ExecException exp) {
            throw exp;
        } catch (Exception err) {
            int errCode = 2106;
            String msg = "Error while computing string format in " +
                this.getClass().getSimpleName() + " -- " + err.toString();
            throw new ExecException(msg, errCode, PigException.BUG, err);
        } finally {
            formatter.close();
        }
    }

    @Override
    public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(null, DataType.CHARARRAY));
    }

    @Override
    public SchemaType getSchemaType() {
        return SchemaType.VARARG;
    }
}

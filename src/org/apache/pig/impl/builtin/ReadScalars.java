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
package org.apache.pig.impl.builtin;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * ReadScalars reads a line from a file and returns it as its value. The
 * file is only read once, and the same line is returned over and over again.
 * This is useful for incorporating a result from an agregation into another
 * evaluation.
 */
public class ReadScalars extends EvalFunc<String> {
    private String scalarfilename = null;
    private String charset = "UTF-8";
    private String value = null;

    /**
     * Java level API
     * 
     * @param input
     *            expects a single constant that is the name of the file to be
     *            read
     */
    @Override
    public String exec(Tuple input) throws IOException {
        if (value == null) {
            if (input == null || input.size() == 0)
                return null;

            InputStream is;
            BufferedReader reader;
            int pos;
            try {
                pos = DataType.toInteger(input.get(0));
                scalarfilename = DataType.toString(input.get(1));

                is = FileLocalizer.openDFSFile(scalarfilename);
                reader = new BufferedReader(new InputStreamReader(is, charset));
            } catch (Exception e) {
                throw new ExecException("Failed to open file '" + scalarfilename
                        + "'; error = " + e.getMessage());
            }
            try {
                String line = reader.readLine();
                if(line == null) {
                    log.warn("No scalar field to read, returning null");
                    return null;
                }
                String[] lineTok = line.split("\t");
                if(pos > lineTok.length) {
                    log.warn("No scalar field to read, returning null");
                    return null;
                }
                value = lineTok[pos];
                if(reader.readLine() != null) {
                    throw new ExecException("Scalar has more than one row in the output");
                }
            } catch (Exception e) {
                throw new ExecException(e.getMessage());
            } finally {
                reader.close();
                is.close();
            }
        }
        return value;
    }

    @Override
    public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(getSchemaName("ReadScalars", input),
                DataType.CHARARRAY));
    }
}

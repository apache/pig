/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * This is the main driver for parameter substitution 
 * http://wiki.apache.org/pig/ParameterSubstitution
 */
package org.apache.pig.tools.parameters;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.StringReader;
import java.io.Writer;
import java.util.Arrays;

public class ParameterSubstitutionPreprocessor {

    private PreprocessorContext pc;
    private final Log log = LogFactory.getLog(getClass());
    private PigFileParser pigParser;

    /**
     * @param limit - max number of parameters to expect. Smaller values would
     * would not cause incorrect behavior but would impact performance
     */
    public ParameterSubstitutionPreprocessor(int limit) {
        this(new PreprocessorContext(limit));
    }

    public ParameterSubstitutionPreprocessor(PreprocessorContext pc) {
        this.pc = pc;
        StringReader sr = null;
        pigParser = new PigFileParser(sr);
        pigParser.setContext(pc);
    }

    /**
     * This is the main API that takes script template and produces pig script 
     * @param pigInput - input stream that contains pig file
     * @param pigOutput - stream where transformed file is written
     * @param paramVal - map of parameter names to values
     */
    public void genSubstitutedFile(BufferedReader pigInput, Writer pigOutput) throws ParseException {
        // In case there is no EOL before EOF, add EOL for each line
        String line = null;
        StringBuilder blder = new StringBuilder();       
        try {
            while ((line = pigInput.readLine()) != null) { 
                blder.append(line).append("\n");
            }
        } catch (IOException e) {
            throw new ParseException(e.getMessage());
        }
                
        pigInput = new BufferedReader(new StringReader(blder.toString()));

        // perform the substitution
        parsePigFile(pigInput , pigOutput);
    }

    // Kept for compatibility with old interface
    public void genSubstitutedFile(BufferedReader pigInput, Writer pigOutput,
                                   String[] params, String[] paramFiles) throws ParseException {
        try {
            pc.loadParamVal(params == null ? null : Arrays.asList(params),
                            paramFiles == null ? null : Arrays.asList(paramFiles));
            genSubstitutedFile(pigInput, pigOutput);
        } catch (IOException e) {
            throw new ParseException(e.getMessage());
        }
    }

    private void parsePigFile(BufferedReader in, Writer out) throws ParseException {
        pigParser.setOutputWriter(out);
        pigParser.ReInit(in);
        try {
            pigParser.Parse();
            //close input and output streams
            in.close();
            out.flush();
            out.close();
        } catch (IOException e) {
            RuntimeException rte = new RuntimeException(e.getMessage() , e);
            throw rte;
        }
    }
}

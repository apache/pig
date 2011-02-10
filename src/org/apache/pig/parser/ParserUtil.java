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

package org.apache.pig.parser;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;

import org.antlr.runtime.ANTLRReaderStream;
import org.antlr.runtime.Token;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.tools.pigstats.ScriptState;

public abstract class ParserUtil {

    private static final Log LOG = LogFactory.getLog(ParserUtil.class);

    public static String expandMacros(Reader rd) throws IOException {
        // import macro files
        ANTLRReaderStream input1 = new ANTLRReaderStream(rd);
        MacroImport importer = new MacroImport(input1);
        Token token = Token.EOF_TOKEN;
        while ((token = importer.nextToken()) != Token.EOF_TOKEN);
        
        String ret1 = importer.getResultString();
        LOG.info("Macro imported script:\n" + ret1);
        
        // expand macros
        StringReader srd = new StringReader(ret1);
        ANTLRReaderStream input2 = new ANTLRReaderStream(srd);
        MacroExpansion expander = new MacroExpansion(input2);
        while ((token = expander.nextToken()) != Token.EOF_TOKEN);
        
        String ret2 = expander.getResultString();
        LOG.info("Macro expanded script:\n" + ret2);
        
        return ret2;
    }
    
    public static BufferedReader getImportScriptAsReader(String scriptPath)
            throws FileNotFoundException {
        File f = new File(scriptPath);
        if (f.exists() || f.isAbsolute() || scriptPath.startsWith("./")
                || scriptPath.startsWith("../")) {
            return new BufferedReader(new FileReader(f));
        }

        ScriptState state = ScriptState.get();
        if (state != null && state.getPigContext() != null) {
            String srchPath = state.getPigContext().getProperties()
                    .getProperty("pig.import.search.path");
            if (srchPath != null) {
                String[] paths = srchPath.split(",");
                for (String path : paths) {
                    File f1 = new File(path + File.separator + scriptPath);
                    if (f1.exists()) {
                        return new BufferedReader(new FileReader(f1));
                    }
                }
            }
        }
        
        throw new FileNotFoundException("Can't find the Specified file " + scriptPath );
    }
    
    public static BufferedReader getExpandedMacroAsBufferedReader(Reader rd)
            throws IOException {
        return new BufferedReader(new StringReader(expandMacros(rd)));
    }

}


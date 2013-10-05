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
package org.apache.pig.scripting.streaming.python;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.ExecType;
import org.apache.pig.FuncSpec;
import org.apache.pig.impl.PigContext;
import org.apache.pig.scripting.ScriptEngine;
import org.apache.pig.tools.pigstats.PigStats;

public class PythonScriptEngine extends ScriptEngine {
    private static final Log log = LogFactory.getLog(PythonScriptEngine.class);

    @Override
    public void registerFunctions(String path, String namespace,
            PigContext pigContext) throws IOException {
        
        String fileName = path.substring(0, path.length() - ".py".length());
        log.debug("Path: " + path + " FileName: " + fileName + " Namespace: " + namespace);
        File f = new File(path);

        if (!f.canRead()) {
            throw new IOException("Can't read file: " + path);
        }
        
        FileInputStream fin = new FileInputStream(f);
        List<String[]> functions = getFunctions(fin);
        namespace = namespace == null ? "" : namespace + NAMESPACE_SEPARATOR;
        for(String[] functionInfo : functions) {
            String name = functionInfo[0];
            String schemaString = functionInfo[1];
            String schemaLineNumber = functionInfo[2];
            String alias = namespace + name;
            String execType = (pigContext.getExecType() == ExecType.LOCAL? "local" : "mapreduce");
            String isIllustrate = (Boolean.valueOf(pigContext.inIllustrator)).toString();
            log.debug("Registering Function: " + alias);
            pigContext.registerFunction(alias, 
                                        new FuncSpec("StreamingUDF", 
                                                new String[] {
                                                    "python", 
                                                    fileName, name, 
                                                    schemaString, schemaLineNumber,
                                                    execType, isIllustrate
                                        }));
        }
        fin.close();
    }

    @Override
    protected Map<String, List<PigStats>> main(PigContext context,
            String scriptFile) throws IOException {
        log.warn("ScriptFile: " + scriptFile);
        registerFunctions(scriptFile, null, context);
        return getPigStatsMap();
    }

    @Override
    protected String getScriptingLang() {
        return "streaming_python";
    }

    @Override
    protected Map<String, Object> getParamsFromVariables() throws IOException {
        throw new IOException("Unsupported Operation");
    }
    
    private static final Pattern pSchema = Pattern.compile("^\\s*\\W+outputSchema.*");
    private static final Pattern pDef = Pattern.compile("^\\s*def\\s+(\\w+)\\s*.+");

    private static List<String[]> getFunctions(InputStream is) throws IOException {
        List<String[]> functions = new ArrayList<String[]>();
        InputStreamReader in = new InputStreamReader(is, Charset.defaultCharset());
        BufferedReader br = new BufferedReader(in);
        String line = br.readLine();
        String schemaString = null;
        String schemaLineNumber = null;
        int lineNumber = 1;
        while (line != null) {
            if (pSchema.matcher(line).matches()) {
                int start = line.indexOf("(") + 2; //drop brackets/quotes
                int end = line.lastIndexOf(")") - 1;
                schemaString = line.substring(start,end).trim();
                schemaLineNumber = "" + lineNumber;
            } else if (pDef.matcher(line).matches()) {
                int start = line.indexOf("def ") + "def ".length();
                int end = line.indexOf('(');
                String functionName = line.substring(start, end).trim();
                if (schemaString != null) {
                    String[] funcInfo = {functionName, schemaString, "" + schemaLineNumber};
                    functions.add(funcInfo);
                    schemaString = null;
                }
            }
            line = br.readLine();
            lineNumber++;
        }
        return functions;
    }
}

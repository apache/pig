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

package org.apache.pig.scripting.jython;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.FuncSpec;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.logicalLayer.parser.ParseException;
import org.apache.pig.impl.util.Utils;
import org.apache.pig.scripting.ScriptEngine;
import org.apache.pig.tools.pigstats.PigStats;
import org.python.core.Py;
import org.python.core.PyException;
import org.python.core.PyFrame;
import org.python.core.PyFunction;
import org.python.core.PyObject;
import org.python.core.PyStringMap;
import org.python.core.PyTuple;
import org.python.util.PythonInterpreter;

/**
 * Implementation of the script engine for Jython
 */
public class JythonScriptEngine extends ScriptEngine {
    
    private static final Log LOG = LogFactory.getLog(JythonScriptEngine.class);
    
    // Decorators -
    // "schemaFunction"
    // "outputSchema"
    // "outputSchemaFunction"
    
    /**
     * Language Interpreter Uses static holder pattern
     */
    private static class Interpreter {
        static final PythonInterpreter interpreter = new PythonInterpreter();
        static volatile ArrayList<String> filesLoaded = new ArrayList<String>();
        
        static synchronized void init(String path) throws IOException {           
            if (!filesLoaded.contains(path)) {
                // attempt addition of schema decorator handler, fail silently
                interpreter.exec("def outputSchema(schema_def):\n"
                        + "    def decorator(func):\n"
                        + "        func.outputSchema = schema_def\n"
                        + "        return func\n" 
                        + "    return decorator\n\n");

                interpreter.exec("def outputSchemaFunction(schema_def):\n"
                        + "    def decorator(func):\n"
                        + "        func.outputSchemaFunction = schema_def\n"
                        + "        return func\n"
                        + "    return decorator\n");
                
                interpreter.exec("def schemaFunction(schema_def):\n"
                        + "     def decorator(func):\n"
                        + "         func.schemaFunction = schema_def\n"    
                        + "         return func\n"
                        + "     return decorator\n\n");
                
                InputStream is = getScriptAsStream(path);
                try {
                    execfile(is);
                    filesLoaded.add(path);
                } finally {
                    is.close();
                }
            }           
        }        
        
        static void execfile(InputStream script) throws ExecException {
            try {
                interpreter.execfile(script);
            } catch (PyException e) {
                String message = "Python Error. "+e.toString();
                throw new ExecException(message, 1121, e);
            }
        }
        
        static String get(String name) {
            return interpreter.get(name).toString();
        }
        
        static void setMain(boolean isMain) {
            if (isMain) {
                interpreter.set("__name__", "__main__");
            } else {
                interpreter.set("__name__", "__lib__");
            }
        }
    }

    @Override
    public void registerFunctions(String path, String namespace, PigContext pigContext)
    throws IOException{  
        Interpreter.setMain(false);
        Interpreter.init(path);
        pigContext.scriptJars.add(getJarPath(PythonInterpreter.class));
        PythonInterpreter pi = Interpreter.interpreter;
        @SuppressWarnings("unchecked")
        List<PyTuple> locals = (List<PyTuple>) ((PyStringMap) pi.getLocals()).items();
        namespace = (namespace == null) ? "" : namespace + NAMESPACE_SEPARATOR;
        try {
            for (PyTuple item : locals) {
                String key = (String) item.get(0);
                Object value = item.get(1);
                FuncSpec funcspec = null;
                if (!key.startsWith("__") && !key.equals("schemaFunction")
                        && !key.equals("outputSchema")
                        && !key.equals("outputSchemaFunction")
                        && (value instanceof PyFunction)
                        && (((PyFunction)value).__findattr__("schemaFunction".intern())== null)) {
                    PyObject obj = ((PyFunction)value).__findattr__("outputSchema".intern());
                    if(obj != null) {
                        Utils.getSchemaFromString(obj.toString());
                    }
                    funcspec = new FuncSpec(JythonFunction.class.getCanonicalName() + "('"
                            + path + "','" + key +"')");
                    pigContext.registerFunction(namespace + key, funcspec);           
                    LOG.info("Register scripting UDF: " + namespace + key);
                }
            }
        } catch (ParseException pe) {
            throw new IOException(
                    "Error parsing schema for script function from the decorator",
                    pe);
        }
        pigContext.addScriptFile(path);
        Interpreter.setMain(true);   
    }

    /**
     * Gets the Python function object.
     * @param path Path of the jython script file containing the function.
     * @param functionName Name of the function
     * @return a function object
     * @throws IOException
     */
    public static PyFunction getFunction(String path, String functionName) throws IOException {
        Interpreter.setMain(false);
        Interpreter.init(path);
        return (PyFunction) Interpreter.interpreter.get(functionName);
    }
    
    @Override
    protected Map<String, List<PigStats>> main(PigContext pigContext, String scriptFile) 
            throws IOException {
        PigServer pigServer = new PigServer(pigContext, false);
        
        // register dependencies
        String jythonJar = getJarPath(PythonInterpreter.class); 
        if (jythonJar != null) {
            pigServer.registerJar(jythonJar);
        }
                       
        File f = new File(scriptFile);

        if (!f.canRead()) {
            throw new IOException("Can't read file: " + scriptFile);
        }
        
        // TODO: fis1 is not closed
        FileInputStream fis1 = new FileInputStream(scriptFile);
        if (hasFunction(fis1)) { 
            registerFunctions(scriptFile, null, pigContext);    
        }
        
        Interpreter.setMain(true);       
        FileInputStream fis = new FileInputStream(scriptFile);
        try {
            load(fis);
        } finally {
            fis.close();
        }   
        return getPigStatsMap();
    }
   
//    @Override
    public void load(InputStream script) throws IOException {
        Interpreter.execfile(script);
    }

    @Override
    protected String getScriptingLang() {
        return "jython";
    }

    @Override
    protected Map<String, Object> getParamsFromVariables() throws IOException {
        PyFrame frame = Py.getFrame();
        @SuppressWarnings("unchecked")
        List<PyTuple> locals = (List<PyTuple>) ((PyStringMap) frame.getLocals()).items();
        Map<String, Object> vars = new HashMap<String, Object>();
        for (PyTuple item : locals) {
            String key = (String) item.get(0);
            Object obj = item.get(1);
            if (obj != null) {
                String value = item.get(1).toString();
                vars.put(key, value);
            }
        }
        return vars;
    }
    
    private static final Pattern p = Pattern.compile("^\\s*def\\s+(\\w+)\\s*.+");
    private static final Pattern p1 = Pattern.compile("^\\s*if\\s+__name__\\s+==\\s+[\"']__main__[\"']\\s*:\\s*$");
   
    private static boolean hasFunction(InputStream is) throws IOException {
        boolean hasFunction = false;    
        boolean hasMain = false;     
        InputStreamReader in = new InputStreamReader(is);
        BufferedReader br = new BufferedReader(in);
        String line = br.readLine();
        while (line != null) {
            if (p.matcher(line).matches()) {
                hasFunction = true;                 
            } else if (p1.matcher(line).matches()) {
                hasMain = true;
            }
            line = br.readLine();            
        }        
        if (hasFunction && !hasMain) {
            String msg = "Embedded script cannot mix UDFs with top level code. " +
            		"Please use if __name__ == '__main__': construct";
            throw new IOException(msg);
        }
        return hasFunction;    
    }
}

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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.pig.FuncSpec;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.logicalLayer.parser.ParseException;
import org.apache.pig.impl.util.Utils;
import org.apache.pig.scripting.ScriptEngine;
import org.python.core.PyFunction;
import org.python.core.PyObject;
import org.python.core.PyStringMap;
import org.python.core.PyTuple;
import org.python.util.PythonInterpreter;

/**
 * Implementation of the script engine for Jython
 */
public class JythonScriptEngine extends ScriptEngine {
    
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

                InputStream is = null;
                File file = new File(path);
                if (file.exists()) {
                    is = new FileInputStream(file);
                }
                else {
                    if (file.isAbsolute()) {
                        is = Interpreter.class.getResourceAsStream(path);
                    } else {
                        is = Interpreter.class.getResourceAsStream("/" + path);
                    }
                }
                if (is != null) {
                    interpreter.execfile(is);
                    filesLoaded.add(path);
                    is.close();
                } else {
                    throw new IOException(
                    "Could not initialize interpreter with "+ path);
                }
            }
        }
    }

    @Override
    public void registerFunctions(String path, String namespace, PigContext pigContext)
    throws IOException{
        Interpreter.init(path);
        pigContext.scriptJars.add(getJarPath(PythonInterpreter.class));
        PythonInterpreter pi = Interpreter.interpreter;
        @SuppressWarnings("unchecked")
        List<PyTuple> locals = (List<PyTuple>) ((PyStringMap) pi.getLocals()).items();
        if(namespace == null) {
            namespace = "";
        }
        else {
            namespace = namespace + namespaceSeparator;
        }
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
                }
            }
        } catch (ParseException pe) {
            throw new IOException("Error parsing schema for script function from the decorator " + pe);
        }
    }

    public static PyFunction getFunction(String path, String functionName) throws IOException {
        Interpreter.init(path);
        return (PyFunction) Interpreter.interpreter.get(functionName);
    }
}

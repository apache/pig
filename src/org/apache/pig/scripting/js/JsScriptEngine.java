/**
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
package org.apache.pig.scripting.js;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.FuncSpec;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.util.UDFContext;
import org.apache.pig.scripting.ScriptEngine;
import org.apache.pig.tools.pigstats.PigStats;
import org.mozilla.javascript.Context;
import org.mozilla.javascript.EcmaError;
import org.mozilla.javascript.Function;
import org.mozilla.javascript.ImporterTopLevel;
import org.mozilla.javascript.NativeFunction;
import org.mozilla.javascript.Scriptable;

/**
 * {@link ScriptEngine} implementation for JavaScript
 */
public class JsScriptEngine extends ScriptEngine {
    private static final Log LOG = LogFactory.getLog(JsScriptEngine.class);

    /** holds the instance created on client side to parse the js */
    private static JsScriptEngine clientInstance;

    /** holds the singleton for UDFs */
    private static final class Holder {
        static JsScriptEngine instance;
        static {
            if (clientInstance!=null) { 
                /** on client side UDFs re-use the same instance */ 
                instance = clientInstance;
            } else {
                /** on the hadoop slave a new instance is created using the UDFContext for initialization */
                instance = new JsScriptEngine();
                String scriptPath = (String) UDFContext.getUDFContext().getUDFProperties(JsFunction.class).get(JsScriptEngine.class.getName()+".scriptFile");
                instance.scriptPath = scriptPath;
                if (scriptPath == null) {
                    throw new IllegalStateException("could not get script path from UDFContext");
                }
                InputStream is = getScriptAsStream(scriptPath);
                try {
                    instance.load(scriptPath, is);
                } finally {
                    try {
                        is.close();
                    } catch (IOException e) {
                        LOG.warn("Could not close stream for file "+scriptPath, e);
                    }
                }

            }
        }
    }

    public static JsScriptEngine getInstance() {
        JsScriptEngine instance = Holder.instance;
        /** add script path information in the UDFContext */
        UDFContext.getUDFContext().getUDFProperties(JsFunction.class).put(JsScriptEngine.class.getName()+".scriptFile", instance.scriptPath);        
        return instance;
    }

    private ThreadLocal<Context> cx = new ThreadLocal<Context>();
    private Scriptable scope;
    private String scriptPath;

    /** print functions to help with debugging */
    private static final String printSource =
        "function print(str) {                \n" +
        "    if (typeof(str) == 'undefined') {         \n" +
        "        str = 'undefined';                    \n" +
        "    } else if (str == null) {                 \n" +
        "        str = 'null';                         \n" +
        "    }\n" +
        "    java.lang.System.out.print(String(str));\n" +
        "}\n" +
        "function println(str) {                       \n" +
        "    if (typeof(str) == 'undefined') {         \n" +
        "        str = 'undefined';                    \n" +
        "    } else if (str == null) {                 \n" +
        "        str = 'null';                         \n" +
        "    }\n" +
        "    java.lang.System.out.println(String(str));\n" +
        "}";

    /**
     * @return the javascript context for the current thread
     */
    private Context getContext() {
        Context context = cx.get();
        if (context == null) {
            context = Context.enter();
            cx.set(context);
        }
        return context;
    }

    /**
     * evaluate a javascript String
     * @param name the name of the script (for error messages)
     * @param script the content of the script
     * @return the result
     */
    public Object jsEval(String name, String script) {
        try {
            return getContext().evaluateString(scope, script, name, 1, null);
        } catch (EcmaError e) {
            throw new RuntimeException("can't evaluate "+name+": "+script,e);
        }
    }

    /**
     * evaluate javascript from a reader
     * @param name the name of the script (for error messages)
     * @param scriptReader the content of the script
     * @return the result
     */
    public Object jsEval(String name, Reader scriptReader) {
        try {
            return getContext().evaluateReader(scope, scriptReader, name, 1, null);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * put a value in the current scope
     * @param name the name of the variable
     * @param value its value
     */
    public void jsPut(String name, Object value) {
        scope.put(name, scope, value);
    }

    /**
     * call a javascript function
     * @param functionName the name of the function
     * @param passedParams the parameters to pass
     * @return the result of the function
     */
    public Object jsCall(String functionName, Object[] passedParams) {
        Function f = (Function)scope.get(functionName, scope);
        Object result = f.call(getContext(), scope, scope, passedParams);
        return result;
    }

    Scriptable getScope() {
        return scope;
    }

    /**
     * creates a new JavaScript object
     * @return an empty object
     */
    public Scriptable jsNewObject() {
        return getContext().newObject(scope);
    }

    /**
     * creates a new javascript array
     * @param size the size of the array
     * @return an empty array of the given size
     */
    public Scriptable jsNewArray(long size) {
        return getContext().newArray(scope, (int)size);
    }

    public JsScriptEngine() {
        Context context = getContext();
        scope = new ImporterTopLevel(context); // so that we can use importPackage()
        context.evaluateString(scope, printSource, "print", 1, null); // so that we can print on std out

    }

    protected Object eval(String expr) {
        return jsEval(expr, "new String("+expr+")");
    }

    protected void load(String name, InputStream script) {
        jsEval(name, new InputStreamReader(script));
    }

    @Override
    protected Map<String, List<PigStats>> main(PigContext pigContext,
            String scriptFile) throws IOException {

        File f = new File(scriptFile);

        if (!f.canRead()) {
            throw new IOException("Can't read file: " + scriptFile);
        }

        registerFunctions(scriptFile, null, pigContext);    

        // run main
        jsEval("main", "main();");

        return getPigStatsMap();
    }

    @Override
    public void registerFunctions(String path, String namespace,
            PigContext pigContext) throws IOException {
        // to enable passing of information to the slave
        this.scriptPath = path;
        this.clientInstance = this;
        
        pigContext.scriptJars.add(getJarPath(Context.class));
        namespace = (namespace == null) ? "" : namespace + NAMESPACE_SEPARATOR;
        FileInputStream fis = new FileInputStream(path);
        try {
            load(path, fis);
        } finally {
            fis.close();
        }
        Object[] ids = scope.getIds();
        for (Object id : ids) {
            if (id instanceof String) {
                String name = (String) id;
                Object value = scope.get(name, scope);
                if (value instanceof Function
                        && !name.equals("print") // ignoring the print functions that were added by this class in the initialization
                        && !name.equals("println")) {

                    FuncSpec funcspec = new FuncSpec(JsFunction.class.getCanonicalName() + "('" + name + "')");
                    LOG.info("Register scripting UDF: " + name);
                    pigContext.registerFunction(namespace + name, funcspec);           
                }
            }
        }
        pigContext.addScriptFile(path);

    }


    @Override
    protected Map<String, Object> getParamsFromVariables() throws IOException {
        Map<String, Object> params = new HashMap<String, Object>();
        Object[] ids = scope.getIds();
        for (Object id : ids) {
            if (id instanceof String) {
                String name = (String) id;
                Object value = scope.get(name, scope);
                if (!(value instanceof NativeFunction)) {
                    LOG.debug("Registering parameter "+name+" => "+scope.get(name, scope));
                    params.put(name, eval(name).toString());
                }
            }
        }
        return params;
    }

    @Override
    protected String getScriptingLang() {
        return "javascript";
    }

}

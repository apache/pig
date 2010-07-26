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

package org.apache.pig.scripting;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.StringReader;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.logicalLayer.parser.ParseException;
import org.apache.pig.impl.logicalLayer.parser.QueryParser;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * Base class for various scripting implementations
 */
abstract public class ScriptEngine {
    /**
     * Pig supported scripting languages with their keywords
     */
    private static final Map<String, String> supportedScriptLangs = new HashMap<String, String>();
    static {
        // Python
        supportedScriptLangs.put("jython", "org.apache.pig.scripting.jython.JythonScriptEngine");
        // Ruby
        //supportedScriptLangs.put("jruby", "org.apache.pig.scripting.jruby.JrubyScriptEngine");
    }
    public static final String namespaceSeparator = ".";
    
    /**
     * registers the Jython functions as Pig functions with given namespace
     * 
     * @param path path of the script
     * @param namespace namespace for the functions
     * @param pigContext pigcontext to register functions to pig in the given namespace
     * @throws IOException
     */
    public abstract void registerFunctions(String path, String namespace, PigContext pigContext) throws IOException;

    /** 
     * figure out the jar location from the class 
     * @param clazz
     * @return the jar file location, null if the class was not loaded from a jar
     * @throws FileNotFoundException 
     */
    protected static String getJarPath(Class<?> clazz) throws FileNotFoundException {
        URL resource = clazz.getClassLoader().getResource(clazz.getCanonicalName().replace(".","/")+".class");
        if (resource.getProtocol().equals("jar")) {
            return resource.getPath().substring(resource.getPath().indexOf(':')+1,resource.getPath().indexOf('!'));
        }
        throw new FileNotFoundException("Jar for "+ clazz.getName() +" class is not found");
    }

    /**
     * get instance of the scriptEngine for the given scriptingLang
     * 
     * @param scriptingLang ScriptEngine classname or keyword for the scriptingLang
     * @return scriptengine for the given scripting language
     * @throws IOException
     */
    public static ScriptEngine getInstance(String scriptingLang)
    throws IOException {
        String scriptingEngine = scriptingLang;
        try {
            if(supportedScriptLangs.containsKey(scriptingLang)) {
                scriptingEngine = supportedScriptLangs.get(scriptingLang);
            }
            return (ScriptEngine) Class.forName(scriptingEngine).newInstance();
        } catch (Exception e) {
            throw new IOException("Could not load ScriptEngine: "
                    + scriptingEngine + ": " + e);
        }
    }
}

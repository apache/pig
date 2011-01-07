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
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.impl.PigContext;
import org.apache.pig.tools.pigstats.PigStats;

/**
 * Base class for various scripting implementations
 */
public abstract class ScriptEngine {
    /**
     * Pig supported scripting languages with their keywords
     */
    private static final Map<String, String> supportedScriptLangs = new HashMap<String, String>();
    static {
        // Python
        supportedScriptLangs.put("jython",
                "org.apache.pig.scripting.jython.JythonScriptEngine");
        // Ruby
        // supportedScriptLangs.put("jruby",
        // "org.apache.pig.scripting.jruby.JrubyScriptEngine");
    }
    
    public static final String NAMESPACE_SEPARATOR = ".";
       
    private Map<String, List<PigStats>> statsMap = new HashMap<String, List<PigStats>>();   
    
    /**
     * Registers scripting language functions as Pig functions with given namespace
     * 
     * @param path path of the script
     * @param namespace namespace for the functions
     * @param pigContext pigcontext to register functions to pig in the given namespace
     * @throws IOException
     */
    public abstract void registerFunctions(String path, String namespace,
            PigContext pigContext) throws IOException;
    
    /**
     * Actually runs the script file. This method will be implemented by 
     * individual script engines.
     * 
     * @param context {@link ScriptPigContext} to run the script file
     * @param scriptFile the file
     * @throws IOException
     */
    protected abstract Map<String, List<PigStats>> main(
           PigContext context, String scriptFile) throws IOException;
    
    /**
     * Loads the script in the interpreter.
     * @param script
     */
    protected abstract void load(InputStream script);

    /**
     * Gets ScriptEngine classname or keyword for the scripting language
     */
    protected abstract String getScriptingLang();

    /**
     * Returns a map from local variable names to their values
     * @throws IOException
     */
    protected abstract Map<String, String> getParamsFromVariables()
            throws IOException;
    
    /** 
     * Figures out the jar location from the class 
     * @param clazz class in the jar file
     * @return the jar file location, null if the class was not loaded from a jar
     * @throws FileNotFoundException 
     */
    public static String getJarPath(Class<?> clazz)
            throws FileNotFoundException {
        URL resource = clazz.getClassLoader().getResource(
                clazz.getCanonicalName().replace(".", "/") + ".class");
        if (resource.getProtocol().equals("jar")) {
            return resource.getPath().substring(
                    resource.getPath().indexOf(':') + 1,
                    resource.getPath().indexOf('!'));
        }
        throw new FileNotFoundException("Jar for " + clazz.getName()
                + " class is not found");
    }

    /**
     * Gets instance of the scriptEngine for the given scripting language
     * 
     * @param scriptingLang ScriptEngine classname or keyword for the scriptingLang
     * @return scriptengine for the given scripting language
     * @throws IOException
     */
    public static ScriptEngine getInstance(String scriptingLang)
            throws IOException {
        String scriptingEngine = scriptingLang;
        try {
            if (supportedScriptLangs.containsKey(scriptingLang)) {
                scriptingEngine = supportedScriptLangs.get(scriptingLang);
            }
            return (ScriptEngine) Class.forName(scriptingEngine).newInstance();
        } catch (Exception e) {
            throw new IOException("Could not load ScriptEngine: "
                    + scriptingEngine + ": " + e);
        }
    }
    
    /**
     * Runs a script file. 
     * @param pigContext {@link PigContext} to run the script file
     * @param scriptFile the file
     * @return a collection of {@link PigStats} objects. One for each runtime 
     * instance of {@link Pig} in the script. For named pipelines, the
     * map key is the name of the pipeline. For unnamed pipeline, the map key 
     * is the script id associated with the instance. 
     * @throws ExecException
     * @throws IOException
     */
    public Map<String, List<PigStats>> run(PigContext pigContext, String scriptFile)
            throws ExecException, IOException {
        ScriptPigContext.set(pigContext, this);
        return main(pigContext, scriptFile);
    }
        
    /**
     * Gets the collection of {@link PigStats} after the script is run.
     */
    protected Map<String, List<PigStats>> getPigStatsMap() {
        return statsMap;
    }
        
    void setPigStats(String key, PigStats stats) {
        List<PigStats> lst = statsMap.get(key);
        if (lst == null) {
            lst = new ArrayList<PigStats>();
            statsMap.put(key, lst);
        }
        lst.add(stats);        
    }
    
    void setPigStats(String key, List<PigStats> stats) {
        statsMap.put(key, stats); 
    }
               
}

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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.impl.PigContext;
import org.apache.pig.tools.pigstats.PigStats;

/**
 * Base class for various scripting implementations
 */
public abstract class ScriptEngine {
    
    public static enum SupportedScriptLang {

        // possibly jruby in the future
        jruby(new String[]{"ruby", "jruby"}, new String[]{"rb"}, "org.apache.pig.scripting.jruby.JrubyScriptEngine"),
        jython(new String[]{"python", "jython"}, new String[]{"py"}, "org.apache.pig.scripting.jython.JythonScriptEngine"), 
        javascript(new String[]{}, new String[]{"js"}, "org.apache.pig.scripting.js.JsScriptEngine");
        
        private static Set<String> supportedScriptLangs;
        static {
            supportedScriptLangs = new HashSet<String>();
            for (SupportedScriptLang value : SupportedScriptLang.values()) {
                supportedScriptLangs.add(value.name());
            }
            supportedScriptLangs = Collections.unmodifiableSet(supportedScriptLangs);
        }
        
        public static boolean contains(String supportedScriptLang) {
            return supportedScriptLangs.contains(supportedScriptLang);
        }
        
        private String[] shebangs;
        private String[] extensions;
        /** Class implementing the engine. As a string as dependencies are possibly not on the class path*/
        private String engineClassName;
        
        private SupportedScriptLang(String[] shebangs, String[] extensions, String engineClassName) {
            this.shebangs = shebangs;
            this.extensions = extensions;
            this.engineClassName = engineClassName;
        }
        
        
        /**
         * If other discovery mechanisms come up they can also override accepts()
         * @param file the path of the file
         * @param firstLine The first line of the file (possibly containing #!...)
         */
        public boolean accepts(String file, String firstLine) {
            if( firstLine == null )
            	return false;
            
            for (String shebang : shebangs) {
                Pattern p = Pattern.compile("^#!.*/" + shebang + "\\s*$");
                if (p.matcher(firstLine).matches()) {
                    return true;
                }
            } 
            
            for (String ext : extensions) {
                if (file.endsWith("."+ext)) {
                    return true;
                }
            } 
            
            return false;
        }

        public String getEngineClassName() {
            return engineClassName;
        }
        
    }
    
    private static final Pattern shebangPattern = Pattern.compile("^#!.+");
    
    private static boolean declaresShebang(String firstLine) {
    	if( firstLine == null )
    		return false;
        return shebangPattern.matcher(firstLine).matches();
    }
    
    /**
     * open a stream load a script locally or in the classpath
     * @param scriptPath the path of the script
     * @return a stream (it is the responsibility of the caller to close it)
     * @throws IllegalStateException if we could not open a stream
     */
    public static InputStream getScriptAsStream(String scriptPath) {
    //protected static InputStream getScriptAsStream(String scriptPath) {
        InputStream is = null;
        File file = new File(scriptPath);
        if (file.exists()) {
            try {
                is = new FileInputStream(file);
            } catch (FileNotFoundException e) {
                throw new IllegalStateException("could not find existing file "+scriptPath, e);
            }
        } else {
            // Try system, current and context classloader.
            is = ScriptEngine.class.getResourceAsStream(scriptPath);
            if (is == null) {
                is = getResourceUsingClassLoader(scriptPath, ScriptEngine.class.getClassLoader());
            }
            if (is == null) {
                is = getResourceUsingClassLoader(scriptPath, Thread.currentThread().getContextClassLoader());
            }
            if (is == null && !file.isAbsolute()) {
                String path = "/" + scriptPath;
                is = ScriptEngine.class.getResourceAsStream(path);
                if (is == null) {
                    is = getResourceUsingClassLoader(path, ScriptEngine.class.getClassLoader());
                }
                if (is == null) {
                    is = getResourceUsingClassLoader(path, Thread.currentThread().getContextClassLoader());
                }
            }
        }
        
        // TODO: discuss if we want to add logic here to load a script from HDFS

        if (is == null) {
            throw new IllegalStateException(
                    "Could not initialize interpreter (from file system or classpath) with " + scriptPath);
        }      
        return is;
    }
    
    private static InputStream getResourceUsingClassLoader(String fullFilename, ClassLoader loader) {
        if (loader != null) {
            return loader.getResourceAsStream(fullFilename);
        }
        return null;
    } 
    
    public static final String NAMESPACE_SEPARATOR = ".";
       
    /**
     * @param file the file to inspect
     * @return the Supported Script Lang if this is a supported script language
     * @throws IOException if there was an error reading the file or if the file defines explicitly an unknown #!
     */
    public static SupportedScriptLang getSupportedScriptLang(String file) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(file));
        String firstLine;
        try {
            firstLine = br.readLine();
        } finally {
            br.close();
        }
        for (SupportedScriptLang supportedScriptLang : SupportedScriptLang.values()) {
            if (supportedScriptLang.accepts(file, firstLine)) {
                return supportedScriptLang;
            }
        }
        if (declaresShebang(firstLine)) {
            throw new IOException("Unsupported script type is specified: " + firstLine);
        }
        return null;
    }
    
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
    
// Not needed as a general abstraction so far
//    /**
//     * Loads the script in the interpreter.
//     * @param script
//     */
//    protected abstract void load(InputStream script) throws IOException;

    /**
     * Gets ScriptEngine classname or keyword for the scripting language
     */
    protected abstract String getScriptingLang();

    /**
     * Returns a map from local variable names to their values
     * @throws IOException
     */
    protected abstract Map<String, Object> getParamsFromVariables()
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
            if (SupportedScriptLang.contains(scriptingLang)) {
                SupportedScriptLang supportedScriptLang = SupportedScriptLang.valueOf(scriptingLang);
                scriptingEngine = supportedScriptLang.getEngineClassName();
            }
            return (ScriptEngine) Class.forName(scriptingEngine).newInstance();
        } catch (Exception e) {
            throw new IOException("Could not load ScriptEngine: "
                    + scriptingEngine + " for "+scriptingLang+" (Supported langs: "+SupportedScriptLang.supportedScriptLangs+") : " + e, e);
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

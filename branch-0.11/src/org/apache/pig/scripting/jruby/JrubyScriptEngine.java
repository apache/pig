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

package org.apache.pig.scripting.jruby;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.FuncSpec;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.Utils;
import org.apache.pig.scripting.ScriptEngine;
import org.apache.pig.tools.pigstats.PigStats;

import org.jruby.Ruby;
import org.jruby.RubyArray;
import org.jruby.RubyBoolean;
import org.jruby.embed.LocalContextScope;
import org.jruby.embed.LocalVariableBehavior;
import org.jruby.embed.ScriptingContainer;
import org.jruby.javasupport.JavaEmbedUtils.EvalUnit;
import org.jruby.CompatVersion;

import com.google.common.collect.Maps;

/**
 * Implementation of the script engine for Jruby, which facilitates the registration
 * of scripts as UDFs, and also provides information (via the nested class RubyFunctions)
 * on the registered functions.
 */
public class JrubyScriptEngine extends ScriptEngine {
    private static final Log LOG = LogFactory.getLog(JrubyScriptEngine.class);

    //TODO test if it is necessary to have a per script (or even per method) runtime. PRO: avoid collisions CON: a bunch of runtimes, which could be slow
    protected static final ScriptingContainer rubyEngine;

    private boolean isInitialized = false;

    static {
        rubyEngine = new ScriptingContainer(LocalContextScope.SINGLETHREAD, LocalVariableBehavior.PERSISTENT);
        rubyEngine.setCompatVersion(CompatVersion.RUBY1_9);
    }

    /**
     * This is a static class which provides functionality around the functions that are registered with Pig.
     */
    static class RubyFunctions {
        /**
         * This cache maps function type to a map that maps path to a map of function name to the object
         * which contains information about that function. In the case of an EvalFunc, there is a special
         * function which encapsulates information about the function. In the case of an Accumulator or
         * Algebraic, it is just an instance of the Class object that extends AccumulatorPigUdf or
         * AlgebraicPigUdf, respectively.
         */
        private static Map<String, Map<String, Map<String, Object>>> functionsCache = Maps.newHashMap();

        private static Map<String, Boolean> alreadyRunCache = Maps.newHashMap();

        private static Map<String, String> cacheFunction = Maps.newHashMap();

        static {
            //TODO use an enum instead?
            cacheFunction.put("evalfunc", "PigUdf.get_functions_to_register");
            cacheFunction.put("accumulator", "AccumulatorPigUdf.classes_to_register");
            cacheFunction.put("algebraic", "AlgebraicPigUdf.classes_to_register");

            functionsCache.put("evalfunc", new HashMap<String,Map<String,Object>>());
            functionsCache.put("accumulator", new HashMap<String,Map<String,Object>>());
            functionsCache.put("algebraic", new HashMap<String,Map<String,Object>>());
        }

        @SuppressWarnings("unchecked")
        private static Map<String,Object> getFromCache(String path, Map<String,Map<String,Object>> cacheToUpdate, String regCommand) {
            Boolean runCheck = alreadyRunCache.get(path);
            if (runCheck == null || !runCheck.booleanValue()) {
                for (Map.Entry<String, Map<String, Map<String, Object>>> entry : functionsCache.entrySet())
                    entry.getValue().remove(path);

                rubyEngine.runScriptlet(getScriptAsStream(path), path);

                alreadyRunCache.put(path, true);
            }

            Map<String,Object> funcMap = cacheToUpdate.get(path);

            if (funcMap == null) {
                funcMap = (Map<String,Object>)rubyEngine.runScriptlet(regCommand);
                cacheToUpdate.put(path, funcMap);
            }

            return funcMap;
        }

        public static Map<String, Object> getFunctions(String cache, String path) {
            return getFromCache(path, functionsCache.get(cache), cacheFunction.get(cache));
        }
    }

    /**
     * Evaluates the script containing ruby udfs to determine what udfs are defined as well as
     * what libaries and other external resources are necessary. These libraries and resources
     * are then packaged with the job jar itself.
     */
    @Override
    public void registerFunctions(String path, String namespace, PigContext pigContext) throws IOException {
        if (!isInitialized) {
            pigContext.scriptJars.add(getJarPath(Ruby.class));
            pigContext.addScriptFile("pigudf.rb", "pigudf.rb");
            isInitialized = true;
        }

        for (Map.Entry<String,Object> entry : RubyFunctions.getFunctions("evalfunc", path).entrySet()) {
            String method = entry.getKey();

            String functionType = rubyEngine.callMethod(entry.getValue(), "name", String.class);

            FuncSpec funcspec = new FuncSpec(JrubyEvalFunc.class.getCanonicalName() + "('" + path + "','" + method +"')");
            pigContext.registerFunction(namespace + "." + method, funcspec);
        }

        for (Map.Entry<String,Object> entry : RubyFunctions.getFunctions("accumulator", path).entrySet()) {
            String method = entry.getKey();

            if (rubyEngine.callMethod(entry.getValue(), "check_if_necessary_methods_present", RubyBoolean.class).isFalse())
                throw new RuntimeException("Method " + method + " does not have all of the required methods present!");

            pigContext.registerFunction(namespace + "." + method, new FuncSpec(JrubyAccumulatorEvalFunc.class.getCanonicalName() + "('" + path + "','" + method +"')"));
        }

        for (Map.Entry<String,Object> entry : RubyFunctions.getFunctions("algebraic", path).entrySet()) {
            String method = entry.getKey();

            if (rubyEngine.callMethod(entry.getValue(), "check_if_necessary_methods_present", RubyBoolean.class).isFalse())
                throw new RuntimeException("Method " + method + " does not have all of the required methods present!");

            Schema schema = PigJrubyLibrary.rubyToPig(rubyEngine.callMethod(entry.getValue(), "get_output_schema", RubySchema.class));
            String canonicalName = JrubyAlgebraicEvalFunc.class.getCanonicalName() + "$";

            // In the case of an Algebraic UDF, a type specific EvalFunc is necessary (ie not EvalFunc<Object>), so we
            // inspect the type and instantiated the proper class.
            switch (schema.getField(0).type) {
                case DataType.BAG: canonicalName += "Bag"; break;
                case DataType.TUPLE: canonicalName += "Tuple"; break;
                case DataType.CHARARRAY: canonicalName += "Chararray"; break;
                case DataType.DOUBLE: canonicalName += "Double"; break;
                case DataType.FLOAT: canonicalName += "Float"; break;
                case DataType.INTEGER: canonicalName += "Integer"; break;
                case DataType.LONG: canonicalName += "Long"; break;
                case DataType.DATETIME: canonicalName += "DateTime"; break;
                case DataType.MAP: canonicalName += "Map"; break;
                case DataType.BYTEARRAY: canonicalName += "DataByteArray"; break;
                default: throw new ExecException("Unable to instantiate Algebraic EvalFunc " + method + " as schema type is invalid");
            }

            canonicalName += "JrubyAlgebraicEvalFunc";

            pigContext.registerFunction(namespace + "." + method, new FuncSpec(canonicalName + "('" + path + "','" + method +"')"));
        }

        // Determine what external dependencies to ship with the job jar
        HashSet<String> toShip = libsToShip();
        for (String lib : toShip) {
            File libFile = new File(lib);
            if (lib.endsWith(".rb")) {
                pigContext.addScriptFile(lib);
            } else if (libFile.isDirectory()) {
                //
                // Need to package entire contents of directory
                //
                List<File> files = listRecursively(libFile);
                for (File file : files) {
                    if (file.isDirectory()) {
                        continue;
                    } else if (file.getName().endsWith(".jar") || file.getName().endsWith(".zip")) {
                        pigContext.scriptJars.add(file.getPath());
                    } else {
                        String localPath = libFile.getName() + file.getPath().replaceFirst(libFile.getPath(), "");
                        pigContext.addScriptFile(localPath, file.getPath());
                    }
                }
            } else {
                pigContext.scriptJars.add(lib);
            }
        }
    }
    /**
     * Consults the scripting container, after the script has been evaluated, to
     * determine what dependencies to ship.
     * <p>
     * FIXME: Corner cases like the following: "def foobar; require 'json'; end"
     * are NOT dealt with using this method
     */
    private HashSet<String> libsToShip() {
        RubyArray loadedLibs = (RubyArray)rubyEngine.get("$\"");
        RubyArray loadPaths = (RubyArray)rubyEngine.get("$LOAD_PATH");
        // Current directory first
        loadPaths.add(0, "");

        HashSet<String> toShip = new HashSet<String>();
        HashSet<Object> shippedLib = new HashSet<Object>();

        for (Object loadPath : loadPaths) {
            for (Object lib : loadedLibs) {
                if (lib.toString().equals("pigudf.rb"))
                    continue;
                if (shippedLib.contains(lib))
                    continue;
                String possiblePath = (loadPath.toString().isEmpty()?"":loadPath.toString() +
                        File.separator) + lib.toString();
                if ((new File(possiblePath)).exists()) {
                    // remove prefix ./
                    toShip.add(possiblePath.startsWith("./")?possiblePath.substring(2):
                        possiblePath);
                    shippedLib.add(lib);
                }
            }
        }
        return toShip;
    }

    private static List<File> listRecursively(File directory) {
        File[] entries = directory.listFiles();

        ArrayList<File> files = new ArrayList<File>();

        // Go over entries
        for (File entry : entries) {
            files.add(entry);
            if (entry.isDirectory()) {
                files.addAll(listRecursively(entry));
            }
        }
        return files;
    }

    @Override
    protected Map<String, List<PigStats>> main(PigContext pigContext, String scriptFile) throws IOException {
        throw new UnsupportedOperationException("Unimplemented");
    }

    @Override
    protected String getScriptingLang() {
        return "jruby";
    }

    @Override
    protected Map<String, Object> getParamsFromVariables() throws IOException {
        Map<String, Object> vars = Maps.newHashMap();
        return vars;
    }
}

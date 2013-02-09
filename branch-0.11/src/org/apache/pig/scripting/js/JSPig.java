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
package org.apache.pig.scripting.js;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.pig.scripting.BoundScript;
import org.apache.pig.scripting.Pig;
import org.apache.pig.scripting.ScriptEngine;
import org.mozilla.javascript.NativeFunction;
import org.mozilla.javascript.NativeObject;

/**
 * Pig entry point from javascript
 * @author Julien Le Dem
 *
 */
public class JSPig {

    /**
     * I'm using composition instead of inheritance because javascript to java calls finds the existing bind() methods ambiguous
     */
    private Pig pig;
    
    private JSPig(Pig pig) {
        this.pig = pig;
    }
    
    /**
     * See {@link Pig}
     * @param cmd Filesystem command to run along with its arguments as one string.
     * @throws IOException
     */
    public static int fs(String cmd) throws IOException {
        return Pig.fs(cmd);
    }

    /**
     * See {@link Pig}
     * @param jarfile Path of jar to include.
     * @throws IOException if the indicated jarfile cannot be found.
     */
    public static void registerJar(String jarfile) throws IOException {
        Pig.registerJar(jarfile);
    }

    /**
     * See {@link Pig}
     * @param udffile Path of the script UDF file
     * @param namespace namespace of the UDFs
     * @throws IOException
     */
    public static void registerUDF(String udffile, String namespace)
            throws IOException {
        Pig.registerUDF(udffile, namespace);        
    }

    /**
     * See {@link Pig}
     * @param alias name of the defined alias
     * @param definition string this alias is defined as
     */
    public static void define(String alias, String definition)
            throws IOException {
        Pig.define(alias, definition);
    }

    /**
     * See {@link Pig}
     * @param var variable to set
     * @param value to set it to
     */
    public static void set(String var, String value) throws IOException {
        Pig.set(var, value);
    }
            
    //-------------------------------------------------------------------------
   
    /**
     * Define a Pig pipeline.  
     * @param pl Pig Latin definition of the pipeline.
     * @return Pig object representing this pipeline.
     * @throws IOException if the Pig Latin does not compile.
     */
    public static JSPig compile(String pl) throws IOException {
        return new JSPig(Pig.compile(pl));
    }

    /**
     * Define a named portion of a Pig pipeline.  This allows it
     * to be imported into another pipeline.
     * @param name Name that will be used to define this pipeline.
     * The namespace is global.
     * @param pl Pig Latin definition of the pipeline.
     * @return Pig object representing this pipeline.
     * @throws IOException if the Pig Latin does not compile.
     */
    public static JSPig compile(String name, String pl) throws IOException {
        return new JSPig(Pig.compile(name, pl));
    }

    /**
     * Define a Pig pipeline based on Pig Latin in a separate file.
     * @param filename File to read Pig Latin from.  This must be a purely 
     * Pig Latin file.  It cannot contain host language constructs in it.
     * @return Pig object representing this pipeline.
     * @throws IOException if the Pig Latin does not compile or the file
     * cannot be found.
     */
    public static JSPig compileFromFile(String filename)
            throws IOException {
        return new JSPig(Pig.compileFromFile(filename));
    }

    /**
     * Define a named Pig pipeline based on Pig Latin in a separate file.
     * This allows it to be imported into another pipeline.
     * @param name Name that will be used to define this pipeline.
     * The namespace is global.
     * @param filename File to read Pig Latin from.  This must be a purely 
     * Pig Latin file.  It cannot contain host language constructs in it.
     * @return Pig object representing this pipeline.
     * @throws IOException if the Pig Latin does not compile or the file
     * cannot be found.
     */
    public static JSPig compileFromFile(String name,
                                      String filename) throws IOException {
        return new JSPig(Pig.compileFromFile(name, filename));
    }


    /**
     * javascript helper for binding parameters. 
     * See: {@link Pig#bind(Map)}
     * @param o a javascript object to be converted into a Map
     * @return the bound script
     * @throws IOException if {@link Pig#bind(Map)} throws an IOException
     */
    public BoundScript bind(Object o) throws IOException {
        NativeObject vars = (NativeObject)o;
        Map<String, Object> params = new HashMap<String, Object>();
        Object[] ids = vars.getIds();
        for (Object id : ids) {
            if (id instanceof String) {
                String name = (String) id;
                Object value = vars.get(name, vars);
                if (!(value instanceof NativeFunction) && value != null) {
                    params.put(name, value.toString());
                }
            }
        }
        return pig.bind(params);
    }

    /**
     * See: {@link Pig#bind()}
     * @throws IOException if host language variables are not found to resolve all
     * Pig Latin parameters or if they contain unsupported types.
     */
    public BoundScript bind() throws IOException {
        return pig.bind();
    }
}

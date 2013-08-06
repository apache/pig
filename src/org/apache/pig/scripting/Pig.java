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
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FsShell;
import org.apache.pig.PigServer;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.impl.PigContext;
import org.apache.pig.tools.grunt.GruntParser;

/**
 * The class being used in scripts to interact with Pig
 */
public class Pig {

    private static final Log LOG = LogFactory.getLog(Pig.class);

    private static List<String> defineCache = new ArrayList<String>();

    private static List<String> scriptUDFCache = new ArrayList<String>();

    /**
     * Run a filesystem command.  Any output from this command is written to
     * stdout or stderr as appropriate.
     * @param cmd Filesystem command to run along with its arguments as one
     * string.
     * @throws IOException
     */
    public static int fs(String cmd) throws IOException {
        ScriptPigContext ctx = getScriptContext();
        FsShell shell = new FsShell(ConfigurationUtil.toConfiguration(ctx
                .getPigContext().getProperties()));
        int code = -1;
        if (cmd != null) {
            String[] cmdTokens = cmd.split("\\s+");
            if (!cmdTokens[0].startsWith("-")) cmdTokens[0] = "-" + cmdTokens[0];
            try {
                code = shell.run(cmdTokens);
            } catch (Exception e) {
                throw new IOException("Run filesystem command failed", e);
            }
        }
        return code;
    }

    /**
     * Run a sql command.  Any output from this command is written to
     * stdout or stderr as appropriate.
     * @param cmd sql command to run along with its arguments as one
     * string. Currently only hcat is supported as a sql backend
     * @throws IOException
     */
    public static int sql(String cmd) throws IOException {
        ScriptPigContext ctx = getScriptContext();
        if (!ctx.getPigContext().getProperties().get("pig.sql.type").equals("hcat")) {
            throw new IOException("sql command only support hcat currently");
        }
        if (ctx.getPigContext().getProperties().get("hcat.bin")==null) {
            throw new IOException("hcat.bin is not defined. Define it to be your hcat script (Usually $HCAT_HOME/bin/hcat");
        }
        String hcatBin = (String)ctx.getPigContext().getProperties().get("hcat.bin");
        if (new File("hcat.bin").exists()) {
            throw new IOException(hcatBin + " does not exist. Please check your 'hcat.bin' setting in pig.properties.");
        }
        int ret = GruntParser.runSQLCommand(hcatBin, cmd, false);
        return ret;
    }

    /**
     * Register a jar for use in Pig.  Once this is done this jar will be
     * registered for <b>all subsequent</b> Pig pipelines in this script.
     * If you wish to register it for only a single Pig pipeline, use
     * register within that definition.
     * @param jarfile Path of jar to include.
     * @throws IOException if the indicated jarfile cannot be found.
     */
    public static void registerJar(String jarfile) throws IOException {
        LOG.info("Register jar: "+ jarfile);
        ScriptPigContext ctx = getScriptContext();
        PigServer pigServer = new PigServer(ctx.getPigContext(), false);
        pigServer.registerJar(jarfile);
    }

    /**
     * Register scripting UDFs for use in Pig. Once this is done all UDFs
     * defined in the file will be available for <b>all subsequent</b>
     * Pig pipelines in this script. If you wish to register UDFS for
     * only a single Pig pipeline, use register within that definition.
     * @param udffile Path of the script UDF file
     * @param namespace namespace of the UDFs
     * @throws IOException
     */
    public static void registerUDF(String udffile, String namespace)
            throws IOException {
        LOG.info("Register script UFD file: "+ udffile);
        ScriptPigContext ctx = getScriptContext();
        ScriptEngine engine = ctx.getScriptEngine();
        // script file contains only functions, no need to separate
        // functions from control flow code
        if (namespace != null && namespace.isEmpty()) namespace = null;
        engine.registerFunctions(udffile, namespace, ctx.getPigContext());
        addRegisterScriptUDFClause(udffile, namespace);
    }

    /**
     * Define an alias for a UDF or a streaming command.  This definition
     * will then be present for <b>all subsequent</b> Pig pipelines defined in this
     * script.  If you wish to define it for only a single Pig pipeline, use
     * define within that definition.
     * @param alias name of the defined alias
     * @param definition string this alias is defined as
     */
    public static void define(String alias, String definition)
            throws IOException {
        LOG.info("Add define clause: "+ alias + " -- " + definition);
        addDefineClause(alias, definition);
    }

    /**
     * Set a variable for use in Pig Latin.  This set
     * will then be present for <b>all subsequent</b> Pig pipelines defined in this
     * script.  If you wish to set it for only a single Pig pipeline, use
     * set within that definition.
     * @param var variable to set
     * @param value to set it to
     */
    public static void set(String var, String value) throws IOException {
        ScriptPigContext ctx = getScriptContext();
        PigServer pigServer = new PigServer(ctx.getPigContext(), false);
        pigServer.getPigContext().getProperties().setProperty(var, value);
    }

    /**
     * Define a Pig pipeline.
     * @param pl Pig Latin definition of the pipeline.
     * @return Pig object representing this pipeline.
     * @throws IOException if the Pig Latin does not compile.
     */
    public static Pig compile(String pl) throws IOException {
        return compile(null, pl);
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
    public static Pig compile(String name, String pl) throws IOException {
        ScriptPigContext ctx = getScriptContext();
        StringBuilder sb = new StringBuilder();
        sb.append(getRegisterScriptUDFClauses()).append(getDefineClauses());
        sb.append(pl).append("\n");
        return new Pig(sb.toString(), ctx, name);
    }

    /**
     * Define a Pig pipeline based on Pig Latin in a separate file.
     * @param filename File to read Pig Latin from.  This must be a purely
     * Pig Latin file.  It cannot contain host language constructs in it.
     * @return Pig object representing this pipeline.
     * @throws IOException if the Pig Latin does not compile or the file
     * cannot be found.
     */
    public static Pig compileFromFile(String filename)
            throws IOException {
        return compileFromFile(null, filename);
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
    public static Pig compileFromFile(String name,
                                      String filename) throws IOException {
        return compile(name, getScriptFromFile(filename));
    }

    //-------------------------------------------------------------------------

    /**
     * Bind this to a set of variables. Values must be provided
     * for all Pig Latin parameters.
     * @param vars map of variables to bind.  Keys should be parameters defined
     * in the Pig Latin.  Values should be strings that provide values for those
     * parameters.  They can be either constants or variables from the host
     * language.  Host language variables must contain strings.
     * @return a {@link BoundScript} object
     * @throws IOException if there is not a key for each
     * Pig Latin parameter or if they contain unsupported types.
     */
    public BoundScript bind(Map<String, Object> vars) throws IOException {
        return new BoundScript(replaceParameters(script, vars), scriptContext, name);
    }

    /**
     * Bind this to multiple sets of variables.  This will
     * cause the Pig Latin script to be executed in parallel over these sets of
     * variables.
     * @param vars list of maps of variables to bind.  Keys should be parameters defined
     * in the Pig Latin.  Values should be strings that provide values for those
     * variables.  They can be either constants or variables from the host
     * language.  Host language variables must be strings.
     * @return a {@link BoundScript} object
     * @throws IOException  if there is not a key for each
     * Pig Latin parameter or if they contain unsupported types.
     */
    public BoundScript bind(List<Map<String, Object>> vars) throws IOException {
        List<String> lst = new ArrayList<String>();
        for (Map<String, Object> var : vars) {
            lst.add(replaceParameters(script, var));
        }
        return new BoundScript(lst, scriptContext, name);
    }

    /**
     * Bind a Pig object to variables in the host language (optional
     * operation).  This does an implicit mapping of variables in the host
     * language to parameters in Pig Latin.  For example, if the user
     * provides a Pig Latin statement
     * <tt> p = Pig.compile("A = load '$input';");</tt>
     * and then calls this function it will look for a variable called
     * <tt>input</tt> in the host language.  Scoping rules of the host
     * language will be followed in selecting which variable to bind.  The
     * variable bound must contain a string value.  This method is optional
     * because not all host languages may support searching for in scope
     * variables.
     * @throws IOException if host language variables are not found to resolve all
     * Pig Latin parameters or if they contain unsupported types.
     */
    public BoundScript bind() throws IOException {
        ScriptEngine engine = scriptContext.getScriptEngine();
        int index = script.indexOf('$');
        if (index == -1) { // no parameter substitution is needed
            return new BoundScript(script, scriptContext, name);
        }
        Map<String, Object> vars = engine.getParamsFromVariables();
        return bind(vars);
    }

    //-------------------------------------------------------------------------

    private String script = null;

    private ScriptPigContext scriptContext = null;

    private String name = null;

    protected Pig(String script, ScriptPigContext scriptContext, String name) {
        this.script = script;
        this.scriptContext = scriptContext;
        this.name = name;
    }

    /**
     * Replaces the $<identifier> with their actual values
     * @param qstr the pig script to rewrite
     * @param vars parameters and their values
     * @return the modified version
     */
    private String replaceParameters(String qstr, Map<String, Object> vars)
            throws IOException {

        List<String> params = new ArrayList<String>();
        for (Entry<String, Object> entry : vars.entrySet()) {
            params.add(entry.getKey() + "="
                    + fixNonEscapedDollarSign(entry.getValue().toString()));
        }

        PigContext context = getScriptContext().getPigContext();
        List<String> contextParams = context.getParams();
        if (contextParams != null) {
            for (String param : contextParams) {
                params.add(param);
            }
        }

        BufferedReader reader = new BufferedReader(new StringReader(qstr));
        String substituted =  context.doParamSubstitution(reader, params, context.getParamFiles());
        context.setParams(contextParams); // reset params that were originally in PigContext
        return substituted;
    }

    // Escape the $ so that we can use the parameter substitution
    // to perform bind operation. Parameter substitution will un-escape $
    private static String fixNonEscapedDollarSign(String s) {
        String[] tkns = s.split("\\$", -1);

        if (tkns.length == 1) return s;

        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < tkns.length -1; i++) {
            if (tkns[i].isEmpty()) {
                sb.append("\\\\");
            } else {
                sb.append(tkns[i]);
                if (tkns[i].charAt(tkns[i].length()-1) != '\\') {
                    sb.append("\\\\");
                }
            }
            sb.append("$");
        }
        sb.append(tkns[tkns.length - 1]);

        return sb.toString();
    }

    //-------------------------------------------------------------------------

    private static String getScriptFromFile(String filename) throws IOException {
        LineNumberReader rd = new LineNumberReader(new FileReader(filename));
        StringBuilder sb = new StringBuilder();
        String line = rd.readLine();
        while (line != null) {
            sb.append(line);
            sb.append("\n");
            line = rd.readLine();
        }
        rd.close();
        return sb.toString();
    }

    private static void addDefineClause(String alias, String definition) {
        defineCache.add("DEFINE " + alias + " " + definition + ";\n");
    }

    private static void addRegisterScriptUDFClause(String path, String namespace)
            throws IOException {
        ScriptPigContext ctx = getScriptContext();
        ScriptEngine engine = ctx.getScriptEngine();
        String clause = "REGISTER '" + path + "' USING "
                + engine.getScriptingLang();
        if (namespace != null && !namespace.isEmpty()) {
            clause += " AS " + namespace;
        }
        scriptUDFCache.add(clause + ";\n");
    }

    private static String getDefineClauses() {
        StringBuilder sb = new StringBuilder();
        for (String def : defineCache) {
            sb.append(def);
        }
        return sb.toString();
    }

    private static String getRegisterScriptUDFClauses() {
        StringBuilder sb = new StringBuilder();
        for (String udf : scriptUDFCache) {
            sb.append(udf);
        }
        return sb.toString();
    }

    private static ScriptPigContext getScriptContext() throws IOException {
        ScriptPigContext ctx = ScriptPigContext.get();
        if (ctx == null) {
            throw new IOException("Script context is not set");
        }
        return ctx;
    }


}

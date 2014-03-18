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
package org.apache.pig;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.io.StringReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.pig.backend.datastorage.ContainerDescriptor;
import org.apache.pig.backend.datastorage.DataStorage;
import org.apache.pig.backend.datastorage.ElementDescriptor;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.executionengine.ExecJob;
import org.apache.pig.backend.executionengine.ExecJob.JOB_STATUS;
import org.apache.pig.backend.hadoop.executionengine.HJob;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.classification.InterfaceAudience;
import org.apache.pig.classification.InterfaceStability;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.io.FileLocalizer.FetchFileRet;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.plan.CompilationMessageCollector;
import org.apache.pig.impl.plan.CompilationMessageCollector.MessageType;
import org.apache.pig.impl.streaming.StreamingCommand;
import org.apache.pig.impl.util.LogUtils;
import org.apache.pig.impl.util.PropertiesUtil;
import org.apache.pig.impl.util.UDFContext;
import org.apache.pig.impl.util.UriUtil;
import org.apache.pig.impl.util.Utils;
import org.apache.pig.newplan.DependencyOrderWalker;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.logical.expression.LogicalExpressionPlan;
import org.apache.pig.newplan.logical.expression.LogicalExpressionVisitor;
import org.apache.pig.newplan.logical.expression.ScalarExpression;
import org.apache.pig.newplan.logical.optimizer.AllExpressionVisitor;
import org.apache.pig.newplan.logical.optimizer.DanglingNestedNodeRemover;
import org.apache.pig.newplan.logical.relational.LOForEach;
import org.apache.pig.newplan.logical.relational.LOLoad;
import org.apache.pig.newplan.logical.relational.LOStore;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.newplan.logical.relational.LogicalPlanData;
import org.apache.pig.newplan.logical.relational.LogicalRelationalOperator;
import org.apache.pig.newplan.logical.relational.LogicalSchema;
import org.apache.pig.newplan.logical.visitor.CastLineageSetter;
import org.apache.pig.newplan.logical.visitor.ColumnAliasConversionVisitor;
import org.apache.pig.newplan.logical.visitor.DuplicateForEachColumnRewriteVisitor;
import org.apache.pig.newplan.logical.visitor.ImplicitSplitInsertVisitor;
import org.apache.pig.newplan.logical.visitor.ScalarVariableValidator;
import org.apache.pig.newplan.logical.visitor.ScalarVisitor;
import org.apache.pig.newplan.logical.visitor.SchemaAliasVisitor;
import org.apache.pig.newplan.logical.visitor.TypeCheckingRelVisitor;
import org.apache.pig.newplan.logical.visitor.UnionOnSchemaSetter;
import org.apache.pig.parser.QueryParserDriver;
import org.apache.pig.parser.QueryParserUtils;
import org.apache.pig.pen.ExampleGenerator;
import org.apache.pig.scripting.ScriptEngine;
import org.apache.pig.tools.grunt.GruntParser;
import org.apache.pig.tools.pigstats.JobStats;
import org.apache.pig.tools.pigstats.OutputStats;
import org.apache.pig.tools.pigstats.PigStats;
import org.apache.pig.tools.pigstats.PigStats.JobGraph;
import org.apache.pig.tools.pigstats.ScriptState;
import org.apache.pig.tools.pigstats.EmptyPigStats;
import org.apache.pig.validator.BlackAndWhitelistFilter;
import org.apache.pig.validator.BlackAndWhitelistValidator;
import org.apache.pig.validator.PigCommandFilter;

/**
 *
 * A class for Java programs to connect to Pig. Typically a program will create a PigServer
 * instance. The programmer then registers queries using registerQuery() and
 * retrieves results using openIterator() or store(). After doing so, the
 * shutdown() method should be called to free any resources used by the current
 * PigServer instance. Not doing so could result in a memory leak.
 *
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class PigServer {

    protected final Log log = LogFactory.getLog(getClass());

    public static final String PRETTY_PRINT_SCHEMA_PROPERTY = "pig.pretty.print.schema";
    private static final String PIG_LOCATION_CHECK_STRICT = "pig.location.check.strict";

    /*    
     * The data structure to support grunt shell operations.
     * The grunt shell can only work on one graph at a time.
     * If a script is contained inside another script, the grunt
     * shell first saves the current graph on the stack and works
     * on a new graph. After the nested script is done, the grunt
     * shell pops up the saved graph and continues working on it.
     */
    protected final Deque<Graph> graphs = new LinkedList<Graph>();

    /*
     * The current Graph the grunt shell is working on.
     */
    private Graph currDAG;

    protected final PigContext pigContext;

    private String jobName;

    private String jobPriority;

    private final static AtomicInteger scopeCounter = new AtomicInteger(0);

    protected final String scope = constructScope();


    private boolean isMultiQuery = true;
    private boolean aggregateWarning = true;

    private boolean validateEachStatement = false;
    private boolean skipParseInRegisterForBatch = false;

    private final BlackAndWhitelistFilter filter;

    private String constructScope() {
        // scope servers for now as a session id

        // String user = System.getProperty("user.name", "DEFAULT_USER_ID");
        // String date = (new Date()).toString();

        // scope is not really used in the system right now. It will
        // however make your explain statements look lengthy if set to
        // username-date. For now let's simplify the scope, if a real
        // scope is needed again, we might need to update all the
        // operators to not include scope in their name().
        return "" + scopeCounter.incrementAndGet();
    }

    /**
     * @param execTypeString can be 'mapreduce' or 'local'.  Local mode will
     * use Hadoop's local job runner to execute the job on the local machine.
     * Mapreduce mode will connect to a cluster to execute the job. If 
     * execTypeString is not one of these two, Pig will deduce the ExecutionEngine
     * if it is on the classpath and use it for the backend execution.
     * @throws ExecException 
     * @throws IOException
     */
    public PigServer(String execTypeString) throws ExecException, IOException {
        this(addExecTypeProperty(PropertiesUtil.loadDefaultProperties(), execTypeString));
    }

    public PigServer(Properties properties) throws ExecException, IOException {
        this(new PigContext(properties));
    }

    private static Properties addExecTypeProperty(Properties properties, String execType) {
        properties.setProperty("exectype", execType);
        return properties;
    }

    /**
     * @param execType execution type to start the engine.  Local mode will
     * use Hadoop's local job runner to execute the job on the local machine.
     * Mapreduce mode will connect to a cluster to execute the job.
     * @throws ExecException
     */
    public PigServer(ExecType execType) throws ExecException {
        this(execType, PropertiesUtil.loadDefaultProperties());
    }

    public PigServer(ExecType execType, Properties properties) throws ExecException {
        this(new PigContext(execType, properties));
    }

    public PigServer(ExecType execType, Configuration conf) throws ExecException {
        this(new PigContext(execType, conf));
    }

    public PigServer(PigContext context) throws ExecException {
        this(context, true);
    }

    public PigServer(PigContext context, boolean connect) throws ExecException {
        this.pigContext = context;
        currDAG = new Graph(false);

        aggregateWarning = "true".equalsIgnoreCase(pigContext.getProperties().getProperty("aggregate.warning"));
        isMultiQuery = "true".equalsIgnoreCase(pigContext.getProperties().getProperty("opt.multiquery","true"));

        jobName = pigContext.getProperties().getProperty(
                PigContext.JOB_NAME,
                PigContext.JOB_NAME_PREFIX + ":DefaultJobName");

        if (connect) {
            pigContext.connect();
        }

        this.filter = new BlackAndWhitelistFilter(this);

        addJarsFromProperties();
        markPredeployedJarsFromProperties();

        PigStats.start(pigContext.getExecutionEngine().instantiatePigStats());

        if (ScriptState.get() == null) {
            // If Pig was started via command line, ScriptState should have been
            // already initialized in Main. If so, we should not overwrite it.
            ScriptState.start(pigContext.getExecutionEngine().instantiateScriptState());
        }
    }

    private void addJarsFromProperties() throws ExecException {
        //add jars from properties to extraJars
        String jar_str = pigContext.getProperties().getProperty("pig.additional.jars");

        if(jar_str != null){
            // Use File.pathSeparator (":" on Linux, ";" on Windows)
            // to correctly handle path aggregates as they are represented
            // on the Operating System.
            for(String jar : jar_str.split(File.pathSeparator)){
                try {
                    registerJar(jar);
                } catch (IOException e) {
                    int errCode = 4010;
                    String msg =
                        "Failed to register jar :" + jar + ". Caught exception.";
                    throw new ExecException(
                            msg,
                            errCode,
                            PigException.USER_ENVIRONMENT,
                            e
                    );
                }
            }
        }
    }

    private void markPredeployedJarsFromProperties() throws ExecException {
        // mark jars as predeployed from properties
        String jar_str = pigContext.getProperties().getProperty("pig.predeployed.jars");

        if(jar_str != null){
            // Use File.pathSeparator (":" on Linux, ";" on Windows)
            // to correctly handle path aggregates as they are represented
            // on the Operating System.
            for(String jar : jar_str.split(File.pathSeparator)){
                if (jar.length() > 0) {
                    pigContext.markJarAsPredeployed(jar);
                }
            }
        }
    }

    public PigContext getPigContext(){
        return pigContext;
    }

    /**
     * Current DAG
     * 
     * @return
     */
    public Graph getCurrentDAG() {
        return this.currDAG;
    }
    /**
     * Set the logging level to DEBUG.
     */
    public void debugOn() {
        Logger.getLogger("org.apache.pig").setLevel(Level.DEBUG);
        pigContext.getLog4jProperties().setProperty("log4j.logger.org.apache.pig", Level.DEBUG.toString());
    }

    /**
     * Set the logging level to the default.
     */
    public void debugOff() {
        Logger.getLogger("org.apache.pig").setLevel(pigContext.getDefaultLogLevel());
        pigContext.getLog4jProperties().setProperty("log4j.logger.org.apache.pig", pigContext.getDefaultLogLevel().toString());
    }

    /**
     * Set the default parallelism for this job
     * @param p default number of reducers to use for this job.
     */
    public void setDefaultParallel(int p) {
        pigContext.defaultParallel = p;
    }

    /**
     * Starts batch execution mode.
     */
    public void setBatchOn() {
        log.debug("Create a new graph.");

        if (currDAG != null) {
            graphs.push(currDAG);
        }
        currDAG = new Graph(isMultiQuery);
    }

    /**
     * Retrieve the current execution mode.
     *
     * @return true if the execution mode is batch; false otherwise.
     */
    public boolean isBatchOn() {
        // Batch is on when there are multiple graphs on the
        // stack. That gives the right response even if multiquery was
        // turned off.
        return graphs.size() > 0;
    }

    /**
     * Returns whether there is anything to process in the current batch.
     * @throws FrontendException
     * @return true if there are no stores to process in the current
     * batch, false otherwise.
     */
    public boolean isBatchEmpty() throws FrontendException {
        if (currDAG == null) {
            int errCode = 1083;
            String msg = "setBatchOn() must be called first.";
            throw new FrontendException(msg, errCode, PigException.INPUT);
        }

        return currDAG.isBatchEmpty();
    }

    /**
     * This method parses the scripts and builds the LogicalPlan. This method
     * should be followed by {@link PigServer#executeBatch(boolean)} with
     * argument as false. Do Not use {@link PigServer#executeBatch()} after
     * calling this method as that will re-parse and build the script.
     * 
     * @throws IOException
     */
    public void parseAndBuild() throws IOException {
        if (currDAG == null || !isBatchOn()) {
            int errCode = 1083;
            String msg = "setBatchOn() must be called first.";
            throw new FrontendException(msg, errCode, PigException.INPUT);
        }
        currDAG.parseQuery();
        currDAG.buildPlan( null );
    }

    /**
     * Submits a batch of Pig commands for execution.
     * 
     * @return list of jobs being executed
     * @throws IOException
     */
    public List<ExecJob> executeBatch() throws IOException {
        return executeBatch(true);
    }

    /**
     * Submits a batch of Pig commands for execution. Parse and build of script
     * should be skipped if user called {@link PigServer#parseAndBuild()}
     * before. Pass false as an argument in which case.
     * 
     * @param parseAndBuild
     * @return
     * @throws IOException
     */
    public List<ExecJob> executeBatch(boolean parseAndBuild) throws IOException {
        if (parseAndBuild) {
            parseAndBuild();
        }

        PigStats stats = null;
        if( !isMultiQuery ) {
            // ignore if multiquery is off
            stats = PigStats.get();
        } else {
            stats = execute();
        }

        return getJobs(stats);
    }

    /**
     * Retrieves a list of Job objects from the PigStats object
     * @param stats
     * @return A list of ExecJob objects
     */
    protected List<ExecJob> getJobs(PigStats stats) {
        LinkedList<ExecJob> jobs = new LinkedList<ExecJob>();
        if (stats instanceof EmptyPigStats) {
            HJob job = new HJob(HJob.JOB_STATUS.COMPLETED, pigContext, stats.result(null)
                    .getPOStore(), null);
            jobs.add(job);
            return jobs;
        }
        JobGraph jGraph = stats.getJobGraph();
        Iterator<JobStats> iter = jGraph.iterator();
        while (iter.hasNext()) {
            JobStats js = iter.next();
            for (OutputStats output : js.getOutputs()) {
                if (js.isSuccessful()) {
                    jobs.add(new HJob(HJob.JOB_STATUS.COMPLETED, pigContext, output
                            .getPOStore(), output.getAlias(), stats));
                } else {
                    HJob hjob = new HJob(HJob.JOB_STATUS.FAILED, pigContext, output
                            .getPOStore(), output.getAlias(), stats);
                    hjob.setException(js.getException());
                    jobs.add(hjob);
                }
            }
        }
        return jobs;
    }

    /**
     * Discards a batch of Pig commands.
     *
     * @throws FrontendException
     */
    public void discardBatch() throws FrontendException {
        if (currDAG == null || !isBatchOn()) {
            int errCode = 1083;
            String msg = "setBatchOn() must be called first.";
            throw new FrontendException(msg, errCode, PigException.INPUT);
        }

        currDAG = graphs.pop();
    }

    /**
     * Add a path to be skipped while automatically shipping binaries for
     * streaming.
     *
     * @param path path to be skipped
     */
    public void addPathToSkip(String path) {
        pigContext.addPathToSkip(path);
    }

    /**
     * Defines an alias for the given function spec. This
     * is useful for functions that require arguments to the
     * constructor.
     *
     * @param function - the new function alias to define.
     * @param funcSpec - the FuncSpec object representing the name of
     * the function class and any arguments to constructor.
     */
    public void registerFunction(String function, FuncSpec funcSpec) {
        pigContext.registerFunction(function, funcSpec);
    }

    /**
     * Defines an alias for the given streaming command.
     *
     * @param commandAlias - the new command alias to define
     * @param command - streaming command to be executed
     */
    public void registerStreamingCommand(String commandAlias, StreamingCommand command) {
        pigContext.registerStreamCmd(commandAlias, command);
    }

    private URL locateJarFromResources(String jarName) throws IOException {
        Enumeration<URL> urls = ClassLoader.getSystemResources(jarName);
        URL resourceLocation = null;

        if (urls.hasMoreElements()) {
            resourceLocation = urls.nextElement();
        }

        if (urls.hasMoreElements()) {
            StringBuffer sb = new StringBuffer("Found multiple resources that match ");
            sb.append(jarName);
            sb.append(": ");
            sb.append(resourceLocation);

            while (urls.hasMoreElements()) {
                sb.append(urls.nextElement());
                sb.append("; ");
            }

            log.debug(sb.toString());
        }

        return resourceLocation;
    }

    /**
     * Registers a jar file. Name of the jar file can be an absolute or
     * relative path.
     *
     * If multiple resources are found with the specified name, the
     * first one is registered as returned by getSystemResources.
     * A warning is issued to inform the user.
     *
     * @param name of the jar file to register
     * @throws IOException
     */
    public void registerJar(String name) throws IOException {
        // Check if this operation is permitted
        filter.validate(PigCommandFilter.Command.REGISTER);

        if (pigContext.hasJar(name)) {
            log.debug("Ignoring duplicate registration for jar " + name);
            return;
        }

        // first try to locate jar via system resources
        // if this fails, try by using "name" as File (this preserves
        // compatibility with case when user passes absolute path or path
        // relative to current working directory.)
        if (name != null) {
            if (name.isEmpty()) {
                log.warn("Empty string specified for jar path");
                return;
            }

            URL resource = locateJarFromResources(name);

            if (resource == null) {
                FetchFileRet[] files = FileLocalizer.fetchFiles(pigContext.getProperties(), name);
                for (FetchFileRet file : files) {
                    File f = file.file;
                    if (!f.canRead()) {
                        int errCode = 4002;
                        String msg = "Can't read jar file: " + name;
                        throw new FrontendException(msg, errCode, PigException.USER_ENVIRONMENT);
                    }

                    pigContext.addJar(f.toURI().toURL(), name);
                }
            } else {
                pigContext.addJar(resource, name);
            }
        }
    }

    /**
     * Universal Scripting Language Support, see PIG-928
     *
     * @param path path of the script file
     * @param scriptingLang language keyword or scriptingEngine used to interpret the script
     * @param namespace namespace defined for functions of this script
     * @throws IOException
     */
    public void registerCode(String path, String scriptingLang, String namespace)
                             throws IOException {
        if (pigContext.scriptingUDFs.containsKey(path) &&
            pigContext.scriptingUDFs.get(path).equals(namespace)) {
            log.debug("Ignoring duplicate registration for scripting udf file " + path + " in namespace " + namespace);
            return;
        } else {
            pigContext.scriptingUDFs.put(path, namespace);
        }

        File f = FileLocalizer.fetchFile(pigContext.getProperties(), path).file;
        if (!f.canRead()) {
            int errCode = 4002;
            String msg = "Can't read file: " + path;
            throw new FrontendException(msg, errCode,
                    PigException.USER_ENVIRONMENT);
        }
        String cwd = new File(".").getCanonicalPath();
        String filePath = f.getCanonicalPath();
        //Use the relative path in the jar, if the path specified is relative
        String nameInJar = filePath.equals(cwd + File.separator + path) ?
                filePath.substring(cwd.length() + 1) : filePath;
        pigContext.addScriptFile(nameInJar, filePath);
        if(scriptingLang != null) {
            ScriptEngine se = ScriptEngine.getInstance(scriptingLang);
            se.registerFunctions(nameInJar, namespace, pigContext);
        }
    }

    /**
     * Register a query with the Pig runtime. The query is parsed and registered, but it is not
     * executed until it is needed.
     *
     * @param query
     *            a Pig Latin expression to be evaluated.
     * @param startLine
     *            line number of the query within the whole script
     * @throws IOException
     */
    public void registerQuery(String query, int startLine) throws IOException {
        currDAG.registerQuery(query, startLine, validateEachStatement, skipParseInRegisterForBatch);
    }

    /**
     * Register a query with the Pig runtime. The query is parsed and registered, but it is not
     * executed until it is needed.  Equivalent to calling {@link #registerQuery(String, int)}
     * with startLine set to 1.
     *
     * @param query
     *            a Pig Latin expression to be evaluated.
     * @throws IOException
     */
    public void registerQuery(String query) throws IOException {
        registerQuery(query, 1);
    }

    /**
     * Register a pig script from InputStream source which is more general and extensible
     * the pig script can be from local file, then you can use FileInputStream.
     * or pig script can be in memory which you build it dynamically, the you can use ByteArrayInputStream
     * even pig script can be in remote machine, which you get wrap it as SocketInputStream
     * @param in
     * @throws IOException
     */
    public void registerScript(InputStream in) throws IOException{
        registerScript(in, null, null);
    }

    /**
     * Register a pig script from InputStream source which is more general and extensible
     * the pig script can be from local file, then you can use FileInputStream.
     * or pig script can be in memory which you build it dynamically, the you can use ByteArrayInputStream
     * even pig script can be in remote machine, which you get wrap it as SocketInputStream.
     * The parameters in the pig script will be substituted with the values in params
     * @param in
     * @param params the key is the parameter name, and the value is the parameter value
     * @throws IOException
     */
    public void registerScript(InputStream in, Map<String,String> params) throws IOException{
        registerScript(in, params, null);
    }

    /**
     * Register a pig script from InputStream source which is more general and extensible
     * the pig script can be from local file, then you can use FileInputStream.
     * or pig script can be in memory which you build it dynamically, the you can use ByteArrayInputStream
     * even pig script can be in remote machine, which you get wrap it as SocketInputStream
     * The parameters in the pig script will be substituted with the values in the parameter files
     * @param in
     * @param paramsFiles  files which have the parameter setting
     * @throws IOException
     */
    public void registerScript(InputStream in, List<String> paramsFiles) throws IOException {
        registerScript(in, null, paramsFiles);
    }

    /**
     * Register a pig script from InputStream.<br>
     * The pig script can be from local file, then you can use FileInputStream.
     * Or pig script can be in memory which you build it dynamically, the you can use ByteArrayInputStream
     * Pig script can even be in remote machine, which you get wrap it as SocketInputStream.<br>
     * The parameters in the pig script will be substituted with the values in the map and the parameter files.
     * The values in params Map will override the value in parameter file if they have the same parameter
     * @param in
     * @param params the key is the parameter name, and the value is the parameter value
     * @param paramsFiles  files which have the parameter setting
     * @throws IOException
     */
    public void registerScript(InputStream in, Map<String,String> params,List<String> paramsFiles) throws IOException {
        try {
            String substituted = pigContext.doParamSubstitution(in, paramMapToList(params), paramsFiles);
            GruntParser grunt = new GruntParser(new StringReader(substituted), this);
            grunt.setInteractive(false);
            grunt.parseStopOnError(true);
        } catch (org.apache.pig.tools.pigscript.parser.ParseException e) {
            log.error(e.getLocalizedMessage());
            throw new IOException(e);
        }
    }

    protected List<String> paramMapToList(Map<String, String> params) {
        List<String> paramList = new ArrayList<String>();
        if (params != null) {
            for (Map.Entry<String, String> entry : params.entrySet()) {
                paramList.add(entry.getKey() + "=" + entry.getValue());
            }
        }
        return paramList;
    }

    /**
     * Creates a clone of the current DAG
     * @return A Graph object which is a clone of the current DAG
     * @throws IOException
     */
    protected Graph getClonedGraph() throws IOException {
        Graph graph = currDAG.duplicate();

        if (graph == null) {
            int errCode = 2127;
            String msg = "Cloning of plan failed.";
            throw new FrontendException(msg, errCode, PigException.BUG);
        }
        return graph;
    }


    /**
     * Register a query with the Pig runtime.  The query will be read from the indicated file.
     * @param fileName file to read query from.
     * @throws IOException
     */
    public void registerScript(String fileName) throws IOException {
        registerScript(fileName, null, null);
    }

    /**
     * Register a pig script file.  The parameters in the file will be substituted with the values in params
     * @param fileName  pig script file
     * @param params  the key is the parameter name, and the value is the parameter value
     * @throws IOException
     */
    public void registerScript(String fileName, Map<String,String> params) throws IOException {
        registerScript(fileName, params, null);
    }



    /**
     * Register a pig script file.  The parameters in the file will be substituted with the values in the parameter files
     * @param fileName pig script file
     * @param paramsFiles  files which have the parameter setting
     * @throws IOException
     */
    public void registerScript(String fileName, List<String> paramsFiles) throws IOException {
        registerScript(fileName, null, paramsFiles);
    }

    /**
     * Register a pig script file.  The parameters in the file will be substituted with the values in the map and the parameter files
     * The values in params Map will override the value in parameter file if they have the same parameter
     * @param fileName  pig script
     * @param params  the key is the parameter name, and the value is the parameter value
     * @param paramsFiles   files which have the parameter setting
     * @throws IOException
     */
    public void registerScript(String fileName, Map<String,String> params,List<String> paramsFiles) throws IOException {
        FileInputStream fis = null;
        try{
            fis = new FileInputStream(fileName);
            registerScript(fis, params, paramsFiles);
        }catch (FileNotFoundException e){
            log.error(e.getLocalizedMessage());
            throw new IOException(e.getCause());
        } finally {
            if (fis != null) {
                fis.close();
            }
        }
    }

    /**
     * Intended to be used by unit tests only.
     * Print a list of all aliases in in the current Pig Latin script.  Output is written to
     * System.out.
     * @throws FrontendException
     */
    public void printAliases () throws FrontendException {
        System.out.println("aliases: " + currDAG.getAliasOp().keySet());
    }

    /**
     * Write the schema for an alias to System.out.
     * @param alias Alias whose schema will be written out
     * @return Schema of alias dumped
     * @throws IOException
     */
    public Schema dumpSchema(String alias) throws IOException {
        try {
            if ("@".equals(alias)) {
                alias = getLastRel();
            }
            LogicalRelationalOperator op = getOperatorForAlias( alias );
            LogicalSchema schema = op.getSchema();

            boolean pretty = "true".equals(pigContext.getProperties()
                                   .getProperty(PRETTY_PRINT_SCHEMA_PROPERTY));

            if (schema != null) {
                Schema s = org.apache.pig.newplan.logical.Util.translateSchema(schema);
                System.out.println(alias + ": " + (pretty ? s.prettyPrint() : s.toString()));
                return s;
            } else {
                System.out.println("Schema for " + alias + " unknown.");
                return null;
            }
        } catch (FrontendException fee) {
            int errCode = 1001;
            String msg = "Unable to describe schema for alias " + alias;
            throw new FrontendException (msg, errCode, PigException.INPUT, false, null, fee);
        }
    }

    /**
     * Write the schema for a nestedAlias to System.out. Denoted by
     * alias::nestedAlias.
     *
     * @param alias Alias whose schema has nestedAlias
     * @param nestedAlias Alias whose schema will be written out
     * @return Schema of alias dumped
     * @throws IOException
     */
    public Schema dumpSchemaNested(String alias, String nestedAlias) throws IOException {
        if ("@".equals(alias)) {
            alias = getLastRel();
        }
        Operator op = getOperatorForAlias( alias );
        if( op instanceof LOForEach ) {
            LogicalSchema nestedSc = ((LOForEach)op).dumpNestedSchema(alias, nestedAlias);
            if (nestedSc!=null) {
                Schema s = org.apache.pig.newplan.logical.Util.translateSchema(nestedSc);
                System.out.println(alias+ "::" + nestedAlias + ": " + s.toString());
                return s;
            }
            else {
                System.out.println("Schema for "+ alias+ "::" + nestedAlias + " unknown.");
                return null;
            }
        }
        else {
            int errCode = 1001;
            String msg = "Unable to describe schema for " + alias + "::" + nestedAlias;
            throw new FrontendException (msg, errCode, PigException.INPUT, false, null);
        }
    }

    /**
     * Set the name of the job.  This name will get translated to mapred.job.name.
     * @param name of job
     */
    public void setJobName(String name) {
        jobName = PigContext.JOB_NAME_PREFIX + ":" + name;
    }

    /**
     * Set Hadoop job priority.  This value will get translated to mapred.job.priority.
     * @param priority valid values are found in {@link org.apache.hadoop.mapred.JobPriority}
     */
    public void setJobPriority(String priority) {
        jobPriority = priority;
    }

    /**
     * Executes a Pig Latin script up to and including indicated alias.  That is, if a user does:
     * <pre>
     * PigServer server = new PigServer();
     * server.registerQuery("A = load 'foo';");
     * server.registerQuery("B = filter A by $0 &gt; 0;");
     * server.registerQuery("C = order B by $1;");
     * </pre>
     * Then
     * <pre>
     * server.openIterator("B");
     * </pre>
     * filtered but unsorted data will be returned.  If instead a user does
     * <pre>
     * server.openIterator("C");
     * </pre>
     * filtered and sorted data will be returned.
     * @param id Alias to open iterator for
     * @return iterator of tuples returned from the script
     * @throws IOException
     */
    public Iterator<Tuple> openIterator(String id) throws IOException {
        try {
            pigContext.getProperties().setProperty( PigContext.JOB_NAME, jobName );
            if( jobPriority != null ) {
                pigContext.getProperties().setProperty( PigContext.JOB_PRIORITY, jobPriority );
            }
            ExecJob job = store(id, FileLocalizer.getTemporaryPath(pigContext)
                    .toString(), Utils.getTmpFileCompressorName(pigContext)
                    + "()");

            // invocation of "execute" is synchronous!

            if (job.getStatus() == JOB_STATUS.COMPLETED) {
                return job.getResults();
            } else if (job.getStatus() == JOB_STATUS.FAILED
                    && job.getException() != null) {
                // throw the backend exception in the failed case
                Exception e = job.getException();
                int errCode = 1066;
                String msg = "Unable to open iterator for alias " + id
                        + ". Backend error : " + e.getMessage();
                throw new FrontendException(msg, errCode, PigException.INPUT, e);
            } else {
                throw new IOException("Job terminated with anomalous status "
                        + job.getStatus().toString());
            }
        } catch (FrontendException e) {
            throw e;
        } catch (Exception e) {
            int errCode = 1066;
            String msg = "Unable to open iterator for alias " + id;
            throw new FrontendException(msg, errCode, PigException.INPUT, e);
        }
    }

    /**
     * Executes a Pig Latin script up to and including indicated alias and stores the resulting
     * records into a file.  That is, if a user does:
     * <pre>
     * PigServer server = new PigServer();
     * server.registerQuery("A = load 'foo';");
     * server.registerQuery("B = filter A by $0 &gt; 0;");
     * server.registerQuery("C = order B by $1;");
     * </pre>
     * Then
     * <pre>
     * server.store("B", "bar");
     * </pre>
     * filtered but unsorted data will be stored to the file <tt>bar</tt>.  If instead a user does
     * <pre>
     * server.store("C", "bar");
     * </pre>
     * filtered and sorted data will be stored to the file <tt>bar</tt>.
     * Equivalent to calling {@link #store(String, String, String)} with
     * <tt>org.apache.pig.PigStorage</tt> as the store function.
     * @param id The alias to store
     * @param filename The file to which to store to
     * @return {@link ExecJob} containing information about this job
     * @throws IOException
     */
    public ExecJob store(String id, String filename) throws IOException {
        return store(id, filename, PigStorage.class.getName() + "()");   // SFPig is the default store function
    }

    /**
     * Executes a Pig Latin script up to and including indicated alias and stores the resulting
     * records into a file.  That is, if a user does:
     * <pre>
     * PigServer server = new PigServer();
     * server.registerQuery("A = load 'foo';");
     * server.registerQuery("B = filter A by $0 &gt; 0;");
     * server.registerQuery("C = order B by $1;");
     * </pre>
     * Then
     * <pre>
     * server.store("B", "bar", "mystorefunc");
     * </pre>
     * filtered but unsorted data will be stored to the file <tt>bar</tt> using
     * <tt>mystorefunc</tt>.  If instead a user does
     * <pre>
     * server.store("C", "bar", "mystorefunc");
     * </pre>
     * filtered and sorted data will be stored to the file <tt>bar</tt> using
     * <tt>mystorefunc</tt>.
     * <p>
     * @param id The alias to store
     * @param filename The file to which to store to
     * @param func store function to use
     * @return {@link ExecJob} containing information about this job
     * @throws IOException
     */
    public ExecJob store(String id, String filename, String func)
            throws IOException {
        PigStats stats = storeEx(id, filename, func);
        if (stats.getOutputStats().size() < 1) {
            throw new IOException("Couldn't retrieve job.");
        }
        OutputStats output = stats.getOutputStats().get(0);

        if(stats.isSuccessful()){
            return  new HJob(JOB_STATUS.COMPLETED, pigContext, output
                    .getPOStore(), output.getAlias(), stats);
        }else{
            HJob job = new HJob(JOB_STATUS.FAILED, pigContext,
                    output.getPOStore(), output.getAlias(), stats);

            //check for exception
            Exception ex = null;
            for(JobStats js : stats.getJobGraph()){
                if(js.getException() != null) {
                    ex = js.getException();
                }
            }
            job.setException(ex);
            return job;
        }
    }

    private PigStats storeEx(String alias, String filename, String func)
    throws IOException {
        if ("@".equals(alias)) {
            alias = getLastRel();
        }
        currDAG.parseQuery();
        currDAG.buildPlan( alias );

        try {
            QueryParserUtils.attachStorePlan(scope, currDAG.lp, filename, func, currDAG.getOperator( alias ), alias, pigContext);
            currDAG.compile();
            return executeCompiledLogicalPlan();
        } catch (PigException e) {
            int errCode = 1002;
            String msg = "Unable to store alias " + alias;
            throw new PigException(msg, errCode, PigException.INPUT, e);
        }
    }

    /**
     * Provide information on how a pig query will be executed.  For now
     * this information is very developer focussed, and probably not very
     * useful to the average user.
     * @param alias Name of alias to explain.
     * @param stream PrintStream to write explanation to.
     * @throws IOException if the requested alias cannot be found.
     */
    public void explain(String alias,
                        PrintStream stream) throws IOException {
        explain(alias, "text", true, false, stream, stream, null, null);
    }

    /**
     * Provide information on how a pig query will be executed.
     * @param alias Name of alias to explain.
     * @param format Format in which the explain should be printed.  If text, then the plan will
     * be printed in plain text.  Otherwise, the execution plan will be printed in
     * <a href="http://en.wikipedia.org/wiki/DOT_language">DOT</a> format.
     * @param verbose Controls the amount of information printed
     * @param markAsExecute When set will treat the explain like a
     * call to execute in the respoect that all the pending stores are
     * marked as complete.
     * @param lps Stream to print the logical tree
     * @param eps Stream to print the ExecutionEngine trees. If null, then will print to files
     * @param dir Directory to print ExecutionEngine trees. If null, will use eps
     * @param suffix Suffix of file names 
     * @throws IOException if the requested alias cannot be found.
     */
    public void explain(String alias,
                        String format,
                        boolean verbose,
                        boolean markAsExecute,
                        PrintStream lps,
                        PrintStream eps,
                        File dir,
                        String suffix) throws IOException {
        try {
            pigContext.inExplain = true;
            buildStorePlan( alias );
            
            //Only add root xml node if all plans are being written to same stream.
            if (format == "xml" && lps == eps) {
                lps.println("<plan>");
            }

            currDAG.lp.explain(lps, format, verbose);

            if( currDAG.lp.size() == 0 ) {
                if (format == "xml" && lps == eps) {
                    lps.println("</plan>");
                }
                return;
            }

            pigContext.getExecutionEngine().explain(currDAG.lp, pigContext, eps, format, verbose, dir, suffix );

            if (format.equals("xml") && lps == eps) {
                lps.println("</plan>");
            }
            
            if (markAsExecute) {
                currDAG.markAsExecuted();
            }
        } catch (Exception e) {
            int errCode = 1067;
            String msg = "Unable to explain alias " + alias;
            throw new FrontendException(msg, errCode, PigException.INPUT, e);
        } finally {
            pigContext.inExplain = false;
        }
    }

    /**
     * Returns the unused byte capacity of an HDFS filesystem. This value does
     * not take into account a replication factor, as that can vary from file
     * to file. Thus if you are using this to determine if you data set will fit
     * in the HDFS, you need to divide the result of this call by your specific replication
     * setting.
     * @return unused byte capacity of the file system.
     * @throws IOException
     */
    public long capacity() throws IOException {
        if (pigContext.getExecType().isLocal()) {
            throw new IOException("capacity only supported for non-local execution");
        }
        else {
            DataStorage dds = pigContext.getDfs();

            Map<String, Object> stats = dds.getStatistics();

            String rawCapacityStr = (String) stats.get(DataStorage.RAW_CAPACITY_KEY);
            String rawUsedStr = (String) stats.get(DataStorage.RAW_USED_KEY);

            if ((rawCapacityStr == null) || (rawUsedStr == null)) {
                throw new IOException("Failed to retrieve capacity stats");
            }

            long rawCapacityBytes = new Long(rawCapacityStr).longValue();
            long rawUsedBytes = new Long(rawUsedStr).longValue();

            return rawCapacityBytes - rawUsedBytes;
        }
    }

    /**
     * Returns the length of a file in bytes which exists in the HDFS (accounts for replication).
     * @param filename
     * @return length of the file in bytes
     * @throws IOException
     */
    public long fileSize(String filename) throws IOException {
        DataStorage dfs = pigContext.getDfs();
        ElementDescriptor elem = dfs.asElement(filename);
        Map<String, Object> stats = elem.getStatistics();
        long length = (Long) stats.get(ElementDescriptor.LENGTH_KEY);
        int replication = (Short) stats
                .get(ElementDescriptor.BLOCK_REPLICATION_KEY);

        return length * replication;
    }

    /**
     * Test whether a file exists.
     * @param filename to test
     * @return true if file exists, false otherwise
     * @throws IOException
     */
    public boolean existsFile(String filename) throws IOException {
        ElementDescriptor elem = pigContext.getDfs().asElement(filename);
        return elem.exists();
    }

    /**
     * Delete a file.
     * @param filename to delete
     * @return true
     * @throws IOException
     */
    public boolean deleteFile(String filename) throws IOException {
        // Check if this operation is permitted
        filter.validate(PigCommandFilter.Command.RM);
        filter.validate(PigCommandFilter.Command.RMF);

        ElementDescriptor elem = pigContext.getDfs().asElement(filename);
        elem.delete();
        return true;
   }

    /**
     * Rename a file.
     * @param source file to rename
     * @param target new file name
     * @return true
     * @throws IOException
     */
    public boolean renameFile(String source, String target) throws IOException {
        // Check if this operation is permitted
        filter.validate(PigCommandFilter.Command.MV);

        pigContext.rename(source, target);
        return true;
    }

    /**
     * Make a directory.
     * @param dirs directory to make
     * @return true
     * @throws IOException
     */
    public boolean mkdirs(String dirs) throws IOException {
        // Check if this operation is permitted
        filter.validate(PigCommandFilter.Command.MKDIR);

        ContainerDescriptor container = pigContext.getDfs().asContainer(dirs);
        container.create();
        return true;
    }

    /**
     * List the contents of a directory.
     * @param dir name of directory to list
     * @return array of strings, one for each file name
     * @throws IOException
     */
    public String[] listPaths(String dir) throws IOException {
        // Check if this operation is permitted
        filter.validate(PigCommandFilter.Command.LS);

        Collection<String> allPaths = new ArrayList<String>();
        ContainerDescriptor container = pigContext.getDfs().asContainer(dir);
        Iterator<ElementDescriptor> iter = container.iterator();

        while (iter.hasNext()) {
            ElementDescriptor elem = iter.next();
            allPaths.add(elem.toString());
        }

        String[] type = new String[1];
        return allPaths.toArray(type);
    }

    /**
     * Return a map containing the logical plan associated with each alias.
     *
     * @return map
     */
    public Map<String, LogicalPlan> getAliases() {
        Map<String, LogicalPlan> aliasPlans = new HashMap<String, LogicalPlan>();
        for (LogicalRelationalOperator op : currDAG.getAliases().keySet()) {
            String alias = op.getAlias();
            if(null != alias) {
                aliasPlans.put(alias, currDAG.getAliases().get(op));
            }
        }
        return aliasPlans;
    }

    /**
     * Reclaims resources used by this instance of PigServer. This method
     * deletes all temporary files generated by the current thread while
     * executing Pig commands.
     */
    public void shutdown() {
        // clean-up activities
        // TODO: reclaim scope to free up resources. Currently
        // this is not implemented and throws an exception
        // hence, for now, we won't call it.
        //
        // pigContext.getExecutionEngine().reclaimScope(this.scope);

        FileLocalizer.deleteTempFiles();
    }

    /**
     * Get the set of all current aliases.
     * @return set
     */
    public Set<String> getAliasKeySet() {
        return currDAG.getAliasOp().keySet();
    }

    public Map<Operator, DataBag> getExamples(String alias) throws IOException {
        try {
            if (currDAG.isBatchOn() && alias != null) {
                currDAG.parseQuery();
                currDAG.buildPlan( null );
                execute();
            }
            currDAG.parseQuery();
            currDAG.buildPlan( alias );
            currDAG.compile();
        } catch (IOException e) {
            //Since the original script is parsed anyway, there should not be an
            //error in this parsing. The only reason there can be an error is when
            //the files being loaded in load don't exist anymore.
            e.printStackTrace();
        }

        ExampleGenerator exgen = new ExampleGenerator( currDAG.lp, pigContext );
        try {
            return exgen.getExamples();
        } catch (ExecException e) {
            e.printStackTrace(System.out);
            throw new IOException("ExecException" , e);
        } catch (Exception e) {
            e.printStackTrace(System.out);
            throw new IOException("Exception ", e);
        }

    }

    public void printHistory(boolean withNumbers) {

        List<String> sc = currDAG.getScriptCache();

        if(!sc.isEmpty()) {
            for(int i = 0 ; i < sc.size(); i++) {
                if(withNumbers) System.out.print((i+1)+"   ");
                System.out.println(sc.get(i));
            }
        }

    }

    private void buildStorePlan(String alias) throws IOException {
        currDAG.parseQuery();
        currDAG.buildPlan( alias );

        if( !isBatchOn() || alias != null ) {
            // MRCompiler needs a store to be the leaf - hence
            // add a store to the plan to explain
            QueryParserUtils.attachStorePlan(scope, currDAG.lp, "fakefile", null, currDAG.getOperator( alias ),
                    "fake", pigContext );
        }
        currDAG.compile();
    }

    /**
     * Compile and execute the current plan.
     * @return
     * @throws IOException
     */
    private PigStats execute() throws IOException {
        pigContext.getProperties().setProperty( PigContext.JOB_NAME, jobName );
        if( jobPriority != null ) {
            pigContext.getProperties().setProperty( PigContext.JOB_PRIORITY, jobPriority );
        }

        // In this plan, all stores in the plan will be executed. They should be ignored if the plan is reused.
        currDAG.countExecutedStores();

        currDAG.compile();

        if( currDAG.lp.size() == 0 ) {
           return PigStats.get();
        }

        pigContext.getProperties().setProperty("pig.logical.plan.signature", currDAG.lp.getSignature());

        PigStats stats = executeCompiledLogicalPlan();

        return stats;
    }

    private PigStats executeCompiledLogicalPlan() throws ExecException,
            FrontendException {
        // discover pig features used in this script
        ScriptState.get().setScriptFeatures(currDAG.lp);

        BlackAndWhitelistValidator validator = new BlackAndWhitelistValidator(getPigContext(), currDAG.lp);
        validator.validate();

        return launchPlan(currDAG.lp, "job_pigexec_");
    }

    /**
     * A common method for launching the jobs according to the logical plan
     * @param lp The logical plan
     * @param jobName A String containing the job name to be used
     * @return The PigStats object
     * @throws ExecException
     * @throws FrontendException
     */
    protected PigStats launchPlan(LogicalPlan lp, String jobName) throws ExecException, FrontendException {

        PigStats stats = null;
        try {
            stats = pigContext.getExecutionEngine().launchPig(lp, jobName, pigContext);
        } catch (ExecException e) {
            throw (ExecException) e;
        } catch (FrontendException e) {
            throw (FrontendException) e;
        } catch (Exception e) {
            // There are a lot of exceptions thrown by the launcher.  If this
            // is an ExecException, just let it through.  Else wrap it.
            int errCode = 2043;
            String msg = "Unexpected error during execution.";
            throw new ExecException(msg, errCode, PigException.BUG, e);
        }

        return stats;
    }

    /**
     * NOTE: For testing only. Don't use.
     * @throws IOException
     */
    @SuppressWarnings("unused")
    private LogicalPlan buildLp() throws IOException {
        currDAG.buildPlan( null);
        currDAG.compile();
        return currDAG.lp;
    }

    private LogicalRelationalOperator getOperatorForAlias(String alias) throws IOException {
        buildStorePlan (alias);
        LogicalRelationalOperator op = (LogicalRelationalOperator)currDAG.getOperator( alias );
        if( op == null ) {
            int errCode = 1005;
            String msg = "No plan for " + alias + " to describe";
            throw new FrontendException(msg, errCode, PigException.INPUT, false, null);
        }
        return op;
    }
    
    /**
     * Returns data associated with LogicalPlan. It makes
     * sense to call this method only after a query/script
     * has been registered with one of the {@link #registerQuery(String)}
     * or {@link #registerScript(InputStream)} methods.
     * 
     * @return LogicalPlanData
     */
    public LogicalPlanData getLogicalPlanData() {
        return new LogicalPlanData(currDAG.lp);
    }

    /*
     * This class holds the internal states of a grunt shell session.
     */
    protected class Graph {

        private final Map<LogicalRelationalOperator, LogicalPlan> aliases = new HashMap<LogicalRelationalOperator, LogicalPlan>();

        private Map<String, Operator> operators = new HashMap<String, Operator>();
        private String lastRel;

        private final List<String> scriptCache = new ArrayList<String>();

        // the fileNameMap contains filename to canonical filename
        // mappings. This is done so we can reparse the cached script
        // and remember the translation (current directory might only
        // be correct during the first parse
        private Map<String, String> fileNameMap = new HashMap<String, String>();

        private final boolean batchMode;

        private int processedStores = 0;

        private LogicalPlan lp;

        private int currentLineNum = 0;

        public Graph(boolean batchMode) {
            this.batchMode = batchMode;
            this.lp = new LogicalPlan();
        };

        /**
         * Call back method for counting executed stores.
         */
        private void countExecutedStores() {
            for( Operator sink : lp.getSinks() ) {
                if( sink instanceof LOStore ) {
                    processedStores++;
                }
            }
        }

        Map<LogicalRelationalOperator, LogicalPlan> getAliases() {
            return aliases;
        }

        Map<String, Operator> getAliasOp() {
            return operators;
        }

        boolean isBatchOn() {
            return batchMode;
        };

        boolean isBatchEmpty() {
            for( Operator op : lp.getSinks() ) {
                if( op instanceof LOStore )
                    return false;
            }
            return true;
        }

        void markAsExecuted() {
        }

        public LogicalPlan getLogicalPlan() {
            return this.lp;
        }

        /**
         * Get the operator with the given alias in the raw plan. Null if not
         * found.
         */
        Operator getOperator(String alias) throws FrontendException {
            return operators.get( alias );
        }

        public LogicalPlan getPlan(String alias) throws IOException {
            LogicalPlan plan = lp;

            if (alias != null) {
                LogicalRelationalOperator op = (LogicalRelationalOperator) operators.get(alias);
                if(op == null) {
                    int errCode = 1003;
                    String msg = "Unable to find an operator for alias " + alias;
                    throw new FrontendException(msg, errCode, PigException.INPUT);
                }
                plan = aliases.get(op);
            }
            return plan;
        }


        /**
         * Build a plan for the given alias. Extra branches and child branch under alias
         * will be ignored. Dependent branch (i.e. scalar) will be kept.
         * @throws IOException
         */
        void buildPlan(String alias) throws IOException {
            if( alias == null )
                skipStores();

            final Queue<Operator> queue = new LinkedList<Operator>();
            if( alias != null ) {
                Operator op = getOperator( alias );
                if (op == null) {
                    String msg = "Unable to find an operator for alias " + alias;
                    throw new FrontendException( msg, 1003, PigException.INPUT );
                }
                queue.add( op );
            } else {
                List<Operator> sinks = lp.getSinks();
                if( sinks != null ) {
                    for( Operator sink : sinks ) {
                        if( sink instanceof LOStore )
                            queue.add( sink );
                    }
                }
            }

            LogicalPlan plan = new LogicalPlan();

            while( !queue.isEmpty() ) {
                Operator currOp = queue.poll();
                plan.add( currOp );

                List<Operator> preds = lp.getPredecessors( currOp );
                if( preds != null ) {
                    List<Operator> ops = new ArrayList<Operator>( preds );
                    for( Operator pred : ops ) {
                        if( !queue.contains( pred ) )
                            queue.add( pred );
                        plan.connect( pred, currOp );
                    }
                }

                // visit expression associated with currOp. If it refers to any other operator
                // that operator is also going to be enqueued.
                currOp.accept( new AllExpressionVisitor( plan, new DependencyOrderWalker( plan ) ) {
                        @Override
                        protected LogicalExpressionVisitor getVisitor(LogicalExpressionPlan exprPlan)
                        throws FrontendException {
                            return new LogicalExpressionVisitor( exprPlan, new DependencyOrderWalker( exprPlan ) ) {
                                @Override
                                public void visit(ScalarExpression expr) throws FrontendException {
                                    Operator refOp = expr.getImplicitReferencedOperator();
                                    if( !queue.contains( refOp ) )
                                        queue.add( refOp );
                                }
                            };
                        }
                    }
                );

                currOp.setPlan( plan );
            }
            lp = plan;
        }

        /**
         *  Remove stores that have been executed previously from the overall plan.
         */
        private void skipStores() throws IOException {
            List<Operator> sinks = lp.getSinks();
            List<Operator> sinksToRemove = new ArrayList<Operator>();
            int skipCount = processedStores;
            if( skipCount > 0 ) {
                for( Operator sink : sinks ) {
                    if( sink instanceof LOStore ) {
                        sinksToRemove.add( sink );
                        skipCount--;
                        if( skipCount == 0 )
                            break;
                    }
                }
            }

            for( Operator op : sinksToRemove ) {
                Operator pred = lp.getPredecessors( op ).get(0);
                lp.disconnect( pred, op );
                lp.remove( op );
            }
        }

        /**
         * Accumulate the given statement to previous query statements and generate
         * an overall (raw) plan.
         */
        void registerQuery(String query, int startLine, boolean validateEachStatement,
                boolean skipParseForBatch) throws IOException {
            if( batchMode ) {
                if( startLine == currentLineNum ) {
                    String line = scriptCache.remove( scriptCache.size() - 1 );
                    scriptCache.add( line + query );
                } else {
                    while( startLine > currentLineNum + 1 ) {
                        scriptCache.add( "" );
                        currentLineNum++;
                    }
                    BufferedReader br = new BufferedReader(new StringReader(query));
                    String line = br.readLine();
                    while (line != null) {
                        scriptCache.add(line);
                        currentLineNum++;
                        line = br.readLine();
                    }
                }
                if (skipParseForBatch) {
                    return;
                }
            } else {
                scriptCache.add( query );
            }

            if(validateEachStatement){
                validateQuery();
            }
            parseQuery();

            if( !batchMode ) {
                buildPlan( null );
                for( Operator sink : lp.getSinks() ) {
                    if( sink instanceof LOStore ) {
                        try {
                            execute();
                        } catch (Exception e) {
                            int errCode = 1002;
                            String msg = "Unable to store alias "
                                + ((LOStore) sink).getAlias();
                            throw new FrontendException(msg, errCode,
                                    PigException.INPUT, e);
                        }
                        break; // We should have at most one store, so break here.
                    }
                }
            }
        }

        void validateQuery() throws FrontendException {
            String query = buildQuery();
            QueryParserDriver parserDriver = new QueryParserDriver( pigContext, scope, fileNameMap );
            try {
                LogicalPlan plan = parserDriver.parse( query );
                compile( plan );
            } catch(FrontendException ex) {
                scriptCache.remove( scriptCache.size() -1 );
                throw ex;
            }
        }

        public List<String> getScriptCache() {
            return scriptCache;
        }

        /**
         * Parse the accumulated pig statements and generate an overall plan.
         */
        private void parseQuery() throws FrontendException {
            UDFContext.getUDFContext().reset();
            UDFContext.getUDFContext().setClientSystemProps(pigContext.getProperties());

            String query = buildQuery();

            if( query.isEmpty() ) {
                lp = new LogicalPlan();
                return;
            }

            try {
                QueryParserDriver parserDriver = new QueryParserDriver( pigContext, scope, fileNameMap );
                lp = parserDriver.parse( query );
                operators = parserDriver.getOperators();
                lastRel = parserDriver.getLastRel();
            } catch(Exception ex) {
                scriptCache.remove( scriptCache.size() -1 ); // remove the bad script from the cache.
                PigException pe = LogUtils.getPigException(ex);
                int errCode = 1000;
                String msg = "Error during parsing. "
                        + (pe == null ? ex.getMessage() : pe.getMessage());
                log.error("exception during parsing: " + msg, ex);
                if (null == pe) {
                    throw new FrontendException (msg, errCode, PigException.INPUT , ex);
                } else {
                    throw new FrontendException (msg, errCode, PigException.INPUT , ex, pe.getSourceLocation() );
                }
            }
        }

        public String getLastRel() {
            return lastRel;
        }

        private String buildQuery() {
            StringBuilder accuQuery = new StringBuilder();
            for( String line : scriptCache ) {
                accuQuery.append( line + "\n" );
            }

            return accuQuery.toString();
        }

        private void compile() throws IOException {
            compile( lp );
            currDAG.postProcess();
        }

        private void compile(LogicalPlan lp) throws FrontendException  {
            DanglingNestedNodeRemover DanglingNestedNodeRemover = new DanglingNestedNodeRemover( lp );
            DanglingNestedNodeRemover.visit();
            
            new ColumnAliasConversionVisitor(lp).visit();
            new SchemaAliasVisitor(lp).visit();
            new ScalarVisitor(lp, pigContext, scope).visit();

            // ImplicitSplitInsertVisitor has to be called before
            // DuplicateForEachColumnRewriteVisitor.  Detail at pig-1766
            new ImplicitSplitInsertVisitor(lp).visit();

            // DuplicateForEachColumnRewriteVisitor should be before
            // TypeCheckingRelVisitor which does resetSchema/getSchema
            // heavily
            new DuplicateForEachColumnRewriteVisitor(lp).visit();

            CompilationMessageCollector collector = new CompilationMessageCollector() ;

            new TypeCheckingRelVisitor( lp, collector).visit();
            new UnionOnSchemaSetter( lp ).visit();
            new CastLineageSetter(lp, collector).visit();
            new ScalarVariableValidator(lp).visit();
            if(aggregateWarning) {
                CompilationMessageCollector.logMessages(collector, MessageType.Warning, aggregateWarning, log);
            } else {
                for(Enum type: MessageType.values()) {
                    CompilationMessageCollector.logAllMessages(collector, log);
                }
            }

        }

        private void postProcess() throws IOException {
            // The following code deals with store/load combination of
            // intermediate files. In this case we will replace the load
            // operator
            // with a (implicit) split operator, iff the load/store
            // func is reversible (because that's when we can safely
            // skip the load and keep going with the split output). If
            // the load/store func is not reversible (or they are
            // different functions), we connect the store and the load
            // to remember the dependency.

            Set<LOLoad> loadOps = new HashSet<LOLoad>();
            List<Operator> sources = lp.getSources();
            for (Operator source : sources) {
                if (source instanceof LOLoad) {
                    loadOps.add((LOLoad)source);
                }
            }

            Set<LOStore> storeOps = new HashSet<LOStore>();
            List<Operator> sinks = lp.getSinks();
            for (Operator sink : sinks) {
                if (sink instanceof LOStore) {
                    storeOps.add((LOStore)sink);
                }
            }

            if ("true".equals(pigContext.getProperties().getProperty(PIG_LOCATION_CHECK_STRICT))) {
                log.info("Output location strick check enabled");
                checkDuplicateStoreLoc(storeOps);
            }

            for (LOLoad load : loadOps) {
                for (LOStore store : storeOps) {
                    String ifile = load.getFileSpec().getFileName();
                    String ofile = store.getFileSpec().getFileName();
                    if (ofile.compareTo(ifile) == 0) {
                        // if there is no path from the load to the store,
                        // then connect the store to the load to create the
                        // dependency of the store on the load. If there is
                        // a path from the load to the store, then we should
                        // not connect the store to the load and create a cycle
                        if (!store.getPlan().pathExists(load, store)) {
                            store.getPlan().connect(store, load);
                        }
                    }
                }
            }
        }

        /**
         * This method checks whether the multiple sinks (STORE) use the same
         * "file-based" location. If yes, throws a RuntimeException
         * 
         * @param storeOps
         */
        private void checkDuplicateStoreLoc(Set<LOStore> storeOps) {
            Set<String> uniqueStoreLoc = new HashSet<String>();
            for(LOStore store : storeOps) {
                String fileName = store.getFileSpec().getFileName();
                if(!uniqueStoreLoc.add(fileName) && UriUtil.isHDFSFileOrLocalOrS3N(fileName)) {
                    throw new RuntimeException("Script contains 2 or more STORE statements writing to same location : "+ fileName);
                }
            }
        }

        protected Graph duplicate() {
            // There are two choices on how we duplicate the logical plan
            // 1 - we really clone each operator and connect up the cloned operators
            // 2 - we cache away the script till the point we need to clone
            // and then simply re-parse the script.
            // The latter approach is used here
            // FIXME: There is one open issue with this now:
            // Consider the following script:
            // A = load 'file:/somefile';
            // B = filter A by $0 > 10;
            // store B into 'bla';
            // rm 'file:/somefile';
            // A = load 'file:/someotherfile'
            // when we try to clone - we try to reparse
            // from the beginning and currently the parser
            // checks for file existence of files in the load
            // in the case where the file is a local one -i.e. with file: prefix
            // This will be a known issue now and we will need to revisit later

            // parse each line of the cached script
            int lineNumber = 1;

            // create data structures needed for parsing
            Graph graph = new Graph(isBatchOn());
            graph.processedStores = processedStores;
            graph.fileNameMap = new HashMap<String, String>(fileNameMap);

            try {
                for (Iterator<String> it = scriptCache.iterator(); it.hasNext(); lineNumber++) {
                    // always doing registerQuery irrespective of the batch mode
                    // TODO: Need to figure out if anything different needs to happen if batch
                    // mode is not on
                    // Don't have to do the validation again, so set validateEachStatement param to false
                    graph.registerQuery(it.next(), lineNumber, false, false);
                }
                graph.postProcess();
            } catch (IOException ioe) {
                ioe.printStackTrace();
                graph = null;
            }
            return graph;
        }
    }

    /**
     * This can be called to indicate if the query is being parsed/compiled
     * in a mode that expects each statement to be validated as it is
     * entered, instead of just doing it once for whole script.
     * @param validateEachStatement
     */
    public void setValidateEachStatement(boolean validateEachStatement) {
        this.validateEachStatement = validateEachStatement;
    }

    /**
     * Set whether to skip parsing while registering the query in batch mode
     * @param skipParseInRegisterForBatch
     */
    public void setSkipParseInRegisterForBatch(boolean skipParseInRegisterForBatch) {
        this.skipParseInRegisterForBatch = skipParseInRegisterForBatch;
    }

    public String getLastRel() {
        return currDAG.getLastRel();
    }
}

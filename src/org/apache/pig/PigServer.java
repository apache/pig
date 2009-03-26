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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.datastorage.ContainerDescriptor;
import org.apache.pig.backend.datastorage.DataStorage;
import org.apache.pig.backend.datastorage.ElementDescriptor;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.executionengine.ExecJob;
import org.apache.pig.backend.executionengine.ExecJob.JOB_STATUS;
import org.apache.pig.builtin.BinStorage;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.LogicalOperator;
import org.apache.pig.impl.logicalLayer.LogicalPlan;
import org.apache.pig.impl.logicalLayer.LogicalPlanBuilder;
import org.apache.pig.impl.logicalLayer.LOPrinter;
import org.apache.pig.impl.logicalLayer.PlanSetter;
import org.apache.pig.impl.logicalLayer.optimizer.LogicalOptimizer;
import org.apache.pig.impl.logicalLayer.parser.ParseException;
import org.apache.pig.impl.logicalLayer.parser.QueryParser;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.validators.LogicalPlanValidationExecutor;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.impl.plan.CompilationMessageCollector;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.CompilationMessageCollector.MessageType;
import org.apache.pig.impl.streaming.StreamingCommand;
import org.apache.pig.impl.util.PropertiesUtil;
import org.apache.pig.impl.logicalLayer.LODefine;
import org.apache.pig.impl.logicalLayer.LOStore;
import org.apache.pig.pen.ExampleGenerator;
import org.apache.pig.impl.util.LogUtils;
import org.apache.pig.tools.grunt.GruntParser;


/**
 * 
 * This class is the program's connection to Pig. Typically a program will create a PigServer
 * instance. The programmer then registers queries using registerQuery() and
 * retrieves results using openIterator() or store().
 * 
 */
public class PigServer {
    
    private final Log log = LogFactory.getLog(getClass());
    
    public static ExecType parseExecType(String str) throws IOException {
        String normStr = str.toLowerCase();
        
        if (normStr.equals("local")) return ExecType.LOCAL;
        if (normStr.equals("mapreduce")) return ExecType.MAPREDUCE;
        if (normStr.equals("mapred")) return ExecType.MAPREDUCE;
        if (normStr.equals("pig")) return ExecType.PIG;
        if (normStr.equals("pigbody")) return ExecType.PIG;
   
        int errCode = 2040;
        String msg = "Unknown exec type: " + str;
        throw new PigException(msg, errCode, PigException.BUG);
    }


    Map<LogicalOperator, LogicalPlan> aliases = new HashMap<LogicalOperator, LogicalPlan>();
    Map<OperatorKey, LogicalOperator> opTable = new HashMap<OperatorKey, LogicalOperator>();
    Map<String, LogicalOperator> aliasOp = new HashMap<String, LogicalOperator>();
    PigContext pigContext;
    
    private String scope = constructScope();
    private ArrayList<String> cachedScript = new ArrayList<String>();
    private boolean aggregateWarning = true;
    
    private String constructScope() {
        // scope servers for now as a session id
        // scope = user_id + "-" + time_stamp;
        
        String user = System.getProperty("user.name", "DEFAULT_USER_ID");
        String date = (new Date()).toString();
       
        return user + "-" + date;
    }
    
    public PigServer(String execTypeString) throws ExecException, IOException {
        this(parseExecType(execTypeString));
    }
    
    public PigServer(ExecType execType) throws ExecException {
        this(execType, PropertiesUtil.loadPropertiesFromFile());
    }

    public PigServer(ExecType execType, Properties properties) throws ExecException {
        this(new PigContext(execType, properties), true);
    }
  
    public PigServer(PigContext context) throws ExecException {
        this(context, true);
    }
    
    public PigServer(PigContext context, boolean connect) throws ExecException {
        this.pigContext = context;
        if (this.pigContext.getProperties().getProperty(PigContext.JOB_NAME) == null) {
            setJobName("DefaultJobName") ;
        }
        
        aggregateWarning = "true".equalsIgnoreCase(pigContext.getProperties().getProperty("aggregate.warning"));
        
        if (connect) {
            pigContext.connect();
        }
    }

    public PigContext getPigContext(){
        return pigContext;
    }
    
    public void debugOn() {
        pigContext.debug = true;
    }
    
    public void debugOff() {
        pigContext.debug = false;
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
     * @param functionSpec - the name of the function and any arguments.
     * It should have the form: classname('arg1', 'arg2', ...)
     */
    @Deprecated
    public void registerFunction(String function, String functionSpec) {
        registerFunction(function, new FuncSpec(functionSpec));
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
        
        if (pigContext.debug && urls.hasMoreElements()) {
            String logMessage = "Found multiple resources that match " 
                + jarName + ": " + resourceLocation;
            
            while (urls.hasMoreElements()) {
                logMessage += (logMessage + urls.nextElement() + "; ");
            }
            
            log.debug(logMessage);
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
        // first try to locate jar via system resources
        // if this fails, try by using "name" as File (this preserves 
        // compatibility with case when user passes absolute path or path 
        // relative to current working directory.)        
        if (name != null) {
            URL resource = locateJarFromResources(name);

            if (resource == null) {
                File f = new File(name);
                
                if (!f.canRead()) {
                    int errCode = 4002;
                    String msg = "Can't read jar file: " + name;
                    throw new FrontendException(msg, errCode, PigException.USER_ENVIRONMENT);
                }
                
                resource = f.toURI().toURL();
            }

            pigContext.addJar(resource);        
        }
    }
    
    /**
     * Register a query with the Pig runtime. The query is parsed and registered, but it is not
     * executed until it is needed.
     * 
     * @param query
     *            a Pig Latin expression to be evaluated.
     * @param startLine
     *            line number of the query within the whold script
     * @throws IOException
     */    
    public void registerQuery(String query, int startLine) throws IOException {
            
        LogicalPlan lp = parseQuery(query, startLine, aliases, opTable, aliasOp);
        // store away the query for use in cloning later
        cachedScript .add(query);
        
        if (lp.getLeaves().size() == 1)
        {
            LogicalOperator op = lp.getSingleLeafPlanOutputOp();
            // No need to do anything about DEFINE 
            if (op instanceof LODefine) {
                return;
            }
        
            // Check if we just processed a LOStore i.e. STORE
            if (op instanceof LOStore) {
                try{
                    execute(null);
                } catch (Exception e) {
                    int errCode = 1002;
                    String msg = "Unable to store alias " + op.getOperatorKey().getId();
                    throw new FrontendException(msg, errCode, PigException.INPUT, e);
                }
            }
        }
    }
    
    private LogicalPlan parseQuery(String query, int startLine, Map<LogicalOperator, LogicalPlan> aliasesMap, 
            Map<OperatorKey, LogicalOperator> opTableMap, Map<String, LogicalOperator> aliasOpMap) throws IOException {
        if(query != null) {
            query = query.trim();
            if(query.length() == 0) return null;
        }else {
            return null;
        }
        try {
            return new LogicalPlanBuilder(pigContext).parse(scope, query,
                    aliasesMap, opTableMap, aliasOpMap, startLine);
        } catch (ParseException e) {
            //throw (IOException) new IOException(e.getMessage()).initCause(e);
            PigException pe = LogUtils.getPigException(e);
            int errCode = 1000;
            String msg = "Error during parsing. " + (pe == null? e.getMessage() : pe.getMessage());
            throw new FrontendException(msg, errCode, PigException.INPUT, false, null, e);
        }
    }

    public LogicalPlan clonePlan(String alias) throws IOException {
        // There are two choices on how we clone the logical plan
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
        
        // parse each line of the cached script and the
        // final logical plan is the clone that we want
        LogicalPlan lp = null;
        int lineNumber = 1;
        // create data structures needed for parsing
        Map<LogicalOperator, LogicalPlan> cloneAliases = new HashMap<LogicalOperator, LogicalPlan>();
        Map<OperatorKey, LogicalOperator> cloneOpTable = new HashMap<OperatorKey, LogicalOperator>();
        Map<String, LogicalOperator> cloneAliasOp = new HashMap<String, LogicalOperator>();
        for (Iterator<String> it = cachedScript.iterator(); it.hasNext(); lineNumber++) {
            lp = parseQuery(it.next(), lineNumber, cloneAliases, cloneOpTable, cloneAliasOp);
        }
        
        if(alias == null) {
            // a store prompted the execution - so return
            // the entire logical plan
            return lp;
        } else {
            // return the logical plan corresponding to the 
            // alias supplied
            LogicalOperator op = cloneAliasOp.get(alias);
            if(op == null) {
                int errCode = 1003;
                String msg = "Unable to find an operator for alias " + alias;
                throw new FrontendException(msg, errCode, PigException.INPUT);
            }
            return cloneAliases.get(op);
        }
    }
    
    public void registerQuery(String query) throws IOException {
        registerQuery(query, 1);
    }
    
    public void registerScript(String fileName) throws IOException {
        try {
            GruntParser grunt = new GruntParser(new FileReader(new File(fileName)));
            grunt.setInteractive(false);
            grunt.setParams(this);
            grunt.parseStopOnError();
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            throw new IOException(e.getCause());
        } catch (org.apache.pig.tools.pigscript.parser.ParseException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            throw new IOException(e.getCause());
        }
    }

    public void printAliases () throws FrontendException {
        System.out.println("aliases: " + aliasOp.keySet());
    }

    public Schema dumpSchema(String alias) throws IOException{
        try {
            LogicalPlan lp = getPlanFromAlias(alias, "describe");
            lp = compileLp(alias, false);
            Schema schema = lp.getLeaves().get(0).getSchema();
            if (schema != null) System.out.println(alias + ": " + schema.toString());    
            else System.out.println("Schema for " + alias + " unknown.");
            return schema;
        } catch (FrontendException fee) {
            int errCode = 1001;
            String msg = "Unable to describe schema for alias " + alias; 
            throw new FrontendException (msg, errCode, PigException.INPUT, false, null, fee);
        }
    }

    public void setJobName(String name){
        pigContext.getProperties().setProperty(PigContext.JOB_NAME, PigContext.JOB_NAME_PREFIX + ":" + name);
    }
    
    /**
     * Forces execution of query (and all queries from which it reads), in order to materialize
     * result
     */
    public Iterator<Tuple> openIterator(String id) throws IOException {
        try {
            LogicalOperator op = aliasOp.get(id);
            if(null == op) {
                int errCode = 1003;
                String msg = "Unable to find an operator for alias " + id;
                throw new FrontendException(msg, errCode, PigException.INPUT);
            }
//            ExecJob job = execute(getPlanFromAlias(id, op.getClass().getName()));
            ExecJob job = store(id, FileLocalizer.getTemporaryPath(null, pigContext).toString(), BinStorage.class.getName() + "()");
            // invocation of "execute" is synchronous!

            if (job.getStatus() == JOB_STATUS.COMPLETED) {
                    return job.getResults();
            } else {
                throw new IOException("Job terminated with anomalous status "
                    + job.getStatus().toString());
            }
        } catch (Exception e) {
            int errCode = 1066;
            String msg = "Unable to open iterator for alias " + id; 
            throw new FrontendException(msg, errCode, PigException.INPUT, e);
        }
    }
    
    /**
     * Store an alias into a file
     * @param id The alias to store
     * @param filename The file to which to store to
     * @throws IOException
     */

    public ExecJob store(String id, String filename) throws IOException {
        return store(id, filename, PigStorage.class.getName() + "()");   // SFPig is the default store function
    }
        
    /**
     *  forces execution of query (and all queries from which it reads), in order to store result in file
     */
    public ExecJob store(
            String id,
            String filename,
            String func) throws IOException{
        if (!aliasOp.containsKey(id))
            throw new IOException("Invalid alias: " + id);
        
        try {
            LogicalPlan readFrom = getPlanFromAlias(id, "store");
            return store(id, readFrom, filename, func);
        } catch (FrontendException fe) {
            int errCode = 1002;
            String msg = "Unable to store alias " + id;
            throw new FrontendException(msg, errCode, PigException.INPUT, fe);
        }
    }
        
    public ExecJob store(
            String id,
            LogicalPlan readFrom,
            String filename,
            String func) throws IOException {
        try {
            LogicalPlan lp = compileLp(id);
            
            // MRCompiler needs a store to be the leaf - hence
            // add a store to the plan to explain
            
            // figure out the leaf to which the store needs to be added
            List<LogicalOperator> leaves = lp.getLeaves();
            LogicalOperator leaf = null;
            if(leaves.size() == 1) {
                leaf = leaves.get(0);
            } else {
                for (Iterator<LogicalOperator> it = leaves.iterator(); it.hasNext();) {
                    LogicalOperator leafOp = it.next();
                    if(leafOp.getAlias().equals(id))
                        leaf = leafOp;
                }
            }
            
            LogicalPlan storePlan = QueryParser.generateStorePlan(scope, lp, filename, func, leaf);
            return executeCompiledLogicalPlan(storePlan);
        } catch (Exception e) {
            int errCode = 1002;
            String msg = "Unable to store alias " + id;
            throw new FrontendException(msg, errCode, PigException.INPUT, e);
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
        try {
            LogicalPlan lp = compileLp(alias);
            
            // MRCompiler needs a store to be the leaf - hence
            // add a store to the plan to explain
            
            // figure out the leaf to which the store needs to be added
            List<LogicalOperator> leaves = lp.getLeaves();
            LogicalOperator leaf = null;
            if(leaves.size() == 1) {
                leaf = leaves.get(0);
            } else {
                for (Iterator<LogicalOperator> it = leaves.iterator(); it.hasNext();) {
                    LogicalOperator leafOp = it.next();
                    if(leafOp.getAlias().equals(alias))
                        leaf = leafOp;
                }
            }
            
            LogicalPlan storePlan = QueryParser.generateStorePlan(
                scope, lp, "fakefile", PigStorage.class.getName(), leaf);
            stream.println("Logical Plan:");
            LOPrinter lv = new LOPrinter(stream, storePlan);
            lv.visit();

            PhysicalPlan pp = compilePp(storePlan);
            stream.println("-----------------------------------------------");
            stream.println("Physical Plan:");

            stream.println("-----------------------------------------------");
            pigContext.getExecutionEngine().explain(pp, stream);
      
        } catch (Exception e) {
            int errCode = 1067;
            String msg = "Unable to explain alias " + alias;
            throw new FrontendException(msg, errCode, PigException.INPUT, e);
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
        if (pigContext.getExecType() == ExecType.LOCAL) {
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
    
    public boolean existsFile(String filename) throws IOException {
        ElementDescriptor elem = pigContext.getDfs().asElement(filename);
        return elem.exists();
    }
    
    public boolean deleteFile(String filename) throws IOException {
        ElementDescriptor elem = pigContext.getDfs().asElement(filename);
        elem.delete();
        return true;
    }
    
    public boolean renameFile(String source, String target) throws IOException {
        pigContext.rename(source, target);
        return true;
    }
    
    public boolean mkdirs(String dirs) throws IOException {
        ContainerDescriptor container = pigContext.getDfs().asContainer(dirs);
        container.create();
        return true;
    }
    
    public String[] listPaths(String dir) throws IOException {
        Collection<String> allPaths = new ArrayList<String>();
        ContainerDescriptor container = pigContext.getDfs().asContainer(dir);
        Iterator<ElementDescriptor> iter = container.iterator();
            
        while (iter.hasNext()) {
            ElementDescriptor elem = iter.next();
            allPaths.add(elem.toString());
        }
            
        return (String[])(allPaths.toArray());
    }
    
    public long totalHadoopTimeSpent() {
//      TODO FIX Need to uncomment this with the right logic
//        return MapReduceLauncher.totalHadoopTimeSpent;
        return 0L;
    }
  
    public Map<String, LogicalPlan> getAliases() {
        Map<String, LogicalPlan> aliasPlans = new HashMap<String, LogicalPlan>();
        for(LogicalOperator op: this.aliases.keySet()) {
            String alias = op.getAlias();
            if(null != alias) {
                aliasPlans.put(alias, this.aliases.get(op));
            }
        }
        return aliasPlans;
    }
    
    public void shutdown() {
        // clean-up activities
            // TODO: reclaim scope to free up resources. Currently
        // this is not implemented and throws an exception
            // hence, for now, we won't call it.
        //
        // pigContext.getExecutionEngine().reclaimScope(this.scope);
    }

    public Set<String> getAliasKeySet() {
        return aliasOp.keySet();
    }

    public Map<LogicalOperator, DataBag> getExamples(String alias) {
        //LogicalPlan plan = aliases.get(aliasOp.get(alias));
        LogicalPlan plan = null;
        try {
            plan = clonePlan(alias);
        } catch (IOException e) {
            //Since the original script is parsed anyway, there should not be an
            //error in this parsing. The only reason there can be an error is when
            //the files being loaded in load don't exist anymore.
            e.printStackTrace();
        }
        ExampleGenerator exgen = new ExampleGenerator(plan, pigContext);
        return exgen.getExamples();
    }
    
    private ExecJob execute(String alias) throws FrontendException, ExecException {
        ExecJob job = null;
//        lp.explain(System.out, System.err);
        LogicalPlan typeCheckedLp = compileLp(alias);
        
        return executeCompiledLogicalPlan(typeCheckedLp);
//        typeCheckedLp.explain(System.out, System.err);
        
    }
    
    private ExecJob executeCompiledLogicalPlan(LogicalPlan compiledLp) throws ExecException {
        PhysicalPlan pp = compilePp(compiledLp);
        // execute using appropriate engine
        FileLocalizer.clearDeleteOnFail();
        ExecJob execJob = pigContext.getExecutionEngine().execute(pp, "execute");
        if (execJob.getStatus()==ExecJob.JOB_STATUS.FAILED)
            FileLocalizer.triggerDeleteOnFail();
        return execJob;
    }

    private LogicalPlan compileLp(
            String alias) throws FrontendException {
        return compileLp(alias, true);
    }

    private LogicalPlan compileLp(
            String alias,
            boolean optimize) throws FrontendException {
        
        // create a clone of the logical plan and give it
        // to the operations below
        LogicalPlan lpClone;
        try {
             lpClone = clonePlan(alias);
        } catch (IOException e) {
            int errCode = 2001;
            String msg = "Unable to clone plan before compiling";
            throw new FrontendException(msg, errCode, PigException.BUG, e);
        }

        
        // Set the logical plan values correctly in all the operators
        PlanSetter ps = new PlanSetter(lpClone);
        ps.visit();
        
        //(new SplitIntroducer(lp)).introduceImplSplits();
        
        // run through validator
        CompilationMessageCollector collector = new CompilationMessageCollector() ;
        FrontendException caught = null;
        try {
            LogicalPlanValidationExecutor validator = 
                new LogicalPlanValidationExecutor(lpClone, pigContext);
            validator.validate(lpClone, collector);
        } catch (FrontendException fe) {
            // Need to go through and see what the collector has in it.  But
            // remember what we've caught so we can wrap it into what we
            // throw.
            caught = fe;            
        }
        
        if(aggregateWarning) {
        	CompilationMessageCollector.logMessages(collector, MessageType.Warning, aggregateWarning, log);
        } else {
        	for(Enum type: MessageType.values()) {
        		CompilationMessageCollector.logAllMessages(collector, log);
        	}
        }
        
        if (caught != null) {
            throw caught;
        }

        // optimize
        if (optimize) {
            //LogicalOptimizer optimizer = new LogicalOptimizer(lpClone);
            LogicalOptimizer optimizer = new LogicalOptimizer(lpClone, pigContext.getExecType());
            optimizer.optimize();
        }

        return lpClone;
    }

    private PhysicalPlan compilePp(LogicalPlan lp) throws ExecException {
        // translate lp to physical plan
        PhysicalPlan pp = pigContext.getExecutionEngine().compile(lp, null);

        // TODO optimize

        return pp;
    }

    private LogicalPlan getPlanFromAlias(
            String alias,
            String operation) throws FrontendException {
        LogicalOperator lo = aliasOp.get(alias);
        if (lo == null) {
            int errCode = 1004;
            String msg = "No alias " + alias + " to " + operation;
            throw new FrontendException(msg, errCode, PigException.INPUT, false, null);
        }
        LogicalPlan lp = aliases.get(lo);
        if (lp == null) {
            int errCode = 1005;
            String msg = "No plan for " + alias + " to " + operation;
            throw new FrontendException(msg, errCode, PigException.INPUT, false, null);
        }        
        return lp;
    }


}

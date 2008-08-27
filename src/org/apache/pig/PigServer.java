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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.datastorage.ContainerDescriptor;
import org.apache.pig.backend.datastorage.DataStorage;
import org.apache.pig.backend.datastorage.ElementDescriptor;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.executionengine.ExecJob;
import org.apache.pig.backend.executionengine.ExecPhysicalPlan;
import org.apache.pig.backend.executionengine.ExecJob.JOB_STATUS;
import org.apache.pig.backend.executionengine.ExecutionEngine;
import org.apache.pig.builtin.BinStorage;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.logicalLayer.ExpressionOperator;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.LOVisitor;
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
import org.apache.pig.impl.plan.DependencyOrderWalker;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.SplitIntroducer;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.util.WrappedIOException;
import org.apache.pig.impl.util.PropertiesUtil;
import org.apache.pig.impl.logicalLayer.LODefine;
import org.apache.pig.impl.logicalLayer.LOStore;

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
   
        throw new IOException("Unrecognized exec type: " + str);
    }


    Map<LogicalOperator, LogicalPlan> aliases = new HashMap<LogicalOperator, LogicalPlan>();
    Map<OperatorKey, LogicalOperator> opTable = new HashMap<OperatorKey, LogicalOperator>();
    Map<String, LogicalOperator> aliasOp = new HashMap<String, LogicalOperator>();
    PigContext pigContext;
    
    private String scope = constructScope();
    
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
        this.pigContext = new PigContext(execType, properties);
        if (this.pigContext.getProperties().getProperty(PigContext.JOB_NAME) == null) {
            setJobName("DefaultJobName") ;
        }
        pigContext.connect();
    }
    
    public PigServer(PigContext context) throws ExecException {
        this.pigContext = context;
        pigContext.connect();
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
                    throw new IOException("Can't read jar file: " + name);
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
        // Bugzilla Bug 1006706 -- ignore empty queries
        //=============================================
        if(query != null) {
            query = query.trim();
            if(query.length() == 0) return;
        }else {
            return;
        }
            
        LogicalPlan lp = null;
        LogicalOperator op = null;
        try {
            lp = (new LogicalPlanBuilder(pigContext).parse(scope, query,
                    aliases, opTable, aliasOp, startLine));
        } catch (ParseException e) {
            throw (IOException) new IOException(e.getMessage()).initCause(e);
        }
        
        if (lp.getLeaves().size() == 1)
        {
            op = lp.getSingleLeafPlanOutputOp();
            // No need to do anything about DEFINE 
            if (op instanceof LODefine) {
                return;
            }
        
            // Check if we just processed a LOStore i.e. STORE
            if (op instanceof LOStore) {
                try{
                    execute(lp);
                } catch (Exception e) {
                    throw WrappedIOException.wrap("Unable to store for alias: " + op.getOperatorKey().getId(), e);
                }
            }
        }
    }

    public void registerQuery(String query) throws IOException {
        registerQuery(query, 1);
    }

    public void dumpSchema(String alias) throws IOException{
        try {
            LogicalPlan lp = getPlanFromAlias(alias, "describe");
            try {
                lp = compileLp(lp, "describe", false);
            } catch (ExecException e) {
                throw new FrontendException(e.getMessage());
            }
            Schema schema = lp.getLeaves().get(0).getSchema();
            if (schema != null) System.out.println(schema.toString());    
            else System.out.println("Schema for " + alias + " unknown.");
        } catch (FrontendException fe) {
            throw WrappedIOException.wrap(
                "Unable to describe schema for alias " + alias, fe);
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
                throw new IOException("Unable to find an operator for alias " + id);
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
            throw WrappedIOException.wrap(
                "Unable to open iterator for alias: " + id, e);
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
            throw WrappedIOException.wrap("Unable to store alias " + id, fe);
        }
    }
        
    public ExecJob store(
            String id,
            LogicalPlan readFrom,
            String filename,
            String func) throws IOException {
        try {
            LogicalPlan storePlan = QueryParser.generateStorePlan(opTable,
                scope, readFrom, filename, func, aliasOp.get(id), aliases);
            return execute(storePlan);
        } catch (Exception e) {
            throw WrappedIOException.wrap("Unable to store for alias: " +
                id, e);
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
            LogicalOperator op = aliasOp.get(alias);
            if(null == op) {
                throw new IOException("Unable to find an operator for alias " + alias);
            }
            LogicalPlan lp = compileLp(getPlanFromAlias(alias, op.getClass().getName()), "explain");
            stream.println("Logical Plan:");
            LOPrinter lv = new LOPrinter(stream, lp);
            lv.visit();

            PhysicalPlan pp = compilePp(lp);
            stream.println("-----------------------------------------------");
            stream.println("Physical Plan:");

            stream.println("-----------------------------------------------");
            pigContext.getExecutionEngine().explain(pp, stream);
        
        } catch (Exception e) {
            throw WrappedIOException.wrap("Unable to explain alias " +
                alias, e);
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

    private ExecJob execute(
            LogicalPlan lp) throws FrontendException, ExecException {
        ExecJob job = null;
//        lp.explain(System.out, System.err);
        LogicalPlan typeCheckedLp = compileLp(lp, "execute");
//        typeCheckedLp.explain(System.out, System.err);
        PhysicalPlan pp = compilePp(typeCheckedLp);
        // execute using appropriate engine
        FileLocalizer.clearDeleteOnFail();
        ExecJob execJob = pigContext.getExecutionEngine().execute(pp, "execute");
        if (execJob.getStatus()==ExecJob.JOB_STATUS.FAILED)
            FileLocalizer.triggerDeleteOnFail();
        return execJob;
    }

    private LogicalPlan compileLp(
            LogicalPlan lp,
            String operation) throws ExecException, FrontendException {
        return compileLp(lp, operation, true);
    }

    private LogicalPlan compileLp(
            LogicalPlan lp,
            String operation,
            boolean optimize) throws ExecException, FrontendException {
        // Look up the logical plan in the aliases map.  That plan will be
        // properly connected to all the others.

        if(null == lp) {
            throw new FrontendException("Cannot operate on null logical plan");
        }

        // Set the logical plan values correctly in all the operators
        PlanSetter ps = new PlanSetter(lp);
        ps.visit();
        
        (new SplitIntroducer(lp)).introduceImplSplits();
        
        // run through validator
        CompilationMessageCollector collector = new CompilationMessageCollector() ;
        FrontendException caught = null;
        try {
            LogicalPlanValidationExecutor validator = 
                new LogicalPlanValidationExecutor(lp, pigContext);
            validator.validate(lp, collector);
        } catch (FrontendException fe) {
            // Need to go through and see what the collector has in it.  But
            // remember what we've caught so we can wrap it into what we
            // throw.
            caught = fe;
        }
        // Check to see if we had any problems.
        StringBuilder sb = new StringBuilder();
        for (CompilationMessageCollector.Message msg : collector) {
            switch (msg.getMessageType()) {
            case Info:
                log.info(msg.getMessage());
                break;

            case Warning:
                log.warn(msg.getMessage());
                break;

            case Unknown:
            case Error:
                log.error(msg.getMessage());
                sb.append(msg.getMessage());
                break;

            default:
                throw new AssertionError("Unknown message type " +
                    msg.getMessageType());

            }
        }

        if (sb.length() > 0 || caught != null) {
            throw new ExecException(sb.toString(), caught);
        }

        // optimize
        if (optimize) {
            LogicalOptimizer optimizer = new LogicalOptimizer(lp);
            optimizer.optimize();
        }

        return lp;
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
            throw new FrontendException("No alias " + alias + " to " +
                operation);
        }
        LogicalPlan lp = aliases.get(lo);
        if (lp == null) {
            throw new FrontendException("No plan for " + alias + " to " +
                operation);
        }        
        return lp;
    }


}

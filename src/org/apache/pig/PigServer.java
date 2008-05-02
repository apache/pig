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
import java.util.Map;
import java.util.Properties ;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.datastorage.ContainerDescriptor;
import org.apache.pig.backend.datastorage.DataStorage;
import org.apache.pig.backend.datastorage.ElementDescriptor;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.executionengine.ExecJob;
import org.apache.pig.backend.executionengine.ExecPhysicalPlan;
import org.apache.pig.backend.executionengine.ExecJob.JOB_STATUS;
import org.apache.pig.backend.hadoop.executionengine.mapreduceExec.MapReduceLauncher;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.logicalLayer.LODefine;
import org.apache.pig.impl.logicalLayer.LOStore;
import org.apache.pig.impl.logicalLayer.LogicalOperator;
import org.apache.pig.impl.logicalLayer.LogicalPlan;
import org.apache.pig.impl.logicalLayer.LogicalPlanBuilder;
import org.apache.pig.impl.logicalLayer.OperatorKey;
import org.apache.pig.impl.logicalLayer.optimizer.Optimizer;
import org.apache.pig.impl.logicalLayer.optimizer.streaming.LoadOptimizer;
import org.apache.pig.impl.logicalLayer.optimizer.streaming.StoreOptimizer;
import org.apache.pig.impl.logicalLayer.parser.ParseException;
import org.apache.pig.impl.logicalLayer.parser.QueryParser;
import org.apache.pig.impl.logicalLayer.schema.TupleSchema;
import org.apache.pig.impl.streaming.StreamingCommand;
import org.apache.pig.impl.util.WrappedIOException;
import org.apache.pig.impl.util.PropertiesUtil;
import org.apache.pig.pen.DisplayExamples;
import org.apache.pig.pen.ExGen;


/**
 * 
 * This class is the program's connection to Pig. Typically a program will create a PigServer
 * instance. The programmer then registers queries using registerQuery() and
 * retrieves results using openIterator() or store().
 * 
 */
public class PigServer {
    
    private final Log log = LogFactory.getLog(getClass());
    
    public String Result;
    
    /**
     * The type of query execution
     */
    static public enum ExecType {
        /**
         * Run everything on the local machine
         */
        LOCAL,
        /**
         * Use the Hadoop Map/Reduce framework
         */
        MAPREDUCE,
        /**
         * Use the Experimental Hadoop framework; not available yet.
         */
        PIG
    }
    
    public static ExecType parseExecType(String str) throws IOException {
        String normStr = str.toLowerCase();
        
        if (normStr.equals("local")) return ExecType.LOCAL;
        if (normStr.equals("mapreduce")) return ExecType.MAPREDUCE;
        if (normStr.equals("mapred")) return ExecType.MAPREDUCE;
        if (normStr.equals("pig")) return ExecType.PIG;
        if (normStr.equals("pigbody")) return ExecType.PIG;
   
        throw new IOException("Unrecognized exec type: " + str);
    }


    Map<String, LogicalPlan> aliases = new HashMap<String, LogicalPlan>();
    Map<OperatorKey, LogicalOperator> opTable = new HashMap<OperatorKey, LogicalOperator>();
    
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
        if (this.pigContext.getProperties().getProperty(PigContext.JOB_NAME) == null) {
            setJobName("DefaultJobName") ;
        }
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
     * @param aliases - the new function alias to define.
     * @param functionSpec - the name of the function and any arguments.
     * It should have the form: classname('arg1', 'arg2', ...)
     */
    public void registerFunction(String function, String functionSpec) {
        pigContext.registerFunction(function, functionSpec);
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
     * @return a handle to the query.
     * @throws IOException
     */
    public void registerQuery(String query) throws IOException {
        // Bugzilla Bug 1006706 -- ignore empty queries
        //=============================================
        if(query != null) {
            query = query.trim();
            if(query.length() == 0) return;
        }else {
            return;
        }
            
        // parse the query into a logical plan
        LogicalPlan lp = null;
        LogicalOperator op = null;
        try {
            lp = (new LogicalPlanBuilder(pigContext).parse(scope, query, aliases, opTable));
            op = opTable.get(lp.getRoot());
        } catch (ParseException e) {
            throw (IOException) new IOException(e.getMessage()).initCause(e);
        }
        
        if (lp.getAlias() != null) {
            aliases.put(lp.getAlias(), lp);
        }
        
        // No need to do anything about DEFINE 
        if (op instanceof LODefine) {
            return;
        }
        
        // Check if we just processed a LOStore i.e. STORE
        if (op instanceof LOStore) {
            try {
                optimizeAndRunQuery(lp);
            }
            catch (ExecException e) {
                throw WrappedIOException.wrap("Unable to store alias " + 
                        lp.getAlias(), e);
            }
        }
    }
      
    public void dumpSchema(String alias) throws IOException{
    LogicalPlan lp = aliases.get(alias);
    if (lp == null)
        throw new IOException("Invalid alias - " + alias);

    TupleSchema schema = lp.getOpTable().get(lp.getRoot()).outputSchema();

    System.out.println(schema.toString());    
    }

    public void setJobName(String name){
        pigContext.getProperties().setProperty(PigContext.JOB_NAME, PigContext.JOB_NAME_PREFIX + ":" + name);
    }
    
    /**
     * Forces execution of query (and all queries from which it reads), in order to materialize
     * result
     */
    public Iterator<Tuple> openIterator(String id) throws IOException {
        if (!aliases.containsKey(id))
            throw new IOException("Invalid alias: " + id);

        // TODO: front-end could actually remember what logical plans have been
        // already submitted to the back-end for compilation and
        // execution.
        
        LogicalPlan readFrom = aliases.get(id);
        
        // Run
        try {
            ExecJob job = optimizeAndRunQuery(readFrom);

            // invocation of "execute" is synchronous!
            if (job.getStatus() == JOB_STATUS.COMPLETED) {
                return job.getResults();
            }
            else {
                throw new IOException("Job terminated with anomalous status " + job.getStatus().toString());
            }
        }
        catch (ExecException e) {
            throw WrappedIOException.wrap("Unable to open iterator for alias: " + id, e);
        }
    }
    
    /**
     * Store an alias into a file
     * @param id: The alias to store
     * @param filename: The file to which to store to
     * @throws IOException
     */

    public void store(String id, String filename) throws IOException {
        store(id, filename, PigStorage.class.getName() + "()");   // SFPig is the default store function
    }
        
    /**
     *  forces execution of query (and all queries from which it reads), in order to store result in file
     */
    public void store(String id, String filename, String func) throws IOException{
        if (!aliases.containsKey(id))
            throw new IOException("Invalid alias: " + id);
        
        if (FileLocalizer.fileExists(filename, pigContext.getDfs())) {
            StringBuilder sb = new StringBuilder();
            sb.append("Output file ");
            sb.append(filename);
            sb.append(" already exists. Can't overwrite.");
            throw new IOException(sb.toString());
        }

        LogicalPlan readFrom = aliases.get(id);
        
        store(readFrom,filename,func);
    }
        
    public void store(LogicalPlan readFrom, String filename, String func) throws IOException {
        LogicalPlan storePlan = QueryParser.generateStorePlan(readFrom.getOpTable(),
                                                              scope,
                                                              readFrom,
                                                              filename,
                                                              func,
                                                              pigContext);

        // Optimize 
        Optimizer optimizer = new LoadOptimizer();
        optimizer.optimize(readFrom);


        try {
            optimizeAndRunQuery(storePlan);
        }
        catch (ExecException e) {
            throw WrappedIOException.wrap("Unable to store alias " + 
                    storePlan.getAlias(), e);

        }
    }

    private ExecJob optimizeAndRunQuery(LogicalPlan root) throws ExecException {
        // Optimize the LogicalPlan
        Optimizer loadOptimizer = new LoadOptimizer();
        loadOptimizer.optimize(root);

        Optimizer storeOptimizer = new StoreOptimizer();
        storeOptimizer.optimize(root);

        // Execute
        ExecPhysicalPlan pp = 
            pigContext.getExecutionEngine().compile(root, null);

        return pigContext.getExecutionEngine().execute(pp);
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
        stream.println("Logical Plan:");
        LogicalPlan lp = aliases.get(alias);
        if (lp == null) {
            log.error("Invalid alias: " + alias);
            stream.println("Invalid alias: " + alias);
            throw new IOException("Invalid alias: " + alias);
        }

        lp.explain(stream);
        
        stream.println("-----------------------------------------------");
        stream.println("Physical Plan:");
        try {
            ExecPhysicalPlan pp = 
                pigContext.getExecutionEngine().compile(lp, null);
        
            pp.explain(stream);
        }
        catch (ExecException e) {
            StringBuilder sbException = new StringBuilder();
            sbException.append("Failed to compile to phyiscal plan: ");
            sbException.append(alias);
            if (log.isErrorEnabled()) {
                log.error(sbException.toString());
            }
            StringBuilder sb = new StringBuilder();
            sb.append("Failed to compile the logical plan for ");
            sb.append(alias);
            sb.append(" into a physical plan");
            stream.println(sb.toString());
            throw WrappedIOException.wrap(sbException.toString(), e);
        }
    }

    /**
     * Returns the unused byte capacity of an HDFS filesystem. This value does
     * not take into account a replication factor, as that can vary from file
     * to file. Thus if you are using this to determine if you data set will fit
     * in the HDFS, you need to divide the result of this call by your specific replication
     * setting. 
     * @return
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
        return MapReduceLauncher.totalHadoopTimeSpent;
    }
  
    public Map<String, LogicalPlan> getAliases() {
        return this.aliases;
    }
    
    public void shutdown() {
        // clean-up activities
            // TODO: reclaim scope to free up resources. Currently
        // this is not implemented and throws an exception
            // hence, for now, we won't call it.
        //
        // pigContext.getExecutionEngine().reclaimScope(this.scope);
    }
    
    public void showExamples(String id) throws IOException {
		if(!aliases.containsKey(id))
			throw new IOException("Invalid alias : " + id);
		
		LogicalPlan root = aliases.get(id);
		showExamples(root);
	}
	
	public void showExamples(LogicalPlan lp) throws IOException{
		Map<LogicalOperator, DataBag> exampleData = ExGen.GenerateExamples(lp, pigContext);
		this.Result = DisplayExamples.PrintTabular(lp, exampleData);
		System.out.println(Result);
	}
}

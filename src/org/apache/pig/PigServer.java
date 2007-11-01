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
import java.io.InputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.dfs.DistributedFileSystem;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.Datum;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.BufferedPositionedInputStream;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.logicalLayer.LOLoad;
import org.apache.pig.impl.logicalLayer.LORead;
import org.apache.pig.impl.logicalLayer.LOStore;
import org.apache.pig.impl.logicalLayer.LogicalOperator;
import org.apache.pig.impl.logicalLayer.LogicalPlan;
import org.apache.pig.impl.logicalLayer.LogicalPlanBuilder;
import org.apache.pig.impl.logicalLayer.parser.ParseException;
import org.apache.pig.impl.logicalLayer.schema.TupleSchema;
import org.apache.pig.impl.mapreduceExec.MapReduceLauncher;
import org.apache.pig.impl.physicalLayer.IntermedResult;
import org.apache.pig.impl.physicalLayer.POMapreduce;
import org.apache.pig.impl.physicalLayer.POStore;
import org.apache.pig.impl.physicalLayer.PhysicalPlan;



/**
 * 
 * This class is the program's connection to Pig. Typically a program will create a PigServer
 * instance. The programmer then registers queries using registerQuery() and
 * retrieves results using openIterator() or store().
 * 
 */
public class PigServer {
    
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
        /**
     *  a table mapping intermediate results to physical plans that read them (the state
     *  of how much has been read is maintained in PORead 
     */
    Map<IntermedResult, PhysicalPlan> physicalPlans = new HashMap<IntermedResult, PhysicalPlan>();
    /**
     * a table mapping ID's to intermediate results
     */
    Map<String, IntermedResult> queryResults = new HashMap<String, IntermedResult>();

    PigContext pigContext;
    
    public PigServer(String execType) throws IOException {
        this(parseExecType(execType));
    }
    
    public PigServer() {
    	this(ExecType.MAPREDUCE);
    }
    
    public PigServer(ExecType execType) {
        this.pigContext = new PigContext(execType);
        pigContext.connect();
    }
    
    public PigServer(PigContext context) {
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
     * @param aliases - the new function alias to define.
     * @param functionSpec - the name of the function and any arguments.
     * It should have the form: classname('arg1', 'arg2', ...)
     */
    public void registerFunction(String function, String functionSpec) {
    	pigContext.registerFunction(function, functionSpec);
    }
    
    public void registerJar(String path) throws IOException{
    	File f = new File(path);
    	if (!f.canRead())
    		throw new IOException("Can't read " + path);
        pigContext.addJar(path);
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
        try {
            lp = (new LogicalPlanBuilder(pigContext).parse(query, queryResults));
            // System.out.println(lp.toString());
        } catch (ParseException e) {
            throw (IOException) new IOException(e.getMessage()).initCause(e);
        }
        if (lp.getAlias() != null)
            queryResults.put(lp.getAlias(), new IntermedResult(lp, pigContext));
    }
      
    public void dumpSchema(String alias) throws IOException{
	IntermedResult ir = queryResults.get(alias);
	if (ir == null)
		throw new IOException("Invalid alias - " + alias);

	TupleSchema schema = ir.lp.getRoot().outputSchema();

	System.out.println(schema.toString());	
    }

    public void setJobName(String name){
        pigContext.setJobName(name);
    }

    public void newRelation(String id) {
        queryResults.put(id, new IntermedResult());
    }
    
    public void insertTuple(String id, Tuple t) throws IOException {
        if (!queryResults.containsKey(id)) throw new IOException("Attempt to insert tuple into nonexistent relation.");
        queryResults.get(id).add(t);
    }

    
    public Iterator<Tuple> openIterator(String id, boolean continueFromLast) throws IOException{
    	if (!continueFromLast)
    		return openIterator(id);
    	
    	if (pigContext.getExecType()!= ExecType.LOCAL){
    		System.err.println("Streaming execution not supported in non-local mode.");
    		System.exit(-1);
    	}
		
        if (!queryResults.containsKey(id))
            throw new IOException("Invalid alias: " + id);

        IntermedResult readFrom = (IntermedResult) queryResults.get(id);
        
        if (readFrom.getOutputType()==LogicalOperator.FIXED){
        	//Its not a continuous plan, just let it go through the non-continuous channel
        	return openIterator(id);
        }
        
        PhysicalPlan pp = null;
        
    	if (!physicalPlans.containsKey(readFrom)){
    		
    		//Not trying to do sharing, so won't compile and exec it as in the other cases
    		//First check if some other operator tried to execute this intermediate result
    		if (readFrom.executed() || readFrom.compiled()){
    			//Compile it again, so that POSlaves are not added in between query execution
    			readFrom.compile(queryResults);
    		}
    		LogicalPlan lp = new LogicalPlan(new LORead(readFrom), pigContext);
    		pp = new PhysicalPlan(lp,queryResults);
    		physicalPlans.put(readFrom, pp);
    	}else{
       		pp = physicalPlans.get(readFrom);
    	}
    	
		// Data bags are guaranteed to contain tuples.
    	//return pp.exec(continueFromLast).content();
		// A direct subversion of the type system, this has to be bad.
		Iterator<Datum> i = pp.exec(continueFromLast).content();
		Object o = i;
    	return (Iterator<Tuple>)o;
    	
    }
    
    /**
     * Forces execution of query (and all queries from which it reads), in order to materialize
     * result
     */
    public Iterator<Tuple> openIterator(String id) throws IOException {
        if (!queryResults.containsKey(id))
            throw new IOException("Invalid alias: " + id);

        IntermedResult readFrom = (IntermedResult) queryResults.get(id);

        readFrom.compile(queryResults);
        readFrom.exec();
        if (pigContext.getExecType() == ExecType.LOCAL) {
            Object o = readFrom.read().content();
			return (Iterator<Tuple>)o;
		}
        final LoadFunc p;
        
        try{
        	 p = (LoadFunc)PigContext.instantiateFuncFromSpec(readFrom.outputFileSpec.getFuncSpec());
        }catch (Exception e){
        	IOException ioe = new IOException(e.getMessage());
        	ioe.setStackTrace(e.getStackTrace());
        	throw ioe;
        }
        InputStream is = FileLocalizer.open(readFrom.outputFileSpec.getFileName(), pigContext);
        p.bindTo(readFrom.outputFileSpec.getFileName(), new BufferedPositionedInputStream(is), 0, Long.MAX_VALUE);
        return new Iterator<Tuple>() {
            Tuple   t;
            boolean atEnd;

            public boolean hasNext() {
                if (atEnd)
                    return false;
                try {
                    if (t == null)
                        t = p.getNext();
                    if (t == null)
                        atEnd = true;
                } catch (Exception e) {
                    e.printStackTrace();
                    t = null;
                    atEnd = true;
                }
                return !atEnd;
            }

            public Tuple next() {
                Tuple next = t;
                if (next != null) {
                    t = null;
                    return next;
                }
                try {
                    next = p.getNext();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                if (next == null)
                    atEnd = true;
                return next;
            }

            public void remove() {
                throw new RuntimeException("Removal not supported");
            }

        };
        
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
	 * Continuous case of store: store the updates to particular alias into the given file
	 * @param id
	 * @param filename
	 * @throws IOException
	 */
	
	public void update(String id, String filename, boolean append) throws IOException {
		update(id, filename, PigStorage.class.getName() + "()", append);
	}
	
	/**
	 *  forces execution of query (and all queries from which it reads), in order to store result in file
	 */
	public void store(String id, String filename, String func) throws IOException{
        if (!queryResults.containsKey(id))
            throw new IOException("Invalid alias: " + id);
        
        if (FileLocalizer.fileExists(filename, pigContext))
        	throw new IOException("Output file " + filename + " already exists. Can't overwrite.");

        IntermedResult readFrom = (IntermedResult) queryResults.get(id);
        
        store(readFrom,filename,func);
	}
        
    public void store(IntermedResult readFrom, String filename, String func) throws IOException{
		if (func == null)
			func = PigStorage.class.getName();
		
		filename = removeQuotes(filename);
        FileSpec fileSpec = new FileSpec(filename,func);

        if (pigContext.getExecType() == ExecType.LOCAL){
        	if (!readFrom.executed()){
        		readFrom.compile(queryResults);
        		readFrom.exec();
        	}
            // generate a simple logical plan to store the results
            LogicalPlan lp = new LogicalPlan(new LOStore(new LORead(readFrom), new FileSpec(filename, func),false), pigContext);

            // temporary code to test the ExampleGenerator:
            //ExGen.PrintExamples(lp, ExGen.GenerateExamples(lp, pigContext));
            
            // then execute it to write to disk
            (new PhysicalPlan(lp, queryResults)).exec(false);
            return;
        }else{
        	if (!readFrom.executed()) {
                readFrom.compile(queryResults);
            	if (pigContext.getExecType() == ExecType.MAPREDUCE){
            		POMapreduce pom = (POMapreduce)readFrom.pp.root;    	
            		pom.outputFileSpec = fileSpec;
            	}
            	readFrom.setPermanentFileSpec(fileSpec);
            	readFrom.exec();
        	}else{
        		if (readFrom.outputFileSpec == null){
        			readFrom.toDFSFile(fileSpec, pigContext);
        		}else{
	                String src = readFrom.outputFileSpec.getFileName();
	                String dst = filename;
	                boolean islocal = false;
	                if (dst.startsWith(FileLocalizer.LOCAL_PREFIX)) {
	                    dst = dst.substring(FileLocalizer.LOCAL_PREFIX.length());
	                    islocal = true;
	                }
	                if (readFrom.outputFileSpec.getFuncSpec().equals(func)){
	                	if (readFrom.isTemporary()) {
		                	if (islocal) {
			                    pigContext.copy(src, dst, islocal);
			                } else {
			                    pigContext.rename(src, dst);
			                }
		                	readFrom.setPermanentFileSpec(new FileSpec(filename,func));
	                	}else{
	                		pigContext.copy(src, dst, islocal);
	                	}
	                }else{
				try{
	                	store(new IntermedResult(new LogicalPlan(new LOLoad(readFrom.outputFileSpec),pigContext), pigContext), filename, func);
 				} catch (ParseException e) {
            			throw (IOException) new IOException(e.getMessage()).initCause(e);
        			}
	                }
        		}
        	}
        }
    }

	/**
	 * Continuous version of the store function above
	 *
	 */
	public void update(String id, String filename, String func, boolean append) throws IOException{
		
		
		if (pigContext.getExecType()!=ExecType.LOCAL){
    		System.err.println("Streaming execution not supported in non-local mode.");
    		System.exit(-1);
    	}
		
		filename = removeQuotes(filename);
		
        if (!queryResults.containsKey(id))
            throw new IOException("Invalid alias: " + id);

        IntermedResult readFrom = (IntermedResult) queryResults.get(id);
        
        if (readFrom.getOutputType()==LogicalOperator.FIXED){
        	//Its not a continuous plan, just let it go through the non-continuous channel
        	store(id,filename,func);
        	return;
        }
        
        PhysicalPlan pp = null;
        
    	if (!physicalPlans.containsKey(readFrom)){
    		
    		//Not trying to do sharing, so won't compile and exec it as in the other cases
    		//First check if some other operator tried to execute this intermediate result
    		if (readFrom.executed() || readFrom.compiled()){
    			//Compile it again, so that POSlaves are not added in between query execution
    			readFrom.compile(queryResults);
    		}
    		LogicalPlan lp = new LogicalPlan(new LORead(readFrom), pigContext);
    		pp = new PhysicalPlan(lp,queryResults);
    		physicalPlans.put(readFrom, pp);
    	}else{
       		pp = physicalPlans.get(readFrom);
    	}
    	new PhysicalPlan(new POStore(pp.root,new FileSpec(filename,func),append,pigContext)).exec(true);
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
        } else {
            DistributedFileSystem dfs = (DistributedFileSystem) pigContext.getDfs();
            return dfs.getRawCapacity() - dfs.getRawUsed();
        }
    }

    /**
     * Returns the length of a file in bytes which exists in the HDFS (accounts for replication).
     * @param filename
     * @return
     * @throws IOException
     */
    public long fileSize(String filename) throws IOException {
        FileSystem dfs = pigContext.getDfs();
        Path p = new Path(filename);
        long len = dfs.getLength(p);
        long replication = dfs.getDefaultReplication(); // did not work, for some reason: dfs.getReplication(p);
        return len * replication;
    }
    
    public boolean existsFile(String filename) throws IOException {
        return pigContext.getDfs().exists(new Path(filename));
    }
    
    public boolean deleteFile(String filename) throws IOException {
        return pigContext.getDfs().delete(new Path(filename));
    }
    
    public boolean renameFile(String source, String target) throws IOException {
        return pigContext.getDfs().rename(new Path(source), new Path(target));
    }
    
    public boolean mkdirs(String dirs) throws IOException {
        return pigContext.getDfs().mkdirs(new Path(dirs));
    }
    
    public String[] listPaths(String dir) throws IOException {
        Path paths[] = pigContext.getDfs().listPaths(new Path(dir));
        String strPaths[] = new String[paths.length];
        for (int i = 0; i < paths.length; i++) {
            strPaths[i] = paths[i].toString();
        }
        return strPaths;
    }
    
    public long totalHadoopTimeSpent() {
        return MapReduceLauncher.totalHadoopTimeSpent;
    }

//    public static class PStat {
//        // These are the members of the unix fstat and stat structure
//        // we currently don't implement all of them, but try to provide
//        // a mirror for the function and values for HDFS files
//        // ====================================================
//        // struct stat {
//        // dev_t st_dev; /* device inode resides on */
//        // ino_t st_ino; /* inode's number */
//        // mode_t st_mode; /* inode protection mode */
//        // nlink_t st_nlink; /* number or hard links to the file */
//        // uid_t st_uid; /* user-id of owner */
//        // gid_t st_gid; /* group-id of owner */
//        // dev_t st_rdev; /* device type, for special file inode */
//        // struct timespec st_atimespec; /* time of last access */
//        // struct timespec st_mtimespec; /* time of last data modification */
//        // struct timespec st_ctimespec; /* time of last file status change */
//        // off_t st_size; /* file size, in bytes */
//        // quad_t st_blocks; /* blocks allocated for file */
//        // u_long st_blksize;/* optimal file sys I/O ops blocksize */
//        // u_long st_flags; /* user defined flags for file */
//        // u_long st_gen; /* file generation number */
//        // };
//        public long st_size;
//        public long st_blocks;
//    }

    private String removeQuotes(String str) {
        if (str.startsWith("\'") && str.endsWith("\'"))
            return str.substring(1, str.length() - 1);
        else
            return str;
    }

	public Map<String, IntermedResult> getQueryResults() {
		return queryResults;
	}
}

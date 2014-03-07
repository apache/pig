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

import java.io.FileInputStream;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.PigServer;
import org.apache.pig.PigRunner.ReturnCode;
import org.apache.pig.impl.PigContext;
import org.apache.pig.tools.grunt.GruntParser;
import org.apache.pig.tools.pigscript.parser.ParseException;
import org.apache.pig.tools.pigstats.PigProgressNotificationListener;
import org.apache.pig.tools.pigstats.PigStats;
import org.apache.pig.tools.pigstats.PigStatsUtil;
import org.apache.pig.tools.pigstats.ScriptState;

/**
 * This represents an instance of a bound pipeline.
 */
public class BoundScript {
    
    private static final Log LOG = LogFactory.getLog(BoundScript.class);
    
    private List<String> queries = new ArrayList<String>();

    private String name = null;
   
    private ScriptPigContext scriptContext = null;
    
    BoundScript(String query, ScriptPigContext scriptContext, String name) {
        this.queries.add(query);
        this.scriptContext = scriptContext;
        this.name = name;               
    }
    
    BoundScript(List<String> queries, ScriptPigContext scriptContext,
            String name) {
        this.queries.addAll(queries);
        this.scriptContext = ScriptPigContext.get();
        this.name = name;        
    }
    
    /**
     * Run a pipeline on Hadoop.  
     * If there are no stores in this pipeline then nothing will be run. 
     * @return {@link PigStats}, null if there is no bound query to run.
     * @throws IOException
     */
    public PigStats runSingle() throws IOException {
        return runSingle((Properties)null);
    }
     
    /**
     * Run a pipeline on Hadoop.  
     * If there are no stores in this pipeline then nothing will be run.  
     * @param prop Map of properties that Pig should set when running the script.
     * This is intended for use with scripting languages that do not support
     * the Properties object.
     * @return {@link PigStats}, null if there is no bound query to run.
     * @throws IOException
     */
    public PigStats runSingle(Properties prop) throws IOException {
        if (queries.size() > 1) {
            throw new IOException(
                    "This pipeline contains multiple queries. Use run() method instead");
        }
        if (queries.isEmpty()) {
            LOG.info("No bound query to run");
            return null;
        }
        if (prop != null) {
            scriptContext.getPigContext().getProperties().putAll(prop);
        }
        PigStats ret = exec(queries.get(0)); 
        setPigStats(ret);
        return ret;
    }
    
    /**
     * Run a pipeline on Hadoop.  
     * If there are no stores in this pipeline then nothing will be run.  
     * @param propfile File with properties that Pig should set when running the script.
     * @return {@link PigStats}, null if there is no bound query to run.
     * @throws IOException
     */
    public PigStats runSingle(String propfile) throws IOException {
        Properties props = new Properties();
        FileInputStream fin = null;
        try {
            fin = new FileInputStream(propfile);
            props.load(fin);
        } finally {
            if (fin != null) fin.close();
        }
        return runSingle(props);
    }

    /**
     * Run multiple instances of bound pipeline on Hadoop in parallel.  
     * If there are no stores in this pipeline then nothing will be run.  
     * Bind is called first with the list of maps of variables to bind. 
     * @return a list of {@link PigStats}, one for each map of variables passed
     * to bind.
     * @throws IOException
     */    
    public List<PigStats> run() throws IOException {    
        return run((Properties)null);
    }
    
    /**
     * Run multiple instances of bound pipeline on Hadoop in parallel.
     * @param prop Map of properties that Pig should set when running the script.
     * This is intended for use with scripting languages that do not support
     * the Properties object.
     * @return a list of {@link PigStats}, one for each map of variables passed
     * to bind.
     * @throws IOException
     */
    public List<PigStats> run(Properties prop) throws IOException {
        List<PigStats> stats = new ArrayList<PigStats>();
        if (queries.isEmpty()) {
            LOG.info("No bound query to run.");
            return stats;
        } 
        if (queries.size() == 1) {
            PigStats ps = runSingle();
            stats.add(ps);
            return stats;
        }
        if (prop != null) {
            scriptContext.getPigContext().getProperties().putAll(prop);
        }
        List<PigProgressNotificationListener> listeners 
            = ScriptState.get().getAllListeners();
        SyncProgressNotificationAdaptor adaptor 
            = new SyncProgressNotificationAdaptor(listeners);
        List<Future<PigStats>> futures = new ArrayList<Future<PigStats>>();
        ExecutorService executor = Executors.newFixedThreadPool(queries.size());
        for (int i=0; i<queries.size(); i++) {          
            Properties props = new Properties();
            props.putAll(scriptContext.getPigContext().getProperties());
            PigContext ctx = new PigContext(scriptContext.getPigContext().getExecType(), props);
            MyCallable worker = new MyCallable(queries.get(i), ctx, adaptor);
            Future<PigStats> submit = executor.submit(worker);
            futures.add(submit);
        }           
        for (Future<PigStats> future : futures) {
            try {
                stats.add(future.get());
            } catch (InterruptedException e) {
                LOG.error("Pig pipeline failed to complete", e);
                PigStatsUtil.getEmptyPigStats();
                PigStatsUtil.setErrorMessage(e.getMessage());
                PigStats failed = PigStatsUtil.getPigStats(ReturnCode.FAILURE);                    
                stats.add(failed);
            } catch (ExecutionException e) {
                LOG.error("Pig pipeline failed to complete", e);
                PigStatsUtil.getEmptyPigStats();
                PigStatsUtil.setErrorMessage(e.getMessage());                  
                PigStats failed = PigStatsUtil.getPigStats(ReturnCode.FAILURE);                    
                stats.add(failed);
            }
        }
    
        if (!stats.isEmpty()) {
            setPigStats(stats);;
        }
        return stats;
    }
    
    /**
     * Run multiple instances of bound pipeline on Hadoop in parallel.
     * @param propfile File with properties that Pig should set when running the script.
     * @return a list of PigResults, one for each map of variables passed
     * to bind.
     * @throws IOException
     */
    public List<PigStats> run(String propfile) throws IOException {
        Properties prop = new Properties();
        FileInputStream fin = null;
        try {
            fin = new FileInputStream(propfile);
            prop.load(fin);
        } finally {
            if (fin != null) fin.close();
        }        
        return run(prop);
    }

    /**
     * Run illustrate for this pipeline.  Results will be printed to stdout.  
     * @throws IOException if illustrate fails.
     */
    public void illustrate() throws IOException {
        if (queries.isEmpty()) {
            LOG.info("No bound query to illustrate");
            return;
        }
        PigServer pigServer = new PigServer(scriptContext.getPigContext(), false);
        registerQuery(pigServer, queries.get(0));
        pigServer.getExamples(null);
    }

    /**
     * Explain this pipeline.  Results will be printed to stdout.
     * @throws IOException if explain fails.
     */
    public void explain() throws IOException {
        if (queries.isEmpty()) {
            LOG.info("No bound query to explain");
            return;
        }
        PigServer pigServer = new PigServer(scriptContext.getPigContext(), false);
        registerQuery(pigServer, queries.get(0));
        pigServer.explain(null, System.out);
    }

    /**
     * Describe the schema of an alias in this pipeline.
     * Results will be printed to stdout.
     * @param alias to be described
     * @throws IOException if describe fails.
     */
    public void describe(String alias) throws IOException {
        if (queries.isEmpty()) {
            LOG.info("No bound query to describe");
            return;
        }
        PigServer pigServer = new PigServer(scriptContext.getPigContext(), false);
        registerQuery(pigServer, queries.get(0));
        pigServer.dumpSchema(alias);        
    }

    //-------------------------------------------------------------------------      

    private PigStats exec(String query) throws IOException {
        LOG.info("Query to run:\n" + query);
        List<PigProgressNotificationListener> listeners = ScriptState.get().getAllListeners();
        PigContext pc = scriptContext.getPigContext();
        ScriptState scriptState = pc.getExecutionEngine().instantiateScriptState();
        ScriptState.start(scriptState);
        ScriptState.get().setScript(query);
        for (PigProgressNotificationListener listener : listeners) {
            ScriptState.get().registerListener(listener);
        }
        PigServer pigServer = new PigServer(scriptContext.getPigContext(), false);
        pigServer.setBatchOn();
        GruntParser grunt = new GruntParser(new StringReader(query), pigServer);
        grunt.setInteractive(false);
        try {
            grunt.parseStopOnError(true);
        } catch (ParseException e) {
            throw new IOException("Failed to parse script " + e.getMessage(), e);
        }
        pigServer.executeBatch();
        return PigStats.get();
    }

    private void registerQuery(PigServer pigServer, String pl) throws IOException {
        GruntParser grunt = new GruntParser(new StringReader(pl), pigServer);
        grunt.setInteractive(false);
        pigServer.setBatchOn();
      try {
            grunt.parseStopOnError(true);
        } catch (ParseException e) {
            throw new IOException("Failed to parse query: " + pl, e);
        }
    }
    
    private void setPigStats(PigStats stats) {        
        ScriptEngine engine = scriptContext.getScriptEngine();
        if (name != null) {
            engine.setPigStats(name, stats);
        } else {
            engine.setPigStats(stats.getScriptId(), stats);
        }
    }

    private void setPigStats(List<PigStats> lst) {
        if (lst == null || lst.isEmpty()) return;        
        String key = (name != null) ? name : this.toString();
        ScriptEngine engine = scriptContext.getScriptEngine();
        for (PigStats stats : lst) {
            engine.setPigStats(key, stats);
        } 
    }
        
    //-------------------------------------------------------------------------
    
    private class MyCallable implements Callable<PigStats> {
        
        private String query = null;
        private PigContext ctx = null;
        private PigProgressNotificationListener adaptor;
        
        public MyCallable(String pl, PigContext ctx, PigProgressNotificationListener adaptor) {
            query = pl;
            this.ctx = ctx;
            this.adaptor = adaptor;
        }
        
        @Override
        public PigStats call() throws Exception {
            LOG.info("Query to run:\n" + query);
            PigContext pc = scriptContext.getPigContext();
            ScriptState scriptState = pc.getExecutionEngine().instantiateScriptState();
            ScriptState.start(scriptState);
            ScriptState.get().setScript(query);
            ScriptState.get().registerListener(adaptor);
            PigServer pigServer = new PigServer(ctx, true);
            pigServer.setBatchOn();
            GruntParser grunt = new GruntParser(new StringReader(query), pigServer);
            grunt.setInteractive(false);
            try {
                grunt.parseStopOnError(true);
            } catch (ParseException e) {
                throw new IOException("Failed to parse script", e);
            }
            pigServer.executeBatch();
            return PigStats.get();
        }
    }

}

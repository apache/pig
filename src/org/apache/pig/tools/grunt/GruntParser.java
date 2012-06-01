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
package org.apache.pig.tools.grunt;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.AbstractList;
import java.util.Arrays;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import jline.ConsoleReader;
import jline.ConsoleReaderInputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.pig.LoadFunc;
import org.apache.pig.PigServer;
import org.apache.pig.backend.datastorage.ContainerDescriptor;
import org.apache.pig.backend.datastorage.DataStorage;
import org.apache.pig.backend.datastorage.DataStorageException;
import org.apache.pig.backend.datastorage.ElementDescriptor;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.backend.hadoop.datastorage.HDataStorage;
import org.apache.pig.backend.hadoop.executionengine.HExecutionEngine;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.io.FileLocalizer.FetchFileRet;
import org.apache.pig.impl.util.LogUtils;
import org.apache.pig.impl.util.TupleFormat;
import org.apache.pig.tools.parameters.ParameterSubstitutionPreprocessor;
import org.apache.pig.tools.pigscript.parser.ParseException;
import org.apache.pig.tools.pigscript.parser.PigScriptParser;
import org.apache.pig.tools.pigscript.parser.PigScriptParserTokenManager;
import org.apache.pig.tools.pigstats.JobStats;
import org.apache.pig.tools.pigstats.PigStats;
import org.apache.pig.tools.pigstats.PigStats.JobGraph;

@SuppressWarnings("deprecation")
public class GruntParser extends PigScriptParser {

    private static final Log log = LogFactory.getLog(GruntParser.class);

    public GruntParser(Reader stream) {
        super(stream);
        init();
    }

    public GruntParser(InputStream stream, String encoding) {
        super(stream, encoding);
        init();
    }

    public GruntParser(InputStream stream) {
        super(stream);
        init();
    }

    public GruntParser(PigScriptParserTokenManager tm) {
        super(tm);
        init();
    }

    private void init() {
        mDone = false;
        mLoadOnly = false;
        mExplain = null;
        mScriptIllustrate = false;
    }

    @Override
    public void setInteractive(boolean isInteractive){
        super.setInteractive(isInteractive);
        if(isInteractive){
            setValidateEachStatement(true);
        }
    }
    
    public void setValidateEachStatement(boolean b) {
        mPigServer.setValidateEachStatement(b);
    }

    private void setBatchOn() {
        mPigServer.setBatchOn();
    }

    private void executeBatch() throws IOException {
        if (mPigServer.isBatchOn()) {
            if (mExplain != null) {
                explainCurrentBatch();
            }

            if (!mLoadOnly) {
                mPigServer.executeBatch();
                PigStats stats = PigStats.get();
                JobGraph jg = stats.getJobGraph();
                Iterator<JobStats> iter = jg.iterator();
                while (iter.hasNext()) {
                    JobStats js = iter.next();
                    if (!js.isSuccessful()) {
                        mNumFailedJobs++;
                        Exception exp = (js.getException() != null) ? js.getException()
                                : new ExecException(
                                        "Job failed, hadoop does not return any error message",
                                        2244);                        
                        LogUtils.writeLog(exp, 
                                mPigServer.getPigContext().getProperties().getProperty("pig.logfile"), 
                                log, 
                                "true".equalsIgnoreCase(mPigServer.getPigContext().getProperties().getProperty("verbose")),
                                "Pig Stack Trace");
                    } else {
                        mNumSucceededJobs++;
                    }
                }
            }
        }
    }

    private void discardBatch() throws IOException {
        if (mPigServer.isBatchOn()) {
            mPigServer.discardBatch();
        }
    }

    public int[] parseStopOnError() throws IOException, ParseException
    {
	return parseStopOnError(false);
    }
    
    /** 
     * Parses Pig commands in either interactive mode or batch mode. 
     * In interactive mode, executes the plan right away whenever a 
     * STORE command is encountered.
     *
     * @throws IOException, ParseException
     */
    public int[] parseStopOnError(boolean sameBatch) throws IOException, ParseException
    {
        if (mPigServer == null) {
            throw new IllegalStateException();
        }

        if (!mInteractive && !sameBatch) {
            setBatchOn();
        }

        try {
            prompt();
            mDone = false;
            while(!mDone) {
                parse();
            }
            
            if (!sameBatch) {
                executeBatch();
            }
        } finally {
            if (!sameBatch) {
                discardBatch();
            }
        }
        int [] res = { mNumSucceededJobs, mNumFailedJobs };
        return res;
    }

    public void setLoadOnly(boolean loadOnly) 
    {
        mLoadOnly = loadOnly;
    }

    public void setParams(PigServer pigServer)
    {
        mPigServer = pigServer;
        
        mDfs = mPigServer.getPigContext().getDfs();
        mLfs = mPigServer.getPigContext().getLfs();
        mConf = mPigServer.getPigContext().getProperties();
        shell = new FsShell(ConfigurationUtil.toConfiguration(mConf));
        
        // TODO: this violates the abstraction layer decoupling between
        // front end and back end and needs to be changed.
        // Right now I am not clear on how the Job Id comes from to tell
        // the back end to kill a given job (mJobClient is used only in 
        // processKill)
        //
        HExecutionEngine execEngine = mPigServer.getPigContext().getExecutionEngine();
        mJobConf = execEngine.getJobConf();
    }

    public void setScriptIllustrate() {
        mScriptIllustrate = true;
    }
    
    @Override
    public void prompt()
    {
        if (mInteractive) {
            mConsoleReader.setDefaultPrompt("grunt> ");
        }
    }
    
    @Override
    protected void quit()
    {
        mDone = true;
    }

    public boolean isDone() {
        return mDone;
    }

    /*
     * parseOnly method added for supporting penny
     */
    public void parseOnly() throws IOException, ParseException {
        if (mPigServer == null) {
            throw new IllegalStateException();
        }

        mDone = false;
        while(!mDone) {
            parse();
        }
    }    
    
    @Override
    protected void processDescribe(String alias) throws IOException {
        String nestedAlias = null;
        if(mExplain == null) { // process only if not in "explain" mode
            if(alias==null) {
                alias = mPigServer.getPigContext().getLastAlias();
            }
            if(alias.contains("::")) {
                nestedAlias = alias.substring(alias.indexOf("::") + 2);
                alias = alias.substring(0, alias.indexOf("::"));
                mPigServer.dumpSchemaNested(alias, nestedAlias);
            }
            else {
                mPigServer.dumpSchema(alias);
            }
        } else {
            log.warn("'describe' statement is ignored while processing 'explain -script' or '-check'");
        }
    }

    @Override
    protected void processExplain(String alias, String script, boolean isVerbose, 
                                  String format, String target, 
                                  List<String> params, List<String> files) 
    throws IOException, ParseException {
        processExplain(alias, script, isVerbose, format, target, params, files, 
                false);
    }

    protected void processExplain(String alias, String script, boolean isVerbose, 
                                  String format, String target, 
                                  List<String> params, List<String> files,
                                  boolean dontPrintOutput) 
        throws IOException, ParseException {
        
        if (null != mExplain) {
            return;
        }

        try {
            mExplain = new ExplainState(alias, target, script, isVerbose, format);
            
            if (script != null) {
                if (!"true".equalsIgnoreCase(mPigServer.
                                             getPigContext()
                                             .getProperties().
                                             getProperty("opt.multiquery","true"))) {
                    throw new ParseException("Cannot explain script if multiquery is disabled.");
                }
                setBatchOn();
                try {
                    loadScript(script, true, true, false, params, files);
                } catch(IOException e) {
                    discardBatch();
                    throw e;
            } catch (ParseException e) {
                    discardBatch();
                    throw e;
                }
            }

            mExplain.mLast = true;
            explainCurrentBatch(dontPrintOutput);

        } finally {
            if (script != null) {
                discardBatch();
            }
            mExplain = null;
        }
    }

    protected void explainCurrentBatch() throws IOException {
        explainCurrentBatch(false);
    }

    /**
     * A {@link PrintStream} implementation which does not write anything 
     * Used with '-check' command line option to pig Main 
     * (through {@link GruntParser#explainCurrentBatch(boolean) } )
     */
    static class NullPrintStream extends PrintStream {
        public NullPrintStream(String fileName) throws FileNotFoundException {
            super(fileName);
        }
        @Override
        public void write(byte[] buf, int off, int len) {}
        @Override
        public void write(int b) {}
        @Override
        public void write(byte [] b) {}
    }
    
    protected void explainCurrentBatch(boolean dontPrintOutput) throws IOException {
        PrintStream lp = (dontPrintOutput) ? new NullPrintStream("dummy") : System.out;
        PrintStream pp = (dontPrintOutput) ? new NullPrintStream("dummy") : System.out;
        PrintStream ep = (dontPrintOutput) ? new NullPrintStream("dummy") : System.out;
        
        if (!(mExplain.mLast && mExplain.mCount == 0)) {
            if (mPigServer.isBatchEmpty()) {
                return;
            }
        }

        mExplain.mCount++;
        boolean markAsExecuted = (mExplain.mScript != null);

        if (mExplain.mTarget != null) {
            File file = new File(mExplain.mTarget);
            
            if (file.isDirectory()) {
                String sCount = (mExplain.mLast && mExplain.mCount == 1)?"":"_"+mExplain.mCount;
                lp = new PrintStream(new File(file, "logical_plan-"+mExplain.mTime+sCount+"."+mExplain.mFormat));
                pp = new PrintStream(new File(file, "physical_plan-"+mExplain.mTime+sCount+"."+mExplain.mFormat));
                ep = new PrintStream(new File(file, "exec_plan-"+mExplain.mTime+sCount+"."+mExplain.mFormat));
                mPigServer.explain(mExplain.mAlias, mExplain.mFormat, 
                                   mExplain.mVerbose, markAsExecuted, lp, pp, ep);
                lp.close();
                pp.close();
                ep.close();
            }
            else {
                boolean append = !(mExplain.mCount==1);
                lp = pp = ep = new PrintStream(new FileOutputStream(mExplain.mTarget, append));
                mPigServer.explain(mExplain.mAlias, mExplain.mFormat, 
                                   mExplain.mVerbose, markAsExecuted, lp, pp, ep);
                lp.close();
            }
        }
        else {
            mPigServer.explain(mExplain.mAlias, mExplain.mFormat, 
                               mExplain.mVerbose, markAsExecuted, lp, pp, ep);
        }
    }

    @Override
    protected void printAliases() throws IOException {
        if(mExplain == null) { // process only if not in "explain" mode
            mPigServer.printAliases();
        } else {
            log.warn("'aliases' statement is ignored while processing 'explain -script' or '-check'");
        }
    }
    
    @Override
    protected void processRegister(String jar) throws IOException {
        mPigServer.registerJar(jar);
    }
    
    @Override
    protected void processRegister(String path, String scriptingLang, String namespace) throws IOException, ParseException {
        if(path.endsWith(".jar")) {
            if(scriptingLang != null || namespace != null) {
                throw new ParseException("Cannot register a jar with a scripting language or namespace");
            }
            mPigServer.registerJar(path);
        }
        else {
            mPigServer.registerCode(path, scriptingLang, namespace);
        }
    }    

    private String runPreprocessor(String script, List<String> params, 
                                   List<String> files) 
        throws IOException, ParseException {

        ParameterSubstitutionPreprocessor psp = new ParameterSubstitutionPreprocessor(50);
        StringWriter writer = new StringWriter();

        try{
            psp.genSubstitutedFile(new BufferedReader(new FileReader(script)), 
                                   writer,  
                                   params.size() > 0 ? params.toArray(new String[0]) : null, 
                                   files.size() > 0 ? files.toArray(new String[0]) : null);
        } catch (org.apache.pig.tools.parameters.ParseException pex) {
            throw new ParseException(pex.getMessage());
        }

        return writer.toString();
    }

    @Override
    protected void processScript(String script, boolean batch, 
                                 List<String> params, List<String> files) 
        throws IOException, ParseException {
        
        if(mExplain == null) { // process only if not in "explain" mode
            if (script == null) {
                executeBatch();
                return;
            }
            
            if (batch) {
                setBatchOn();
                mPigServer.setJobName(script);
                try {
                    loadScript(script, true, false, mLoadOnly, params, files);
                    executeBatch();
                } finally {
                    discardBatch();
                }
            } else {
                loadScript(script, false, false, mLoadOnly, params, files);
            }
        } else {
            log.warn("'run/exec' statement is ignored while processing 'explain -script' or '-check'");
        }
    }

    private void loadScript(String script, boolean batch, boolean loadOnly, boolean illustrate,
                            List<String> params, List<String> files) 
        throws IOException, ParseException {
        
        Reader inputReader;
        ConsoleReader reader;
        boolean interactive;
         
        try {
            FetchFileRet fetchFile = FileLocalizer.fetchFile(mConf, script);
            String cmds = runPreprocessor(fetchFile.file.getAbsolutePath(), params, files);

            if (mInteractive && !batch) { // Write prompt and echo commands
                // Console reader treats tabs in a special way
                cmds = cmds.replaceAll("\t","    ");

                reader = new ConsoleReader(new ByteArrayInputStream(cmds.getBytes()),
                                           new OutputStreamWriter(System.out));
                reader.setHistory(mConsoleReader.getHistory());
                InputStream in = new ConsoleReaderInputStream(reader);
                inputReader = new BufferedReader(new InputStreamReader(in));
                interactive = true;
            } else { // Quietly parse the statements
                inputReader = new StringReader(cmds);
                reader = null;
                interactive = false;
            }
        } catch (FileNotFoundException fnfe) {
            throw new ParseException("File not found: " + script);
        } catch (SecurityException se) {
            throw new ParseException("Cannot access file: " + script);
        }

        GruntParser parser = new GruntParser(inputReader);
        parser.setParams(mPigServer);
        parser.setConsoleReader(reader);
        parser.setInteractive(interactive);
        parser.setLoadOnly(loadOnly);
        if (illustrate)
            parser.setScriptIllustrate();
        parser.mExplain = mExplain;
        
        parser.prompt();
        while(!parser.isDone()) {
            parser.parse();
        }

        if (interactive) {
            System.out.println("");
        }
    }

    @Override
    protected void processSet(String key, String value) throws IOException, ParseException {
        if (key.equals("debug"))
        {
            if (value.equals("on"))
                mPigServer.debugOn();
            else if (value.equals("off"))
                mPigServer.debugOff();
            else
                throw new ParseException("Invalid value " + value + " provided for " + key);
        }
        else if (key.equals("job.name"))
        {
            mPigServer.setJobName(value);
        }
        else if (key.equals("job.priority"))
        {
            mPigServer.setJobPriority(value);
        }
        else if (key.equals("stream.skippath")) {
            // Validate
            File file = new File(value);
            if (!file.exists() || file.isDirectory()) {
                throw new IOException("Invalid value for stream.skippath:" + 
                                      value); 
            }
            mPigServer.addPathToSkip(value);
        }
        else if (key.equals("default_parallel")) {
            // Validate
            try {
                mPigServer.setDefaultParallel(Integer.parseInt(value));
            } catch (NumberFormatException e) {
                throw new ParseException("Invalid value for default_parallel");
            }
        }
        else
        {
            //mPigServer.getPigContext().getProperties().setProperty(key, value);
            // PIG-2508 properties need to be managed through JobConf
            // since all other code depends on access to properties, 
            // we need to re-populate from updated JobConf 
            //java.util.HashSet<?> keysBefore = new java.util.HashSet<Object>(mPigServer.getPigContext().getProperties().keySet());        	
            // set current properties on jobConf
            Properties properties = mPigServer.getPigContext().getProperties();
            Configuration jobConf = mPigServer.getPigContext().getExecutionEngine().getJobConf();
            Enumeration<Object> propertiesIter = properties.keys();
            while (propertiesIter.hasMoreElements()) {
                String pkey = (String) propertiesIter.nextElement();
                String val = properties.getProperty(pkey);
                // We do not put user.name, See PIG-1419
                if (!pkey.equals("user.name"))
                   jobConf.set(pkey, val);
            }
            // set new value, JobConf will handle deprecation etc.
            jobConf.set(key, value);
            // re-initialize to reflect updated JobConf
            properties.clear();
            Iterator<Map.Entry<String, String>> iter = jobConf.iterator();
            while (iter.hasNext()) {
                Map.Entry<String, String> entry = iter.next();
                properties.put(entry.getKey(), entry.getValue());
            } 
            //keysBefore.removeAll(mPigServer.getPigContext().getProperties().keySet());
            //log.info("PIG-2508: keys dropped from properties: " + keysBefore);
        }
    }
    
    @Override
    protected void processCat(String path) throws IOException
    {
        if(mExplain == null) { // process only if not in "explain" mode
            
            executeBatch();

            try {
                byte buffer[] = new byte[65536];
                ElementDescriptor dfsPath = mDfs.asElement(path);
                int rc;
                
                if (!dfsPath.exists())
                    throw new IOException("Directory " + path + " does not exist.");
        
                if (mDfs.isContainer(path)) {
                    ContainerDescriptor dfsDir = (ContainerDescriptor) dfsPath;
                    Iterator<ElementDescriptor> paths = dfsDir.iterator();
                    
                    while (paths.hasNext()) {
                        ElementDescriptor curElem = paths.next();
                        
                        if (mDfs.isContainer(curElem.toString())) {
                            continue;
                        }
                        
                        InputStream is = curElem.open();
                        while ((rc = is.read(buffer)) > 0) {
                            System.out.write(buffer, 0, rc);
                        }
                        is.close();                
                    }
                }
                else {
                    InputStream is = dfsPath.open();
                    while ((rc = is.read(buffer)) > 0) {
                        System.out.write(buffer, 0, rc);
                    }
                    is.close();            
                }
            }
            catch (DataStorageException e) {
                throw new IOException("Failed to Cat: " + path, e);
            }
        } else {
            log.warn("'cat' statement is ignored while processing 'explain -script' or '-check'");
        }
    }

    @Override
    protected void processCD(String path) throws IOException
    {    
        ContainerDescriptor container;
        if(mExplain == null) { // process only if not in "explain" mode
            try {
                if (path == null) {
                    container = mDfs.asContainer(((HDataStorage)mDfs).getHFS().getHomeDirectory().toString());
                    mDfs.setActiveContainer(container);
                }
                else
                {
                    container = mDfs.asContainer(path);
        
                    if (!container.exists()) {
                        throw new IOException("Directory " + path + " does not exist.");
                    }
                    
                    if (!mDfs.isContainer(path)) {
                        throw new IOException(path + " is not a directory.");
                    }
                    
                    mDfs.setActiveContainer(container);
                }
            }
            catch (DataStorageException e) {
                throw new IOException("Failed to change working directory to " + 
                                      ((path == null) ? (((HDataStorage)mDfs).getHFS().getHomeDirectory().toString()) 
                                                         : (path)), e);
            }
        } else {
            log.warn("'cd' statement is ignored while processing 'explain -script' or '-check'");
        }
    }

    @Override
    protected void processDump(String alias) throws IOException
    {
        if(mExplain == null) { // process only if not in "explain" mode
        	executeBatch();
            Iterator<Tuple> result = mPigServer.openIterator(alias);
            while (result.hasNext())
            {
                Tuple t = result.next();
                System.out.println(TupleFormat.format(t));
            }
        } else {
            log.warn("'dump' statement is ignored while processing 'explain -script' or '-check'");
        }
    }
    
    @Override
    protected void processIllustrate(String alias, String script, String target, List<String> params, List<String> files) throws IOException, ParseException
    {
        if (mScriptIllustrate)
            throw new ParseException("'illustrate' statement can not appear in a script that is illustrated opon.");

        if ((alias != null) && (script != null))
            throw new ParseException("'illustrate' statement on an alias does not work when a script is in effect");
        else if (mExplain != null)
            log.warn("'illustrate' statement is ignored while processing 'explain -script' or '-check'");
        else {
            try {
                if (script != null) {
                    if (!"true".equalsIgnoreCase(mPigServer.
                                                 getPigContext()
                                                 .getProperties().
                                                 getProperty("opt.multiquery","true"))) {
                        throw new ParseException("Cannot explain script if multiquery is disabled.");
                    }
                    setBatchOn();
                    try {
                        loadScript(script, true, true, true, params, files);
                    } catch(IOException e) {
                        discardBatch();
                        throw e;
                    } catch (ParseException e) {
                        discardBatch();
                        throw e;
                    }
                } else if (alias == null) {
                    throw new ParseException("'illustrate' statement must be on an alias or on a script.");
                }
                mPigServer.getExamples(alias);
            } finally {
                if (script != null) {
                    discardBatch();
                }
            }
        }
    }

    @Override
    protected void processKill(String jobid) throws IOException
    {
        if (mJobConf != null) {
            JobClient jc = new JobClient(mJobConf);
            JobID id = JobID.forName(jobid);
            RunningJob job = jc.getJob(id);
            if (job == null)
                System.out.println("Job with id " + jobid + " is not active");
            else
            {    
                job.killJob();
                log.info("Kill " + id + " submitted.");
            }
        }
    }
        
    @Override
    protected void processLS(String path) throws IOException
    {
        if(mExplain == null) { // process only if not in "explain" mode
            try {
                ElementDescriptor pathDescriptor;
                
                if (path == null) {
                    pathDescriptor = mDfs.getActiveContainer();
                }
                else {
                    pathDescriptor = mDfs.asElement(path);
                }
    
                if (!pathDescriptor.exists()) {
                    throw new IOException("File or directory " + path + " does not exist.");                
                }
                
                if (mDfs.isContainer(pathDescriptor.toString())) {
                    ContainerDescriptor container = (ContainerDescriptor) pathDescriptor;
                    Iterator<ElementDescriptor> elems = container.iterator();
                    
                    while (elems.hasNext()) {
                        ElementDescriptor curElem = elems.next();
                        
                        if (mDfs.isContainer(curElem.toString())) {
                               System.out.println(curElem.toString() + "\t<dir>");
                        } else {
                            printLengthAndReplication(curElem);
                        }
                    }
                } else {
                    printLengthAndReplication(pathDescriptor);
                }
            }
            catch (DataStorageException e) {
                throw new IOException("Failed to LS on " + path, e);
            }
        } else {
            log.warn("'ls' statement is ignored while processing 'explain -script' or '-check'");
        }
    }

    private void printLengthAndReplication(ElementDescriptor elem)
            throws IOException {
        Map<String, Object> stats = elem.getStatistics();

        long replication = (Short) stats
                .get(ElementDescriptor.BLOCK_REPLICATION_KEY);
        long len = (Long) stats.get(ElementDescriptor.LENGTH_KEY);

        System.out.println(elem.toString() + "<r " + replication + ">\t" + len);
    }
    
    @Override
    protected void processPWD() throws IOException 
    {
        if(mExplain == null) { // process only if not in "explain" mode
            System.out.println(mDfs.getActiveContainer().toString());
        } else {
            log.warn("'pwd' statement is ignored while processing 'explain -script' or '-check'");
        }
    }
    
    @Override
	protected void processHistory(boolean withNumbers) {
    	mPigServer.printHistory(withNumbers);
    }
    
    @Override
    protected void printHelp() 
    {
        System.out.println("Commands:");
        System.out.println("<pig latin statement>; - See the PigLatin manual for details: http://hadoop.apache.org/pig");
        System.out.println("File system commands:");
	System.out.println("    fs <fs arguments> - Equivalent to Hadoop dfs command: http://hadoop.apache.org/common/docs/current/hdfs_shell.html");	
        System.out.println("Diagnostic commands:");
        System.out.println("    describe <alias>[::<alias] - Show the schema for the alias. Inner aliases can be described as A::B.");
        System.out.println("    explain [-script <pigscript>] [-out <path>] [-brief] [-dot] [-param <param_name>=<param_value>]");
        System.out.println("        [-param_file <file_name>] [<alias>] - Show the execution plan to compute the alias or for entire script.");
        System.out.println("        -script - Explain the entire script.");
        System.out.println("        -out - Store the output into directory rather than print to stdout.");
        System.out.println("        -brief - Don't expand nested plans (presenting a smaller graph for overview).");
        System.out.println("        -dot - Generate the output in .dot format. Default is text format.");
        System.out.println("        -param <param_name - See parameter substitution for details.");
        System.out.println("        -param_file <file_name> - See parameter substitution for details.");
        System.out.println("        alias - Alias to explain.");
        System.out.println("    dump <alias> - Compute the alias and writes the results to stdout.");
        System.out.println("Utility Commands:");
        System.out.println("    exec [-param <param_name>=param_value] [-param_file <file_name>] <script> - ");    
        System.out.println("        Execute the script with access to grunt environment including aliases.");
        System.out.println("        -param <param_name - See parameter substitution for details.");
        System.out.println("        -param_file <file_name> - See parameter substitution for details.");
        System.out.println("        script - Script to be executed.");
        System.out.println("    run [-param <param_name>=param_value] [-param_file <file_name>] <script> - ");    
        System.out.println("        Execute the script with access to grunt environment. ");
        System.out.println("        -param <param_name - See parameter substitution for details.");
        System.out.println("        -param_file <file_name> - See parameter substitution for details.");
        System.out.println("        script - Script to be executed.");
        System.out.println("    sh  <shell command> - Invoke a shell command."); 
        System.out.println("    kill <job_id> - Kill the hadoop job specified by the hadoop job id.");
        System.out.println("    set <key> <value> - Provide execution parameters to Pig. Keys and values are case sensitive.");
        System.out.println("        The following keys are supported: ");
        System.out.println("        default_parallel - Script-level reduce parallelism. Basic input size heuristics used by default.");
        System.out.println("        debug - Set debug on or off. Default is off.");
        System.out.println("        job.name - Single-quoted name for jobs. Default is PigLatin:<script name>");
        System.out.println("        job.priority - Priority for jobs. Values: very_low, low, normal, high, very_high. Default is normal");
        System.out.println("        stream.skippath - String that contains the path. This is used by streaming.");
        System.out.println("        any hadoop property.");
        System.out.println("    help - Display this message.");
        System.out.println("    history [-n] - Display the list statements in cache.");
        System.out.println("        -n Hide line numbers. ");
        System.out.println("    quit - Quit the grunt shell.");
    }

    @Override
    protected void processMove(String src, String dst) throws IOException
    {
        if(mExplain == null) { // process only if not in "explain" mode

            executeBatch();
        
            try {
                ElementDescriptor srcPath = mDfs.asElement(src);
                ElementDescriptor dstPath = mDfs.asElement(dst);
                
                if (!srcPath.exists()) {
                    throw new IOException("File or directory " + src + " does not exist.");                
                }
                
                srcPath.rename(dstPath);
            }
            catch (DataStorageException e) {
                throw new IOException("Failed to move " + src + " to " + dst, e);
            }
        } else {
            log.warn("'mv' statement is ignored while processing 'explain -script' or '-check'");
        }
    }
    
    @Override
    protected void processCopy(String src, String dst) throws IOException
    {
        if(mExplain == null) { // process only if not in "explain" mode

            executeBatch();
        
            try {
                ElementDescriptor srcPath = mDfs.asElement(src);
                ElementDescriptor dstPath = mDfs.asElement(dst);
                
                srcPath.copy(dstPath, mConf, false);
            }
            catch (DataStorageException e) {
                throw new IOException("Failed to copy " + src + " to " + dst, e);
            }
        } else {
            log.warn("'cp' statement is ignored while processing 'explain -script' or '-check'");
        }
    }
    
    @Override
    protected void processCopyToLocal(String src, String dst) throws IOException
    {
        if(mExplain == null) { // process only if not in "explain" mode
            
            executeBatch();
        
            try {
                ElementDescriptor srcPath = mDfs.asElement(src);
                ElementDescriptor dstPath = mLfs.asElement(dst);
                
                srcPath.copy(dstPath, false);
            }
            catch (DataStorageException e) {
                throw new IOException("Failed to copy " + src + "to (locally) " + dst, e);
            }
        } else {
            log.warn("'copyToLocal' statement is ignored while processing 'explain -script' or '-check'");
        }
    }

    @Override
    protected void processCopyFromLocal(String src, String dst) throws IOException
    {
        if(mExplain == null) { // process only if not in "explain" mode
            
            executeBatch();
        
            try {
                ElementDescriptor srcPath = mLfs.asElement(src);
                ElementDescriptor dstPath = mDfs.asElement(dst);
                
                srcPath.copy(dstPath, false);
            }
            catch (DataStorageException e) {
                throw new IOException("Failed to copy (loally) " + src + "to " + dst, e);
            }
        } else {
            log.warn("'copyFromLocal' statement is ignored while processing 'explain -script' or '-check'");
        }
    }
    
    @Override
    protected void processMkdir(String dir) throws IOException
    {
        if(mExplain == null) { // process only if not in "explain" mode
            ContainerDescriptor dirDescriptor = mDfs.asContainer(dir);
            dirDescriptor.create();
        } else {
            log.warn("'mkdir' statement is ignored while processing 'explain -script' or '-check'");
        }
    }
    
    @Override
    protected void processPig(String cmd) throws IOException
    {
        int start = 1;
        if (!mInteractive) {
            start = getLineNumber();
        }
        
        if (cmd.charAt(cmd.length() - 1) != ';') {
            mPigServer.registerQuery(cmd + ";", start);
        }
        else { 
            mPigServer.registerQuery(cmd, start);
        }
    }

    @Override
    protected void processRemove(String path, String options ) throws IOException
    {
        if(mExplain == null) { // process only if not in "explain" mode

            ElementDescriptor dfsPath = mDfs.asElement(path);    
            executeBatch();
        
            if (!dfsPath.exists()) {
                if (options == null || !options.equalsIgnoreCase("force")) {
                    throw new IOException("File or directory " + path + " does not exist."); 
                }
            }
            else {
                
                dfsPath.delete();
            }
        } else {
            log.warn("'rm/rmf' statement is ignored while processing 'explain -script' or '-check'");
        }
    }

    @Override
    protected void processFsCommand(String[] cmdTokens) throws IOException{
        if(mExplain == null) { // process only if not in "explain" mode
            
            executeBatch();
            
            int retCode = -1;
            
            try {
                retCode = shell.run(cmdTokens);
            } catch (Exception e) {
                throw new IOException(e);
            }
            
            if (retCode != 0 && !mInteractive) {
                String s = LoadFunc.join(
                        (AbstractList<String>) Arrays.asList(cmdTokens), " ");
                throw new IOException("fs command '" + s
                        + "' failed. Please check output logs for details");
            }
        } else {
            log.warn("'fs' statement is ignored while processing 'explain -script' or '-check'");
        }
    }
    
    @Override
    protected void processShCommand(String[] cmdTokens) throws IOException{
        if(mExplain == null) { // process only if not in "explain" mode
            try {
                executeBatch();
                
                Process executor = Runtime.getRuntime().exec(cmdTokens);
                StreamPrinter outPrinter = new StreamPrinter(executor.getInputStream(), null, System.out);
                StreamPrinter errPrinter = new StreamPrinter(executor.getErrorStream(), null, System.err);
    
                outPrinter.start();
                errPrinter.start();
    
                int ret = executor.waitFor();
                outPrinter.join();
                errPrinter.join();
                if (ret != 0 && !mInteractive) {
                    String s = LoadFunc.join(
                            (AbstractList<String>) Arrays.asList(cmdTokens), " ");
                    throw new IOException("sh command '" + s
                            + "' failed. Please check output logs for details");
                }
            } catch (Exception e) {
                throw new IOException(e);
            }
        } else {
            log.warn("'sh' statement is ignored while processing 'explain -script' or '-check'");
        }
    }
    
    public static int runSQLCommand(String hcatBin, String cmd, boolean mInteractive) throws IOException {
        String[] tokens = new String[3];
        tokens[0] = hcatBin;
        tokens[1] = "-e";
        tokens[2] = cmd.substring(cmd.indexOf("sql")).substring(4);
        
        // create new environment = environment - HADOOP_CLASSPATH
        // This is because of antlr version conflict between Pig and Hive
        Map<String, String> envs = System.getenv();
        Set<String> envSet = new HashSet<String>();
        for (Map.Entry<String, String> entry : envs.entrySet()) {
            if (!entry.getKey().equals("HADOOP_CLASSPATH")) {
                envSet.add(entry.getKey() + "=" + entry.getValue());
            }
        }
        
        log.info("Going to run hcat command: " + tokens[2]);
        Process executor = Runtime.getRuntime().exec(tokens, envSet.toArray(new String[0]));
        StreamPrinter outPrinter = new StreamPrinter(executor.getInputStream(), null, System.out);
        StreamPrinter errPrinter = new StreamPrinter(executor.getErrorStream(), null, System.err);

        outPrinter.start();
        errPrinter.start();
        
        int ret;
        try {
            ret = executor.waitFor();
            
            outPrinter.join();
            errPrinter.join();
            if (ret != 0 && !mInteractive) {
                throw new IOException("sql command '" + cmd
                        + "' failed. ");
            }
        } catch (InterruptedException e) {
            log.warn("Exception raised from sql command " + e.getLocalizedMessage());
        }
        return 0;
    }
    
    @Override
    protected void processSQLCommand(String cmd) throws IOException{
        if(mExplain == null) { // process only if not in "explain" mode
            if (!mPigServer.getPigContext().getProperties().get("pig.sql.type").equals("hcat")) {
                throw new IOException("sql command only support hcat currently");
            }
            if (mPigServer.getPigContext().getProperties().get("hcat.bin")==null) {
                throw new IOException("hcat.bin is not defined. Define it to be your hcat script (Usually $HCAT_HOME/bin/hcat");
            }
            String hcatBin = (String)mPigServer.getPigContext().getProperties().get("hcat.bin");
            if (new File("hcat.bin").exists()) {
                throw new IOException(hcatBin + " does not exist. Please check your 'hcat.bin' setting in pig.properties.");
            }
            executeBatch();
            runSQLCommand(hcatBin, cmd, mInteractive);
        } else {
            log.warn("'sql' statement is ignored while processing 'explain -script' or '-check'");
        }
    }
    
    /**
     * StreamPrinter.
     *
     */
    public static class StreamPrinter extends Thread {
    	InputStream is;
	    String type;
	    PrintStream os;

	    public StreamPrinter(InputStream is, String type, PrintStream os) {
	    	this.is = is;
	    	this.type = type;
	    	this.os = os;
	    }

	    @Override
	    public void run() {
	        try {
	        	InputStreamReader isr = new InputStreamReader(is);
	        	BufferedReader br = new BufferedReader(isr);
	        	String line = null;
	        	if (type != null) {
		            while ((line = br.readLine()) != null) {
		            	os.println(type + ">" + line);
		            }
	        	} else {
	        		while ((line = br.readLine()) != null) {
	        			os.println(line);
	        		}
	        	}
	        } catch (IOException ioe) {
	        	ioe.printStackTrace();
	        }
	    }
    }
    
    private static class ExplainState {
        public long mTime;
        public int mCount;
        public String mAlias;
        public String mTarget;
        public String mScript;
        public boolean mVerbose;
        public String mFormat;
        public boolean mLast;

        public ExplainState(String alias, String target, String script,
                            boolean verbose, String format) {
            mTime = new Date().getTime();
            mCount = 0;
            mAlias = alias;
            mTarget = target;
            mScript = script;
            mVerbose = verbose;
            mFormat = format;
            mLast = false;
        }
    }        

    private PigServer mPigServer;
    private DataStorage mDfs;
    private DataStorage mLfs;
    private Properties mConf;
    private JobConf mJobConf;
    private boolean mDone;
    private boolean mLoadOnly;
    private ExplainState mExplain;
    private int mNumFailedJobs;
    private int mNumSucceededJobs;
    private FsShell shell;
    private boolean mScriptIllustrate;
    
}

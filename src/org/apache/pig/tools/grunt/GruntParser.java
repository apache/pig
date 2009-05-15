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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.FileReader;
import java.io.FileInputStream;
import java.io.OutputStreamWriter;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.FileNotFoundException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.Iterator;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Date;
import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
import java.io.PrintStream;

import jline.ConsoleReader;
import jline.ConsoleReaderInputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.JobID;
import org.apache.pig.FuncSpec;
import org.apache.pig.PigServer;
import org.apache.pig.backend.datastorage.ContainerDescriptor;
import org.apache.pig.backend.datastorage.DataStorage;
import org.apache.pig.backend.datastorage.DataStorageException;
import org.apache.pig.backend.datastorage.ElementDescriptor;
import org.apache.pig.backend.executionengine.ExecutionEngine;
import org.apache.pig.backend.hadoop.executionengine.HExecutionEngine;
import org.apache.pig.backend.executionengine.ExecJob;
import org.apache.pig.backend.executionengine.ExecJob.JOB_STATUS;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.WrappedIOException;
import org.apache.pig.tools.pigscript.parser.ParseException;
import org.apache.pig.tools.pigscript.parser.PigScriptParser;
import org.apache.pig.tools.pigscript.parser.PigScriptParserTokenManager;
import org.apache.pig.tools.parameters.ParameterSubstitutionPreprocessor;
import org.apache.pig.impl.util.LogUtils;

public class GruntParser extends PigScriptParser {

    private final Log log = LogFactory.getLog(getClass());

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
                List<ExecJob> jobs = mPigServer.executeBatch();
                for(ExecJob job: jobs) {
                    if (job.getStatus() == ExecJob.JOB_STATUS.FAILED) {
                        mNumFailedJobs++;
                        if (job.getException() != null) {
                            LogUtils.writeLog(
                              job.getException(), 
                              mPigServer.getPigContext().getProperties().getProperty("pig.logfile"), 
                              log, 
                              "true".equalsIgnoreCase(mPigServer.getPigContext().getProperties().getProperty("verbose")));
                        }
                    }
                    else {
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
        } 
        finally {
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
        
        // TODO: this violates the abstraction layer decoupling between
        // front end and back end and needs to be changed.
        // Right now I am not clear on how the Job Id comes from to tell
        // the back end to kill a given job (mJobClient is used only in 
        // processKill)
        //
        ExecutionEngine execEngine = mPigServer.getPigContext().getExecutionEngine();
        if (execEngine instanceof HExecutionEngine) {
            mJobClient = ((HExecutionEngine)execEngine).getJobClient();
        }
        else {
            mJobClient = null;
        }
    }

    public void prompt()
    {
        if (mInteractive) {
            mConsoleReader.setDefaultPrompt("grunt> ");
        }
    }
    
    protected void quit()
    {
        mDone = true;
    }

    public boolean isDone() {
        return mDone;
    }
    
    protected void processDescribe(String alias) throws IOException {
        if(alias==null) {
            alias = mPigServer.getPigContext().getLastAlias();
        }
        mPigServer.dumpSchema(alias);
    }

    protected void processExplain(String alias, String script, boolean isVerbose, 
                                  String format, String target, 
                                  List<String> params, List<String> files) 
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
                    loadScript(script, true, true, params, files);
                } catch(IOException e) {
                    discardBatch();
                    throw e;
            } catch (ParseException e) {
                    discardBatch();
                    throw e;
                }
            }

            mExplain.mLast = true;
            explainCurrentBatch();

        } finally {
            if (script != null) {
                discardBatch();
            }
            mExplain = null;
        }
    }

    protected void explainCurrentBatch() throws IOException {
        PrintStream lp = System.out;
        PrintStream pp = System.out;
        PrintStream ep = System.out;
        
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

    protected void printAliases() throws IOException {
        mPigServer.printAliases();
    }
    
    protected void processRegister(String jar) throws IOException {
        mPigServer.registerJar(jar);
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

    protected void processScript(String script, boolean batch, 
                                 List<String> params, List<String> files) 
        throws IOException, ParseException {
        
        if (script == null) {
            executeBatch();
            return;
        }
        
        if (batch) {
            setBatchOn();
            mPigServer.setJobName(script);
            try {
                loadScript(script, true, mLoadOnly, params, files);
                executeBatch();
            } finally {
                discardBatch();
            }
        } else {
            loadScript(script, false, mLoadOnly, params, files);
        }
    }

    private void loadScript(String script, boolean batch, boolean loadOnly,
                            List<String> params, List<String> files) 
        throws IOException, ParseException {
        
        Reader inputReader;
        ConsoleReader reader;
        boolean interactive;
         
        try {
            String cmds = runPreprocessor(script, params, files);

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
        parser.mExplain = mExplain;
        
        parser.prompt();
        while(!parser.isDone()) {
            parser.parse();
        }

        if (interactive) {
            System.out.println("");
        }
    }

    protected void processSet(String key, String value) throws IOException, ParseException {
        if (key.equals("debug"))
        {
            if (value.equals("on") || value.equals("'on'"))
                mPigServer.debugOn();
            else if (value.equals("off") || value.equals("'off'"))
                mPigServer.debugOff();
            else
                throw new ParseException("Invalid value " + value + " provided for " + key);
        }
        else if (key.equals("job.name"))
        {
            mPigServer.setJobName(value);
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
        else
        {
            // other key-value pairs can go there
            // for now just throw exception since we don't support
            // anything else
            throw new ParseException("Unrecognized set key: " + key);
        }
    }
    
    protected void processCat(String path) throws IOException
    {
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
            throw WrappedIOException.wrap("Failed to Cat: " + path, e);
        }
    }

    protected void processCD(String path) throws IOException
    {    
        ContainerDescriptor container;

        try {
            if (path == null) {
                container = mDfs.asContainer("/user/" + System.getProperty("user.name"));
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
            throw WrappedIOException.wrap("Failed to change working directory to " + 
                                  ((path == null) ? ("/user/" + System.getProperty("user.name")) 
                                                     : (path)), e);
        }
    }

    protected void processDump(String alias) throws IOException
    {
        Iterator<Tuple> result = mPigServer.openIterator(alias);
        while (result.hasNext())
        {
            Tuple t = result.next();
            System.out.println(t);
        }
    }
    
    protected void processIllustrate(String alias) throws IOException
    {
	mPigServer.getExamples(alias);
    }

    protected void processKill(String jobid) throws IOException
    {
        if (mJobClient != null) {
            JobID id = JobID.forName(jobid);
            RunningJob job = mJobClient.getJob(id);
            if (job == null)
                System.out.println("Job with id " + jobid + " is not active");
            else
            {    
                job.killJob();
                log.error("kill submitted.");
            }
        }
    }
        
    protected void processLS(String path) throws IOException
    {
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
            throw WrappedIOException.wrap("Failed to LS on " + path, e);
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
    
    protected void processPWD() throws IOException 
    {
        System.out.println(mDfs.getActiveContainer().toString());
    }

    protected void printHelp() 
    {
        System.out.println("Commands:");
        System.out.println("<pig latin statement>;");
        System.out.println("store <alias> into <filename> [using <functionSpec>]");
        System.out.println("dump <alias>");
        System.out.println("describe <alias>");
        System.out.println("kill <job_id>");
        System.out.println("ls <path>\r\ndu <path>\r\nmv <src> <dst>\r\ncp <src> <dst>\r\nrm <src>");
        System.out.println("copyFromLocal <localsrc> <dst>\r\ncd <dir>\r\npwd");
        System.out.println("cat <src>\r\ncopyToLocal <src> <localdst>\r\nmkdir <path>");
        System.out.println("cd <path>");
        System.out.println("define <functionAlias> <functionSpec>");
        System.out.println("register <udfJar>");
        System.out.println("set key value");
        System.out.println("quit");
    }

    protected void processMove(String src, String dst) throws IOException
    {
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
            throw WrappedIOException.wrap("Failed to move " + src + " to " + dst, e);
        }
    }
    
    protected void processCopy(String src, String dst) throws IOException
    {
        executeBatch();

        try {
            ElementDescriptor srcPath = mDfs.asElement(src);
            ElementDescriptor dstPath = mDfs.asElement(dst);
            
            srcPath.copy(dstPath, mConf, false);
        }
        catch (DataStorageException e) {
            throw WrappedIOException.wrap("Failed to copy " + src + " to " + dst, e);
        }
    }
    
    protected void processCopyToLocal(String src, String dst) throws IOException
    {
        executeBatch();

        try {
            ElementDescriptor srcPath = mDfs.asElement(src);
            ElementDescriptor dstPath = mLfs.asElement(dst);
            
            srcPath.copy(dstPath, false);
        }
        catch (DataStorageException e) {
            throw WrappedIOException.wrap("Failed to copy " + src + "to (locally) " + dst, e);
        }
    }

    protected void processCopyFromLocal(String src, String dst) throws IOException
    {
        executeBatch();

        try {
            ElementDescriptor srcPath = mLfs.asElement(src);
            ElementDescriptor dstPath = mDfs.asElement(dst);
            
            srcPath.copy(dstPath, false);
        }
        catch (DataStorageException e) {
            throw WrappedIOException.wrap("Failed to copy (loally) " + src + "to " + dst, e);
        }
    }
    
    protected void processMkdir(String dir) throws IOException
    {
        ContainerDescriptor dirDescriptor = mDfs.asContainer(dir);
        dirDescriptor.create();
    }
    
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

    protected void processRemove(String path, String options ) throws IOException
    {
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
    }

    private class ExplainState {
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
    private JobClient mJobClient;
    private boolean mDone;
    private boolean mLoadOnly;
    private ExplainState mExplain;
    private int mNumFailedJobs;
    private int mNumSucceededJobs;
}

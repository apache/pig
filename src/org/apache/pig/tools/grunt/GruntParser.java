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
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.pig.LoadFunc;
import org.apache.pig.PigServer;
import org.apache.pig.backend.datastorage.ContainerDescriptor;
import org.apache.pig.backend.datastorage.DataStorage;
import org.apache.pig.backend.datastorage.DataStorageException;
import org.apache.pig.backend.datastorage.ElementDescriptor;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.backend.hadoop.datastorage.HDataStorage;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.io.FileLocalizer.FetchFileRet;
import org.apache.pig.impl.util.LogUtils;
import org.apache.pig.impl.util.TupleFormat;
import org.apache.pig.tools.pigscript.parser.ParseException;
import org.apache.pig.tools.pigscript.parser.PigScriptParser;
import org.apache.pig.tools.pigscript.parser.PigScriptParserTokenManager;
import org.apache.pig.tools.pigscript.parser.TokenMgrError;
import org.apache.pig.tools.pigstats.JobStats;
import org.apache.pig.tools.pigstats.PigStats;
import org.apache.pig.tools.pigstats.PigStats.JobGraph;
import org.apache.pig.validator.BlackAndWhitelistFilter;
import org.apache.pig.validator.PigCommandFilter;
import org.fusesource.jansi.Ansi;
import org.fusesource.jansi.AnsiConsole;

import com.google.common.collect.Lists;

public class GruntParser extends PigScriptParser {

    private static final Log log = LogFactory.getLog(GruntParser.class);
    private PigCommandFilter filter;

    public GruntParser(Reader reader) {
        this(reader, null);
        init();
    }

    public GruntParser(Reader reader, PigServer pigServer) {
        super(reader);
        mPigServer = pigServer;
        init();
    }

    public GruntParser(InputStream stream, String encoding) {
        this(stream, encoding, null);
    }

    public GruntParser(InputStream stream, String encoding, PigServer pigServer) {
        super(stream, encoding);
        mPigServer = pigServer;
        init();
    }

    public GruntParser(InputStream stream) {
        super(stream);
        init();
    }

    public GruntParser(InputStream stream, PigServer pigServer) {
        super(stream);
        mPigServer = pigServer;
        init();
    }

    public GruntParser(PigScriptParserTokenManager tm) {
        this(tm, null);
    }

    public GruntParser(PigScriptParserTokenManager tm, PigServer pigServer) {
        super(tm);
        mPigServer = pigServer;
        init();
    }

    private void init() {
        mDone = false;
        mLoadOnly = false;
        mExplain = null;
        mScriptIllustrate = false;

        setProps();

        filter = new BlackAndWhitelistFilter(mPigServer);
    }

    private void setProps() {
        mDfs = mPigServer.getPigContext().getDfs();
        mLfs = mPigServer.getPigContext().getLfs();
        mConf = mPigServer.getPigContext().getProperties();
        shell = new FsShell(ConfigurationUtil.toConfiguration(mConf));
    }

    @Override
    public void setInteractive(boolean isInteractive) {
        super.setInteractive(isInteractive);
        if(isInteractive) {
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
                                        "Job " + (js.getJobId() == null ? "" : js.getJobId() + " ") +
                                        "failed, hadoop does not return any error message",
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
            mPigServer.setSkipParseInRegisterForBatch(true);
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
        } catch (TokenMgrError tme) {
            // This error from PigScriptParserTokenManager is not intuitive and always refers to the
            // last line, if we are reading whole query without parsing it line by line during batch
            // So executeBatch and get the error from QueryParser
            if (!mInteractive && !sameBatch) {
                executeBatch();
            }
            throw tme;
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

            executeBatch();

            if(alias==null) {
                alias = mPigServer.getPigContext().getLastAlias();
                // if describe is used immediately after launching grunt shell then
                // last defined alias will be null
                if (alias == null) {
                    throw new IOException(
                            "No previously defined alias found. Please define an alias and use 'describe' operator.");
                }
            }
            if(alias.contains("::")) {
                nestedAlias = alias.substring(alias.indexOf("::") + 2);
                alias = alias.substring(0, alias.indexOf("::"));
                mPigServer.dumpSchemaNested(alias, nestedAlias);
            }
            else {
                if ("@".equals(alias)) {
                    alias = mPigServer.getLastRel();
                }
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
        if (mPigServer.isBatchOn()) {
            mPigServer.parseAndBuild();
        }
        if (alias == null && script == null) {
            if (mInteractive) {
                alias = mPigServer.getPigContext().getLastAlias();
                // if explain is used immediately after launching grunt shell then
                // last defined alias will be null
                if (alias == null) {
                    throw new ParseException("'explain' statement must be on an alias or on a script.");
                }
            }
        }
        if ("@".equals(alias)) {
            alias = mPigServer.getLastRel();
        }
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
                String suffix = mExplain.mTime+sCount+"."+mExplain.mFormat;
                lp = new PrintStream(new File(file, "logical_plan-"+suffix));
                mPigServer.explain(mExplain.mAlias, mExplain.mFormat,
                                   mExplain.mVerbose, markAsExecuted, lp, null, file, suffix);
                lp.close();
                ep.close();
            }
            else {
                boolean append = !(mExplain.mCount==1);
                lp = ep = new PrintStream(new FileOutputStream(mExplain.mTarget, append));
                mPigServer.explain(mExplain.mAlias, mExplain.mFormat,
                                   mExplain.mVerbose, markAsExecuted, lp, ep, null, null);
                lp.close();
            }
        }
        else {
            mPigServer.explain(mExplain.mAlias, mExplain.mFormat,
                               mExplain.mVerbose, markAsExecuted, lp, ep, null, null);
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
    protected void printClear() {
        AnsiConsole.systemInstall();
        Ansi ansi = Ansi.ansi();
        System.out.println( ansi.eraseScreen() );
        System.out.println( ansi.cursor(0, 0) );
        AnsiConsole.systemUninstall();
    }

    @Override
    protected void processRegister(String jar) throws IOException {
        filter.validate(PigCommandFilter.Command.REGISTER);
        mPigServer.registerJar(jar);
    }

    @Override
    protected void processRegister(String path, String scriptingLang, String namespace) throws IOException, ParseException {
        filter.validate(PigCommandFilter.Command.REGISTER);
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

    private String runPreprocessor(String scriptPath, List<String> params, List<String> paramFiles)
        throws IOException, ParseException {

        PigContext context = mPigServer.getPigContext();
        BufferedReader reader = new BufferedReader(new FileReader(scriptPath));
        return context.doParamSubstitution(reader, params, paramFiles);
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

        mPigServer.getPigContext().setParams(params);
        mPigServer.getPigContext().setParamFiles(files);

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

        GruntParser parser = new GruntParser(inputReader, mPigServer);
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
        filter.validate(PigCommandFilter.Command.SET);
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
           mPigServer.getPigContext().getExecutionEngine().setProperty(key, value);
        }
    }

    @Override
    protected void processSet() throws IOException, ParseException {
        filter.validate(PigCommandFilter.Command.SET);
        Properties jobProps = mPigServer.getPigContext().getProperties();
        Properties sysProps = System.getProperties();

        List<String> jobPropsList = Lists.newArrayList();
        List<String> sysPropsList = Lists.newArrayList();

        for (Object key : jobProps.keySet()) {
            String propStr = key + "=" + jobProps.getProperty((String) key);
            if (sysProps.containsKey(key)) {
                sysPropsList.add("system: " + propStr);
            } else {
                jobPropsList.add(propStr);
            }
        }
        Collections.sort(jobPropsList);
        Collections.sort(sysPropsList);
        jobPropsList.addAll(sysPropsList);
        for (String prop : jobPropsList) {
            System.out.println(prop);
        }
    }

    @Override
    protected void processCat(String path) throws IOException {
        filter.validate(PigCommandFilter.Command.CAT);
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
    protected void processCD(String path) throws IOException {
        filter.validate(PigCommandFilter.Command.CD);
        ContainerDescriptor container;
        if(mExplain == null) { // process only if not in "explain" mode

            executeBatch();

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
    protected void processDump(String alias) throws IOException {
        filter.validate(PigCommandFilter.Command.DUMP);
        if (alias == null) {
            if (mPigServer.isBatchOn()) {
                mPigServer.parseAndBuild();
            }
            alias = mPigServer.getPigContext().getLastAlias();
            // if dump is used immediately after launching grunt shell then
            // last defined alias will be null
            if (alias == null) {
                throw new IOException(
                        "No previously defined alias found. Please define an alias and use 'dump' operator.");
            }
        }

        if(mExplain == null) { // process only if not in "explain" mode

            executeBatch();

            if ("@".equals(alias)) {
                alias = mPigServer.getLastRel();
            }
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
        filter.validate(PigCommandFilter.Command.ILLUSTRATE);
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
                    if (mPigServer.isBatchOn()) {
                        mPigServer.parseAndBuild();
                    }
                    alias = mPigServer.getPigContext().getLastAlias();
                    // if illustrate is used immediately after launching grunt shell then
                    // last defined alias will be null
                    if (alias == null) {
                        throw new ParseException("'illustrate' statement must be on an alias or on a script.");
                    }
                }
                if ("@".equals(alias)) {
                    if (mPigServer.isBatchOn()) {
                        mPigServer.parseAndBuild();
                    }
                    alias = mPigServer.getLastRel();
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
        filter.validate(PigCommandFilter.Command.KILL);
        mPigServer.getPigContext().getExecutionEngine().killJob(jobid);
    }

    @Override
    protected void processLS(String path) throws IOException {
        filter.validate(PigCommandFilter.Command.LS);

        if (mExplain == null) { // process only if not in "explain" mode

            executeBatch();

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
        filter.validate(PigCommandFilter.Command.PWD);
        if(mExplain == null) { // process only if not in "explain" mode

            executeBatch();

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
        System.out.println("    explain [-script <pigscript>] [-out <path>] [-brief] [-dot|-xml] [-param <param_name>=<param_value>]");
        System.out.println("        [-param_file <file_name>] [<alias>] - Show the execution plan to compute the alias or for entire script.");
        System.out.println("        -script - Explain the entire script.");
        System.out.println("        -out - Store the output into directory rather than print to stdout.");
        System.out.println("        -brief - Don't expand nested plans (presenting a smaller graph for overview).");
        System.out.println("        -dot - Generate the output in .dot format. Default is text format.");
        System.out.println("        -xml - Generate the output in .xml format. Default is text format.");
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
        filter.validate(PigCommandFilter.Command.MV);
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
        filter.validate(PigCommandFilter.Command.CP);
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
        filter.validate(PigCommandFilter.Command.COPYTOLOCAL);
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
        filter.validate(PigCommandFilter.Command.COPYFROMLOCAL);
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
        filter.validate(PigCommandFilter.Command.MKDIR);
        if(mExplain == null) { // process only if not in "explain" mode

            executeBatch();

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
    protected void processRemove(String path, String options) throws IOException {
        filter.validate(PigCommandFilter.Command.RM);
        filter.validate(PigCommandFilter.Command.RMF);
        int MAX_MS_TO_WAIT_FOR_FILE_DELETION = 10 * 60 * 1000;
        int MS_TO_SLEEP_WHILE_WAITING_FOR_FILE_DELETION = 250;

        if(mExplain == null) { // process only if not in "explain" mode
            Path filePath = new Path(path);
            ElementDescriptor dfsPath = null;
            FileSystem fs = filePath.getFileSystem(ConfigurationUtil.toConfiguration(mConf));

            executeBatch();
            if (!fs.exists(filePath)) {
                if (options == null || !options.equalsIgnoreCase("force")) {
                    throw new IOException("File or directory " + path + " does not exist.");
                }
            }
            else {
                boolean deleteSuccess = fs.delete(filePath, true);
                if (!deleteSuccess) {
                    log.warn("Unable to delete " + path);
                    return;
                }
                long startTime = System.currentTimeMillis();
                long duration = 0;
                while(fs.exists(filePath)) {
                    duration = System.currentTimeMillis() - startTime;
                    if (duration > MAX_MS_TO_WAIT_FOR_FILE_DELETION) {
                        throw new IOException("Timed out waiting to delete file: " + dfsPath);
                    } else {
                        try {
                            Thread.sleep(MS_TO_SLEEP_WHILE_WAITING_FOR_FILE_DELETION);
                        } catch (InterruptedException e) {
                            throw new IOException("Error waiting for file deletion", e);
                        }
                    }
                }
                log.info("Waited " + duration + "ms to delete file");
            }
        } else {
            log.warn("'rm/rmf' statement is ignored while processing 'explain -script' or '-check'");
        }
    }

    @Override
    protected void processFsCommand(String[] cmdTokens) throws IOException {
        filter.validate(PigCommandFilter.Command.FS);
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
    protected void processShCommand(String[] cmdTokens) throws IOException {
        filter.validate(PigCommandFilter.Command.SH);
        if(mExplain == null) { // process only if not in "explain" mode
            try {
                executeBatch();

                // For sh command, create a process with the following syntax
                // <shell exe> <invoke arg> <command-as-string>
                String  shellName = "sh";
                String  shellInvokeArg = "-c";

                // Insert cmd /C in front of the array list to execute to
                // support built-in shell commands like mkdir on Windows
                if (System.getProperty("os.name").startsWith("Windows")) {
                    shellName      = "cmd";
                    shellInvokeArg = "/C";
                }

                List<String> stringList = new ArrayList<String>();
                stringList.add(shellName);
                stringList.add(shellInvokeArg);

                StringBuffer commandString = new StringBuffer();
                for (String currToken : cmdTokens) {
                    commandString.append(" ");
                    commandString.append(currToken);
                }

                stringList.add(commandString.toString());

                String[] newCmdTokens = stringList.toArray(new String[0]);

                Process executor = Runtime.getRuntime().exec(newCmdTokens);

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
            String hcatBin = (String)mPigServer.getPigContext().getProperties().get("hcat.bin");
            if (hcatBin == null) {
                throw new IOException("hcat.bin is not defined. Define it to be your hcat script (Usually $HCAT_HOME/bin/hcat");
            }
            if (!(new File(hcatBin).exists())) {
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

    protected static class ExplainState {
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
    private boolean mDone;
    private boolean mLoadOnly;
    private ExplainState mExplain;
    private int mNumFailedJobs;
    private int mNumSucceededJobs;
    private FsShell shell;
    private boolean mScriptIllustrate;

    //For Testing Only
    protected void setExplainState(ExplainState explainState) {
        this.mExplain = explainState;
    }

}

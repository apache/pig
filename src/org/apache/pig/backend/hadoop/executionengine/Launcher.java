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
package org.apache.pig.backend.hadoop.executionengine;

import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TaskReport;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.pig.FuncSpec;
import org.apache.pig.PigException;
import org.apache.pig.backend.BackendException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.shims.HadoopShims;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.util.LogUtils;
import org.apache.pig.impl.util.Utils;
import org.apache.pig.tools.pigstats.PigStats;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 *
 * Provides core processing implementation for the backend of Pig
 * if ExecutionEngine chosen decides to delegate it's work to this class.
 * Also contains set of utility methods, including ones centered around
 * Hadoop.
 *
 */
public abstract class Launcher {
    private static final Log log = LogFactory.getLog(Launcher.class);
    private static final String OOM_ERR = "OutOfMemoryError";
    private boolean pigException = false;
    private boolean outOfMemory = false;
    private String newLine = "\n";

    // Used to track the exception thrown by the job control which is run in a
    // separate thread
    protected String jobControlExceptionStackTrace = null;
    protected Exception jobControlException = null;
    protected long totalHadoopTimeSpent;

    protected Map<FileSpec, Exception> failureMap;
    protected JobControl jc = null;

    class HangingJobKiller extends Thread {
        public HangingJobKiller() {}

        @Override
        public void run() {
            try {
                kill();
            } catch (Exception e) {
                log.warn("Error in killing Execution Engine: " + e);
            }
        }
    }

    protected Launcher() {
        Runtime.getRuntime().addShutdownHook(new HangingJobKiller());
        // handle the windows portion of \r
        if (System.getProperty("os.name").toUpperCase().startsWith("WINDOWS")) {
            newLine = "\r\n";
        }
        reset();
    }

    /**
     * Resets the state after a launch
     */
    public void reset() {
        failureMap = Maps.newHashMap();
        totalHadoopTimeSpent = 0;
        jc = null;
    }

    /**
     * Method to launch pig for hadoop either for a cluster's job tracker or for
     * a local job runner. THe only difference between the two is the job
     * client. Depending on the pig context the job client will be initialize to
     * one of the two. Launchers for other frameworks can overide these methods.
     * Given an input PhysicalPlan, it compiles it to get a MapReduce Plan. The
     * MapReduce plan which has multiple MapReduce operators each one of which
     * has to be run as a map reduce job with dependency information stored in
     * the plan. It compiles the MROperPlan into a JobControl object. Each Map
     * Reduce operator is converted into a Job and added to the JobControl
     * object. Each Job also has a set of dependent Jobs that are created using
     * the MROperPlan. The JobControl object is obtained from the
     * JobControlCompiler Then a new thread is spawned that submits these jobs
     * while respecting the dependency information. The parent thread monitors
     * the submitted jobs' progress and after it is complete, stops the
     * JobControl thread.
     *
     * @param php
     * @param grpName
     * @param pc
     * @throws Exception
     */
    public abstract PigStats launchPig(PhysicalPlan php, String grpName,
            PigContext pc) throws Exception;

    /**
     * Explain how a pig job will be executed on the underlying infrastructure.
     *
     * @param pp
     *            PhysicalPlan to explain
     * @param pc
     *            PigContext to use for configuration
     * @param ps
     *            PrintStream to write output on.
     * @param format
     *            Format to write in
     * @param verbose
     *            Amount of information to print
     * @throws VisitorException
     * @throws IOException
     */
    public abstract void explain(PhysicalPlan pp, PigContext pc,
            PrintStream ps, String format, boolean verbose)
            throws PlanException, VisitorException, IOException;

    public abstract void kill() throws BackendException;

    public abstract void killJob(String jobID, Configuration conf)
            throws BackendException;

    protected boolean isComplete(double prog) {
        return (int) (Math.ceil(prog)) == 1;
    }

    protected long computeTimeSpent(TaskReport[] taskReports) {
        long timeSpent = 0;
        for (TaskReport r : taskReports) {
            timeSpent += (r.getFinishTime() - r.getStartTime());
        }
        return timeSpent;
    }

    protected void getErrorMessages(TaskReport reports[], String type,
            boolean errNotDbg, PigContext pigContext) throws Exception {
        for (int i = 0; i < reports.length; i++) {
            String msgs[] = reports[i].getDiagnostics();
            ArrayList<Exception> exceptions = new ArrayList<Exception>();
            String exceptionCreateFailMsg = null;
            boolean jobFailed = false;
            if (msgs.length > 0) {
                if (HadoopShims.isJobFailed(reports[i])) {
                    jobFailed = true;
                }
                Set<String> errorMessageSet = new HashSet<String>();
                for (int j = 0; j < msgs.length; j++) {
                    if (!errorMessageSet.contains(msgs[j])) {
                        errorMessageSet.add(msgs[j]);
                        if (errNotDbg) {
                            // errNotDbg is used only for failed jobs
                            // keep track of all the unique exceptions
                            try {
                                LogUtils.writeLog("Backend error message",
                                        msgs[j], pigContext.getProperties()
                                                .getProperty("pig.logfile"),
                                        log);
                                Exception e = getExceptionFromString(msgs[j]);
                                exceptions.add(e);
                            } catch (Exception e1) {
                                exceptionCreateFailMsg = msgs[j];

                            }
                        } else {
                            log.debug("Error message from task (" + type + ") "
                                    + reports[i].getTaskID() + msgs[j]);
                        }
                    }
                }
            }
            // if there are no valid exception that could be created, report
            if (jobFailed && (exceptions.size() == 0) && (exceptionCreateFailMsg != null)) {
                int errCode = 2997;
                String msg = "Unable to recreate exception from backed error: "
                        + exceptionCreateFailMsg;
                throw new ExecException(msg, errCode, PigException.BUG);
            }

            // if its a failed job then check if there is more than one
            // exception
            // more than one exception implies possibly different kinds of
            // failures
            // log all the different failures and throw the exception
            // corresponding
            // to the first failure
            if (jobFailed) {
                if (exceptions.size() > 1) {
                    for (int j = 0; j < exceptions.size(); ++j) {
                        String headerMessage = "Error message from task ("
                                + type + ") " + reports[i].getTaskID();
                        LogUtils.writeLog(exceptions.get(j), pigContext
                                .getProperties().getProperty("pig.logfile"),
                                log, false, headerMessage, false, false);
                    }
                    throw exceptions.get(0);
                } else if (exceptions.size() == 1) {
                    throw exceptions.get(0);
                } else {
                    int errCode = 2115;
                    String msg = "Internal error. Expected to throw exception from the backend. Did not find any exception to throw.";
                    throw new ExecException(msg, errCode, PigException.BUG);
                }
            }
        }
    }

    /**
     * Compute the progress of the current job submitted through the JobControl
     * object jc to the JobClient jobClient
     *
     * @param jc
     *            - The JobControl object that has been submitted
     * @param jobClient
     *            - The JobClient to which it has been submitted
     * @return The progress as a precentage in double format
     * @throws IOException
     */
    protected double calculateProgress(JobControl jc, JobClient jobClient)
            throws IOException {
        double prog = 0.0;
        prog += jc.getSuccessfulJobs().size();

        List<Job> runnJobs = jc.getRunningJobs();
        for (Object object : runnJobs) {
            Job j = (Job) object;
            prog += progressOfRunningJob(j, jobClient);
        }
        return prog;
    }

    /**
     * Returns the progress of a Job j which is part of a submitted JobControl
     * object. The progress is for this Job. So it has to be scaled down by the
     * num of jobs that are present in the JobControl.
     *
     * @param j
     *            - The Job for which progress is required
     * @param jobClient
     *            - the JobClient to which it has been submitted
     * @return Returns the percentage progress of this Job
     * @throws IOException
     */
    protected double progressOfRunningJob(Job j, JobClient jobClient)
            throws IOException {
        JobID mrJobID = j.getAssignedJobID();
        RunningJob rj = jobClient.getJob(mrJobID);
        if (rj == null && j.getState() == Job.SUCCESS)
            return 1;
        else if (rj == null)
            return 0;
        else {
            double mapProg = rj.mapProgress();
            double redProg = rj.reduceProgress();
            return (mapProg + redProg) / 2;
        }
    }

    public long getTotalHadoopTimeSpent() {
        return totalHadoopTimeSpent;
    }

    /**
     * An exception handler class to handle exceptions thrown by the job controller thread
     * Its a local class. This is the only mechanism to catch unhandled thread exceptions
     * Unhandled exceptions in threads are handled by the VM if the handler is not registered
     * explicitly or if the default handler is null
     */
    public class JobControlThreadExceptionHandler implements Thread.UncaughtExceptionHandler {
        @Override
        public void uncaughtException(Thread thread, Throwable throwable) {
            jobControlExceptionStackTrace = Utils.getStackStraceStr(throwable);
            try {
                jobControlException = getExceptionFromString(jobControlExceptionStackTrace);
            } catch (Exception e) {
                String errMsg = "Could not resolve error that occured when launching job: "
                        + jobControlExceptionStackTrace;
                jobControlException = new RuntimeException(errMsg, throwable);
            }
        }
    }

    /**
     *
     * @param stackTraceLine
     *            The string representation of
     *            {@link Throwable#printStackTrace() printStackTrace} Handles
     *            internal PigException and its subclasses that override the
     *            {@link Throwable#toString() toString} method
     * @return An exception object whose string representation of
     *         printStackTrace is the input stackTrace
     * @throws Exception
     */
    public Exception getExceptionFromString(String stackTrace) throws Exception {
        String[] lines = stackTrace.split(newLine);
        Throwable t = getExceptionFromStrings(lines, 0);

        if (!pigException) {
            int errCode = 6015;
            String msg = "During execution, encountered a Hadoop error.";
            ExecException ee = new ExecException(msg, errCode,
                    PigException.REMOTE_ENVIRONMENT, t);
            ee.setStackTrace(t.getStackTrace());
            return ee;
        } else {
            pigException = false;
            if (outOfMemory) {
                outOfMemory = false;
                int errCode = 6016;
                String msg = "Out of memory.";
                ExecException ee = new ExecException(msg, errCode,
                        PigException.REMOTE_ENVIRONMENT, t);
                ee.setStackTrace(t.getStackTrace());
                return ee;
            }
            return (Exception) t;
        }
    }

    /**
     *
     * @param stackTraceLine
     *            An array of strings that represent
     *            {@link Throwable#printStackTrace() printStackTrace} output,
     *            split by newline
     * @return An exception object whose string representation of
     *         printStackTrace is the input stackTrace
     * @throws Exception
     */
    private Throwable getExceptionFromStrings(String[] stackTraceLines,
            int startingLineNum) throws Exception {
        /*
         * parse the array of string and throw the appropriate exception first:
         * from the line startingLineNum extract the exception name extract the
         * message if any fourth: create the appropriate exception and return it
         * An example of the stack trace:
         * org.apache.pig.backend.executionengine.ExecException: ERROR 1075:
         * Received a bytearray from the UDF. Cannot determine how to convert
         * the bytearray to int. at
         * org.apache.pig.backend.hadoop.executionengine
         * .physicalLayer.expressionOperators.POCast.getNext(POCast.java:152) at
         * org.apache.pig.backend.hadoop.executionengine.physicalLayer.
         * expressionOperators.LessThanExpr.getNext(LessThanExpr.java:85) at
         * org.apache.pig.backend.hadoop.executionengine.physicalLayer.
         * relationalOperators.POFilter.getNext(POFilter.java:148) at
         * org.apache.
         * pig.backend.hadoop.executionengine.mapReduceLayer.PigMapBase
         * .runPipeline(PigMapBase.java:184) at
         * org.apache.pig.backend.hadoop.executionengine
         * .mapReduceLayer.PigMapBase.map(PigMapBase.java:174) at
         * org.apache.pig.
         * backend.hadoop.executionengine.mapReduceLayer.PigMapOnly$Map
         * .map(PigMapOnly.java:65) at
         * org.apache.hadoop.mapred.MapRunner.run(MapRunner.java:47) at
         * org.apache.hadoop.mapred.MapTask.run(MapTask.java:227) at
         * org.apache.hadoop
         * .mapred.TaskTracker$Child.main(TaskTracker.java:2207)
         */

        if (stackTraceLines.length > 0
                && startingLineNum < (stackTraceLines.length - 1)) {

            // the regex for matching the exception class name; note the use of
            // the $ for matching nested classes
            String exceptionNameDelimiter = "(\\w+(\\$\\w+)?\\.)+\\w+";
            Pattern exceptionNamePattern = Pattern
                    .compile(exceptionNameDelimiter);

            // from the first line extract the exception name and the exception
            // message
            Matcher exceptionNameMatcher = exceptionNamePattern
                    .matcher(stackTraceLines[startingLineNum]);
            String exceptionName = null;
            String exceptionMessage = null;
            if (exceptionNameMatcher.find()) {
                exceptionName = exceptionNameMatcher.group();
                /*
                 * note that the substring is from end + 2 the regex matcher
                 * ends at one position beyond the match in this case it will
                 * end at colon (:) the exception message will have a preceding
                 * space (after the colon (:))
                 */
                if (exceptionName.contains(OOM_ERR)) {
                    outOfMemory = true;
                }

                if (stackTraceLines[startingLineNum].length() > exceptionNameMatcher
                        .end()) {
                    exceptionMessage = stackTraceLines[startingLineNum]
                            .substring(exceptionNameMatcher.end() + 2);
                }

                ++startingLineNum;
            }

            // the exceptionName should not be null
            if (exceptionName != null) {

                ArrayList<StackTraceElement> stackTraceElements = Lists.newArrayList();

                // Create stack trace elements for the remaining lines
                String stackElementRegex = "\\s+at\\s+(\\w+(\\$\\w+)?\\.)+(\\<)?\\w+(\\>)?";
                Pattern stackElementPattern = Pattern
                        .compile(stackElementRegex);
                String pigExceptionRegex = "org\\.apache\\.pig\\.";
                Pattern pigExceptionPattern = Pattern
                        .compile(pigExceptionRegex);
                String moreElementRegex = "\\s+\\.\\.\\.\\s+\\d+\\s+more";
                Pattern moreElementPattern = Pattern.compile(moreElementRegex);

                int lineNum = startingLineNum;
                for (; lineNum < (stackTraceLines.length - 1); ++lineNum) {
                    Matcher stackElementMatcher = stackElementPattern
                            .matcher(stackTraceLines[lineNum]);

                    if (stackElementMatcher.find()) {
                        StackTraceElement ste = getStackTraceElement(stackTraceLines[lineNum]);
                        stackTraceElements.add(ste);
                        String className = ste.getClassName();
                        Matcher pigExceptionMatcher = pigExceptionPattern
                                .matcher(className);
                        if (pigExceptionMatcher.find()) {
                            pigException = true;
                        }
                    } else {
                        Matcher moreElementMatcher = moreElementPattern
                                .matcher(stackTraceLines[lineNum]);
                        if (moreElementMatcher.find()) {
                            ++lineNum;
                        }
                        break;
                    }
                }

                startingLineNum = lineNum;

                // create the appropriate exception; setup the stack trace and
                // message
                Object object = PigContext
                        .instantiateFuncFromSpec(exceptionName);

                if (object instanceof PigException) {
                    // extract the error code and message the regex for matching
                    // the custom format of ERROR <ERROR CODE>:
                    String errMessageRegex = "ERROR\\s+\\d+:";
                    Pattern errMessagePattern = Pattern
                            .compile(errMessageRegex);
                    Matcher errMessageMatcher = errMessagePattern
                            .matcher(exceptionMessage);

                    if (errMessageMatcher.find()) {
                        String errMessageStub = errMessageMatcher.group();
                        /*
                         * extract the actual exception message sans the ERROR
                         * <ERROR CODE>: again note that the matcher ends at the
                         * space following the colon (:) the exception message
                         * appears after the space and hence the end + 1
                         */
                        exceptionMessage = exceptionMessage
                                .substring(errMessageMatcher.end() + 1);

                        // the regex to match the error code wich is a string of
                        // numerals
                        String errCodeRegex = "\\d+";
                        Pattern errCodePattern = Pattern.compile(errCodeRegex);
                        Matcher errCodeMatcher = errCodePattern
                                .matcher(errMessageStub);

                        String code = null;
                        if (errCodeMatcher.find()) {
                            code = errCodeMatcher.group();
                        }

                        // could receive a number format exception here but it
                        // will be propagated up the stack
                        int errCode;
                        if (code != null)
                            errCode = Integer.parseInt(code);
                        else
                            errCode = 2998;

                        // create the exception with the message and then set
                        // the error code and error source
                        FuncSpec funcSpec = new FuncSpec(exceptionName,
                                exceptionMessage);
                        object = PigContext.instantiateFuncFromSpec(funcSpec);
                        ((PigException) object).setErrorCode(errCode);
                        ((PigException) object).setErrorSource(PigException
                                .determineErrorSource(errCode));
                    } else { // else for if(errMessageMatcher.find())
                        /*
                         * did not find the error code which means that the
                         * PigException or its subclass is not returning the
                         * error code highly unlikely: should never be here
                         */
                        FuncSpec funcSpec = new FuncSpec(exceptionName,
                                exceptionMessage);
                        object = PigContext.instantiateFuncFromSpec(funcSpec);
                        ((PigException) object).setErrorCode(2997);// generic
                                                                    // error
                                                                    // code
                        ((PigException) object)
                                .setErrorSource(PigException.BUG);
                    }
                } else { // else for if(object instanceof PigException)
                    // its not PigException; create the exception with the
                    // message
                    object = PigContext.instantiateFuncFromSpec(new FuncSpec(
                            exceptionName, exceptionMessage));
                }

                StackTraceElement[] steArr = new StackTraceElement[stackTraceElements
                        .size()];
                ((Throwable) object).setStackTrace(stackTraceElements
                        .toArray(steArr));

                if (startingLineNum < (stackTraceLines.length - 1)) {
                    Throwable e = getExceptionFromStrings(stackTraceLines,
                            startingLineNum);
                    ((Throwable) object).initCause(e);
                }

                return (Throwable) object;
            } else { // else for if(exceptionName != null)
                int errCode = 2055;
                String msg = "Did not find exception name to create exception from string: "
                        + Arrays.toString(stackTraceLines);
                throw new ExecException(msg, errCode, PigException.BUG);
            }
        } else { // else for if(lines.length > 0)
            int errCode = 2056;
            String msg = "Cannot create exception from empty string.";
            throw new ExecException(msg, errCode, PigException.BUG);
        }
    }

    /**
     *
     * @param line
     *            the string representation of a stack trace returned by
     *            {@link Throwable#printStackTrace() printStackTrace}
     * @return the StackTraceElement object representing the stack trace
     * @throws Exception
     */
    public StackTraceElement getStackTraceElement(String line) throws Exception {
        /*
         * the format of the line is something like: at
         * org.apache.pig.backend.hadoop
         * .executionengine.mapReduceLayer.PigMapOnly$Map
         * .map(PigMapOnly.java:65) note the white space before the 'at'. Its
         * not of much importance but noted for posterity.
         */
        String[] items;

        /*
         * regex for matching the fully qualified method Name note the use of
         * the $ for matching nested classes and the use of < and > for
         * constructors
         */
        String qualifiedMethodNameRegex = "(\\w+(\\$\\w+)?\\.)+(<)?\\w+(>)?";
        Pattern qualifiedMethodNamePattern = Pattern
                .compile(qualifiedMethodNameRegex);
        Matcher contentMatcher = qualifiedMethodNamePattern.matcher(line);

        // org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigMapOnly$Map.map(PigMapOnly.java:65)
        String content = null;
        if (contentMatcher.find()) {
            content = line.substring(contentMatcher.start());
        } else {
            int errCode = 2057;
            String msg = "Did not find fully qualified method name to reconstruct stack trace: "
                    + line;
            throw new ExecException(msg, errCode, PigException.BUG);
        }

        Matcher qualifiedMethodNameMatcher = qualifiedMethodNamePattern
                .matcher(content);

        // org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigMapOnly$Map.map
        String qualifiedMethodName = null;
        // (PigMapOnly.java:65)
        String fileDetails = null;

        if (qualifiedMethodNameMatcher.find()) {
            qualifiedMethodName = qualifiedMethodNameMatcher.group();
            fileDetails = content
                    .substring(qualifiedMethodNameMatcher.end() + 1);
        } else {
            int errCode = 2057;
            String msg = "Did not find fully qualified method name to reconstruct stack trace: "
                    + line;
            throw new ExecException(msg, errCode, PigException.BUG);
        }

        // From the fully qualified method name, extract the declaring class and
        // method name
        items = qualifiedMethodName.split("\\.");

        // initialize the declaringClass (to org in most cases)
        String declaringClass = items[0];
        // the last member is always the method name
        String methodName = items[items.length - 1];
        StringBuilder sb = new StringBuilder();

        // concatenate the names by adding the dot (.) between the members till
        // the penultimate member
        for (int i = 1; i < items.length - 1; ++i) {
            sb.append('.');
            sb.append(items[i]);
        }

        declaringClass += sb.toString();

        // from the file details extract the file name and the line number
        // PigMapOnly.java:65
        fileDetails = fileDetails.substring(0, fileDetails.length() - 1);
        items = fileDetails.split(":");
        // PigMapOnly.java
        String fileName = null;
        int lineNumber = -1;
        if (items.length > 0) {
            fileName = items[0];
            if (items.length > 1) {
                lineNumber = Integer.parseInt(items[1]);
            }
        }
        return new StackTraceElement(declaringClass, methodName, fileName,
                lineNumber);
    }
}

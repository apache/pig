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
package org.apache.pig.test;

import static java.util.regex.Matcher.quoteReplacement;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.SimpleLayout;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MRCompiler;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MapReduceLauncher;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROperPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.LogToPhyTranslationVisitor;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.util.MapRedUtil;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.LogicalPlan;
import org.apache.pig.impl.logicalLayer.parser.ParseException;
import org.apache.pig.impl.logicalLayer.parser.QueryParser;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.newplan.logical.LogicalPlanMigrationVistor;
import org.apache.pig.newplan.logical.optimizer.LogicalPlanPrinter;
import org.apache.pig.newplan.logical.optimizer.SchemaResetter;
import org.apache.pig.tools.grunt.GruntParser;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

public class Util {
    private static BagFactory mBagFactory = BagFactory.getInstance();
    private static TupleFactory mTupleFactory = TupleFactory.getInstance();

    // Helper Functions
    // =================
    static public Tuple loadFlatTuple(Tuple t, int[] input) throws ExecException {
        for (int i = 0; i < input.length; i++) {
            t.set(i, new Integer(input[i]));
        }
        return t;
    }

    static public Tuple loadTuple(Tuple t, String[] input) throws ExecException {
        for (int i = 0; i < input.length; i++) {
            t.set(i, input[i]);
        }
        return t;
    }

    static public Tuple loadTuple(Tuple t, DataByteArray[] input) throws ExecException {
        for (int i = 0; i < input.length; i++) {
            t.set(i, input[i]);
        }
        return t;
    }

    static public Tuple loadNestTuple(Tuple t, int[] input) throws ExecException {
        DataBag bag = BagFactory.getInstance().newDefaultBag();
        for(int i = 0; i < input.length; i++) {
            Tuple f = TupleFactory.getInstance().newTuple(1);
            f.set(0, input[i]);
            bag.add(f);
        }
        t.set(0, bag);
        return t;
    }

    static public Tuple loadNestTuple(Tuple t, long[] input) throws ExecException {
        DataBag bag = BagFactory.getInstance().newDefaultBag();
        for(int i = 0; i < input.length; i++) {
            Tuple f = TupleFactory.getInstance().newTuple(1);
            f.set(0, new Long(input[i]));
            bag.add(f);
        }
        t.set(0, bag);
        return t;
    }

    // this one should handle String, DataByteArray, Long, Integer etc..
    static public <T> Tuple loadNestTuple(Tuple t, T[] input) throws ExecException {
        DataBag bag = BagFactory.getInstance().newDefaultBag();
        for(int i = 0; i < input.length; i++) {
            Tuple f = TupleFactory.getInstance().newTuple(1);
            f.set(0, input[i]);
            bag.add(f);
        }
        t.set(0, bag);
        return t;
    }

    static public <T>void addToTuple(Tuple t, T[] b)
    {
        for(int i = 0; i < b.length; i++)
            t.append(b[i]);
    }
    
    static public Tuple buildTuple(Object... args) throws ExecException {
        return TupleFactory.getInstance().newTupleNoCopy(Lists.newArrayList(args));
    }

    static public Tuple buildBinTuple(final Object... args) throws IOException {
        return TupleFactory.getInstance().newTuple(Lists.transform(
                Lists.newArrayList(args), new Function<Object, DataByteArray>() {
                    public DataByteArray apply(Object o) {
                        if (o == null) { 
                            return null;
                        }
                        try {
                            return new DataByteArray(DataType.toBytes(o));
                        } catch (ExecException e) {
                            return null;
                        }
                    }
                }));
    }

    static public <T>Tuple createTuple(T[] s)
    {
        Tuple t = mTupleFactory.newTuple();
        addToTuple(t, s);
        return t;
    }
    
    static public DataBag createBag(Tuple[] t)
    {
        DataBag b = mBagFactory.newDefaultBag();
        for(int i = 0; i < t.length; i++)b.add(t[i]);
        return b;
    }
    
    static public<T> DataBag createBagOfOneColumn(T[] input) throws ExecException {
        DataBag result = mBagFactory.newDefaultBag();
        for (int i = 0; i < input.length; i++) {
            Tuple t = mTupleFactory.newTuple(1);
            t.set(0, input[i]);
            result.add(t);
        }
        return result;
    }
    
    static public Map<String, Object> createMap(String[] contents)
    {
        Map<String, Object> m = new HashMap<String, Object>();
        for(int i = 0; i < contents.length; ) {
            m.put(contents[i], contents[i+1]);
            i += 2;
        }
        return m;
    }

    static public<T> DataByteArray[] toDataByteArrays(T[] input) {
        DataByteArray[] dbas = new DataByteArray[input.length];
        for (int i = 0; i < input.length; i++) {
            dbas[i] = (input[i] == null)?null:new DataByteArray(input[i].toString().getBytes());
        }        
        return dbas;
    }
    
    static public Tuple loadNestTuple(Tuple t, int[][] input) throws ExecException {
        for (int i = 0; i < input.length; i++) {
            DataBag bag = BagFactory.getInstance().newDefaultBag();
            Tuple f = loadFlatTuple(TupleFactory.getInstance().newTuple(input[i].length), input[i]);
            bag.add(f);
            t.set(i, bag);
        }
        return t;
    }

    static public Tuple loadTuple(Tuple t, String[][] input) throws ExecException {
        for (int i = 0; i < input.length; i++) {
            DataBag bag = BagFactory.getInstance().newDefaultBag();
            Tuple f = loadTuple(TupleFactory.getInstance().newTuple(input[i].length), input[i]);
            bag.add(f);
            t.set(i, bag);
        }
        return t;
    }

    /**
     * Helper to create a temporary file with given input data for use in test cases.
     *  
     * @param tmpFilenamePrefix file-name prefix
     * @param tmpFilenameSuffix file-name suffix
     * @param inputData input for test cases, each string in inputData[] is written
     *                  on one line
     * @return {@link File} handle to the created temporary file
     * @throws IOException
     */
	static public File createInputFile(String tmpFilenamePrefix, 
			                           String tmpFilenameSuffix, 
			                           String[] inputData) 
	throws IOException {
		File f = File.createTempFile(tmpFilenamePrefix, tmpFilenameSuffix);
        f.deleteOnExit();
        writeToFile(f, inputData);	
		return f;
	}
	
	static public File createLocalInputFile(String filename, String[] inputData) 
    throws IOException {
        File f = new File(filename);
        f.deleteOnExit();
        writeToFile(f, inputData);  
        return f;
    }
	
	private static void writeToFile(File f, String[] inputData) throws
	IOException {
	    PrintWriter pw = new PrintWriter(new OutputStreamWriter(new 
	            FileOutputStream(f), "UTF-8"));
        for (int i=0; i<inputData.length; i++){
            pw.println(inputData[i]);
        }
        pw.close();
	}
	
	/**
     * Helper to create a dfs file on the Minicluster DFS with given
     * input data for use in test cases.
     * 
     * @param miniCluster reference to the Minicluster where the file should be created
     * @param fileName pathname of the file to be created
     * @param inputData input for test cases, each string in inputData[] is written
     *                  on one line
     * @throws IOException
     */
    static public void createInputFile(MiniCluster miniCluster, String fileName, 
                                       String[] inputData) 
    throws IOException {
        FileSystem fs = miniCluster.getFileSystem();
        createInputFile(fs, fileName, inputData);
    }
    
    static public void createInputFile(FileSystem fs, String fileName, 
            String[] inputData) throws IOException {
        if(fs.exists(new Path(fileName))) {
            throw new IOException("File " + fileName + " already exists on the minicluster");
        }
        FSDataOutputStream stream = fs.create(new Path(fileName));
        PrintWriter pw = new PrintWriter(new OutputStreamWriter(stream, "UTF-8"));
        for (int i=0; i<inputData.length; i++){
            pw.println(inputData[i]);
        }
        pw.close();

    }
    
    /**
     * Helper to create a dfs file on the MiniCluster dfs. This returns an
     * outputstream that can be used in test cases to write data.
     * 
     * @param cluster
     *            reference to the MiniCluster where the file should be created
     * @param fileName
     *            pathname of the file to be created
     * @return OutputStream to write any data to the file created on the
     *         MiniCluster.
     * @throws IOException
     */
    static public OutputStream createInputFile(MiniCluster cluster,
            String fileName) throws IOException {
        FileSystem fs = cluster.getFileSystem();
        if (fs.exists(new Path(fileName))) {
            throw new IOException("File " + fileName
                    + " already exists on the minicluster");
        }
        return fs.create(new Path(fileName));
    }
    
    /**
     * Helper to create an empty temp file on local file system
     *  which will be deleted on exit
     * @param prefix
     * @param suffix
     * @return File denoting a newly-created empty file 
     * @throws IOException
     */
    static public File createTempFileDelOnExit(String prefix, String suffix)
        throws IOException {
        File tmpFile = File.createTempFile(prefix, suffix);
        tmpFile.deleteOnExit();
        return tmpFile;
    }

    
    /**
     * Helper to remove a dfs file from the minicluster DFS
     * 
     * @param miniCluster reference to the Minicluster where the file should be deleted
     * @param fileName pathname of the file to be deleted
     * @throws IOException
     */
    static public void deleteFile(MiniCluster miniCluster, String fileName) 
    throws IOException {
        FileSystem fs = miniCluster.getFileSystem();
        fs.delete(new Path(fileName), true);
    }

    static public void deleteFile(PigContext pigContext, String fileName) 
    throws IOException {
        Configuration conf = ConfigurationUtil.toConfiguration(
                pigContext.getProperties());
        FileSystem fs = FileSystem.get(conf);
        fs.delete(new Path(fileName), true);
    }
    
    static public boolean exists(PigContext pigContext, String fileName) 
    throws IOException {
        Configuration conf = ConfigurationUtil.toConfiguration(
                pigContext.getProperties());
        FileSystem fs = FileSystem.get(conf);
        return fs.exists(new Path(fileName));
    }
    
    /**
    * Helper function to check if the result of a Pig Query is in line with 
    * expected results.
    * 
    * @param actualResults Result of the executed Pig query
    * @param expectedResults Expected results to validate against
    */
    static public void checkQueryOutputs(Iterator<Tuple> actualResults, 
                                    Tuple[] expectedResults) {
        for (Tuple expected : expectedResults) {
            Tuple actual = actualResults.next();
            Assert.assertEquals(expected.toString(), actual.toString());
        }
    }

    /**
     * Helper function to check if the result of a Pig Query is in line with 
     * expected results. It sorts actual and expected results before comparison
     * 
     * @param actualResultsIt Result of the executed Pig query
     * @param expectedResList Expected results to validate against
     */
     static public void checkQueryOutputsAfterSort(Iterator<Tuple> actualResultsIt, 
                                     List<Tuple> expectedResList) {
         List<Tuple> actualResList = new ArrayList<Tuple>();
         while(actualResultsIt.hasNext()){
             actualResList.add(actualResultsIt.next());
         }
         
         Collections.sort(actualResList);
         Collections.sort(expectedResList);

         Assert.assertEquals("Comparing actual and expected results. ",
                 expectedResList, actualResList);
     }

     /**
      * Check if subStr is a subString of str . calls org.junit.Assert.fail if it is not 
      * @param str
      * @param subStr
      */
     static public void checkStrContainsSubStr(String str, String subStr){
         if(!str.contains(subStr)){
             fail("String '"+ subStr + "' is not a substring of '" + str + "'");
         }
     }

	/**
	 * Utility method to copy a file form local filesystem to the dfs on
	 * the minicluster for testing in mapreduce mode
	 * @param cluster a reference to the minicluster
	 * @param localFileName the pathname of local file
	 * @param fileNameOnCluster the name with which the file should be created on the minicluster
	 * @throws IOException
	 */
	static public void copyFromLocalToCluster(MiniCluster cluster, String localFileName, String fileNameOnCluster) throws IOException {
        PigServer ps = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        String script = "fs -put " + localFileName + " " + fileNameOnCluster;

	    GruntParser parser = new GruntParser(new StringReader(script));
        parser.setInteractive(false);
        parser.setParams(ps);
        try {
            parser.parseStopOnError();
        } catch (org.apache.pig.tools.pigscript.parser.ParseException e) {
            throw new IOException(e);
        }
	}
	
	static public void copyFromClusterToLocal(MiniCluster cluster, String fileNameOnCluster, String localFileName) throws IOException {
	    PrintWriter writer = new PrintWriter(new FileWriter(localFileName));
	    
	    FileSystem fs = FileSystem.get(ConfigurationUtil.toConfiguration(
	            cluster.getProperties()));
        if(!fs.exists(new Path(fileNameOnCluster))) {
            throw new IOException("File " + fileNameOnCluster + " does not exists on the minicluster");
        }
        
        String line = null;
 	   FileStatus fst = fs.getFileStatus(new Path(fileNameOnCluster));
 	   if(fst.isDir()) {
 	       throw new IOException("Only files from cluster can be copied locally," +
 	       		" " + fileNameOnCluster + " is a directory");
 	   }
        FSDataInputStream stream = fs.open(new Path(fileNameOnCluster));
        BufferedReader reader = new BufferedReader(new InputStreamReader(stream));
        while( (line = reader.readLine()) != null) {
        	writer.println(line);        	
        }
    
        reader.close();
        writer.close();
	}
	
	static public void printQueryOutput(Iterator<Tuple> actualResults, 
               Tuple[] expectedResults) {

	    System.out.println("Expected :") ;
        for (Tuple expected : expectedResults) {
            System.out.println(expected.toString()) ;
        }
	    System.out.println("---End----") ;
	    
        System.out.println("Actual :") ;
        while (actualResults.hasNext()) {
            System.out.println(actualResults.next().toString()) ;
        }
        System.out.println("---End----") ;
    }

	/**
     * Helper method to replace all occurrences of "\" with "\\" in a 
     * string. This is useful to fix the file path string on Windows
     * where "\" is used as the path separator.
     * 
     * @param str Any string
     * @return The resulting string
     */
	public static String encodeEscape(String str) {
	    String regex = "\\\\";
	    String replacement = quoteReplacement("\\\\");
	    return str.replaceAll(regex, replacement);
	}

    public static String generateURI(String filename, PigContext context) 
            throws IOException {
        if (context.getExecType() == ExecType.MAPREDUCE) {
            return FileLocalizer.hadoopify(filename, context);
        } else if (context.getExecType() == ExecType.LOCAL) {
            return filename;
        } else {
            throw new IllegalStateException("ExecType: " + context.getExecType());
        }
    }

    public static Schema getSchemaFromString(String schemaString) throws ParseException {
        return Util.getSchemaFromString(schemaString, DataType.BYTEARRAY);
    }

    static Schema getSchemaFromString(String schemaString, byte defaultType) throws ParseException {
        ByteArrayInputStream stream = new ByteArrayInputStream(schemaString.getBytes()) ;
        QueryParser queryParser = new QueryParser(stream) ;
        Schema schema = queryParser.TupleSchema() ;
        Schema.setSchemaDefaultType(schema, defaultType);
        return schema;
    }
    
    public static Object getPigConstant(String pigConstantAsString) throws ParseException {
        ByteArrayInputStream stream = new ByteArrayInputStream(pigConstantAsString.getBytes()) ;
        QueryParser queryParser = new QueryParser(stream) ;
        return queryParser.Datum();
    }
    
    public static List<Tuple> getTuplesFromConstantTupleStrings(String[] tupleConstants) throws ParseException {
        List<Tuple> result = new ArrayList<Tuple>(tupleConstants.length);
        for(int i = 0; i < tupleConstants.length; i++) {
            result.add((Tuple) getPigConstant(tupleConstants[i]));
        }
        return result;
    }

    public static File createFile(String[] data) throws Exception{
        File f = File.createTempFile("tmp", "");
        PrintWriter pw = new PrintWriter(f);
        for (int i=0; i<data.length; i++){
            pw.println(data[i]);
        }
        pw.close();
        return f;
    }
    
    public static PhysicalPlan buildPhysicalPlan(LogicalPlan lp, PigContext pc) throws Exception {
    	LogToPhyTranslationVisitor visitor = new LogToPhyTranslationVisitor(lp);
    	visitor.setPigContext(pc);
    	visitor.visit();
    	return visitor.getPhysicalPlan();
    }

    /**
     * migrate old logical plan to new logical plan
     * @param lp
     * @return new logical plan
     * @throws FrontendException
     */
    public static org.apache.pig.newplan.logical.relational.LogicalPlan migrateToNewLP(LogicalPlan lp)
    throws FrontendException{
        LogicalPlanMigrationVistor visitor = new LogicalPlanMigrationVistor(lp);        
        visitor.visit();
        org.apache.pig.newplan.logical.relational.LogicalPlan newPlan = visitor.getNewLogicalPlan();
        
        SchemaResetter schemaResetter = new SchemaResetter(newPlan);
        schemaResetter.visit();
        return newPlan;
    }
    
    /**
     * Run default set of optimizer rules on new logical plan
     * @param lp
     * @return optimized logical plan
     * @throws FrontendException
     */
    public static  org.apache.pig.newplan.logical.relational.LogicalPlan optimizeNewLP( 
            org.apache.pig.newplan.logical.relational.LogicalPlan lp)
    throws FrontendException{
        // run optimizer
        org.apache.pig.newplan.logical.optimizer.LogicalPlanOptimizer optimizer = 
            new org.apache.pig.newplan.logical.optimizer.LogicalPlanOptimizer(lp, 100, null);
        optimizer.optimize();        
        return lp;
    }
    
    /**
     * migrate old LP(logical plan) to new LP, optimize it, and build physical 
     * plan
     * @param lp
     * @param pc PigContext
     * @return physical plan
     * @throws Exception
     */
    public static PhysicalPlan buildPhysicalPlanFromNewLP(
            org.apache.pig.newplan.logical.relational.LogicalPlan lp, PigContext pc)
    throws Exception {
         org.apache.pig.newplan.logical.relational.LogToPhyTranslationVisitor visitor =
             new org.apache.pig.newplan.logical.relational.LogToPhyTranslationVisitor(lp);
        visitor.setPigContext(pc);
        visitor.visit();
        return visitor.getPhysicalPlan();
    }
    
    public static PhysicalPlan getNewOptimizedPhysicalPlan(LogicalPlan lp, PigContext pc)
    throws FrontendException, Exception{
        return buildPhysicalPlanFromNewLP(optimizeNewLP(migrateToNewLP(lp)), pc);
    }
    
    public static MROperPlan buildMRPlan(PhysicalPlan pp, PigContext pc) throws Exception{
        MRCompiler comp = new MRCompiler(pp, pc);
        comp.compile();
        return comp.getMRPlan();	
    }
    
    public static MROperPlan buildMRPlanWithOptimizer(PhysicalPlan pp, PigContext pc) throws Exception {
        MapRedUtil.checkLeafIsStore(pp, pc);
        
        MapReduceLauncher launcher = new MapReduceLauncher();

        java.lang.reflect.Method compile = launcher.getClass()
                .getDeclaredMethod("compile",
                        new Class[] { PhysicalPlan.class, PigContext.class });

        compile.setAccessible(true);

        return (MROperPlan) compile.invoke(launcher, new Object[] { pp, pc });
    }
    
    public static void registerMultiLineQuery(PigServer pigServer, String query) throws IOException {
        File f = File.createTempFile("tmp", "");
        PrintWriter pw = new PrintWriter(f);
        pw.println(query);
        pw.close();
        pigServer.registerScript(f.getCanonicalPath());
    }
    
    public static int executeJavaCommand(String cmd) throws Exception {
        return executeJavaCommandAndReturnInfo(cmd).exitCode;
    }
    
    
    public static ProcessReturnInfo executeJavaCommandAndReturnInfo(String cmd) 
    throws Exception {
        String javaHome = System.getenv("JAVA_HOME");
        if(javaHome != null) {
            String fileSeparator = System.getProperty("file.separator");
            cmd = javaHome + fileSeparator + "bin" + fileSeparator + cmd;
        }
        Process cmdProc = Runtime.getRuntime().exec(cmd);
        ProcessReturnInfo pri = new ProcessReturnInfo();
        pri.stdoutContents = getContents(cmdProc.getInputStream());
        pri.stderrContents = getContents(cmdProc.getErrorStream());
        cmdProc.waitFor();
        pri.exitCode = cmdProc.exitValue();
        return pri;
    }
    
    private static String getContents(InputStream istr) throws IOException {
        BufferedReader br = new BufferedReader(
                new InputStreamReader(istr));
        String s = "";
        String line;
        while ( (line = br.readLine()) != null) {
            s += line + "\n";
        }
        return s;
        
    }
    public static class ProcessReturnInfo {
        public int exitCode;
        public String stderrContents;
        public String stdoutContents;
        
        @Override
        public String toString() {
            return "[Exit code: " + exitCode + ", stdout: <" + stdoutContents + ">, " +
            		"stderr: <" + stderrContents + ">"; 
        }
    }
    
    static public boolean deleteDirectory(File path) {
        if(path.exists()) {
            File[] files = path.listFiles();
            for(int i=0; i<files.length; i++) {
                if(files[i].isDirectory()) {
                    deleteDirectory(files[i]);
                }
                else {
                    files[i].delete();
                }
            }
        }
        return(path.delete());
    }

    /**
     * @param pigContext
     * @param fileName
     * @param input
     * @throws IOException 
     */
    public static void createInputFile(PigContext pigContext,
            String fileName, String[] input) throws IOException {
        Configuration conf = ConfigurationUtil.toConfiguration(
                pigContext.getProperties());
        createInputFile(FileSystem.get(conf), fileName, input); 
    }
    
    public static void printPlan(org.apache.pig.newplan.logical.relational.LogicalPlan logicalPlan ) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(out);
        LogicalPlanPrinter pp = new LogicalPlanPrinter(logicalPlan,ps);
        pp.visit();
        System.err.println(out.toString());
    }

    public static void printPlan(LogicalPlan logicalPlan) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(out);
        logicalPlan.explain(ps, "text", true);
        System.err.println(out.toString());
    }

    public static void printPlan(PhysicalPlan physicalPlan) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(out);
        physicalPlan.explain(ps, "text", true);
        System.err.println(out.toString());
    }

    public static List<Tuple> readFile2TupleList(String file, String delimiter) throws IOException{
        List<Tuple> tuples=new ArrayList<Tuple>();
        String line=null;
        BufferedReader reader=new BufferedReader(new InputStreamReader(new FileInputStream(file)));
        while((line=reader.readLine())!=null){
            String[] tokens=line.split(delimiter);
            Tuple tuple=TupleFactory.getInstance().newTuple(Arrays.asList(tokens));
            tuples.add(tuple);
        }
        reader.close();
        return tuples;
    }
    
    /**
     * Delete the existing logFile for the class and set the logging to a 
     * use a new log file and set log level to DEBUG
     * @param clazz class for which the log file is being set
     * @param logFile current log file
     * @return new log file
     * @throws Exception
     */
    public static File resetLog(Class<?> clazz, File logFile) throws Exception {
        if (logFile != null)
            logFile.delete();
        Logger logger = Logger.getLogger(clazz);
        logger.removeAllAppenders();
        logger.setLevel(Level.DEBUG);
        SimpleLayout layout = new SimpleLayout();
        File newLogFile = File.createTempFile("log", "");
        FileAppender appender = new FileAppender(layout, newLogFile.toString(),
                        false, false, 0);
        logger.addAppender(appender);
        return newLogFile;
    }

    /**
     * Check if logFile (does not/)contains the given list of messages. 
     * @param logFile
     * @param messages
     * @param expected if true, the messages are expected in the logFile, 
     *        otherwise messages should not be there in the log
     */
    public static void checkLogFileMessage(File logFile, String[] messages, boolean expected) {
        BufferedReader reader = null;

        try {
            reader = new BufferedReader(new FileReader(logFile));
            String logMessage = "";
            String line;
            while ((line = reader.readLine()) != null) {
                logMessage = logMessage + line + "\n";
            }
            for (int i = 0; i < messages.length; i++) {
                boolean present = logMessage.contains(messages[i]);
                if (expected) {
                    if(!present){
                        fail("The message " + messages[i] + " is not present in" +
                                "log file contents: " + logMessage);
                    }
                }else{
                    if(present){
                        fail("The message " + messages[i] + " is present in" +
                                "log file contents: " + logMessage);
                    }
                }
            }
            return ;
        }
        catch (IOException e) {
            fail("caught exception while checking log message :" + e);
        }
    }
}

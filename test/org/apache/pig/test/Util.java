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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
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
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.SimpleLayout;
import org.apache.pig.ExecType;
import org.apache.pig.LoadCaster;
import org.apache.pig.PigException;
import org.apache.pig.PigServer;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.backend.hadoop.executionengine.HExecutionEngine;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MRCompiler;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MapReduceLauncher;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROperPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.util.MapRedUtil;
import org.apache.pig.builtin.Utf8StorageConverter;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.DefaultBagFactory;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.apache.pig.impl.plan.CompilationMessageCollector;
import org.apache.pig.impl.util.LogUtils;
import org.apache.pig.newplan.logical.optimizer.LogicalPlanPrinter;
import org.apache.pig.newplan.logical.optimizer.SchemaResetter;
import org.apache.pig.newplan.logical.optimizer.UidResetter;
import org.apache.pig.newplan.logical.relational.LogToPhyTranslationVisitor;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.newplan.logical.relational.LogicalSchema;
import org.apache.pig.newplan.logical.relational.LogicalSchema.LogicalFieldSchema;
import org.apache.pig.newplan.logical.visitor.CastLineageSetter;
import org.apache.pig.newplan.logical.visitor.ColumnAliasConversionVisitor;
import org.apache.pig.newplan.logical.visitor.DanglingNestedNodeRemover;
import org.apache.pig.newplan.logical.visitor.ScalarVisitor;
import org.apache.pig.newplan.logical.visitor.SchemaAliasVisitor;
import org.apache.pig.newplan.logical.visitor.SortInfoSetter;
import org.apache.pig.newplan.logical.visitor.StoreAliasSetter;
import org.apache.pig.newplan.logical.visitor.TypeCheckingRelVisitor;
import org.apache.pig.newplan.logical.visitor.UnionOnSchemaSetter;
import org.apache.pig.parser.ParserException;
import org.apache.pig.parser.QueryParserDriver;
import org.apache.pig.tools.grunt.GruntParser;
import org.apache.pig.tools.pigstats.ScriptState;
import org.junit.Assert;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

public class Util {
    private static BagFactory mBagFactory = BagFactory.getInstance();
    private static TupleFactory mTupleFactory = TupleFactory.getInstance();

    // Commonly-checked system state
    // =================
    public static final boolean WINDOWS /* borrowed from Path.WINDOWS, Shell.WINDOWS */
                  = System.getProperty("os.name").startsWith("Windows");

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
                    @Override
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
     * Helper to remove colons (if any exist) from paths to sanitize them for
     * consumption by hdfs.
     *
     * @param origPath original path name
     * @return String  sanitized path with anything prior to : removed
     * @throws IOException
     */
    static public String removeColon(String origPath)
    {
       return origPath.replaceAll(":", "");
    }

    /**
     * Helper to convert \r\n to \n for cross-platform string
     * matching with checked-in baselines.
     *
     * @param origPath original string
     * @return String  newline-standardized string
     * @throws IOException
     */
    static public String standardizeNewline(String origPath)
    {
       return origPath.replaceAll("\r\n", "\n");
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

	public static void writeToFile(File f, String[] inputData) throws
	IOException {
	    PrintWriter pw = new PrintWriter(new OutputStreamWriter(new
	            FileOutputStream(f), "UTF-8"));
        for (int i=0; i<inputData.length; i++){
            pw.print(inputData[i]);
            pw.print("\n");
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
    static public void createInputFile(MiniGenericCluster miniCluster, String fileName,
                                       String[] inputData)
    throws IOException {
        FileSystem fs = miniCluster.getFileSystem();
        createInputFile(fs, fileName, inputData);
    }

    static public void createInputFile(FileSystem fs, String fileName,
            String[] inputData) throws IOException {
        if(Util.WINDOWS){
            fileName = fileName.replace('\\','/');
        }
        if(fs.exists(new Path(fileName))) {
            throw new IOException("File " + fileName + " already exists on the FileSystem");
        }
        FSDataOutputStream stream = fs.create(new Path(fileName));
        PrintWriter pw = new PrintWriter(new OutputStreamWriter(stream, "UTF-8"));
        for (int i=0; i<inputData.length; i++){
            pw.print(inputData[i]);
            pw.print("\n");
        }
        pw.close();

    }

    static public String[] readOutput(FileSystem fs, String fileName) throws IOException {
        if(Util.WINDOWS){
            fileName = fileName.replace('\\','/');
        }
        Path path = new Path(fileName);
        if(!fs.exists(path)) {
            throw new IOException("Path " + fileName + " does not exist on the FileSystem");
        }
        FileStatus fileStatus = fs.getFileStatus(path);
        FileStatus[] files;
        if (fileStatus.isDir()) {
            files = fs.listStatus(path, new PathFilter() {
                @Override
                public boolean accept(Path p) {
                    return !p.getName().startsWith("_");
                }
            });
        } else {
            files = new FileStatus[] { fileStatus };
        }
        List<String> result = new ArrayList<String>();
        for (FileStatus f : files) {
            FSDataInputStream stream = fs.open(f.getPath());
            BufferedReader br = new BufferedReader(new InputStreamReader(stream, "UTF-8"));
            String line;
            while ((line = br.readLine()) != null) {
                result.add(line);
            }
            br.close();
        }
        return result.toArray(new String[result.size()]);
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
    static public OutputStream createInputFile(MiniGenericCluster cluster,
            String fileName) throws IOException {
        FileSystem fs = cluster.getFileSystem();
        if(Util.WINDOWS){
            fileName = fileName.replace('\\','/');
        }
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
    static public void deleteFile(MiniGenericCluster miniCluster, String fileName)
    throws IOException {
        FileSystem fs = miniCluster.getFileSystem();
        if(Util.WINDOWS){
            fileName = fileName.replace('\\','/');
        }
        fs.delete(new Path(fileName), true);
    }

    static public void deleteFile(PigContext pigContext, String fileName)
    throws IOException {
        Configuration conf = ConfigurationUtil.toConfiguration(
                pigContext.getProperties());
        FileSystem fs = FileSystem.get(conf);
        if(Util.WINDOWS){
            fileName = fileName.replace('\\','/');
        }
        fs.delete(new Path(fileName), true);
    }

    static public boolean exists(PigContext pigContext, String fileName)
    throws IOException {
        Configuration conf = ConfigurationUtil.toConfiguration(
                pigContext.getProperties());
        FileSystem fs = FileSystem.get(conf);
        if(Util.WINDOWS){
            fileName = fileName.replace('\\','/');
        }
        return fs.exists(new Path(fileName));
    }

    /**
    * Helper function to check if the result of a Pig Query is in line with
    * expected results.
    *
    * @param actualResults Result of the executed Pig query
    * @param expectedResults Expected results Array to validate against
    */
    static public void checkQueryOutputs(Iterator<Tuple> actualResults,
                                    Tuple[] expectedResults) {
        checkQueryOutputs(actualResults, Arrays.asList(expectedResults));

    }

    /**
     * Helper function to check if the result of a Pig Query is in line with
     * expected results.
     *
     * @param actualResults Result of the executed Pig query
     * @param expectedResults Expected results List to validate against
     */
     static public void checkQueryOutputs(Iterator<Tuple> actualResults,
                                     List<Tuple> expectedResults) {
         int count = 0;
         for (Tuple expected : expectedResults) {
             Tuple actual = actualResults.next();
             count++;
             Assert.assertEquals(expected.toString(), actual.toString());
         }
         Assert.assertEquals(expectedResults.size(), count);
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

         compareActualAndExpectedResults(actualResList, expectedResList);

     }



     static public void compareActualAndExpectedResults(
            List<Tuple> actualResList, List<Tuple> expectedResList) {
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
      * Check if query plan for alias argument produces exception with expected
      * error message in expectedErr argument.
      * @param query
      * @param alias
      * @param expectedErr
      * @throws IOException
      */
     static public void checkExceptionMessage(String query, String alias, String expectedErr)
     throws IOException {
         PigServer pig = new PigServer(ExecType.LOCAL);

         boolean foundEx = false;
         try{
             Util.registerMultiLineQuery(pig, query);
             pig.explain(alias, System.out);
         }catch(FrontendException e){
             foundEx = true;
             checkMessageInException(e, expectedErr);
         }

         if(!foundEx)
             fail("No exception thrown. Exception is expected.");

     }

     public static void checkMessageInException(FrontendException e,
             String expectedErr) {
         PigException pigEx = LogUtils.getPigException(e);
         String message = pigEx.getMessage();
         checkErrorMessageContainsExpected(message, expectedErr);

     }

     public static void checkErrorMessageContainsExpected(String message, String expectedMessage){
         if(!message.contains(expectedMessage)){
             String msg = "Expected error message containing '"
                 + expectedMessage + "' but got '" + message + "'" ;
             fail(msg);
         }
     }

     static private String getMkDirCommandForHadoop2_0(String fileName) {
         if (Util.isHadoop23() || Util.isHadoop2_0()) {
             Path parentDir = new Path(fileName).getParent();
             String mkdirCommand = parentDir.getName().isEmpty() ? "" : "fs -mkdir -p " + parentDir + "\n";
             return mkdirCommand;
         }
         return "";
     }

    /**
	 * Utility method to copy a file form local filesystem to the dfs on
	 * the minicluster for testing in mapreduce mode
	 * @param cluster a reference to the minicluster
	 * @param localFileName the pathname of local file
	 * @param fileNameOnCluster the name with which the file should be created on the minicluster
	 * @throws IOException
	 */
     static public void copyFromLocalToCluster(MiniGenericCluster cluster,
        String localFileName, String fileNameOnCluster) throws IOException {
        if(Util.WINDOWS){
            localFileName = localFileName.replace('\\','/');
            fileNameOnCluster = fileNameOnCluster.replace('\\','/');
        }
        PigServer ps = new PigServer(cluster.getExecType(), cluster.getProperties());
        String script = getMkDirCommandForHadoop2_0(fileNameOnCluster) + "fs -put " + localFileName + " " + fileNameOnCluster;
        GruntParser parser = new GruntParser(new StringReader(script), ps);
        parser.setInteractive(false);
        try {
            parser.parseStopOnError();
        } catch (org.apache.pig.tools.pigscript.parser.ParseException e) {
            throw new IOException(e);
        }
	}

    static public void copyFromLocalToLocal(String fromLocalFileName,
            String toLocalFileName) throws IOException {
        if(Util.WINDOWS){
            fromLocalFileName = fromLocalFileName.replace('\\','/');
            toLocalFileName = toLocalFileName.replace('\\','/');
        }
        PigServer ps = new PigServer(ExecType.LOCAL, new Properties());
        String script = getMkDirCommandForHadoop2_0(toLocalFileName) + "fs -cp " + fromLocalFileName + " " + toLocalFileName;

        new File(toLocalFileName).deleteOnExit();

        GruntParser parser = new GruntParser(new StringReader(script), ps);
        parser.setInteractive(false);
        try {
            parser.parseStopOnError();
        } catch (org.apache.pig.tools.pigscript.parser.ParseException e) {
            throw new IOException(e);
        }

    }

    static public void copyFromClusterToLocal(MiniGenericCluster cluster,
            String fileNameOnCluster, String localFileName) throws IOException {
        if(Util.WINDOWS){
            fileNameOnCluster = fileNameOnCluster.replace('\\','/');
            localFileName = localFileName.replace('\\','/');
        }
	    File parent = new File(localFileName).getParentFile();
	    if (!parent.exists()) {
	        parent.mkdirs();
	    }
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
        if(Util.WINDOWS){
            filename = filename.replace('\\','/');
        }
        if (context.getExecType() == ExecType.MAPREDUCE || context.getExecType().name().equals("TEZ")) {
            return FileLocalizer.hadoopify(filename, context);
        } else if (context.getExecType() == ExecType.LOCAL) {
            return filename;
        } else {
            throw new IllegalStateException("ExecType: " + context.getExecType());
        }
    }

    public static Object getPigConstant(String pigConstantAsString) throws ParserException {
        QueryParserDriver queryParser = new QueryParserDriver( new PigContext(),
        		"util", new HashMap<String, String>() ) ;
        return queryParser.parseConstant(pigConstantAsString);
    }

    /**
     * Parse list of strings in to list of tuples, convert quoted strings into
     * @param tupleConstants
     * @return
     * @throws ParserException
     */
    public static List<Tuple> getTuplesFromConstantTupleStrings(String[] tupleConstants) throws ParserException {
        List<Tuple> result = new ArrayList<Tuple>(tupleConstants.length);
        for(int i = 0; i < tupleConstants.length; i++) {
            result.add((Tuple) getPigConstant(tupleConstants[i]));
        }
        return result;
    }

    /**
     * Parse list of strings in to list of tuples, convert quoted strings into
     * DataByteArray
     * @param tupleConstants
     * @return
     * @throws ParserException
     * @throws ExecException
     */
    public static List<Tuple> getTuplesFromConstantTupleStringAsByteArray(String[] tupleConstants)
    throws ParserException, ExecException {
        List<Tuple> tuples = getTuplesFromConstantTupleStrings(tupleConstants);
        for(Tuple t : tuples){
            convertStringToDataByteArray(t);
        }
        return tuples;
    }

    /**
     * Convert String objects in argument t to DataByteArray objects
     * @param t
     * @throws ExecException
     */
    private static void convertStringToDataByteArray(Tuple t) throws ExecException {
        if(t == null)
            return;
        for(int i=0; i<t.size(); i++){
            Object col = t.get(i);
            if(col == null)
                continue;
            if(col instanceof String){
                DataByteArray dba = (col == null) ?
                        null : new DataByteArray((String)col);
                t.set(i, dba);
            }else if(col instanceof Tuple){
                convertStringToDataByteArray((Tuple)col);
            }else if(col instanceof DataBag){
                Iterator<Tuple> it = ((DataBag)col).iterator();
                while(it.hasNext()){
                    convertStringToDataByteArray(it.next());
                }
            }


        }
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

    /**
     * Run default set of optimizer rules on new logical plan
     * @param lp
     * @return optimized logical plan
     * @throws FrontendException
     */
    public static  LogicalPlan optimizeNewLP(
            LogicalPlan lp)
    throws FrontendException{
        DanglingNestedNodeRemover DanglingNestedNodeRemover = new DanglingNestedNodeRemover( lp );
        DanglingNestedNodeRemover.visit();

        UidResetter uidResetter = new UidResetter( lp );
        uidResetter.visit();

        SchemaResetter schemaResetter =
                new SchemaResetter( lp, true /*disable duplicate uid check*/ );
        schemaResetter.visit();

        StoreAliasSetter storeAliasSetter = new StoreAliasSetter( lp );
        storeAliasSetter.visit();

        // run optimizer
        org.apache.pig.newplan.logical.optimizer.LogicalPlanOptimizer optimizer =
            new org.apache.pig.newplan.logical.optimizer.LogicalPlanOptimizer(lp, 100, null);
        optimizer.optimize();

        SortInfoSetter sortInfoSetter = new SortInfoSetter( lp );
        sortInfoSetter.visit();

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
            LogicalPlan lp, PigContext pc)
    throws Exception {
         LogToPhyTranslationVisitor visitor = new LogToPhyTranslationVisitor(lp);
        visitor.setPigContext(pc);
        visitor.visit();
        return visitor.getPhysicalPlan();
    }

    public static MROperPlan buildMRPlan(PhysicalPlan pp, PigContext pc) throws Exception{
        MRCompiler comp = new MRCompiler(pp, pc);
        comp.compile();
        comp.aggregateScalarsFiles();
        comp.connectSoftLink();
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

    public static MROperPlan buildMRPlan(String query, PigContext pc) throws Exception {
        LogicalPlan lp = Util.parse(query, pc);
        Util.optimizeNewLP(lp);
        PhysicalPlan pp = Util.buildPhysicalPlanFromNewLP(lp, pc);
        MROperPlan mrp = Util.buildMRPlanWithOptimizer(pp, pc);
        return mrp;
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
        pri.stderrContents = getContents(cmdProc.getErrorStream());
        pri.stdoutContents = getContents(cmdProc.getInputStream());
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

    public static String[] readOutput(PigContext pigContext,
            String fileName) throws IOException {
        Configuration conf = ConfigurationUtil.toConfiguration(
                pigContext.getProperties());
        return readOutput(FileSystem.get(conf), fileName);
    }

    public static void printPlan(LogicalPlan logicalPlan ) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(out);
        LogicalPlanPrinter pp = new LogicalPlanPrinter(logicalPlan,ps);
        pp.visit();
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
            reader.close();
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

    public static LogicalPlan buildLp(PigServer pigServer, String query)
    throws Exception {
    	pigServer.setBatchOn();
    	pigServer.registerQuery( query );
        java.lang.reflect.Method buildLp = pigServer.getClass().getDeclaredMethod("buildLp");
        buildLp.setAccessible(true);
        return (LogicalPlan ) buildLp.invoke( pigServer );
    }

    public static PhysicalPlan buildPp(PigServer pigServer, String query)
    throws Exception {
        LogicalPlan lp = buildLp( pigServer, query );
        lp.optimize(pigServer.getPigContext());
        return ((HExecutionEngine)pigServer.getPigContext().getExecutionEngine()).compile(lp, 
                pigServer.getPigContext().getProperties());
    }

    public static LogicalPlan parse(String query, PigContext pc) throws FrontendException {
        Map<String, String> fileNameMap = new HashMap<String, String>();
        QueryParserDriver parserDriver = new QueryParserDriver( pc, "test", fileNameMap );
        org.apache.pig.newplan.logical.relational.LogicalPlan lp = parserDriver.parse( query );

        lp.validate(pc, "test", false);
        return lp;
    }

    public static LogicalPlan parseAndPreprocess(String query, PigContext pc) throws FrontendException {
        Map<String, String> fileNameMap = new HashMap<String, String>();
        QueryParserDriver parserDriver = new QueryParserDriver( pc, "test", fileNameMap );
        org.apache.pig.newplan.logical.relational.LogicalPlan lp = parserDriver.parse( query );

        lp.validate(pc, "test", false);
        return lp;
    }


    /**
     * Replaces any alias in given schema that has name that starts with
     *  "NullAlias" with null . it does  a case insensitive comparison of
     *  the alias name
     * @param sch
     */
    public static void schemaReplaceNullAlias(Schema sch){
        if(sch == null)
            return ;
        for(FieldSchema fs : sch.getFields()){
            if(fs.alias != null && fs.alias.toLowerCase().startsWith("nullalias")){
                fs.alias = null;
            }
            schemaReplaceNullAlias(fs.schema);
        }
    }


    static public void checkQueryOutputsAfterSort(Iterator<Tuple> actualResultsIt,
            Tuple[] expectedResArray) {
        List<Tuple> list = new ArrayList<Tuple>();
        Collections.addAll(list, expectedResArray);
        checkQueryOutputsAfterSort(actualResultsIt, list);
    }


    static private void convertBagToSortedBag(Tuple t) {
        for (int i=0;i<t.size();i++) {
           Object obj = null;
           try {
               obj = t.get(i);
           } catch (ExecException e) {
               // shall not happen
           }
           if (obj instanceof DataBag) {
                DataBag bag = (DataBag)obj;
                Iterator<Tuple> iter = bag.iterator();
                DataBag sortedBag = DefaultBagFactory.getInstance().newSortedBag(null);
                while (iter.hasNext()) {
                    Tuple t2 = iter.next();
                    sortedBag.add(t2);
                    convertBagToSortedBag(t2);
                }
                try {
                    t.set(i, sortedBag);
                } catch (ExecException e) {
                    // shall not happen
                }
           }
        }
    }

    static public void checkQueryOutputsAfterSortRecursive(Iterator<Tuple> actualResultsIt,
            String[] expectedResArray, String schemaString) throws IOException {
        LogicalSchema resultSchema = org.apache.pig.impl.util.Utils.parseSchema(schemaString);
        checkQueryOutputsAfterSortRecursive(actualResultsIt, expectedResArray, resultSchema);
    }
          /**
     * Helper function to check if the result of a Pig Query is in line with
     * expected results. It sorts actual and expected string results before comparison
     *
     * @param actualResultsIt Result of the executed Pig query
     * @param expectedResArray Expected string results to validate against
     * @param fs fieldSchema of expecteResArray
     * @throws IOException
     */
    static public void checkQueryOutputsAfterSortRecursive(Iterator<Tuple> actualResultsIt,
            String[] expectedResArray, LogicalSchema schema) throws IOException {
        LogicalFieldSchema fs = new LogicalFieldSchema("tuple", schema, DataType.TUPLE);
        ResourceFieldSchema rfs = new ResourceFieldSchema(fs);

        LoadCaster caster = new Utf8StorageConverter();
        List<Tuple> actualResList = new ArrayList<Tuple>();
        while(actualResultsIt.hasNext()){
            actualResList.add(actualResultsIt.next());
        }

        List<Tuple> expectedResList = new ArrayList<Tuple>();
        for (String str : expectedResArray) {
            Tuple newTuple = caster.bytesToTuple(str.getBytes(), rfs);
            expectedResList.add(newTuple);
        }

        for (Tuple t : actualResList) {
            convertBagToSortedBag(t);
        }

        for (Tuple t : expectedResList) {
            convertBagToSortedBag(t);
        }

        Collections.sort(actualResList);
        Collections.sort(expectedResList);

        Assert.assertEquals("Comparing actual and expected results. ",
                expectedResList, actualResList);
    }

    public static String readFile(File file) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(file));
        String result = "";
        String line;
        while ((line=reader.readLine())!=null) {
            result += line;
            result += "\n";
        }
        reader.close();
        return result;
    }

    /**
     * this removes the signature from the serialized plan changing the way the
     * unique signature is generated should not break this test
     * @param plan the plan to canonicalize
     * @return the cleaned up plan
     */
    public static String removeSignature(String plan) {
        return plan.replaceAll("','','[^']*','scope','true'\\)\\)", "','','','scope','true'))");
    }

    public static boolean isHadoop23() {
        String version = org.apache.hadoop.util.VersionInfo.getVersion();
        if (version.matches("\\b0\\.23\\..+\\b"))
            return true;
        return false;
    }

    public static boolean isHadoop203plus() {
        String version = org.apache.hadoop.util.VersionInfo.getVersion();
        if (version.matches("\\b0\\.20\\.2\\b"))
            return false;
        return true;
    }

    public static boolean isHadoop205() {
        String version = org.apache.hadoop.util.VersionInfo.getVersion();
        if (version.matches("\\b0\\.20\\.205\\..+"))
            return true;
        return false;
    }

    public static boolean isHadoop1_x() {
        String version = org.apache.hadoop.util.VersionInfo.getVersion();
        if (version.matches("\\b1\\.*\\..+"))
            return true;
        return false;
    }

    public static void assertParallelValues(long defaultParallel,
                                             long requestedParallel,
                                             long estimatedParallel,
                                             long runtimeParallel,
                                             Configuration conf) {
        assertConfLong(conf, "pig.info.reducers.default.parallel", defaultParallel);
        assertConfLong(conf, "pig.info.reducers.requested.parallel", requestedParallel);
        assertConfLong(conf, "pig.info.reducers.estimated.parallel", estimatedParallel);
        assertConfLong(conf, "mapred.reduce.tasks", runtimeParallel);
    }

    private static void assertConfLong(Configuration conf, String param, long expected) {
        assertEquals("Unexpected value found in configs for " + param, expected, conf.getLong(param, -1));
    }

    public static boolean isHadoop2_0() {
        String version = org.apache.hadoop.util.VersionInfo.getVersion();
        if (version.matches("\\b2\\.\\d\\..+"))
            return true;
        return false;
    }

    /**
     * Returns a PathFilter that filters out filenames that start with _.
     * @return PathFilter
     */
    public static PathFilter getSuccessMarkerPathFilter() {
        return new PathFilter() {
            @Override
            public boolean accept(Path p) {
                return !p.getName().startsWith("_");
            }
        };
    }

    /**
     *
     * @param expected
     *            Exception class that is expected to be thrown
     * @param found
     *            Exception that occurred in the test
     * @param message
     *            expected String to verify against
     */
    public static void assertExceptionAndMessage(Class<?> expected,
            Exception found, String message) {
        assertEquals(expected, found.getClass());
        assertEquals(found.getMessage(), message);
    }

    /**
     * Called to reset ThreadLocal or static states that PigServer depends on
     * when a test suite has testcases switching between LOCAL and MAPREDUCE/TEZ
     * execution modes
     */
    public static void resetStateForExecModeSwitch() {
        FileLocalizer.setInitialized(false);
        // TODO: once we have Tez local mode, we can get rid of this. For now,
        // if we run this test suite in Tez mode and there are some tests
        // in LOCAL mode, we need to set ScriptState to
        // null to force ScriptState gets initialized every time.
        ScriptState.start(null);
    }

    public static boolean isMapredExecType(ExecType execType) {
        return execType == ExecType.MAPREDUCE;
    }

}

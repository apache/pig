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

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MRCompiler;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROperPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.LogToPhyTranslationVisitor;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.logicalLayer.LogicalPlan;
import org.apache.pig.impl.logicalLayer.parser.ParseException;
import org.apache.pig.impl.logicalLayer.parser.QueryParser;
import org.apache.pig.impl.logicalLayer.schema.Schema;

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
    
    static public Map<Object, Object> createMap(String[] contents)
    {
        Map<Object, Object> m = new HashMap<Object, Object>();
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
		PrintWriter pw = new PrintWriter(new OutputStreamWriter(new FileOutputStream(f), "UTF-8"));
		for (int i=0; i<inputData.length; i++){
			pw.println(inputData[i]);
		}
		pw.close();
		return f;
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
			Assert.assertEquals(expected, actual);
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
	    BufferedReader reader = new BufferedReader(new FileReader(localFileName));
	    String line = null;
	    List<String> contents = new ArrayList<String>();
	    while((line = reader.readLine()) != null) {
	        contents.add(line);
	    }
	    Util.createInputFile(cluster, fileNameOnCluster, contents.toArray(new String[0]));
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
	
	   /**
     * Helper method to construct URI for local file system. For unix, it will
     * put "file:" in the front of the path; For Windows, it will put "file:/" in 
     * front of the path, and also call encodeEscape to replace "\" with "\\"
     * 
     * @param str absolute path (under cygwin, should be a windows style path)
     * @return The resulting string
     */
    public static String generateURI(String path)
    {
        if (System.getProperty("os.name").toUpperCase().startsWith("WINDOWS"))
            return "file:/"+encodeEscape(path);
        return "file:"+path;
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
    
    static Object getPigConstant(String pigConstantAsString) throws ParseException {
        ByteArrayInputStream stream = new ByteArrayInputStream(pigConstantAsString.getBytes()) ;
        QueryParser queryParser = new QueryParser(stream) ;
        return queryParser.Datum();
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
    
    public static MROperPlan buildMRPlan(PhysicalPlan pp, PigContext pc) throws Exception{
        MRCompiler comp = new MRCompiler(pp, pc);
        comp.compile();
        return comp.getMRPlan();	
    }
    
    public static void registerMultiLineQuery(PigServer pigServer, String query) throws IOException {
        File f = File.createTempFile("tmp", "");
        PrintWriter pw = new PrintWriter(f);
        pw.println(query);
        pw.close();
        pigServer.registerScript(f.getCanonicalPath());
    }
}

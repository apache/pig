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

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import static java.util.regex.Matcher.quoteReplacement;
import junit.framework.Assert;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.*;

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
    
    static public Map<Object, Object> createMap(String[] contents)
    {
        Map<Object, Object> m = new HashMap<Object, Object>();
        for(int i = 0; i < contents.length; ) {
            m.put(contents[i], contents[i+1]);
            i += 2;
        }
        return m;
    }

    static public DataByteArray[] toDataByteArrays(String[] input) {
        DataByteArray[] dbas = new DataByteArray[input.length];
        for (int i = 0; i < input.length; i++) {
            dbas[i] = (input[i] == null)?null:new DataByteArray(input[i].getBytes());
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
		PrintWriter pw = new PrintWriter(f);
		for (int i=0; i<inputData.length; i++){
			pw.println(inputData[i]);
		}
		pw.close();
		return f;
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
}

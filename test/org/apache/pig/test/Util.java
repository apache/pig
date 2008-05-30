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
import java.util.Iterator;
import static java.util.regex.Matcher.quoteReplacement;

import org.apache.pig.data.*;
import org.junit.Assert;

public class Util {
    // Helper Functions
    // =================
    static public Tuple loadFlatTuple(Tuple t, int[] input) throws IOException {
        for (int i = 0; i < input.length; i++) {
            t.setField(i, input[i]);
        }
        return t;
    }

    static public Tuple loadTuple(Tuple t, String[] input) throws IOException {
        for (int i = 0; i < input.length; i++) {
            t.setField(i, input[i]);
        }
        return t;
    }

    static public Tuple loadNestTuple(Tuple t, int[] input) throws IOException {
        DataBag bag = BagFactory.getInstance().newDefaultBag();
        for(int i = 0; i < input.length; i++) {
            Tuple f = new Tuple(1);
            f.setField(0, input[i]);
            bag.add(f);
        }
        t.setField(0, bag);
        return t;
    }

    static public Tuple loadNestTuple(Tuple t, int[][] input) throws IOException {
        for (int i = 0; i < input.length; i++) {
            DataBag bag = BagFactory.getInstance().newDefaultBag();
            Tuple f = loadFlatTuple(new Tuple(input[i].length), input[i]);
            bag.add(f);
            t.setField(i, bag);
        }
        return t;
    }

    static public Tuple loadTuple(Tuple t, String[][] input) throws IOException {
        for (int i = 0; i < input.length; i++) {
            DataBag bag = BagFactory.getInstance().newDefaultBag();
            Tuple f = loadTuple(new Tuple(input[i].length), input[i]);
            bag.add(f);
            t.setField(i, bag);
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
			Assert.assertEquals(expected, actual);
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

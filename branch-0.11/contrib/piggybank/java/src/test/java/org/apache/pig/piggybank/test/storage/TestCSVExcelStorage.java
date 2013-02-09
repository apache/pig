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

package org.apache.pig.piggybank.test.storage;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.test.Util;
import org.junit.Assert;
import org.junit.Test;

public class TestCSVExcelStorage {

    protected static final Log LOG = LogFactory.getLog(TestCSVExcelStorage.class);

    private PigServer pigServer;
    //private MiniCluster cluster;

    Properties props = new Properties();
    ArrayList<String> testMsgs = new ArrayList<String>();

    String testFileCommaName = "testFileComma.csv";
    String testFileTabName = "testFileTab.csv";

    String testStrComma =
    	"John,Doe,10\n" +
    	"Jane, \"nee, Smith\",20\n" +
    	",,\n" +
    	"\"Mac \"\"the knife\"\"\",Cohen,30\n" +
    	"\"Conrad\n" +
    	"Emil\",Dinger,40\n" +
    	"1st Field,\"A poem that continues\n" +
    	"for several lines\n" +
    	"do we\n" +
    	"handle that?\",Good,Fairy\n";

    String[] testStrCommaArray =
    	new String[] {
    		"John,Doe,10",
    		"Jane, \"nee, Smith\",20",
    		",,",
    		"\"Mac \"\"the knife\"\"\",Cohen,30",
    		"\"Conrad\nEmil\",Dinger,40",
                "Emil,\"\nDinger\",40",
                "Quote problem,\"My \"\"famous\"\"\nsong\",60",
    		"1st Field,\"A poem that continues\nfor several lines\ndo we\nhandle that?\",Good,Fairy",
    };

    @SuppressWarnings("serial")
	ArrayList<Tuple> testStrCommaYesMultilineResultTuples =
    	new ArrayList<Tuple>() {
    	{
    		add(Util.createTuple(new String[] {"John","Doe","10"}));
    		add(Util.createTuple(new String[] {"Jane", " nee, Smith","20"}));
    		add(Util.createTuple(new String[] {"", "", ""}));
    		add(Util.createTuple(new String[] {"Mac \"the knife\"", "Cohen", "30"}));
    		add(Util.createTuple(new String[] {"Conrad\nEmil", "Dinger", "40"}));
                add(Util.createTuple(new String[] {"Emil", "\nDinger", "40"}));
                add(Util.createTuple(new String[] {"Quote problem", "My \"famous\"\nsong", "60"}));
    		add(Util.createTuple(new String[] {"1st Field", "A poem that continues\nfor several lines\ndo we\nhandle that?", "Good", "Fairy"}));
    	}
    };

    @SuppressWarnings("serial")
	ArrayList<Tuple> testStrCommaNoMultilineResultTuples =
    	new ArrayList<Tuple>() {
    	{
    		add(Util.createTuple(new String[] {"John","Doe","10"}));
    		add(Util.createTuple(new String[] {"Jane", " nee, Smith","20"}));
    		add(Util.createTuple(new String[] {"", "", ""}));
    		add(Util.createTuple(new String[] {"Mac \"the knife\"", "Cohen", "30"}));
    		add(Util.createTuple(new String[] {"Conrad"}));
    		add(Util.createTuple(new String[] {"Emil,Dinger,40"}));  // Trailing double quote after Emil eats rest of line
                add(Util.createTuple(new String[] {"Emil"}));
                add(Util.createTuple(new String[] {"Dinger,40"}));  // Trailing double quote after Emil eats rest of line
                add(Util.createTuple(new String[] {"Quote problem", "My \"famous\""}));
                add(Util.createTuple(new String[] {"song,60"}));
    		add(Util.createTuple(new String[] {"1st Field", "A poem that continues"}));
    		add(Util.createTuple(new String[] {"for several lines"}));
    		add(Util.createTuple(new String[] {"do we"}));
    		add(Util.createTuple(new String[] {"handle that?,Good,Fairy"})); // Trailing double quote eats rest of line
    	}
    };

    String testStrTab   =
    	"John\tDoe\t50\n" +
    	"\"Foo and CR last\n" +
    	"bar.\"\t\t\n" +
    	"Frank\tClean\t70";

    String[] testStrTabArray   =
    	new String[] {
    		"John\tDoe\t50",
    		"\"Foo and CR last\nbar.\"\t\t",
    		"Frank\tClean\t70"
    };

    @SuppressWarnings("serial")
	ArrayList<Tuple> testStrTabYesMultilineResultTuples =
    	new ArrayList<Tuple>() {
    	{
    		add(Util.createTuple(new String[] {"John","Doe","50"}));
    		add(Util.createTuple(new String[] {"Foo and CR last\nbar.","",""}));
    		add(Util.createTuple(new String[] {"Frank","Clean","70"}));
    	}
    };

    public TestCSVExcelStorage() throws ExecException, IOException {

        pigServer = new PigServer(ExecType.LOCAL);
        pigServer.getPigContext().getProperties()
                .setProperty("mapred.map.max.attempts", "1");
        pigServer.getPigContext().getProperties()
                .setProperty("mapred.reduce.max.attempts", "1");
        pigServer.getPigContext().getProperties()
        .setProperty("mapreduce.job.end-notification.retry.interval", "100");

        Util.createLocalInputFile(testFileCommaName, testStrCommaArray);
        Util.createLocalInputFile(testFileTabName, testStrTabArray);
    }

    @Test
    public void testSimpleCsv() throws IOException {
        String inputFileName = "TestCSVExcelStorage-simple.txt";
        Util.createLocalInputFile(inputFileName, new String[] {"foo,bar,baz", "fee,foe,fum"});
        String script = "a = load '" + inputFileName + "' using org.apache.pig.piggybank.storage.CSVExcelStorage() " +
        "   as (a:chararray, b:chararray, c:chararray); ";
        Util.registerMultiLineQuery(pigServer, script);
        Iterator<Tuple> it = pigServer.openIterator("a");
        assertEquals(Util.createTuple(new String[] {"foo", "bar", "baz"}), it.next());
    }

    @Test
    public void testQuotedCommas() throws IOException {
        String inputFileName = "TestCSVExcelStorage-quotedcommas.txt";
        Util.createLocalInputFile(inputFileName, new String[] {"\"foo,bar,baz\"", "fee,foe,fum"});
        String script = "a = load '" + inputFileName + "' using org.apache.pig.piggybank.storage.CSVExcelStorage() " +
        "   as (a:chararray, b:chararray, c:chararray); ";
        Util.registerMultiLineQuery(pigServer, script);
        Iterator<Tuple> it = pigServer.openIterator("a");
        assertEquals(Util.createTuple(new String[] {"foo,bar,baz", null, null}), it.next());
        assertEquals(Util.createTuple(new String[] {"fee", "foe", "fum"}), it.next());
    }

    @Test
    public void testQuotedQuotes() throws IOException {
        String inputFileName = "TestCSVExcelStorage-quotedquotes.txt";
        Util.createLocalInputFile(inputFileName,
                new String[] {"\"foo,\"\"bar\"\",baz\"", "\"\"\"\"\"\"\"\""});
        String script = "a = load '" + inputFileName + "' using org.apache.pig.piggybank.storage.CSVExcelStorage() " +
        "   as (a:chararray); ";
        Util.registerMultiLineQuery(pigServer, script);
        Iterator<Tuple> it = pigServer.openIterator("a");
        assertEquals(Util.createTuple(new String[] {"foo,\"bar\",baz"}), it.next());
        assertEquals(Util.createTuple(new String[] {"\"\"\"\""}), it.next());
    }

    @Test
    public void testMultiline() throws IOException {
    	// Read the test file:
        String script =
        	"a = LOAD '" + testFileCommaName + "' " +
        	"USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'YES_MULTILINE');";
        Util.registerMultiLineQuery(pigServer, script);
        compareExpectedActual(testStrCommaYesMultilineResultTuples, "a");

        // Store the test file back down into another file using YES_MULTILINE:
        String testOutFileName = createOutputFileName();
        script = "STORE a INTO '" + testOutFileName + "' USING " +
					"org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'YES_MULTILINE');";
        pigServer.registerQuery(script);

        // Read it back out using YES_MULTILINE, and see whether it's still correct:
        script = "b = LOAD '" + testOutFileName + "' " +
        	"USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'YES_MULTILINE');";
        Util.registerMultiLineQuery(pigServer, script);
        compareExpectedActual(testStrCommaYesMultilineResultTuples, "b");

        // Now read it back again, but multilines turned off:
        script = "c = LOAD '" + testOutFileName + "' " +
        	"USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE');";
        Util.registerMultiLineQuery(pigServer, script);
        compareExpectedActual(testStrCommaNoMultilineResultTuples, "c");

        // Store this re-read test file back down again, into another file using NO_MULTILINE:
        testOutFileName = createOutputFileName();
        script = "STORE c INTO '" + testOutFileName + "' USING " +
					"org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE');";
        pigServer.registerQuery(script);

        // Read it back in, again with NO_MULTILINE and see whether it's still correct:
        script = "d = LOAD '" + testOutFileName + "' " +
        	"USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE');";
        Util.registerMultiLineQuery(pigServer, script);
        compareExpectedActual(testStrCommaNoMultilineResultTuples, "d");

    }

    @Test
    public void testTabDelimiter() throws IOException {
    	// Read the test file:
        String script =
        	"e = LOAD '" + testFileTabName + "' " +
        	"USING org.apache.pig.piggybank.storage.CSVExcelStorage('\t', 'YES_MULTILINE');";
        Util.registerMultiLineQuery(pigServer, script);
        compareExpectedActual(testStrTabYesMultilineResultTuples, "e");

        // Store the test file back down into another file using YES_MULTILINE:
        String testOutFileName = createOutputFileName();
        script = "STORE e INTO '" + testOutFileName + "' USING " +
					"org.apache.pig.piggybank.storage.CSVExcelStorage('\t', 'YES_MULTILINE');";
        pigServer.registerQuery(script);

        // Read it back out using YES_MULTILINE, and see whether it's still correct:
        script = "f = LOAD '" + testOutFileName + "' " +
        	"USING org.apache.pig.piggybank.storage.CSVExcelStorage('\t', 'YES_MULTILINE');";
        Util.registerMultiLineQuery(pigServer, script);
        compareExpectedActual(testStrTabYesMultilineResultTuples, "f");
    }

    private void compareExpectedActual(ArrayList<Tuple> theExpected, String theActualPigVarAlias) throws IOException {
    	Iterator<Tuple> actualIt = pigServer.openIterator(theActualPigVarAlias);
    	Iterator<Tuple> expIt = theExpected.iterator();

    	while (actualIt.hasNext()) {
    		Tuple  actual = actualIt.next();
    		if (!expIt.hasNext())
    			Assert.fail("The input contains more records than expected. First unexpected record: " + actual);
    		Tuple  expected = expIt.next();
    		// The following assert does not work, even if
    		// the two tuples are identical in class (BinSedesTuple)
    		// and content. We need to compare element by element:
    		//assertEquals(expected, actual);
    		for (int i=0; i<expected.size(); i++) {
        		String truthEl  = (String) expected.get(i);
        		String actualEl = new String(((DataByteArray) actual.get(i)).get());
        		assertEquals(truthEl, actualEl);
    		}
    	}
    }

	/*
	 * Hack to get a temp file name to store data into.
	 * The file must not exist when the caller subsequently
	 * tries to write to it. In non-testing code this
	 * would be an intolerable race condition. There's
	 * likely a better way.
	 */
	private String createOutputFileName() throws IOException {
		File f = File.createTempFile("CSVExcelStorageTest", "csv");
        f.deleteOnExit();
        f.delete();
        // On Windows this path will be C:\\..., which
        // causes errors in the Hadoop environment. Replace
        // the backslashes with forward slashes:
		return f.getAbsolutePath().replaceAll("\\\\", "/");
	}

    public static void main(String[] args) {
    	TestCSVExcelStorage tester = null;
    	try {
			tester = new TestCSVExcelStorage();
			tester.testSimpleCsv();
			tester.testQuotedCommas();
			tester.testQuotedQuotes();
    		tester.testMultiline();
    		tester.testTabDelimiter();
System.out.println("CSVExcelStorage() passed all tests.");

		} catch (ExecException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
    }

}

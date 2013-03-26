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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Properties;

import junit.framework.Assert;

import org.apache.commons.lang.StringUtils;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.tools.parameters.ParseException;
import org.apache.pig.test.Util;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestCSVExcelStorage  {

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

    private static final String dataDir = "build/test/tmpdata/";
    private static final String testFile = "csv_excel_data";

    private PigServer pig;

    @Before
    public void setup() throws IOException {
        pig = new PigServer(ExecType.LOCAL);
        pig.getPigContext().getProperties()
                 .setProperty("mapred.map.max.attempts", "1");
        pig.getPigContext().getProperties()
                 .setProperty("mapred.reduce.max.attempts", "1");
        pig.getPigContext().getProperties()
                 .setProperty("mapreduce.job.end-notification.retry.interval", "100");

        Util.deleteDirectory(new File(dataDir));

        pig.mkdirs(dataDir);

        Util.createLocalInputFile(dataDir + testFile,
            new String[] {
                "int_field,long_field,float_field,double_field,chararray_field,bytearray_field",
                "1,10,2.718,3.14159,qwerty,uiop",
                "1,10,2.718,3.14159,,",
                "1,10,,3.15159,,uiop",
                "1,10,,3.15159,,uiop, moose",
                "1,,\"2.718\",,\"qwerty\",\"uiop\"",
                "1,,,,\"",
                "qwe",
                "rty\", uiop",
                "1,,,,\"qwe,rty\",uiop",
                "1,,,,\"q\"\"wert\"\"y\", uiop",
                "1,,,,qwerty,\"u\"\"io\"\"p\""
        });

        Util.createLocalInputFile(testFileCommaName, testStrCommaArray);
        Util.createLocalInputFile(testFileTabName, testStrTabArray);
    }

    @After
    public void cleanup() throws IOException {
        Util.deleteDirectory(new File(dataDir));
        pig.shutdown();
    }

    // Load a simple CSV file with no escapes or special options
    @Test
    public void testSimpleCsv() throws IOException {
        String inputFileName = "TestCSVExcelStorage-simple.txt";
        Util.createLocalInputFile(inputFileName, new String[] {"foo,bar,baz", "fee,foe,fum"});
        String script = "a = load '" + inputFileName + "' using org.apache.pig.piggybank.storage.CSVExcelStorage() " +
        "   as (a:chararray, b:chararray, c:chararray); ";
        Util.registerMultiLineQuery(pig, script);
        Iterator<Tuple> it = pig.openIterator("a");
        Assert.assertEquals(Util.createTuple(new String[] {"foo", "bar", "baz"}), it.next());
    }

    // Load a field with commas in it (escaped with quotes)
    @Test
    public void testQuotedCommas() throws IOException {
        String inputFileName = "TestCSVExcelStorage-quotedcommas.txt";
        Util.createLocalInputFile(inputFileName, new String[] {"\"foo,bar,baz\"", "fee,foe,fum"});
        String script = "a = load '" + inputFileName + "' using org.apache.pig.piggybank.storage.CSVExcelStorage() " +
        "   as (a:chararray, b:chararray, c:chararray); ";
        Util.registerMultiLineQuery(pig, script);
        Iterator<Tuple> it = pig.openIterator("a");
        Assert.assertEquals(Util.createTuple(new String[] {"foo,bar,baz", null, null}), it.next());
        Assert.assertEquals(Util.createTuple(new String[] {"fee", "foe", "fum"}), it.next());
    }

    // Two quotes characters should be interpreted as a single literal quotes character
    @Test
    public void testQuotedQuotes() throws IOException {
        String inputFileName = "TestCSVExcelStorage-quotedquotes.txt";
        Util.createLocalInputFile(inputFileName,
                new String[] {"\"foo,\"\"bar\"\",baz\"", "\"\"\"\"\"\"\"\""});
        String script = "a = load '" + inputFileName + "' using org.apache.pig.piggybank.storage.CSVExcelStorage() " +
        "   as (a:chararray); ";
        Util.registerMultiLineQuery(pig, script);
        Iterator<Tuple> it = pig.openIterator("a");
        Assert.assertEquals(Util.createTuple(new String[] {"foo,\"bar\",baz"}), it.next());
        Assert.assertEquals(Util.createTuple(new String[] {"\"\"\"\""}), it.next());
    }

    // Handle newlines in fields
    @Test
    public void testMultiline() throws IOException {
        // Read the test file:
        String script =
            "a = LOAD '" + testFileCommaName + "' " +
            "USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'YES_MULTILINE');";
        pig.registerQuery(script);
        compareExpectedActual(testStrCommaYesMultilineResultTuples, "a");

        // Store the test file back down into another file using YES_MULTILINE:
        String testOutFileName = createOutputFileName();
        script = "STORE a INTO '" + testOutFileName + "' USING " +
                    "org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'YES_MULTILINE', 'UNIX');";
        pig.registerQuery(script);

        // Read it back out using YES_MULTILINE, and see whether it's still correct:
        script = "b = LOAD '" + testOutFileName + "' " +
            "USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'YES_MULTILINE');";
        pig.registerQuery(script);
        compareExpectedActual(testStrCommaYesMultilineResultTuples, "b");

        // Now read it back again, but multilines turned off:
        script = "c = LOAD '" + testOutFileName + "' " +
            "USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE');";
        pig.registerQuery(script);
        compareExpectedActual(testStrCommaNoMultilineResultTuples, "c");

        // Store this re-read test file back down again, into another file using NO_MULTILINE:
        testOutFileName = createOutputFileName();
        script = "STORE c INTO '" + testOutFileName + "' USING " +
                    "org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX');";
        pig.registerQuery(script);

        // Read it back in, again with NO_MULTILINE and see whether it's still correct:
        script = "d = LOAD '" + testOutFileName + "' " +
            "USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE');";
        pig.registerQuery(script);
        compareExpectedActual(testStrCommaNoMultilineResultTuples, "d");
    }

    // Handle non-comma delimiters
    @Test
    public void testTabDelimiter() throws IOException {
        // Read the test file:
        String script =
            "e = LOAD '" + testFileTabName + "' " +
            "USING org.apache.pig.piggybank.storage.CSVExcelStorage('\t', 'YES_MULTILINE');";
        pig.registerQuery(script);
        compareExpectedActual(testStrTabYesMultilineResultTuples, "e");

        // Store the test file back down into another file using YES_MULTILINE:
        String testOutFileName = createOutputFileName();
        script = "STORE e INTO '" + testOutFileName + "' USING " +
                    "org.apache.pig.piggybank.storage.CSVExcelStorage('\t', 'YES_MULTILINE');";
        pig.registerQuery(script);

        // Read it back out using YES_MULTILINE, and see whether it's still correct:
        script = "f = LOAD '" + testOutFileName + "' " +
            "USING org.apache.pig.piggybank.storage.CSVExcelStorage('\t', 'YES_MULTILINE');";
        pig.registerQuery(script);
        compareExpectedActual(testStrTabYesMultilineResultTuples, "f");
    }

    private void compareExpectedActual(ArrayList<Tuple> theExpected, String theActualPigVarAlias) throws IOException {
        Iterator<Tuple> actualIt = pig.openIterator(theActualPigVarAlias);
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
                Assert.assertEquals(truthEl, actualEl);
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

    // Comprehensive loader test: uses several datatypes; skips the header; 
    //                            handles missing/extra fields; handles quotes, commas, newlines
    @Test
    public void load() throws IOException, ParseException {
        String schema = "i: int, l: long, f: float, d: double, c: chararray, b: bytearray";

        pig.registerQuery(
            "data = load '" + dataDir + testFile + "' " +
            "using org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'YES_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER') " + 
            "AS (" + schema + ");"
        );

        Iterator<Tuple> data = pig.openIterator("data");
        String[] expected = {
            // a header in csv_excel_data.csv should be skipped due to 'SKIP_INPUT_HEADER' being set in test_csv_storage_load.pig
            "(1,10,2.718,3.14159,qwerty,uiop)",  // basic data types
            "(1,10,2.718,3.14159,,)",            // missing fields at end
            "(1,10,,3.15159,,uiop)",             // missing field in the middle
            "(1,10,,3.15159,,uiop)",             // extra field (input data has "moose" after "uiop")
            "(1,,2.718,,qwerty,uiop)",           // quoted regular fields (2.718, qwerty, and uiop in quotes)
            "(1,,,,\nqwe\nrty, uiop)",           // newlines in quotes
            "(1,,,,qwe,rty,uiop)",               // commas in quotes
            "(1,,,,q\"wert\"y, uiop)",           // quotes in quotes
            "(1,,,,qwerty,u\"io\"p)"             // quotes in quotes at the end of a line
        };

        Assert.assertEquals(StringUtils.join(expected, "\n"), StringUtils.join(data, "\n"));
    }

    // Comprehensive storer test for non-container fields:
    // uses several datatypes, writes a header, handle nulls, quotes, commas, newlines
    @Test
    public void storeScalarTypes() throws IOException, ParseException {
        String input = testFile;
        String schema = "int_field: int, long_field: long, float_field: float, double_field: double, " +
                        "chararray_field: chararray, bytearray_field: bytearray";
        String output = "csv_excel_scalar_output";

        // Store data

        pig.registerQuery(
            "data = load '" + dataDir + input + "' " +
            "using org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'YES_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER') " + 
            "AS (" + schema + ");"
        );
        pig.store("data", dataDir + output, 
                  "org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'YES_MULTILINE', 'UNIX', 'WRITE_OUTPUT_HEADER')");

        // Read it back

        pig.registerQuery(
            "data = load '" + dataDir + output + "' " +
            "using TextLoader() as (line: chararray);"
        );

        Iterator<Tuple> data = pig.openIterator("data");
        String[] expected = {
            // header should be written because we used the 'WRITE_OUTPUT_HEADER' argument
            "(int_field,long_field,float_field,double_field,chararray_field,bytearray_field)",
            "(1,10,2.718,3.14159,qwerty,uiop)",
            "(1,10,2.718,3.14159,,)",
            "(1,10,,3.15159,,uiop)",
            "(1,10,,3.15159,,uiop)",
            "(1,,2.718,,qwerty,uiop)",
            "(1,,,,\")",                            // since we are just using TextLoader for verification
            "(qwe)",                                // it treats the linebreaks as meaning separate records
            "(rty\", uiop)",                        // but as shown in the load() test, CSVExcelStorage will read these properly
            "(1,,,,\"qwe,rty\",uiop)",
            "(1,,,,\"q\"\"wert\"\"y\", uiop)",
            "(1,,,,qwerty,\"u\"\"io\"\"p\")"
        };

        Assert.assertEquals(StringUtils.join(expected, "\n"), StringUtils.join(data, "\n"));
    }

    // Test that tuples/bags/maps are stored as strings
    @Test
    public void storeComplexTypes() throws IOException, ParseException {
        String input = "csv_excel_complex_input";
        String schema = "a:(b:int,c:int),d:(e:int,f:(g:int,h:int)),i:{j:(k:int,l:int)},m:{n:(o:int,p:{q:(r:int,s:int)})},t:[int],u:[[int]]";
        String output = "csv_excel_complex_output";

        Util.createLocalInputFile(dataDir + input,
            new String[] {
                "(1,2)|(1,(2,3))|{(1,2),(3,4)}|{(1,{(2,3),(4,5)}),(6,{(7,8),(9,0)})}|[a#1,b#2]|[a#[b#1,c#2],d#[e#3,f#4]]",
                "(1,)|(1,(2,))|{(1,),(3,)}|{(1,{(,3),(,5)}),(6,{(7,),(9,)})}|[a#,b#2]|[a#[b#,c#2],d#]"
        });

         pig.registerQuery(
            "data = load '" + dataDir + input + "' " +
            "using PigStorage('|')" + 
            "AS (" + schema + ");"
        );
        pig.store("data", dataDir + output, 
                  "org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'YES_MULTILINE', 'UNIX', 'SKIP_OUTPUT_HEADER')");

        pig.registerQuery(
            "data = load '" + dataDir + output + "' " +
            "using TextLoader() as (line: chararray);"
        );

        Iterator<Tuple> data = pig.openIterator("data");
        String[] expected = {
            "(\"(1,2)\",\"(1,(2,3))\",\"{(1,2),(3,4)}\",\"{(1,{(2,3),(4,5)}),(6,{(7,8),(9,0)})}\",\"{b=2, a=1}\",\"{d={f=4, e=3}, a={b=1, c=2}}\")",
            "(\"(1,)\",\"(1,(2,))\",\"{(1,),(3,)}\",\"{(1,{(,3),(,5)}),(6,{(7,),(9,)})}\",\"{b=2, a=null}\",\"{d=null, a={b=null, c=2}}\")"
        };

        Assert.assertEquals(StringUtils.join(expected, "\n"), StringUtils.join(data, "\n"));
    }
}

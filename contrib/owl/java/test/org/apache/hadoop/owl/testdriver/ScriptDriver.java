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
package org.apache.hadoop.owl.testdriver;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.owl.client.CommandLineOwl;
import org.apache.hadoop.owl.client.OwlClient;
import org.apache.hadoop.owl.common.OwlException;
import org.apache.hadoop.owl.common.OwlUtil;
import org.apache.hadoop.owl.common.OwlUtil.Verb;
import org.apache.hadoop.owl.protocol.OwlObject;
import org.apache.hadoop.owl.protocol.OwlResource;
import org.apache.hadoop.owl.protocol.OwlResultObject;

/**
 * The Driver to read a script file and either generate the expected file or
 * validate against existing expected file.
 */
public class ScriptDriver {

    /** The directory where scripts are present */
    private static String SCRIPT_DIR = "ci/test_scripts";

    /** The directory where script results are present */
    private static String RESULTS_DIR = "ci/test_results";

    /** The COMMANDTOKEN string. */
    private static String COMMANDTOKEN = "<COMMAND>";

    /** The RESULTTOKEN string. */
    private static String RESULTTOKEN = "<RESULT>";

    /** The EXCEPTIONTOKEN string. */
    private static String EXCEPTIONTOKEN = "<EXCEPTION>";

    /** The IGNORETOKEN string. */
    private static String IGNORETOKEN = "<IGNORE>";

    /** The SUCCESSTOKEN string. */
    private static String SUCCESSTOKEN = "<SUCCESS>";

    private static final String PROPERTY_OWL_TEST_SCRIPTS_DIR = "org.apache.hadoop.owl.test.scriptdir";
    private static final String PROPERTY_OWL_TEST_RESULTS_DIR = "org.apache.hadoop.owl.test.resultsdir";

    private static String getExpectedFile(String expectedFile) {
        String dir = System.getProperty(PROPERTY_OWL_TEST_RESULTS_DIR);
        if ((dir == null)||(dir.length() == 0)){
            return RESULTS_DIR + "/" + expectedFile;
        }else{
            return dir + "/" + expectedFile;
        }        
    }

    private static String getScriptFile(String scriptFile) {
        String dir = System.getProperty(PROPERTY_OWL_TEST_SCRIPTS_DIR);
        if ((dir == null)||(dir.length() == 0)){
            return SCRIPT_DIR + "/" + scriptFile;
        }else{
            return dir + "/" + scriptFile;
        }        
    }

    /**
     * The main entry point for the driver.
     * 
     * @param url the url
     * @param scriptFile the script file
     * @param expectedFile the expected file
     * @throws Exception the test failure exception
     */
    public static void runTest(String url, String scriptFile, String expectedFile) throws Exception {

        scriptFile = getScriptFile(scriptFile);
        expectedFile = getExpectedFile(expectedFile);

        File expFile = new File(expectedFile);

        try { 
            if( ! expFile.exists() ) {
                createExpectedFile(url, scriptFile, expectedFile + ".tmp");

                //First run of test case, fail it after creating the expected.tmp file
                throw new Exception("Expected file " + expectedFile +
                        " not found. Check if newly created file " + expectedFile + ".tmp is " +
                "correct and then rename file");
            } else {
                runTestScript(url, scriptFile, expectedFile);
            }
        } catch(Exception e) {
            //Print exception to stdout to make junit output easier to read
            System.out.println("Test failed : " + e.toString());
            throw e;
        }
    }

    /**
     * Creates the expected file, the file name has a .tmp added so that it has to be manually validated.
     * 
     * @param url the url
     * @param scriptFile the script file
     * @param expectedFile the expected file
     * 
     * @throws Exception the test failure exception
     */
    private static void createExpectedFile(String url, String scriptFile, String expectedFile) throws Exception {

        OwlClient client = new OwlClient(url);

        FileReader fileReader = new FileReader(scriptFile);
        BufferedReader reader = new BufferedReader(fileReader);

        FileWriter fileWriter = new FileWriter(expectedFile);
        BufferedWriter writer = new BufferedWriter(fileWriter);

        //Till first non drop command is seen, all commands will have IGNORE tokens written
        //so that the cleanup steps don't effect the test case
        boolean inCleanupStage = true;

        boolean eof = false;
        StringBuffer buffer = new StringBuffer(1000);

        while (eof == false) {
            eof = CommandLineOwl.readCommand(reader, buffer);

            if( buffer.length() > 0 ) {

                writer.write(COMMANDTOKEN + " " + buffer.toString() + "\n");

                if( inCleanupStage ) {
                    if( OwlUtil.getVerbForCommand(
                            OwlUtil.getCommandFromString(buffer.toString())) != Verb.DELETE ) {
                        //First non drop command
                        inCleanupStage = false;
                    }
                }

                try {
                    OwlResultObject result = client.execute(buffer.toString());

                    if( inCleanupStage ) {
                        writer.write(IGNORETOKEN + "\n");
                    }
                    else if (result.getOutput() != null && result.getOutput().size() != 0) {
                        //Print <RESULTTOKEN> className size
                        writer.write(RESULTTOKEN
                                + " " + result.getOutput().get(0).getClass().getSimpleName() 
                                + " " + result.getOutput().size() 
                                + "\n");

                        for(OwlObject object : result.getOutput() ) {

                            //Zero out creation and modification times to make changes in JSON output easier to detect
                            if( object instanceof OwlResource ) {
                                ((OwlResource) object).setCreationTime(0);
                                ((OwlResource) object).setModificationTime(0);
                            }

                            writer.write(OwlUtil.getJSONFromObject(object) + "\n");
                        }

                    } else {
                        writer.write(SUCCESSTOKEN + "\n");

                    }

                } catch(OwlException e) {
                    if( inCleanupStage ) {
                        writer.write(IGNORETOKEN + "\n");
                    } else {
                        writer.write(EXCEPTIONTOKEN + " " + trimToNewLine(e.toString()) + "\n");
                    }
                }
            }
        }

        reader.close();
        fileReader.close();

        writer.close();
        fileWriter.close();
    }


    /**
     * Run the script and validate output against the expected file.
     * 
     * @param url the url
     * @param scriptFile the script file
     * @param expectedFile the expected file
     * @throws Exception the test failure exception
     */
    private static void runTestScript(String url, String scriptFile, String expectedFile) throws Exception {
        OwlClient client = new OwlClient(url);

        FileReader scriptFileReader = new FileReader(scriptFile);
        BufferedReader scriptReader = new BufferedReader(scriptFileReader);

        FileReader expectedFileReader = new FileReader(expectedFile);
        BufferedReader expectedReader = new BufferedReader(expectedFileReader);

        boolean scriptEof = false;
        StringBuffer buffer = new StringBuffer(1000);

        while (scriptEof == false) {
            scriptEof = CommandLineOwl.readCommand(scriptReader, buffer);

            if( buffer.length() > 0 ) {

                matchCommand(buffer.toString(), expectedReader);

                try {
                    System.out.println("Executing " + buffer.toString());
                    OwlResultObject result = client.execute(buffer.toString());

                    if (result.getOutput() != null && result.getOutput().size() != 0) {
                        matchResults(result.getOutput(), expectedReader);
                    } else {
                        matchSuccess(expectedReader);
                    }

                } catch(OwlException e) {
                    matchException(trimToNewLine(e.toString()), expectedReader);
                }
            }
        }

        scriptReader.close();
        scriptFileReader.close();

        expectedReader.close();
        expectedFileReader.close();        
    }

    /**
     * Match against command token.
     * 
     * @param command the command
     * @param expectedReader the expected file reader
     * 
     * @throws Exception the test failure exception
     */
    private static void matchCommand(String command,
            BufferedReader expectedReader) throws Exception {
        String line = expectedReader.readLine();

        String[] tokens = getTokens(line);

        //Command token not ignored
        if( tokens[0].equals(COMMANDTOKEN) ) {     
            if( ! command.equals(tokens[1])) {
                throw new Exception("Expected command <" + tokens[1] +
                        ", got <"  + command + ">. Expected file might need to be updated.");
            }
        } else {
            throw new Exception("Expected command token, got " + tokens[0]);
        }
    }

    /**
     * Match against exception token.
     * 
     * @param message the message
     * @param expectedReader the expected file reader
     * 
     * @throws Exception
     *             the exception
     */
    private static void matchException(String message,
            BufferedReader expectedReader) throws Exception {
        String line = expectedReader.readLine();

        String[] tokens = getTokens(line);

        if( tokens[0].equals(IGNORETOKEN) ) {
            System.out.println("Ignore command execution status");
            return;
        }

        if( tokens[0].equals(EXCEPTIONTOKEN) ) {
            if( ! message.contains(tokens[1]) ) {
                throw new Exception("Expected exception <" + tokens[1] + ">, got <" +
                        message + ">");
            }
        } else {
            throw new Exception("Command failed with exception <" + message + ">");
        }

    }


    /**
     * Match success token.
     * 
     * @param expectedReader the expected file reader
     * @throws Exception the test failure exception
     */
    private static void matchSuccess(BufferedReader expectedReader) throws Exception {
        String line = expectedReader.readLine();

        String[] tokens = getTokens(line);

        if( tokens[0].equals(IGNORETOKEN) ) {
            System.out.println("Ignore command execution status");
            return;
        }

        if( ! tokens[0].equals(SUCCESSTOKEN) ) {
            if( tokens[0].equals(RESULTTOKEN) ) {
                throw new Exception("Command expected to generate results " + tokens[1]);
            } else {
                throw new Exception("Command succeeded, expected exception " + tokens[1]);
            }
        }         
    }


    /**
     * Match results token and rest values.
     * 
     * @param output the output to match against
     * @param expectedReader the expected file reader
     * 
     * @throws Exception the test failure exception
     */
    private static void matchResults(List<? extends OwlObject> output,
            BufferedReader expectedReader) throws Exception {
        String line = expectedReader.readLine();

        String[] tokens = getTokens(line);

        if( tokens[0].equals(IGNORETOKEN) ) {
            System.out.println("Ignore command execution status");
            return;
        }

        String[] resultInfo = getTokens(tokens[1]);
        int count = Integer.parseInt(resultInfo[1]);

        if( output.size() != count ) {
            System.out.print("Command Results ");
            for( OwlObject result : output ) {
                System.out.println(OwlUtil.getJSONFromObject(result));
            }
            throw new Exception("Expected row count " + count + ", got " + output.size());
        }

        String protocolPackage = OwlObject.class.getPackage().getName();
        @SuppressWarnings("unchecked") Class<? extends OwlObject> owlClass = 
            (Class<? extends OwlObject>) Class.forName( protocolPackage + "." + resultInfo[0] );

        List<OwlObject> expectedResults = new ArrayList<OwlObject>();
        for(int i = 0;i < count;i++) {
            String resultLine = expectedReader.readLine();
            expectedResults.add((OwlObject) OwlUtil.getObjectFromJSON(owlClass, owlClass, resultLine));
        }

        CompareOwlObjects.matchObjects(output, expectedResults);
    }




    /**
     * Gets the tokens.
     * @param line the line
     * @return the tokens array
     * @throws Exception the exception
     */
    private static String[] getTokens(String line) throws Exception {
        StringTokenizer st = new StringTokenizer(line, " ");
        if( ! st.hasMoreTokens() ) {
            throw new Exception("Could not match token for line " + line);
        }

        String tok1 = st.nextToken();
        String tok2 = null;

        if( line.length() > (tok1.length() + 1)) {
            tok2 = line.substring(tok1.length() + 1);
        }

        return new String[] { tok1, tok2 };
    }

    /**
     * Trim to new line.
     * @param message the message
     * @return the string
     */
    private static String trimToNewLine(String message) {
        int newLineIndex = message.indexOf("\r");
        if( newLineIndex == -1) {
            newLineIndex = message.indexOf("\n");
        }

        if( newLineIndex != -1 ) {
            return message.substring(0, newLineIndex);
        }

        return message;
    }
}

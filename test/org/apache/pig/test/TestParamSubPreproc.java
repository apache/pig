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

import org.junit.Before;
import org.junit.Test;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;

import org.apache.pig.tools.parameters.*;

import junit.framework.TestCase;

public class TestParamSubPreproc extends TestCase {

    private final Log log = LogFactory.getLog(getClass());

    private BufferedReader pigIStream;
    private FileWriter pigOStream;
    private FileInputStream pigExResultStream;
    private String basedir;


    public TestParamSubPreproc(String name) {
        super(name);
        basedir = "test/org/apache/pig/test/data";
    }

    /* Test case 1   
     * Use a parameter within a pig script and provide value on the command line.
     */
    @Test
    public void testCmdlineParam() throws Exception{
        log.info("Starting test testCmdlineParam() ...");
        try {
            ParameterSubstitutionPreprocessor ps = new ParameterSubstitutionPreprocessor(50);
            pigIStream = new BufferedReader(new FileReader(basedir + "/input1.pig"));
            pigOStream = new FileWriter(basedir + "/output1.pig");

            String[] arg = {"date=20080228"};
            String[] argFiles = null;
            ps.genSubstitutedFile(pigIStream , pigOStream , arg , argFiles);

            FileInputStream pigResultStream = new FileInputStream(basedir + "/output1.pig");
            pigExResultStream = new FileInputStream(basedir + "/ExpectedResult.pig");
            BufferedReader inExpected = new BufferedReader(new InputStreamReader(pigExResultStream));
            BufferedReader inResult = new BufferedReader(new InputStreamReader(pigResultStream));

            String exLine;
            String resLine;
            int lineNum=0;

            while (true) {
                lineNum++;
                exLine = inExpected.readLine();
                resLine = inResult.readLine();
                if (exLine==null || resLine==null)
                    break;
                assertEquals("Command line parameter substitution failed. " + "Expected : "+exLine+" , but got : "+resLine+" in line num : "+lineNum ,exLine.trim(), resLine.trim());
            }
            if (!(exLine==null && resLine==null)) {
                fail ("Command line parameter substitution failed. " + "Expected : "+exLine+" , but got : "+resLine+" in line num : "+lineNum);
            }

            inExpected.close();
            inResult.close();
        } catch (ParseException e) {
            fail ("Got ParseException : " + e.getMessage());
        } catch (RuntimeException e) {
            fail ("Got RuntimeException : " + e.getMessage());
        } catch (Error e) {
            fail ("Got error : " + e.getMessage());
        }

        log.info("Done");

    }

    /* Test case 2
     * Use a parameter within a pig script and provide value through a file.
     */
    @Test
    public void testFileParam() throws Exception{
        log.info ("Starting test  testFileParam()");
        try {
            ParameterSubstitutionPreprocessor ps = new ParameterSubstitutionPreprocessor(50);
            pigIStream = new BufferedReader(new FileReader(basedir + "/input1.pig"));
            pigOStream = new FileWriter(basedir + "/output1.pig");

            String[] arg = null;
            String[] argFiles = {basedir+"/ConfFile1.txt"};
            ps.genSubstitutedFile(pigIStream , pigOStream , arg , argFiles);

            FileInputStream pigResultStream = new FileInputStream(basedir + "/output1.pig");
            pigExResultStream = new FileInputStream(basedir + "/ExpectedResult.pig");
            BufferedReader inExpected = new BufferedReader(new InputStreamReader(pigExResultStream));
            BufferedReader inResult = new BufferedReader(new InputStreamReader(pigResultStream));

            String exLine;
            String resLine;
            int lineNum=0;

            while (true) {
                lineNum++;
                exLine = inExpected.readLine();
                resLine = inResult.readLine();
                if (exLine==null || resLine==null)
                    break;
                assertEquals("Parameter substitution from config file failed. " + "Expected : "+exLine+" , but got : "+resLine+" in line num : "+lineNum ,exLine.trim(), resLine.trim());
            }
            if (!(exLine==null && resLine==null)) {
                fail ("Parameter substitution from config file failed. " + "Expected : "+exLine+" , but got : "+resLine+" in line num : "+lineNum);
            }

            inExpected.close();
            inResult.close();
        } catch (ParseException e) {
            fail ("Got ParseException : " + e.getMessage());
        } catch (RuntimeException e) {
            fail ("Got RuntimeException : " + e.getMessage());
        } catch (Error e) {
            fail ("Got error : " + e.getMessage());
        }

        log.info("Done");
    }

    /* Test case 3
     *  Use a parameter within a pig script and provide value through running a binary or script.
     */
     @Test
     public void testShellCommand() throws Exception{
        log.info("Starting test testShellCommand()");
        try {
            ParameterSubstitutionPreprocessor ps = new ParameterSubstitutionPreprocessor(50);
            pigIStream = new BufferedReader(new FileReader(basedir + "/input4.pig"));
            pigOStream = new FileWriter(basedir + "/output1.pig");

            String[] arg = null; //{"date=`sh generate_date.sh`"};     //`date \\T`"};
            String[] argFiles = null; // {basedir+"/ConfFile1.txt"};
            ps.genSubstitutedFile(pigIStream , pigOStream , arg , argFiles);

            FileInputStream pigResultStream = new FileInputStream(basedir + "/output1.pig");
            pigExResultStream = new FileInputStream(basedir + "/ExpectedResultDefault.pig");
            BufferedReader inExpected = new BufferedReader(new InputStreamReader(pigExResultStream));
            BufferedReader inResult = new BufferedReader(new InputStreamReader(pigResultStream));

            String exLine;
            String resLine;
            int lineNum=0;

            while (true) {
                lineNum++;
                exLine = inExpected.readLine();
                resLine = inResult.readLine();
                if (exLine==null || resLine==null)
                    break;
                assertEquals("Parameter substitution with shell command failed. " + "Expected : "+exLine+" , but got : "+resLine+" in line num : "+lineNum ,exLine.trim(), resLine.trim());
            }
            if (!(exLine==null && resLine==null)) {
                fail ("Parameter substitution with shell command failed. " + "Expected : "+exLine+" , but got : "+resLine+" in line num : "+lineNum);
            }

            inExpected.close();
            inResult.close();
        } catch (ParseException e) {
            fail ("Got ParseException : " + e.getMessage());
        } catch (RuntimeException e) {
            fail ("Got RuntimeException : " + e.getMessage());
        } catch (Error e) {
            fail ("Got error : " + e.getMessage());
        }

        log.info("Done");
    }

    /* Test case 4   
     * Use a Pig-supported parameter like "$0" and ensure that the Pig-supported parameter is not resolved
     */
    @Test
    public void testPigParamNotResolved() throws Exception{
        log.info("Starting test testPigParamNotResolved()");
        ParameterSubstitutionPreprocessor ps = new ParameterSubstitutionPreprocessor(50);
        pigIStream = new BufferedReader(new FileReader(basedir + "/input3.pig"));
        pigOStream = new FileWriter(basedir + "/output1.pig");

        String[] arg = null;
        String[] argFiles = null;
        try {
            ps.genSubstitutedFile(pigIStream , pigOStream , arg , argFiles);
        } catch (RuntimeException e) {
            if (e.getMessage().equals("Undefined parameter : 4")) {
                fail("Pig supported parameter $4 should not have been resolved.");
            }
        }
        log.info("Done");
    }

    /* Test case 5   
     * Use the %declare statement after the command has been used.
     */
    @Test
    public void testUndefinedParam() throws Exception{
        log.info("Starting test testUndefinedParam()");
        ParameterSubstitutionPreprocessor ps = new ParameterSubstitutionPreprocessor(50);
        pigIStream = new BufferedReader(new FileReader(basedir + "/input2.pig"));
        pigOStream = new FileWriter(basedir + "/output1.pig");

        String[] arg = null;
        String[] argFiles = null;
        try {
            ps.genSubstitutedFile(pigIStream , pigOStream , arg , argFiles);
            fail ("Should have thrown an Undefined parameter exception");
        } catch (RuntimeException e) {
            assertEquals(e.getMessage(), "Undefined parameter : param");
        }
        log.info("Done");
    }

    /* Test case 7   
     *  Use a parameter in %declare that is defined in terms of other parameters
     */
    @Test
    public void testSubstitutionWithinValue() throws Exception{
        log.info("Starting test  testSubstitutionWithinValue()");
        try {
            ParameterSubstitutionPreprocessor ps = new ParameterSubstitutionPreprocessor(50);
            pigIStream = new BufferedReader(new FileReader(basedir + "/inputSubstitutionWithinValue.pig"));
            pigOStream = new FileWriter(basedir + "/output1.pig");

            String[] arg = null;
            String[] argFiles = {basedir+"/ConfFile1.txt"};
            ps.genSubstitutedFile(pigIStream , pigOStream , arg , argFiles);

            FileInputStream pigResultStream = new FileInputStream(basedir + "/output1.pig");
            pigExResultStream = new FileInputStream(basedir + "/ExpectedResult4.pig");
            BufferedReader inExpected = new BufferedReader(new InputStreamReader(pigExResultStream));
            BufferedReader inResult = new BufferedReader(new InputStreamReader(pigResultStream));

            String exLine;
            String resLine;
            int lineNum=0;

            while (true) {
                lineNum++;
                exLine = inExpected.readLine();
                resLine = inResult.readLine();
                if (exLine==null || resLine==null)
                    break;
                assertEquals("Parameter substitution within a value failed. " + "Expected : "+exLine+" , but got : "+resLine+" in line num : "+lineNum ,exLine.trim(), resLine.trim());
            }
            if (!(exLine==null && resLine==null)) {
                fail ("Parameter substitution within a value failed. " + "Expected : "+exLine+" , but got : "+resLine+" in line num : "+lineNum);
            }

            inExpected.close();
            inResult.close();
        } catch (ParseException e) {
            fail ("Got ParseException : " + e.getMessage());
        } catch (RuntimeException e) {
            fail ("Got RuntimeException : " + e.getMessage());
        } catch (Error e) {
            fail ("Got error : " + e.getMessage());
        }

        log.info("Done");
    }


    /* Test case 8
     *  Use a parameter within a pig script, provide value for it through running a binary or script. 
     *  The script itself takes an argument that is a parameter and should be resolved
     */
    @Test 
    public void testSubstitutionWithinShellCommand() throws Exception{
        log.info("Starting test testSubstitutionWithinShellCommand()");
        try {
            ParameterSubstitutionPreprocessor ps = new ParameterSubstitutionPreprocessor(50);
            pigIStream = new BufferedReader(new FileReader(basedir + "/inputSubstitutionWithinShellCommand.pig"));
            pigOStream = new FileWriter(basedir + "/output1.pig");

            String[] arg = null; 
            String[] argFiles = null;
            ps.genSubstitutedFile(pigIStream , pigOStream , arg , argFiles);

            FileInputStream pigResultStream = new FileInputStream(basedir + "/output1.pig");
            pigExResultStream = new FileInputStream(basedir + "/ExpectedResult4.pig");
            BufferedReader inExpected = new BufferedReader(new InputStreamReader(pigExResultStream));
            BufferedReader inResult = new BufferedReader(new InputStreamReader(pigResultStream));

            String exLine;
            String resLine;
            int lineNum=0;

            while (true) {
                lineNum++;
                exLine = inExpected.readLine();
                resLine = inResult.readLine();
                if (exLine==null || resLine==null)
                    break;
                assertEquals("Parameter substitution with shell command failed. " + "Expected : "+exLine+" , but got : "+resLine+" in line num : "+lineNum ,exLine.trim(), resLine.trim());
            }
            if (!(exLine==null && resLine==null)) {
                fail ("Parameter substitution with shell command failed. " + "Expected : "+exLine+" , but got : "+resLine+" in line num : "+lineNum);
            }

            inExpected.close();
            inResult.close();
        } catch (ParseException e) {
            fail ("Got ParseException : " + e.getMessage());
        } catch (RuntimeException e) {
            fail ("Got RuntimeException : " + e.getMessage());
        } catch (Error e) {
            fail ("Got error : " + e.getMessage());
        }
        log.info("Done");
    }


    /* Test case 9
     *  Use parameters passed on the command line/file that are not resolved prior to the declare statement.
     */
    @Test
    public void testCmdlineParamPriortoDeclare() throws Exception{
        log.info("Starting test testCmdlineParamPriortoDeclare()");
        try {
            ParameterSubstitutionPreprocessor ps = new ParameterSubstitutionPreprocessor(50);
            pigIStream = new BufferedReader(new FileReader(basedir + "/input2.pig"));
            pigOStream = new FileWriter(basedir + "/output1.pig");

            String[] arg = {"param='20080228'"}; 
            String[] argFiles = null;
            ps.genSubstitutedFile(pigIStream , pigOStream , arg , argFiles);

            FileInputStream pigResultStream = new FileInputStream(basedir + "/output1.pig");
            pigExResultStream = new FileInputStream(basedir + "/ExpectedResultCmdLnPriorDeclare.pig");
            BufferedReader inExpected = new BufferedReader(new InputStreamReader(pigExResultStream));
            BufferedReader inResult = new BufferedReader(new InputStreamReader(pigResultStream));

            String exLine;
            String resLine;
            int lineNum=0;

            while (true) {
                lineNum++;
                exLine = inExpected.readLine();
                resLine = inResult.readLine();
                if (exLine==null || resLine==null)
                    break;
                assertEquals("Parameter substitution of command line arg. prior to declare stmt failed. " + "Expected : "+exLine+" , but got : "+resLine+" in line num : "+lineNum ,exLine.trim(), resLine.trim());
            }
            if (!(exLine==null && resLine==null)) {
                fail ("Parameter substitution of command line arg. prior to declare stmt failed. " + "Expected : "+exLine+" , but got : "+resLine+" in line num : "+lineNum);
            }

            inExpected.close();
            inResult.close();
        } catch (ParseException e) {
            fail ("Got ParseException : " + e.getMessage());
        } catch (RuntimeException e) {
            fail ("Got RuntimeException : " + e.getMessage());
        } catch (Error e) {
            fail ("Got error : " + e.getMessage());
        }

        log.info("Done");
    } 


    /* Test case 10
     *  Use a command name in a %declare statement as a parameter
     */
    @Test
    public void testCmdnameAsParamDeclare() throws Exception{
        log.info("Starting test testCmdnameAsParamDeclare()");
        try {
            ParameterSubstitutionPreprocessor ps = new ParameterSubstitutionPreprocessor(50);
            pigIStream = new BufferedReader(new FileReader(basedir + "/inputCmdnameAsParamDeclare.pig"));
            pigOStream = new FileWriter(basedir + "/output1.pig");

            String[] arg = null; 
            String[] argFiles = null;
            ps.genSubstitutedFile(pigIStream , pigOStream , arg , argFiles);

            FileInputStream pigResultStream = new FileInputStream(basedir + "/output1.pig");
            pigExResultStream = new FileInputStream(basedir + "/ExpectedResult4.pig");
            BufferedReader inExpected = new BufferedReader(new InputStreamReader(pigExResultStream));
            BufferedReader inResult = new BufferedReader(new InputStreamReader(pigResultStream));

            String exLine;
            String resLine;
            int lineNum=0;

            while (true) {
                lineNum++;
                exLine = inExpected.readLine();
                resLine = inResult.readLine();
                if (exLine==null || resLine==null)
                    break;
                assertEquals("Parameter substitution for a command with shell command failed. " + "Expected : "+exLine+" , but got : "+resLine+" in line num : "+lineNum ,exLine.trim(), resLine.trim());
            }
            if (!(exLine==null && resLine==null)) {
                fail ("Parameter substitution for a command with shell command failed. " + "Expected : "+exLine+" , but got : "+resLine+" in line num : "+lineNum);
            }

            inExpected.close();
            inResult.close();
        } catch (ParseException e) {
            fail ("Got ParseException : " + e.getMessage());
        } catch (RuntimeException e) {
            fail ("Got RuntimeException : " + e.getMessage());
        } catch (Error e) {
            fail ("Got error : " + e.getMessage());
        }
        log.info("Done");
    }


    /* Test case 11   
     * Use the same parameter multiple times on the command line.
     * Result : last value used and warning should be thrown
     */
    @Test
    public void testMultipleCmdlineParam() throws Exception{
        log.info("Starting test testCmdnameAsParamDeclare()");
        try {     
            ParameterSubstitutionPreprocessor ps = new ParameterSubstitutionPreprocessor(50);
            pigIStream = new BufferedReader(new FileReader(basedir + "/input1.pig"));
            pigOStream = new FileWriter(basedir + "/output1.pig");

            String[] arg = {"date='092487'","date='20080228'"};
            String[] argFiles = null;
            ps.genSubstitutedFile(pigIStream , pigOStream , arg , argFiles);

            FileInputStream pigResultStream = new FileInputStream(basedir + "/output1.pig");
            pigExResultStream = new FileInputStream(basedir + "/ExpectedResult.pig");
            BufferedReader inExpected = new BufferedReader(new InputStreamReader(pigExResultStream));
            BufferedReader inResult = new BufferedReader(new InputStreamReader(pigResultStream));

            String exLine;
            String resLine;
            int lineNum=0;

            while (true) {
                lineNum++;
                exLine = inExpected.readLine();
                resLine = inResult.readLine();
                if (exLine==null || resLine==null)
                    break;
                assertEquals("Command line parameter substitution failed. " + "Expected : "+exLine+" , but got : "+resLine+" in line num : "+lineNum ,exLine.trim(), resLine.trim());
            }
            if (!(exLine==null && resLine==null)) {
                fail ("Command line parameter substitution failed. " + "Expected : "+exLine+" , but got : "+resLine+" in line num : "+lineNum);
            }

            inExpected.close();
            inResult.close();
        } catch (ParseException e) {
            fail ("Got ParseException : " + e.getMessage());
        } catch (RuntimeException e) {
            fail ("Got RuntimeException : " + e.getMessage());
        } catch (Error e) {
            fail ("Got error : " + e.getMessage());
        }
        log.info("Done");
    }

    /* Test case 12
     * Read parameters from multiple files.
     */
    @Test
    public void testFileParamsFromMultipleFiles() throws Exception{
        log.info("Starting test testFileParamsFromMultipleFiles()");
        try {
            ParameterSubstitutionPreprocessor ps = new ParameterSubstitutionPreprocessor(50);
            pigIStream = new BufferedReader(new FileReader(basedir + "/inputMultipleParams.pig"));
            pigOStream = new FileWriter(basedir + "/output1.pig");

            String[] arg = null;
            String[] argFiles = {basedir+"/ConfFile1.txt" , basedir+"/ConfFile2.txt"};
            ps.genSubstitutedFile(pigIStream , pigOStream , arg , argFiles);

            FileInputStream pigResultStream = new FileInputStream(basedir + "/output1.pig");
            pigExResultStream = new FileInputStream(basedir + "/ExpectedResult.pig");
            BufferedReader inExpected = new BufferedReader(new InputStreamReader(pigExResultStream));
            BufferedReader inResult = new BufferedReader(new InputStreamReader(pigResultStream));

            String exLine;
            String resLine;
            int lineNum=0;

            while (true) {
                lineNum++;
                exLine = inExpected.readLine();
                resLine = inResult.readLine();
                if (exLine==null || resLine==null)
                    break;
                assertEquals("Parameter substitution from multiple config files failed. " + "Expected : "+exLine+" , but got : "+resLine+" in line num : "+lineNum ,exLine.trim(), resLine.trim());
            }
            if (!(exLine==null && resLine==null)) {
                fail ("Parameter substitution from multiple config files failed. " + "Expected : "+exLine+" , but got : "+resLine+" in line num : "+lineNum);
            }

            inExpected.close();
            inResult.close();
        } catch (ParseException e) {
            fail ("Got ParseException : " + e.getMessage());
        } catch (RuntimeException e) {
            fail ("Got RuntimeException : " + e.getMessage());
        } catch (Error e) {
            fail ("Got error : " + e.getMessage());
        }
        log.info("Done");
    }
    
    /* Test case 13
     * Use the same parameter in multiple files.
     * Result: last value used and warning should be thrown
     */
    @Test
    public void testSameParamInMultipleFiles() throws Exception{
        log.info("Starting test testSameParamInMultipleFiles()");
        try {
            ParameterSubstitutionPreprocessor ps = new ParameterSubstitutionPreprocessor(50);
            pigIStream = new BufferedReader(new FileReader(basedir + "/inputMultipleParams.pig"));
            pigOStream = new FileWriter(basedir + "/output1.pig");

            String[] arg = null;
            String[] argFiles = {basedir+"/ConfFile3.txt" , basedir+"/ConfFile2.txt", basedir+"/ConfFile1.txt"};
            ps.genSubstitutedFile(pigIStream , pigOStream , arg , argFiles);

            FileInputStream pigResultStream = new FileInputStream(basedir + "/output1.pig");
            pigExResultStream = new FileInputStream(basedir + "/ExpectedResult.pig");
            BufferedReader inExpected = new BufferedReader(new InputStreamReader(pigExResultStream));
            BufferedReader inResult = new BufferedReader(new InputStreamReader(pigResultStream));

            String exLine;
            String resLine;
            int lineNum=0;

            while (true) {
                lineNum++;
                exLine = inExpected.readLine();
                resLine = inResult.readLine();
                if (exLine==null || resLine==null)
                    break;
                assertEquals("Same Parameter substitution from multiple config files failed. " + "Expected : "+exLine+" , but got : "+resLine+" in line num : "+lineNum ,exLine.trim(), resLine.trim());
            }
            if (!(exLine==null && resLine==null)) {
                fail ("Same Parameter substitution from multiple config files failed. " + "Expected : "+exLine+" , but got : "+resLine+" in line num : "+lineNum);
            }

            inExpected.close();
            inResult.close();
        } catch (ParseException e) {
            fail ("Got ParseException : " + e.getMessage());
        } catch (RuntimeException e) {
            fail ("Got RuntimeException : " + e.getMessage());
        } catch (Error e) {
            fail ("Got error : " + e.getMessage());
        }
        log.info("Done");
    }

    /* Test case 14
     * Use the same parameter multiple times in a single file.
     * Result: last value used and warning should be thrown.
     */
    @Test
    public void testMultipleParamsFromSingleFile() throws Exception{
        try {
            ParameterSubstitutionPreprocessor ps = new ParameterSubstitutionPreprocessor(50);
            pigIStream = new BufferedReader(new FileReader(basedir + "/input1.pig"));
            pigOStream = new FileWriter(basedir + "/output1.pig");

            String[] arg = null;
            String[] argFiles = {basedir+"/ConfFileSameParamMultipleTimes.txt"};
            ps.genSubstitutedFile(pigIStream , pigOStream , arg , argFiles);

            FileInputStream pigResultStream = new FileInputStream(basedir + "/output1.pig");
            pigExResultStream = new FileInputStream(basedir + "/ExpectedResult.pig");
            BufferedReader inExpected = new BufferedReader(new InputStreamReader(pigExResultStream));
            BufferedReader inResult = new BufferedReader(new InputStreamReader(pigResultStream));

            String exLine;
            String resLine;
            int lineNum=0;

            while (true) {
                lineNum++;
                exLine = inExpected.readLine();
                resLine = inResult.readLine();
                if (exLine==null || resLine==null)
                    break;
                assertEquals("Expected : "+exLine+" , but got : "+resLine+" in line num : "+lineNum ,exLine.trim(), resLine.trim());
            }
            if (!(exLine==null && resLine==null)) {
                fail ("Expected : "+exLine+" , but got : "+resLine+" in line num : "+lineNum);
            }

            inExpected.close();
            inResult.close();
        } catch (ParseException e) {
            fail ("Got ParseException : " + e.getMessage());
        } catch (RuntimeException e) {
            fail ("Got RuntimeException : " + e.getMessage());
        } catch (Error e) {
            fail ("Got error : " + e.getMessage());
        }

    }
    
    /* Test case 15,16   
     *   Use an empty lines and Comment lines in the parameter file.
     *  Result: Allowed
     */
    @Test
    public void testEmptyCommentLineinConfigfile() throws Exception{
        ParameterSubstitutionPreprocessor ps = new ParameterSubstitutionPreprocessor(50);
        pigIStream = new BufferedReader(new FileReader(basedir + "/input1.pig"));
        pigOStream = new FileWriter(basedir + "/output1.pig");

        String[] arg = null;
        String[] argFiles = {basedir+"/ConfFileWithEmptyComments.txt"};
        try {
            ps.genSubstitutedFile(pigIStream , pigOStream , arg , argFiles);
        } catch (ParseException e) {
            fail ("Got ParseException : " + e.getMessage());
        } catch (RuntimeException e) {
            fail ("Got RuntimeException : " + e.getMessage());
        } catch (Error e) {
            fail ("Got error : " + e.getMessage());
        }
    }
    
    /* Test case 17   
     *   Use a line in the file that is not empty or a comment but does not conform to param_name=param_value.
     */
    @Test
    public void testInvalidLineinConfigfile() throws Exception{
        ParameterSubstitutionPreprocessor ps = new ParameterSubstitutionPreprocessor(50);
        pigIStream = new BufferedReader(new FileReader(basedir + "/input1.pig"));
        pigOStream = new FileWriter(basedir + "/output1.pig");

        String[] arg = null;
        String[] argFiles = {basedir+"/ConfFileWithInvalidLines.txt"};
        try {
            ps.genSubstitutedFile(pigIStream , pigOStream , arg , argFiles);
            fail ("Should have thrown an exception");
        } catch (ParseException e) {
            assertTrue(e.getMessage().startsWith("Encountered \" <IDENTIFIER> \"is \"\" at line 2, column 6."));
        } catch (RuntimeException e) {
            fail ("Got RuntimeException : " + e.getMessage());
        } catch (Error e) {
            fail ("Got error : " + e.getMessage());
        }
    }
    
    
    /* Test case 18,19
     *   Check a parameter line of form param_name=param_value is allowed.
     *   Check a parameter line of form param_name<white space>=<white space>param_value is allowed.
     */
    @Test
    public void testValidLinesinConfigfile() throws Exception{
        ParameterSubstitutionPreprocessor ps = new ParameterSubstitutionPreprocessor(50);
        pigIStream = new BufferedReader(new FileReader(basedir + "/inputMultipleParams.pig"));
        pigOStream = new FileWriter(basedir + "/output1.pig");

        String[] arg = null;
        String[] argFiles = {basedir+"/ConfFileWithValidLines.txt"};
        try {
            ps.genSubstitutedFile(pigIStream , pigOStream , arg , argFiles);
        } catch (ParseException e) {
            fail ("Got ParseException : " + e.getMessage());
        } catch (RuntimeException e) {
            fail ("Got RuntimeException : " + e.getMessage());
        } catch (Error e) {
            fail ("Got error : " + e.getMessage());
        }
    }
    
     /* Test case 20
     * Use a combination of command line and file parameters.
     */
    @Test
    public void testCmdlineFileCombo() throws Exception{
        try {
            ParameterSubstitutionPreprocessor ps = new ParameterSubstitutionPreprocessor(50);
            pigIStream = new BufferedReader(new FileReader(basedir + "/inputMultipleParams.pig"));
            pigOStream = new FileWriter(basedir + "/output1.pig");

            String[] arg = {"date='20080228'"};
            String[] argFiles = {basedir+"/ConfFile2.txt"};
            ps.genSubstitutedFile(pigIStream , pigOStream , arg , argFiles);

            FileInputStream pigResultStream = new FileInputStream(basedir + "/output1.pig");
            pigExResultStream = new FileInputStream(basedir + "/ExpectedResult.pig");
            BufferedReader inExpected = new BufferedReader(new InputStreamReader(pigExResultStream));
            BufferedReader inResult = new BufferedReader(new InputStreamReader(pigResultStream));

            String exLine;
            String resLine;
            int lineNum=0;

            while (true) {
                lineNum++;
                exLine = inExpected.readLine();
                resLine = inResult.readLine();
                if (exLine==null || resLine==null)
                    break;
                assertEquals("Expected : "+exLine+" , but got : "+resLine+" in line num : "+lineNum ,exLine.trim(), resLine.trim());
            }
            if (!(exLine==null && resLine==null)) {
                fail ("Expected : "+exLine+" , but got : "+resLine+" in line num : "+lineNum);
            }

            inExpected.close();
            inResult.close();
        } catch (ParseException e) {
            fail ("Got ParseException : " + e.getMessage());
        } catch (RuntimeException e) {
            fail ("Got RuntimeException : " + e.getMessage());
        } catch (Error e) {
            fail ("Got error : " + e.getMessage());
        }

    }

    /* Test case 21
     * Use a combination of command line and file parameters where there are duplicate parameters.
     * Result: Command line parameters take precedence over files.
     */
    @Test
    public void testCmdlineFileComboDuplicate() throws Exception{
        try {
            ParameterSubstitutionPreprocessor ps = new ParameterSubstitutionPreprocessor(50);
            pigIStream = new BufferedReader(new FileReader(basedir + "/inputMultipleParams.pig"));
            pigOStream = new FileWriter(basedir + "/output1.pig");

            String[] arg = {"date='20080228'"};
            String[] argFiles = {basedir+"/ConfFileDuplicates.txt"};
            ps.genSubstitutedFile(pigIStream , pigOStream , arg , argFiles);

            FileInputStream pigResultStream = new FileInputStream(basedir + "/output1.pig");
            pigExResultStream = new FileInputStream(basedir + "/ExpectedResult.pig");
            BufferedReader inExpected = new BufferedReader(new InputStreamReader(pigExResultStream));
            BufferedReader inResult = new BufferedReader(new InputStreamReader(pigResultStream));

            String exLine;
            String resLine;
            int lineNum=0;

            while (true) {
                lineNum++;
                exLine = inExpected.readLine();
                resLine = inResult.readLine();
                if (exLine==null || resLine==null)
                    break;
                assertEquals("Expected : "+exLine+" , but got : "+resLine+" in line num : "+lineNum ,exLine.trim(), resLine.trim());
            }
            if (!(exLine==null && resLine==null)) {
                fail ("Expected : "+exLine+" , but got : "+resLine+" in line num : "+lineNum);
            }

            inExpected.close();
            inResult.close();
        } catch (ParseException e) {
            fail ("Got ParseException : " + e.getMessage());
        } catch (RuntimeException e) {
            fail ("Got RuntimeException : " + e.getMessage());
        } catch (Error e) {
            fail ("Got error : " + e.getMessage());
        }

    }
    

    /* Test case 22
     *  Use a combination of command line, file and declare.
     */
    @Test
    public void testCmdlineFileDeclareCombo() throws Exception{
        try {
            ParameterSubstitutionPreprocessor ps = new ParameterSubstitutionPreprocessor(50);
            pigIStream = new BufferedReader(new FileReader(basedir + "/inputThreeParams.pig"));
            pigOStream = new FileWriter(basedir + "/output1.pig");

            String[] arg = {"date='20080228'"};
            String[] argFiles = {basedir+"/ConfFile2.txt"};
            ps.genSubstitutedFile(pigIStream , pigOStream , arg , argFiles);

            FileInputStream pigResultStream = new FileInputStream(basedir + "/output1.pig");
            pigExResultStream = new FileInputStream(basedir + "/ExpectedResult.pig");
            BufferedReader inExpected = new BufferedReader(new InputStreamReader(pigExResultStream));
            BufferedReader inResult = new BufferedReader(new InputStreamReader(pigResultStream));

            String exLine;
            String resLine;
            int lineNum=0;

            while (true) {
                lineNum++;
                exLine = inExpected.readLine();
                resLine = inResult.readLine();
                if (exLine==null || resLine==null)
                    break;
                assertEquals("Expected : "+exLine+" , but got : "+resLine+" in line num : "+lineNum ,exLine.trim(), resLine.trim());
            }
            if (!(exLine==null && resLine==null)) {
                fail ("Expected : "+exLine+" , but got : "+resLine+" in line num : "+lineNum);
            }

            inExpected.close();
            inResult.close();
        } catch (ParseException e) {
            fail ("Got ParseException : " + e.getMessage());
        } catch (RuntimeException e) {
            fail ("Got RuntimeException : " + e.getMessage());
        } catch (Error e) {
            fail ("Got error : " + e.getMessage());
        }

    }
    
    /* Test case 23
     *  Use a combination of command line, file and declare where there are duplicate parameters.
     *  Result: Declare has highest priority.
     */
    @Test
    public void testCmdlineFileDeclareComboDuplicates() throws Exception{
        try {
            ParameterSubstitutionPreprocessor ps = new ParameterSubstitutionPreprocessor(50);
            pigIStream = new BufferedReader(new FileReader(basedir + "/inputThreeParams.pig"));
            pigOStream = new FileWriter(basedir + "/output1.pig");

            String[] arg = {"date='20080228'" , "tableName=\"skip this\""};
            String[] argFiles = {basedir+"/ConfFile2.txt"};
            ps.genSubstitutedFile(pigIStream , pigOStream , arg , argFiles);

            FileInputStream pigResultStream = new FileInputStream(basedir + "/output1.pig");
            pigExResultStream = new FileInputStream(basedir + "/ExpectedResult.pig");
            BufferedReader inExpected = new BufferedReader(new InputStreamReader(pigExResultStream));
            BufferedReader inResult = new BufferedReader(new InputStreamReader(pigResultStream));

            String exLine;
            String resLine;
            int lineNum=0;

            while (true) {
                lineNum++;
                exLine = inExpected.readLine();
                resLine = inResult.readLine();
                if (exLine==null || resLine==null)
                    break;
                assertEquals("Expected : "+exLine+" , but got : "+resLine+" in line num : "+lineNum ,exLine.trim(), resLine.trim());
            }
            if (!(exLine==null && resLine==null)) {
                fail ("Expected : "+exLine+" , but got : "+resLine+" in line num : "+lineNum);
            }

            inExpected.close();
            inResult.close();
        } catch (ParseException e) {
            fail ("Got ParseException : " + e.getMessage());
        } catch (RuntimeException e) {
            fail ("Got RuntimeException : " + e.getMessage());
        } catch (Error e) {
            fail ("Got error : " + e.getMessage());
        }

    }
    
    /* Test case 24
     *   Use multiple declare statement that specify the same parameter.
     *   Result: Scope of a parameter declared using declare is until the next declare statement that defines the same parameter.
     */
    @Test
    public void testMultipleDeclareScope() throws Exception{
        try {
            ParameterSubstitutionPreprocessor ps = new ParameterSubstitutionPreprocessor(50);
            pigIStream = new BufferedReader(new FileReader(basedir + "/inputMultipleDeclares.pig"));
            pigOStream = new FileWriter(basedir + "/output1.pig");

            String[] arg = {"date='20080228'"};
            String[] argFiles = null;
            ps.genSubstitutedFile(pigIStream , pigOStream , arg , argFiles);

            FileInputStream pigResultStream = new FileInputStream(basedir + "/output1.pig");
            pigExResultStream = new FileInputStream(basedir + "/ExpectedResultMulDecs.pig");
            BufferedReader inExpected = new BufferedReader(new InputStreamReader(pigExResultStream));
            BufferedReader inResult = new BufferedReader(new InputStreamReader(pigResultStream));

            String exLine;
            String resLine;
            int lineNum=0;

            while (true) {
                lineNum++;
                exLine = inExpected.readLine();
                resLine = inResult.readLine();
                if (exLine==null || resLine==null)
                    break;
                assertEquals("Expected : "+exLine+" , but got : "+resLine+" in line num : "+lineNum ,exLine.trim(), resLine.trim());
            }
            if (!(exLine==null && resLine==null)) {
                fail ("Expected : "+exLine+" , but got : "+resLine+" in line num : "+lineNum);
            }

            inExpected.close();
            inResult.close();
        } catch (ParseException e) {
            fail ("Got ParseException : " + e.getMessage());
        } catch (RuntimeException e) {
            fail ("Got RuntimeException : " + e.getMessage());
        } catch (Error e) {
            fail ("Got error : " + e.getMessage());
        }

    }
    
    
    /* Test case 25
     *   Use %default to define param values
     */
    @Test
    public void testDefaultParam() throws Exception{
        try {
            ParameterSubstitutionPreprocessor ps = new ParameterSubstitutionPreprocessor(50);
            pigIStream = new BufferedReader(new FileReader(basedir + "/inputDefault.pig"));
            pigOStream = new FileWriter(basedir + "/output1.pig");

            String[] arg = null;
            String[] argFiles = null;
            ps.genSubstitutedFile(pigIStream , pigOStream , arg , argFiles);

            FileInputStream pigResultStream = new FileInputStream(basedir + "/output1.pig");
            pigExResultStream = new FileInputStream(basedir + "/ExpectedResult.pig");
            BufferedReader inExpected = new BufferedReader(new InputStreamReader(pigExResultStream));
            BufferedReader inResult = new BufferedReader(new InputStreamReader(pigResultStream));

            String exLine;
            String resLine;
            int lineNum=0;

            while (true) {
                lineNum++;
                exLine = inExpected.readLine();
                resLine = inResult.readLine();
                if (exLine==null || resLine==null)
                    break;
                assertEquals("Expected : "+exLine+" , but got : "+resLine+" in line num : "+lineNum ,exLine.trim(), resLine.trim());
            }
            if (!(exLine==null && resLine==null)) {
                fail ("Expected : "+exLine+" , but got : "+resLine+" in line num : "+lineNum);
            }

            inExpected.close();
            inResult.close();
        } catch (ParseException e) {
            fail ("Got ParseException : " + e.getMessage());
        } catch (RuntimeException e) {
            fail ("Got RuntimeException : " + e.getMessage());
        } catch (Error e) {
            fail ("Got error : " + e.getMessage());
        }

    }
    

    /* Test case 26
     *  Use a combination of file, command line, declare and default. Default has the lowest precedence.
     */
    @Test
    public void testCmdlineFileDeclareDefaultComboDuplicates() throws Exception{
        try {
            ParameterSubstitutionPreprocessor ps = new ParameterSubstitutionPreprocessor(50);
            pigIStream = new BufferedReader(new FileReader(basedir + "/inputThreeParams.pig"));
            pigOStream = new FileWriter(basedir + "/output1.pig");

            String[] arg = {"date='20080228'" , "tableName=\"skip this\""};
            String[] argFiles = {basedir+"/ConfFile2.txt"};
            ps.genSubstitutedFile(pigIStream , pigOStream , arg , argFiles);

            FileInputStream pigResultStream = new FileInputStream(basedir + "/output1.pig");
            pigExResultStream = new FileInputStream(basedir + "/ExpectedResult.pig");
            BufferedReader inExpected = new BufferedReader(new InputStreamReader(pigExResultStream));
            BufferedReader inResult = new BufferedReader(new InputStreamReader(pigResultStream));

            String exLine;
            String resLine;
            int lineNum=0;

            while (true) {
                lineNum++;
                exLine = inExpected.readLine();
                resLine = inResult.readLine();
                if (exLine==null || resLine==null)
                    break;
                assertEquals("Expected : "+exLine+" , but got : "+resLine+" in line num : "+lineNum ,exLine.trim(), resLine.trim());
            }
            if (!(exLine==null && resLine==null)) {
                fail ("Expected : "+exLine+" , but got : "+resLine+" in line num : "+lineNum);
            }

            inExpected.close();
            inResult.close();
        } catch (ParseException e) {
            fail ("Got ParseException : " + e.getMessage());
        } catch (RuntimeException e) {
            fail ("Got RuntimeException : " + e.getMessage());
        } catch (Error e) {
            fail ("Got error : " + e.getMessage());
        }

    }
    
    
    /* Test case 28
     *  28. More than 1 parameter is present and needs to be substituted within a line e.g. A = load '/data/$name/$date';
     */
    @Test
    public void testMultipleParamsinSingleLine() throws Exception{
        try {
            ParameterSubstitutionPreprocessor ps = new ParameterSubstitutionPreprocessor(50);
            pigIStream = new BufferedReader(new FileReader(basedir + "/inputThreeParams.pig"));
            pigOStream = new FileWriter(basedir + "/output1.pig");

            String[] arg = {"date='20080228'"};
            String[] argFiles = {basedir+"/ConfFile2.txt"};
            ps.genSubstitutedFile(pigIStream , pigOStream , arg , argFiles);

            FileInputStream pigResultStream = new FileInputStream(basedir + "/output1.pig");
            pigExResultStream = new FileInputStream(basedir + "/ExpectedResult.pig");
            BufferedReader inExpected = new BufferedReader(new InputStreamReader(pigExResultStream));
            BufferedReader inResult = new BufferedReader(new InputStreamReader(pigResultStream));

            String exLine;
            String resLine;
            int lineNum=0;

            while (true) {
                lineNum++;
                exLine = inExpected.readLine();
                resLine = inResult.readLine();
                if (exLine==null || resLine==null)
                    break;
                assertEquals("Expected : "+exLine+" , but got : "+resLine+" in line num : "+lineNum ,exLine.trim(), resLine.trim());
            }
            if (!(exLine==null && resLine==null)) {
                fail ("Expected : "+exLine+" , but got : "+resLine+" in line num : "+lineNum);
            }

            inExpected.close();
            inResult.close();
        } catch (ParseException e) {
            fail ("Got ParseException : " + e.getMessage());
        } catch (RuntimeException e) {
            fail ("Got RuntimeException : " + e.getMessage());
        } catch (Error e) {
            fail ("Got error : " + e.getMessage());
        }

    }


    /* Test case 29   
     * Parameter is substituted within a literal e.g. store A into 'output/$name';
     */
    @Test
    public void testSubstituteWithinLiteral() throws Exception{
        try {
            ParameterSubstitutionPreprocessor ps = new ParameterSubstitutionPreprocessor(50);
            pigIStream = new BufferedReader(new FileReader(basedir + "/input1.pig"));
            pigOStream = new FileWriter(basedir + "/output1.pig");

            String[] arg = {"date='20080228'"};
            String[] argFiles = null;
            ps.genSubstitutedFile(pigIStream , pigOStream , arg , argFiles);

            FileInputStream pigResultStream = new FileInputStream(basedir + "/output1.pig");
            pigExResultStream = new FileInputStream(basedir + "/ExpectedResult.pig");
            BufferedReader inExpected = new BufferedReader(new InputStreamReader(pigExResultStream));
            BufferedReader inResult = new BufferedReader(new InputStreamReader(pigResultStream));

            String exLine;
            String resLine;
            int lineNum=0;

            while (true) {
                lineNum++;
                exLine = inExpected.readLine();
                resLine = inResult.readLine();
                if (exLine==null || resLine==null)
                    break;
                assertEquals("Command line parameter substitution failed. " + "Expected : "+exLine+" , but got : "+resLine+" in line num : "+lineNum ,exLine.trim(), resLine.trim());
            }
            if (!(exLine==null && resLine==null)) {
                fail ("Command line parameter substitution failed. " + "Expected : "+exLine+" , but got : "+resLine+" in line num : "+lineNum);
            }

            inExpected.close();
            inResult.close();
        } catch (ParseException e) {
            fail ("Got ParseException : " + e.getMessage());
        } catch (RuntimeException e) {
            fail ("Got RuntimeException : " + e.getMessage());
        } catch (Error e) {
            fail ("Got error : " + e.getMessage());
        }

    }

    
    /* Test case 30   
     * Make sure that escaped values are not substituted e.g. A = load '/data/\$name'
     */
    @Test
    public void testEscaping() throws Exception{
        try {
            ParameterSubstitutionPreprocessor ps = new ParameterSubstitutionPreprocessor(50);
            pigIStream = new BufferedReader(new FileReader(basedir + "/inputEscape.pig"));
            pigOStream = new FileWriter(basedir + "/output1.pig");

            String[] arg = {"date='20080228'"};
            String[] argFiles = null;
            ps.genSubstitutedFile(pigIStream , pigOStream , arg , argFiles);

            FileInputStream pigResultStream = new FileInputStream(basedir + "/output1.pig");
            pigExResultStream = new FileInputStream(basedir + "/ExpectedResult2.pig");
            BufferedReader inExpected = new BufferedReader(new InputStreamReader(pigExResultStream));
            BufferedReader inResult = new BufferedReader(new InputStreamReader(pigResultStream));

            String exLine;
            String resLine;
            int lineNum=0;

            while (true) {
                lineNum++;
                exLine = inExpected.readLine();
                resLine = inResult.readLine();
                if (exLine==null || resLine==null)
                    break;
                assertEquals("Expected : "+exLine+" , but got : "+resLine+" in line num : "+lineNum ,exLine.trim(), resLine.trim());
            }
            if (!(exLine==null && resLine==null)) {
                fail ("Expected : "+exLine+" , but got : "+resLine+" in line num : "+lineNum);
            }

            inExpected.close();
            inResult.close();
        } catch (ParseException e) {
            fail ("Got ParseException : " + e.getMessage());
        } catch (RuntimeException e) {
            fail ("Got RuntimeException : " + e.getMessage());
        } catch (Error e) {
            fail ("Got error : " + e.getMessage());
        }

    }
        
    /* Test case 31   
     * Use of inline command
     */
    @Test
    public void testCmdlineParamWithInlineCmd() throws Exception{
        log.info("Starting test testCmdlineParamWithInlineCmd() ...");
        try {
            ParameterSubstitutionPreprocessor ps = new ParameterSubstitutionPreprocessor(50);
            pigIStream = new BufferedReader(new FileReader(basedir + "/input1.pig"));
            pigOStream = new FileWriter(basedir + "/output1.pig");

            String[] arg = {"date=`perl -e 'print \"20080228\n20070101\"' | head -n 1`"};
            if (System.getProperty("os.name").toUpperCase().startsWith("WINDOWS"))
                arg[0] = "date=`perl -e 'print \\\"20080228\n20070101\\\"' | head -n 1`";
            String[] argFiles = null;
            ps.genSubstitutedFile(pigIStream , pigOStream , arg , argFiles);

            FileInputStream pigResultStream = new FileInputStream(basedir + "/output1.pig");
            pigExResultStream = new FileInputStream(basedir + "/ExpectedResult.pig");
            BufferedReader inExpected = new BufferedReader(new InputStreamReader(pigExResultStream));
            BufferedReader inResult = new BufferedReader(new InputStreamReader(pigResultStream));

            String exLine;
            String resLine;
            int lineNum=0;

            while (true) {
                lineNum++;
                exLine = inExpected.readLine();
                resLine = inResult.readLine();
                if (exLine==null || resLine==null)
                    break;
                assertEquals("Command line parameter substitution failed. " + "Expected : "+exLine+" , but got : "+resLine+" in line num : "+lineNum ,exLine.trim(), resLine.trim());
            }
            if (!(exLine==null && resLine==null)) {
                fail ("Command line parameter substitution failed. " + "Expected : "+exLine+" , but got : "+resLine+" in line num : "+lineNum);
            }

            inExpected.close();
            inResult.close();
        } catch (ParseException e) {
            fail ("Got ParseException : " + e.getMessage());
        } catch (RuntimeException e) {
            fail ("Got RuntimeException : " + e.getMessage());
        } catch (Error e) {
            fail ("Got error : " + e.getMessage());
        }

        log.info("Done");

    }

    /* Test case 32   
     * No substitution
     */
    @Test
    public void testNoVars() throws Exception{
        log.info("Starting test testNoVars() ...");
        try {
            ParameterSubstitutionPreprocessor ps = new ParameterSubstitutionPreprocessor(50);
            pigIStream = new BufferedReader(new FileReader(basedir + "/inputNoVars.pig"));
            pigOStream = new FileWriter(basedir + "/output1.pig");

            String[] arg = null;
            String[] argFiles = null;
            ps.genSubstitutedFile(pigIStream , pigOStream , arg , argFiles);

            FileInputStream pigResultStream = new FileInputStream(basedir + "/output1.pig");
            pigExResultStream = new FileInputStream(basedir + "/inputNoVars.pig");
            BufferedReader inExpected = new BufferedReader(new InputStreamReader(pigExResultStream));
            BufferedReader inResult = new BufferedReader(new InputStreamReader(pigResultStream));

            String exLine;
            String resLine;
            int lineNum=0;

            while (true) {
                lineNum++;
                exLine = inExpected.readLine();
                resLine = inResult.readLine();
                if (exLine==null || resLine==null)
                    break;
                assertEquals("Command line parameter substitution failed. " + "Expected : "+exLine+" , but got : "+resLine+" in line num : "+lineNum ,exLine.trim(), resLine.trim());
            }
            if (!(exLine==null && resLine==null)) {
                fail ("Command line parameter substitution failed. " + "Expected : "+exLine+" , but got : "+resLine+" in line num : "+lineNum);
            }

            inExpected.close();
            inResult.close();
        } catch (ParseException e) {
            fail ("Got ParseException : " + e.getMessage());
        } catch (RuntimeException e) {
            fail ("Got RuntimeException : " + e.getMessage());
        } catch (Error e) {
            fail ("Got error : " + e.getMessage());
        }

        log.info("Done");

    }

    /* Test case 32   
     * Test special values in characters
     */
    @Test
    public void testComplexVals() throws Exception{
        log.info("Starting test testComplexVals() ...");
        try {
            ParameterSubstitutionPreprocessor ps = new ParameterSubstitutionPreprocessor(50);
            pigIStream = new BufferedReader(new FileReader(basedir + "/input5.pig"));
            pigOStream = new FileWriter(basedir + "/output1.pig");

            String[] arg = {"loadfile = /user/pig/tests/data/singlefile/textdoc.txt"};
            String[] argFiles = {basedir+"/ConfFileComplexVal.txt"};
            ps.genSubstitutedFile(pigIStream , pigOStream , arg , argFiles);

            FileInputStream pigResultStream = new FileInputStream(basedir + "/output1.pig");
            pigExResultStream = new FileInputStream(basedir + "/ExpectedResult3.txt");
            BufferedReader inExpected = new BufferedReader(new InputStreamReader(pigExResultStream));
            BufferedReader inResult = new BufferedReader(new InputStreamReader(pigResultStream));

            String exLine;
            String resLine;
            int lineNum=0;

            while (true) {
                lineNum++;
                exLine = inExpected.readLine();
                resLine = inResult.readLine();
                if (exLine==null || resLine==null)
                    break;
                assertEquals("Command line parameter substitution failed. " + "Expected : "+exLine+" , but got : "+resLine+" in line num : "+lineNum ,exLine.trim(), resLine.trim());
            }
            if (!(exLine==null && resLine==null)) {
                fail ("Command line parameter substitution failed. " + "Expected : "+exLine+" , but got : "+resLine+" in line num : "+lineNum);
            }

            inExpected.close();
            inResult.close();
        } catch (ParseException e) {
            fail ("Got ParseException : " + e.getMessage());
        } catch (RuntimeException e) {
            fail ("Got RuntimeException : " + e.getMessage());
        } catch (Error e) {
            fail ("Got error : " + e.getMessage());
        }

        log.info("Done");

    }
    
    /* Test case 25
     *   Test that params in comments are untouched
     */
    @Test
    public void testCommentWithParam() throws Exception{
        try {
            ParameterSubstitutionPreprocessor ps = new ParameterSubstitutionPreprocessor(50);
            pigIStream = new BufferedReader(new FileReader(basedir + "/inputComment.pig"));
            pigOStream = new FileWriter(basedir + "/output1.pig");

            String[] arg = {"date='20080228'"};
            String[] argFiles = null;
            ps.genSubstitutedFile(pigIStream , pigOStream , arg , argFiles);

            FileInputStream pigResultStream = new FileInputStream(basedir + "/output1.pig");
            pigExResultStream = new FileInputStream(basedir + "/ExpectedResultComment.pig");
            BufferedReader inExpected = new BufferedReader(new InputStreamReader(pigExResultStream));
            BufferedReader inResult = new BufferedReader(new InputStreamReader(pigResultStream));

            String exLine;
            String resLine;
            int lineNum=0;

            while (true) {
                lineNum++;
                exLine = inExpected.readLine();
                resLine = inResult.readLine();
                if (exLine==null || resLine==null)
                    break;
                assertEquals("Expected : "+exLine+" , but got : "+resLine+" in line num : "+lineNum ,exLine.trim(), resLine.trim());
            }
            if (!(exLine==null && resLine==null)) {
                fail ("Expected : "+exLine+" , but got : "+resLine+" in line num : "+lineNum);
            }

            inExpected.close();
            inResult.close();
        } catch (ParseException e) {
            fail ("Got ParseException : " + e.getMessage());
        } catch (RuntimeException e) {
            fail ("Got RuntimeException : " + e.getMessage());
        } catch (Error e) {
            fail ("Got error : " + e.getMessage());
        }

    }
    
    /* Test case 
     *  Use a parameter within a pig function argument (containing newline characters).
     *  provide value for it. 
     */
    @Test 
    public void testSubstitutionInFuncArgs() throws Exception{
        log.info("Starting test testSubstitutionInFuncArgs()");
        final String queryString = 
    "  avro = LOAD '/data/part-m-00000.avro' USING PigStorage ();\n" +
    "   avro2 = FOREACH avro GENERATE  browser_id, component_version, " +
                                   "member_id, page_key, session_id, tracking_time, type;\n" +

    "    fs -rmr testOut/out1;\n" +
    "    STORE avro2 INTO 'testOut/out2'\n" +
    "    USING PigStorage (\n" +
    "    ' {\n" +
    "   \"debug\": $debug,\n" +
    "    \"schema\":\n" +
    "        { \"type\":\"record\",\"name\":\"$name\",   \n" +
    "          \"fields\": [ {\"name\":\"browser_id\", \"type\":[\"null\",\"string\"]},  \n" +
    "                      {\"name\":\"component_version\",\"type\":\"int\"},\n" +
    "                      {\"name\":\"member_id\",\"type\":\"int\"},\n" + 
    "                      {\"name\":\"page_key\",\"type\":[\"null\",\"string\"]},\n" + 
    "                      {\"name\":\"session_id\",\"type\":\"long\"},\n" + 
    "                      {\"name\":\"tracking_time\",\"type\":\"long\"},\n" + 
    "                      {\"name\":\"type\",\"type\":[\"null\",\"string\"]}\n" + 
    "                   ]\n" +
    "        }\n" +
    "    }\n"+
    "    ');";

        final String expectedString = 
            "  avro = LOAD '/data/part-m-00000.avro' USING PigStorage ();\n" +
            "   avro2 = FOREACH avro GENERATE  browser_id, component_version, " +
                                           "member_id, page_key, session_id, tracking_time, type;\n" +

            "    fs -rmr testOut/out1;\n" +
            "    STORE avro2 INTO 'testOut/out2'\n" +
            "    USING PigStorage (\n" +
            "    ' {\n" +
            "   \"debug\": 5,\n" +
            "    \"schema\":\n" +
            "        { \"type\":\"record\",\"name\":\"TestRecord\",   \n" +
            "          \"fields\": [ {\"name\":\"browser_id\", \"type\":[\"null\",\"string\"]},  \n" +
            "                      {\"name\":\"component_version\",\"type\":\"int\"},\n" +
            "                      {\"name\":\"member_id\",\"type\":\"int\"},\n" + 
            "                      {\"name\":\"page_key\",\"type\":[\"null\",\"string\"]},\n" + 
            "                      {\"name\":\"session_id\",\"type\":\"long\"},\n" + 
            "                      {\"name\":\"tracking_time\",\"type\":\"long\"},\n" + 
            "                      {\"name\":\"type\",\"type\":[\"null\",\"string\"]}\n" + 
            "                   ]\n" +
            "        }\n" +
            "    }\n"+
            "    ');";
        try {
            ParameterSubstitutionPreprocessor ps = new ParameterSubstitutionPreprocessor(50);
            pigIStream = new BufferedReader(
                                            new InputStreamReader(new ByteArrayInputStream(queryString.getBytes("UTF-8"))));
            pigOStream = new FileWriter(basedir + "/output26.pig");

            String[] arg = {"debug = '5'", "name = 'TestRecord'"}; 
            String[] argFiles = null;
            ps.genSubstitutedFile(pigIStream , pigOStream , arg , argFiles);

            FileInputStream pigResultStream = new FileInputStream(basedir + "/output26.pig");
            InputStream expected = new ByteArrayInputStream(expectedString.getBytes("UTF-8"));
            BufferedReader inExpected = new BufferedReader(new InputStreamReader(expected));
            BufferedReader inResult = new BufferedReader(new InputStreamReader(pigResultStream));

            String exLine;
            String resLine;
            int lineNum=0;

            while (true) {
                lineNum++;
                exLine = inExpected.readLine();
                resLine = inResult.readLine();
                if (exLine==null || resLine==null)
                    break;
                assertEquals("Parameter substitution with shell command failed. " + "Expected : "+exLine+" , but got : "+resLine+" in line num : "+lineNum ,exLine.trim(), resLine.trim());
            }
            if (!(exLine==null && resLine==null)) {
                fail ("Parameter substitution with shell command failed. " + "Expected : "+exLine+" , but got : "+resLine+" in line num : "+lineNum);
            }

            inExpected.close();
            inResult.close();
        } catch (ParseException e) {
            fail ("Got ParseException : " + e.getMessage());
        } catch (RuntimeException e) {
            fail ("Got RuntimeException : " + e.getMessage());
        } catch (Error e) {
            fail ("Got error : " + e.getMessage());
        }
        log.info("Done");
    }

    @Test
    public void testMacroDef() throws Exception{
        log.info("Starting test testMacroDef() ...");
        try {
            ParameterSubstitutionPreprocessor ps = new ParameterSubstitutionPreprocessor(50);
            pigIStream = new BufferedReader(new FileReader(basedir + "/input6.pig"));
            pigOStream = new FileWriter(basedir + "/output1.pig");

            String[] arg = {"date=20080228"};
            String[] argFiles = null;
            ps.genSubstitutedFile(pigIStream , pigOStream , arg , argFiles);

            FileInputStream pigResultStream = new FileInputStream(basedir + "/output1.pig");
            pigExResultStream = new FileInputStream(basedir + "/ExpectedResult6.pig");
            BufferedReader inExpected = new BufferedReader(new InputStreamReader(pigExResultStream));
            BufferedReader inResult = new BufferedReader(new InputStreamReader(pigResultStream));

            String exLine;
            String resLine;
            int lineNum=0;

            while (true) {
                lineNum++;
                exLine = inExpected.readLine();
                resLine = inResult.readLine();
                if (exLine==null || resLine==null)
                    break;
                assertEquals("Command line parameter substitution failed. " + "Expected : "+exLine+" , but got : "+resLine+" in line num : "+lineNum ,exLine.trim(), resLine.trim());
            }
            if (!(exLine==null && resLine==null)) {
                fail ("Command line parameter substitution failed. " + "Expected : "+exLine+" , but got : "+resLine+" in line num : "+lineNum);
            }

            inExpected.close();
            inResult.close();
        } catch (ParseException e) {
            fail ("Got ParseException : " + e.getMessage());
        } catch (RuntimeException e) {
            fail ("Got RuntimeException : " + e.getMessage());
        } catch (Error e) {
            fail ("Got error : " + e.getMessage());
        }

        log.info("Done");

    }
}

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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.Shell;
import org.apache.pig.tools.parameters.ParameterSubstitutionPreprocessor;
import org.apache.pig.tools.parameters.ParseException;
import org.junit.Test;

public class TestParamSubPreproc {
    private final Log log = LogFactory.getLog(getClass());

    private BufferedReader pigIStream;
    private FileWriter pigOStream;
    private FileInputStream pigExResultStream;
    private String basedir = "test/org/apache/pig/test/data";

    /* Test case 1
     * Use a parameter within a pig script and provide value on the command line.
     */
    @Test
    public void testCmdlineParam() throws Exception{
        log.info("Starting test testCmdlineParam() ...");
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

        log.info("Done");

    }

    /* Test case 2
     * Use a parameter within a pig script and provide value through a file.
     */
    @Test
    public void testFileParam() throws Exception{
        log.info ("Starting test  testFileParam()");
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

        log.info("Done");
    }

    /* Test case 3
     *  Use a parameter within a pig script and provide value through running a binary or script.
     */
     @Test
     public void testShellCommand() throws Exception{
        log.info("Starting test testShellCommand()");
        ParameterSubstitutionPreprocessor ps = new ParameterSubstitutionPreprocessor(50);
        pigIStream = WithConditionalReplacement(basedir + "/input4.pig", "sh test/org/apache/pig/test/data/generate_date.sh",
                "test/org/apache/pig/test/data/generate_date.bat", Shell.WINDOWS);
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
        } catch (ParseException e) {
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

        log.info("Done");
    }


    /* Test case 8
     *  Use a parameter within a pig script, provide value for it through running a binary or script.
     *  The script itself takes an argument that is a parameter and should be resolved
     */
    @Test
    public void testSubstitutionWithinShellCommand() throws Exception{
        log.info("Starting test testSubstitutionWithinShellCommand()");
        ParameterSubstitutionPreprocessor ps = new ParameterSubstitutionPreprocessor(50);
        pigIStream = WithConditionalReplacement(basedir + "/inputSubstitutionWithinShellCommand.pig", "sh test/org/apache/pig/test/data/generate_date.sh",
                "test/org/apache/pig/test/data/generate_date.bat", Shell.WINDOWS);
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
        log.info("Done");
    }


    /* Test case 9
     *  Use parameters passed on the command line/file that are not resolved prior to the declare statement.
     */
    @Test
    public void testCmdlineParamPriortoDeclare() throws Exception{
        log.info("Starting test testCmdlineParamPriortoDeclare()");
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

        log.info("Done");
    }


    /* Test case 10
     *  Use a command name in a %declare statement as a parameter
     */
    @Test
    public void testCmdnameAsParamDeclare() throws Exception{
        log.info("Starting test testCmdnameAsParamDeclare()");
        ParameterSubstitutionPreprocessor ps = new ParameterSubstitutionPreprocessor(50);
        pigIStream = WithConditionalReplacement(basedir + "/inputCmdnameAsParamDeclare.pig", "sh \\$cmd.sh \\$date",
                "\\$cmd.bat \\$date", Shell.WINDOWS);
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
        log.info("Done");
    }


    /* Test case 11
     * Use the same parameter multiple times on the command line.
     * Result : last value used and warning should be thrown
     */
    @Test
    public void testMultipleCmdlineParam() throws Exception{
        log.info("Starting test testCmdnameAsParamDeclare()");
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
        log.info("Done");
    }

    /* Test case 12
     * Read parameters from multiple files.
     */
    @Test
    public void testFileParamsFromMultipleFiles() throws Exception{
        log.info("Starting test testFileParamsFromMultipleFiles()");
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
        log.info("Done");
    }

    /* Test case 13
     * Use the same parameter in multiple files.
     * Result: last value used and warning should be thrown
     */
    @Test
    public void testSameParamInMultipleFiles() throws Exception{
        log.info("Starting test testSameParamInMultipleFiles()");
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
        log.info("Done");
    }

    /* Test case 14
     * Use the same parameter multiple times in a single file.
     * Result: last value used and warning should be thrown.
     */
    @Test
    public void testMultipleParamsFromSingleFile() throws Exception{
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
        ps.genSubstitutedFile(pigIStream , pigOStream , arg , argFiles);
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
        ps.genSubstitutedFile(pigIStream , pigOStream , arg , argFiles);
    }

     /* Test case 20
     * Use a combination of command line and file parameters.
     */
    @Test
    public void testCmdlineFileCombo() throws Exception{
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
    }

    /* Test case 21
     * Use a combination of command line and file parameters where there are duplicate parameters.
     * Result: Command line parameters take precedence over files.
     */
    @Test
    public void testCmdlineFileComboDuplicate() throws Exception{
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
    }


    /* Test case 22
     *  Use a combination of command line, file and declare.
     */
    @Test
    public void testCmdlineFileDeclareCombo() throws Exception{
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
    }

    /* Test case 23
     *  Use a combination of command line, file and declare where there are duplicate parameters.
     *  Result: Declare has highest priority.
     */
    @Test
    public void testCmdlineFileDeclareComboDuplicates() throws Exception{
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
    }

    /* Test case 24
     *   Use multiple declare statement that specify the same parameter.
     *   Result: Scope of a parameter declared using declare is until the next declare statement that defines the same parameter.
     */
    @Test
    public void testMultipleDeclareScope() throws Exception{
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
    }

    /* Test case 25
     *   Use %default to define param values
     */
    @Test
    public void testDefaultParam() throws Exception{
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
    }

    /* Test case 26
     *  Use a combination of file, command line, declare and default. Default has the lowest precedence.
     */
    @Test
    public void testCmdlineFileDeclareDefaultComboDuplicates() throws Exception{
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
    }


    /* Test case 28
     *  28. More than 1 parameter is present and needs to be substituted within a line e.g. A = load '/data/$name/$date';
     */
    @Test
    public void testMultipleParamsinSingleLine() throws Exception{
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
    }

    /* Test case 29
     * Parameter is substituted within a literal e.g. store A into 'output/$name';
     */
    @Test
    public void testSubstituteWithinLiteral() throws Exception{
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
    }

    /* Test case 30
     * Make sure that escaped values are not substituted e.g. A = load '/data/\$name'
     */
    @Test
    public void testEscaping() throws Exception{
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
    }

    /* Test case 31
     * Use of inline command
     */
    @Test
    public void testCmdlineParamWithInlineCmd() throws Exception{
        log.info("Starting test testCmdlineParamWithInlineCmd() ...");
        ParameterSubstitutionPreprocessor ps = new ParameterSubstitutionPreprocessor(50);
        pigIStream = new BufferedReader(new FileReader(basedir + "/input1.pig"));
        pigOStream = new FileWriter(basedir + "/output1.pig");

        String[] arg = {"date=`perl -e \"print qq@20080228\\n20070101@\" | head -n 1`"};
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

        log.info("Done");
    }

    /* Test case 32
     * No substitution
     */
    @Test
    public void testNoVars() throws Exception{
        log.info("Starting test testNoVars() ...");
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
        log.info("Done");
    }

    /* Test case 32
     * Test special values in characters
     */
    @Test
    public void testComplexVals() throws Exception{
        log.info("Starting test testComplexVals() ...");

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

        log.info("Done");
    }

    /* Test case 25
     *   Test that params in comments are untouched
     */
    @Test
    public void testCommentWithParam() throws Exception{
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

        log.info("Done");
    }

    /* Test case
     *   Test that $ signs in substitution value are treated as literal replacement strings.
     */
    @Test
    public void testSubstitutionWithDollarSign() throws Exception{
        ParameterSubstitutionPreprocessor ps = new ParameterSubstitutionPreprocessor(50);
        pigIStream = new BufferedReader(new FileReader(basedir + "/inputDollarSign.pig"));
        pigOStream = new FileWriter(basedir + "/output1.pig");

        String[] arg = {"filter=\"($0 == 'x') and ($1 == 'y')\""};
        String[] argFiles = null;
        ps.genSubstitutedFile(pigIStream , pigOStream , arg , argFiles);

        FileInputStream pigResultStream = new FileInputStream(basedir + "/output1.pig");
        pigExResultStream = new FileInputStream(basedir + "/ExpectedResultDollarSign.pig");
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
    }

    @Test
    public void testMacroDef() throws Exception{
        log.info("Starting test testMacroDef() ...");

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

        log.info("Done");
    }

    @Test
    public void testCmdlineParamCurlySyntax() throws Exception{
        ParameterSubstitutionPreprocessor ps = new ParameterSubstitutionPreprocessor(50);
        pigIStream = new BufferedReader(new FileReader(basedir + "/input7.pig"));
        pigOStream = new FileWriter(basedir + "/output1.pig");

        String[] arg = {"date=20080228"};
        String[] argFiles = null;
        ps.genSubstitutedFile(pigIStream , pigOStream , arg , argFiles);

        FileInputStream pigResultStream = new FileInputStream(basedir + "/output1.pig");
        pigExResultStream = new FileInputStream(basedir + "/ExpectedResult7.pig");
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
    }

    @Test
    public void testGruntWithParamSub() throws Exception{
        log.info("Starting test testGruntWithParamSub()");
        File inputFile = Util.createFile(new String[]{"daniel\t10","jenny\t20"});
        File outputFile = File.createTempFile("tmp", "");
        outputFile.delete();
        String command = "a = load '" + Util.encodeEscape(inputFile.toString()) + "' as ($param1:chararray, $param2:int);\n"
                + "store a into '" + outputFile.toString() + "';\n"
                + "quit\n";
        System.setIn(new ByteArrayInputStream(command.getBytes()));
        org.apache.pig.PigRunner.run(new String[] {"-x", "local", "-p", "param1=name", "-p", "param2=age"}, null);
        File[] partFiles = outputFile.listFiles(new FilenameFilter() {
            public boolean accept(File dir, String name) { 
            return name.startsWith("part");
        }
        });
        String resultContent = Util.readFile(partFiles[0]);
        assertEquals(resultContent, "daniel\t10\njenny\t20\n");
    }

    @SuppressWarnings("resource")
    private BufferedReader WithConditionalReplacement(String filename, String orig, String dest, boolean replace) throws IOException {
        BufferedReader pigOrigIStream = new BufferedReader(new FileReader(filename));
        BufferedReader result;
         
         if (replace) {
            File tmpInputFile = File.createTempFile("tmp", "");
            PrintWriter tmppw = new PrintWriter(tmpInputFile);
            String line;
            while ((line = pigOrigIStream.readLine())!=null) {
                line = line.replaceAll(orig, dest);
                tmppw.println(line);
            }
            pigOrigIStream.close();
            tmppw.close();
            result = new BufferedReader(new FileReader(tmpInputFile));
         } else {
            result = pigOrigIStream;
         }
         return result;
     }
}
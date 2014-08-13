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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.logical.expression.ConstantExpression;
import org.apache.pig.newplan.logical.expression.LogicalExpressionPlan;
import org.apache.pig.newplan.logical.relational.LOFilter;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.junit.Test;

public class TestPigScriptParser {

    @Test
    public void testParserWithEscapeCharacters() throws Exception {

        // All the needed variables
        PigContext pigContext = new PigContext(ExecType.LOCAL, new Properties());
        PigServer pigServer = new PigServer( pigContext );
        pigContext.connect();

        String tempFile = this.prepareTempFile();

        String query = String.format("A = LOAD '%s';", Util.encodeEscape(tempFile));
        // Start the real parsing job

        // Initial statement
        Util.buildLp(pigServer, query);

        {
            // Normal condition
            String q = query + "B = filter A by $0 eq 'This is a test string';";
            checkParsedConstContent(pigServer, pigContext, q, "This is a test string");
        }

        {
            // single-quote condition
            String q = query + "B = filter A by $0 eq 'This is a test \\'string';";
            checkParsedConstContent(pigServer, pigContext,
                                    q, "This is a test 'string");
        }

        {
            // escaping dot
            // the reason we have 4 backslashes below is we really want to put two backslashes but
            // since this is to be represented in a Java String, we escape each backslash with one more
            // backslash - hence 4. In a pig script in a file, this would be
            // \\.string
            String q = query + "B = filter A by $0 eq 'This is a test \\\\.string';";
            checkParsedConstContent(pigServer, pigContext,
                                    q, "This is a test \\.string");
        }

        {
            // newline condition
            String q = query + "B = filter A by $0 eq 'This is a test \\nstring';";
            checkParsedConstContent(pigServer, pigContext,
                                    q, "This is a test \nstring");
        }

        {
            // Unicode
            String q = query + "B = filter A by $0 eq 'This is a test \\uD30C\\uC774string';";
            checkParsedConstContent(pigServer, pigContext,
                                    q, "This is a test \uD30C\uC774string");
        }
    }

    @Test
    public void testDefineUDF() throws Exception {
        PigServer ps = new PigServer(ExecType.LOCAL);
        String inputData[] = {
                "dshfdskfwww.xyz.com/sportsjoadfjdslpdshfdskfwww.xyz.com/sportsjoadfjdsl" ,
                "kas;dka;sd" ,
                "jsjsjwww.xyz.com/sports" ,
                "jsdLSJDcom/sports" ,
                "wwwJxyzMcom/sports"
        };
        File f = Util.createFile(inputData);

        String[] queryLines = new String[] {
                // the reason we have 4 backslashes below is we really want to put two backslashes but
                // since this is to be represented in a Java String, we escape each backslash with one more
                // backslash - hence 4. In a pig script in a file, this would be
                // www\\.xyz\\.com
                "define minelogs org.apache.pig.test.RegexGroupCount('www\\\\.xyz\\\\.com/sports');" ,
                "A = load '" + Util.generateURI(Util.encodeEscape(f.getAbsolutePath()), ps.getPigContext()) + "'  using PigStorage() as (source : chararray);" ,
                "B = foreach A generate minelogs(source) as sportslogs;" };
        for (String line : queryLines) {
            ps.registerQuery(line);
        }
        Iterator<Tuple> it = ps.openIterator("B");
        int[] expectedResults = new int[] {2,0,1,0,0};
        int i = 0;
        while (it.hasNext()) {
            Tuple t = it.next();
            assertEquals(expectedResults[i++], t.get(0));
        }
    }

    @Test
    public void testSplitWithNotEvalCondition() throws Exception {
        String defineQ = "define minelogs org.apache.pig.test.RegexGroupCount('www\\\\.xyz\\\\.com/sports');";
        String defineL = "a = load 'nosuchfile' " +
                " using PigStorage() as (source : chararray);";
        String defineSplit = "SPLIT a INTO a1 IF (minelogs(source) > 0 ), a2 IF (NOT (minelogs(source)>0));";//    (NOT ( minelogs(source) ) > 0);";
        PigServer ps = new PigServer(ExecType.LOCAL);
        ps.registerQuery(defineQ);
        ps.registerQuery(defineL);
        ps.registerQuery(defineSplit);
    }


    @Test(expected = FrontendException.class)
    public void testErrorMessageUndefinedAliasInGroupByStatement() throws Exception {
        String queryA = "A = load 'nosuchfile'  using PigStorage() as (f1:chararray,f2:chararray);";
        String queryB = "B = GROUP B by f1;";
        PigServer ps = new PigServer(ExecType.LOCAL);
        ps.registerQuery(queryA);
        try {
            ps.registerQuery(queryB);
        } catch (FrontendException e) {
            assertTrue(e.getMessage().contains("Undefined alias:"));
            throw e;
        }
    }

    private void checkParsedConstContent(PigServer pigServer,
                                         PigContext pigContext,
                                         String query,
                                         String expectedContent)
                                         throws Exception {
        pigContext.connect();
        LogicalPlan lp = Util.buildLp(pigServer, query + "store B into 'output';");
        // Digging down the tree
        Operator load = lp.getSources().get(0);
        Operator filter = lp.getSuccessors( load ).get(0);
        LogicalExpressionPlan comparisonPlan = ((LOFilter)filter).getFilterPlan();
        List<Operator> comparisonPlanRoots = comparisonPlan.getSinks();
        Operator compRootOne = comparisonPlanRoots.get(0);
        Operator compRootTwo = comparisonPlanRoots.get(1);


        // Here is the actual check logic
        if (compRootOne instanceof ConstantExpression) {
            assertEquals("Must be equal", expectedContent,
                        (String)((ConstantExpression)compRootOne).getValue());
        } else { // If not left, it must be right.
            assertEquals("Must be equal", expectedContent,
                        (String)((ConstantExpression)compRootTwo).getValue());
        }
    }

    private String prepareTempFile() throws IOException {
        File inputFile = File.createTempFile("test", "txt");
        inputFile.deleteOnExit();
        PrintStream ps = new PrintStream(new FileOutputStream(inputFile));
        ps.println("hohoho");
        ps.close();
        return inputFile.getPath();
    }
}


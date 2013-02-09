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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.io.FileLocalizer;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

public class TestScalarAliases  {
    private static final String BUILD_TEST_TMP = "build/test/tmp/";
    static MiniCluster cluster = MiniCluster.buildCluster();
    private PigServer pigServer;

    TupleFactory mTf = TupleFactory.getInstance();
    BagFactory mBf = BagFactory.getInstance();

    @Before
    public void setUp() throws Exception{
        //re-init the variables, so that we can switch between
        // local and mapreduce modes
        FileLocalizer.setInitialized(false);
        pigServer = new PigServer(ExecType.LOCAL);
    }

    @AfterClass
    public static void oneTimeTearDown() throws Exception {
        cluster.shutDown();
    }

    public static void deleteDirectory(File file) {
        if (file.exists()) {
            Util.deleteDirectory(file);
        }
    }

    public static File createLocalInputFile(String filename, String[] inputData)
            throws IOException {
        new File(filename).getParentFile().mkdirs();
        return Util.createLocalInputFile(filename, inputData);
    }

    // See PIG-1434
    @Test
    public void testScalarAliasesBatchNobatch() throws Exception{
        String[] input = {
                "1\t5",
                "2\t10",
                "3\t20"
        };

        String output = BUILD_TEST_TMP+"table_testScalarAliasesDir";
        TestScalarAliases.deleteDirectory(new File(output));
        // Test the use of scalars in expressions
        String inputPath = BUILD_TEST_TMP+"table_testScalarAliasesBatch";
        TestScalarAliases.createLocalInputFile(inputPath, input);
        // Test in script mode
        pigServer.setBatchOn();
        pigServer.registerQuery("A = LOAD '" + inputPath + "' as (a0: long, a1: double);");
        pigServer.registerQuery("B = group A all;");
        pigServer.registerQuery("C = foreach B generate COUNT(A) as count, MAX(A.$1) as max;");
        pigServer.registerQuery("Y = foreach A generate (a0 * C.count), (a1 / C.max);");
        pigServer.registerQuery("Store Y into '" + output + "';");
        pigServer.executeBatch();
        // Check output
        pigServer.registerQuery("Z = LOAD '" + output + "' as (a0: int, a1: double);");

        Iterator<Tuple> iter;
        Tuple t;
        iter = pigServer.openIterator("Z");

        t = iter.next();
        assertTrue(t.toString().equals("(3,0.25)"));

        t = iter.next();
        assertTrue(t.toString().equals("(6,0.5)"));

        t = iter.next();
        assertTrue(t.toString().equals("(9,1.0)"));

        assertFalse(iter.hasNext());

        iter = pigServer.openIterator("Y");

        t = iter.next();
        assertTrue(t.toString().equals("(3,0.25)"));

        t = iter.next();
        assertTrue(t.toString().equals("(6,0.5)"));

        t = iter.next();
        assertTrue(t.toString().equals("(9,1.0)"));

        assertFalse(iter.hasNext());


    }

    // See PIG-1434
    @Test
    public void testUseScalarMultipleTimes() throws Exception{
        String[] input = {
                "1\t5",
                "2\t10",
                "3\t20"
        };

        String outputY = BUILD_TEST_TMP+"table_testUseScalarMultipleTimesOutY";
        TestScalarAliases.deleteDirectory(new File(outputY));
        String outputZ = BUILD_TEST_TMP+"table_testUseScalarMultipleTimesOutZ";
        TestScalarAliases.deleteDirectory(new File(outputZ));
        // Test the use of scalars in expressions
        String inputPath = BUILD_TEST_TMP+"table_testUseScalarMultipleTimes";
        TestScalarAliases.createLocalInputFile(inputPath, input);
        pigServer.setBatchOn();
        pigServer.registerQuery("A = LOAD '" + inputPath + "' as (a0: long, a1: double);");
        pigServer.registerQuery("B = group A all;");
        pigServer.registerQuery("C = foreach B generate COUNT(A) as count, MAX(A.$1) as max;");
        pigServer.registerQuery("Y = foreach A generate (a0 * C.count), (a1 / C.max);");
        pigServer.registerQuery("Store Y into '" + outputY + "';");
        pigServer.registerQuery("Z = foreach A generate (a1 + C.count), (a0 * C.max);");
        pigServer.registerQuery("Store Z into '" + outputZ + "';");
        // Test Multiquery store
        pigServer.executeBatch();

        // Check output
        pigServer.registerQuery("M = LOAD '" + outputY + "' as (a0: int, a1: double);");

        Iterator<Tuple> iter;
        Tuple t;
        iter = pigServer.openIterator("M");

        t = iter.next();
        assertTrue(t.toString().equals("(3,0.25)"));

        t = iter.next();
        assertTrue(t.toString().equals("(6,0.5)"));

        t = iter.next();
        assertTrue(t.toString().equals("(9,1.0)"));

        assertFalse(iter.hasNext());

        // Check output
        pigServer.registerQuery("N = LOAD '" + outputZ + "' as (a0: double, a1: double);");

        iter = pigServer.openIterator("N");

        t = iter.next();
        assertTrue(t.toString().equals("(8.0,20.0)"));

        t = iter.next();
        assertTrue(t.toString().equals("(13.0,40.0)"));

        t = iter.next();
        assertTrue(t.toString().equals("(23.0,60.0)"));

        assertFalse(iter.hasNext());

        // Non batch mode
        iter = pigServer.openIterator("Y");

        t = iter.next();
        assertTrue(t.toString().equals("(3,0.25)"));

        t = iter.next();
        assertTrue(t.toString().equals("(6,0.5)"));

        t = iter.next();
        assertTrue(t.toString().equals("(9,1.0)"));

        assertFalse(iter.hasNext());

        // Check in non-batch mode
        iter = pigServer.openIterator("Z");

        t = iter.next();
        assertTrue(t.toString().equals("(8.0,20.0)"));

        t = iter.next();
        assertTrue(t.toString().equals("(13.0,40.0)"));

        t = iter.next();
        assertTrue(t.toString().equals("(23.0,60.0)"));

        assertFalse(iter.hasNext());


    }

    // See PIG-1434
    @Test
    public void testScalarWithNoSchema() throws Exception{
        String[] scalarInput = {
                "1\t5"
        };
        String[] input = {
                "1\t5",
                "2\t10",
                "3\t20"
        };
        String inputPath = BUILD_TEST_TMP+"table_testScalarWithNoSchema";
        TestScalarAliases.createLocalInputFile(inputPath, input);
        String inputPathScalar = BUILD_TEST_TMP+"table_testScalarWithNoSchemaScalar";
        TestScalarAliases.createLocalInputFile(inputPathScalar, scalarInput);
        // Load A as a scalar
        pigServer.registerQuery("A = LOAD '" + inputPath + "';");
        pigServer.registerQuery("scalar = LOAD '" + inputPathScalar + "' as (count, total);");
        pigServer.registerQuery("B = foreach A generate 5 / scalar.total;");

        Iterator<Tuple> iter = pigServer.openIterator("B");

        Tuple t = iter.next();
        assertTrue(t.get(0).toString().equals("1"));

        t = iter.next();
        assertTrue(t.get(0).toString().equals("1"));

        t = iter.next();
        assertTrue(t.get(0).toString().equals("1"));

        assertFalse(iter.hasNext());

    }

    // See PIG-1434
    @Test
    public void testScalarWithTwoBranches() throws Exception{
        String[] inputA = {
                "1\t5",
                "2\t10",
                "3\t20"
        };

        String[] inputX = {
                "pig",
                "hadoop",
                "rocks"
        };

        String output = BUILD_TEST_TMP+"testScalarWithTwoBranchesDir";
        TestScalarAliases.deleteDirectory(new File(output));
        // Test the use of scalars in expressions
        String inputPathA = BUILD_TEST_TMP+"testScalarWithTwoBranchesA";
        TestScalarAliases.createLocalInputFile(inputPathA, inputA);
        String inputPathX = BUILD_TEST_TMP+"testScalarWithTwoBranchesX";
        TestScalarAliases.createLocalInputFile(inputPathX, inputX);
        // Test in script mode
        pigServer.setBatchOn();
        pigServer.registerQuery("A = LOAD '" + inputPathA + "' as (a0: long, a1: double);");
        pigServer.registerQuery("B = group A all;");
        pigServer.registerQuery("C = foreach B generate COUNT(A) as count, MAX(A.$1) as max;");
        pigServer.registerQuery("X = LOAD '" + inputPathX + "' as (names: chararray);");
        pigServer.registerQuery("Y = foreach X generate names, C.max;");
        pigServer.registerQuery("Store Y into '" + output + "';");
        pigServer.executeBatch();
        // Check output
        pigServer.registerQuery("Z = LOAD '" + output + "' as (a0: chararray, a1: double);");

        Iterator<Tuple> iter = pigServer.openIterator("Z");

        Tuple t = iter.next();
        assertTrue(t.toString().equals("(pig,20.0)"));

        t = iter.next();
        assertTrue(t.toString().equals("(hadoop,20.0)"));

        t = iter.next();
        assertTrue(t.toString().equals("(rocks,20.0)"));

        assertFalse(iter.hasNext());

        // Check in non-batch mode
        iter = pigServer.openIterator("Y");

        t = iter.next();
        assertTrue(t.toString().equals("(pig,20.0)"));

        t = iter.next();
        assertTrue(t.toString().equals("(hadoop,20.0)"));

        t = iter.next();
        assertTrue(t.toString().equals("(rocks,20.0)"));

        assertFalse(iter.hasNext());


    }

    // See PIG-1434
    @Test
    public void testFilteredScalarDollarProj() throws Exception{
        String output = BUILD_TEST_TMP+"table_testFilteredScalarDollarProjDir";
        TestScalarAliases.deleteDirectory(new File(output));
        String[] input = {
                "1\t5\t[state#maine,city#portland]\t{(a),(b)}\t(a,b)",
                "2\t10\t\t\t",
                "3\t20\t\t\t"
        };

        // Test the use of scalars in expressions
        String inputPath = BUILD_TEST_TMP+"table_testFilteredScalarDollarProj";
        TestScalarAliases.createLocalInputFile(inputPath, input);
        // Test in script mode
        pigServer.setBatchOn();
        pigServer.registerQuery("A = LOAD '" + inputPath + "'" + " as (a0: long, a1: double, a2 : bytearray, " + "a3: bag{ t : tuple(tc : chararray)}, " + "a4: tuple(c1 : chararray, c2 : chararray) );");
        pigServer.registerQuery("B = filter A by $1 < 8;");
        pigServer.registerQuery("Y = foreach A generate (a0 * B.$0), (a1 / B.$1), B.$2, B.$2#'state', B.$3, B.a4;");
        pigServer.registerQuery("Store Y into '" + output + "';");
        pigServer.explain("Y", System.err);
        pigServer.executeBatch();
        // Check output
        pigServer.registerQuery("Z = LOAD '" + output + "' as (a0: int, a1: double);");
        pigServer.explain("Z", System.err);

        Iterator<Tuple> iter = pigServer.openIterator("Z");

        Tuple t = iter.next();
        assertTrue(t.toString().equals("(1,1.0)"));

        t = iter.next();
        assertTrue(t.toString().equals("(2,2.0)"));

        t = iter.next();
        assertTrue(t.toString().equals("(3,4.0)"));

        assertFalse(iter.hasNext());

        // Check in non-batch mode
        iter = pigServer.openIterator("Y");

        t = iter.next();
        assertEquals(t.toString(),"(1,1.0,[state#maine,city#portland],maine,{(a),(b)},(a,b))");

        t = iter.next();
        assertEquals(t.toString(),"(2,2.0,[state#maine,city#portland],maine,{(a),(b)},(a,b))");

        t = iter.next();
        assertEquals(t.toString(),"(3,4.0,[state#maine,city#portland],maine,{(a),(b)},(a,b))");

        assertFalse(iter.hasNext());


    }

    // See PIG-1434
    @Test
    public void testScalarWithNoSchemaDollarProj() throws Exception{
        String[] scalarInput = {
                "1\t5"
        };
        String[] input = {
                "1\t5",
                "2\t10",
                "3\t20"
        };
        String inputPath = BUILD_TEST_TMP+"table_testScalarWithNoSchemaDollarProj";
        TestScalarAliases.createLocalInputFile(inputPath, input);
        String inputPathScalar = BUILD_TEST_TMP+"table_testScalarWithNoSchemaDollarProjScalar";
        TestScalarAliases.createLocalInputFile(inputPathScalar, scalarInput);
        // Load A as a scalar
        pigServer.registerQuery("A = LOAD '" + inputPath + "';");
        pigServer.registerQuery("scalar = LOAD '" + inputPathScalar + "';");
        pigServer.registerQuery("B = foreach A generate 5 / scalar.$1;");

        Iterator<Tuple> iter = pigServer.openIterator("B");

        Tuple t = iter.next();
        assertTrue(t.get(0).toString().equals("1"));

        t = iter.next();
        assertTrue(t.get(0).toString().equals("1"));

        t = iter.next();
        assertTrue(t.get(0).toString().equals("1"));

        assertFalse(iter.hasNext());

    }

    // See PIG-1434
    @Test
    public void testScalarAliasesJoinClause() throws Exception{
        String[] inputA = {
                "1\t5",
                "2\t10",
                "3\t20"
        };
        String[] inputB = {
                "Total3\tthree",
                "Total2\ttwo",
                "Total1\tone"
        };

        // Test the use of scalars in expressions
        String inputPathA = BUILD_TEST_TMP+"table_testScalarAliasesJoinClauseA";
        TestScalarAliases.createLocalInputFile(inputPathA, inputA);
        String inputPathB = BUILD_TEST_TMP+"table_testScalarAliasesJoinClauseB";
        TestScalarAliases.createLocalInputFile(inputPathB, inputB);
        // Test in script mode
        pigServer.registerQuery("A = LOAD '" + inputPathA + "' as (a0, a1);");
        pigServer.registerQuery("G = group A all;");
        pigServer.registerQuery("C = foreach G generate COUNT(A) as count;");

        pigServer.registerQuery("B = LOAD '" + inputPathB + "' as (b0:chararray, b1:chararray);");
        pigServer.registerQuery("Y = join A by CONCAT('Total', (chararray)C.count), B by $0;");

        Iterator<Tuple> iter = pigServer.openIterator("Y");

        String[] expected = new String[] {
                "(1,5,Total3,three)",
                "(2,10,Total3,three)",
                "(3,20,Total3,three)"
        };

        Util.checkQueryOutputsAfterSortRecursive(iter, expected, org.apache.pig.newplan.logical.Util.translateSchema(pigServer.dumpSchema("Y")));
    }

    // See PIG-1434
    @Test
    public void testScalarAliasesFilterClause() throws Exception{
        String[] input = {
                "1\t5",
                "2\t10",
                "3\t20",
                "4\t12",
                "5\t8"
        };

        // Test the use of scalars in expressions
        String inputPath = BUILD_TEST_TMP+"table_testScalarAliasesFilterClause";
        TestScalarAliases.createLocalInputFile(inputPath, input);
        // Test in script mode
        pigServer.registerQuery("A = LOAD '" + inputPath + "' as (a0, a1);");
        pigServer.registerQuery("G = group A all;");
        pigServer.registerQuery("C = foreach G generate AVG(A.$1) as average;");

        pigServer.registerQuery("Y = filter A by a1 > C.average;");

        Iterator<Tuple> iter = pigServer.openIterator("Y");

        // Average is 11
        Tuple t = iter.next();
        assertTrue(t.toString().equals("(3,20)"));

        t = iter.next();
        assertTrue(t.toString().equals("(4,12)"));

        assertFalse(iter.hasNext());
    }

    // See PIG-1434
    @Test
    public void testScalarAliasesSplitClause() throws Exception{
        pigServer = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        String[] input = {
                "1\t5",
                "2\t10",
                "3\t20"
        };

        // Test the use of scalars in expressions
        String inputPath = "table_testScalarAliasesSplitClause";
        String output = "table_testScalarAliasesSplitClauseDir";
        Util.createInputFile(cluster, inputPath, input);
        // Test in script mode
        pigServer.setBatchOn();
        pigServer.registerQuery("A = LOAD '"+inputPath+"' as (a0: long, a1: double);");
        pigServer.registerQuery("B = group A all;");
        pigServer.registerQuery("C = foreach B generate COUNT(A) as count;");
        pigServer.registerQuery("split A into Y if (2 * C.count) < a1, X if a1 == 5;");
        pigServer.registerQuery("Store Y into '"+output+"';");
        pigServer.executeBatch();
        // Check output
        pigServer.registerQuery("Z = LOAD '"+output+"' as (a0: int, a1: double);");

        Iterator<Tuple> iter = pigServer.openIterator("Z");

        // Y gets only last 2 elements
        Tuple t = iter.next();
        assertTrue(t.toString().equals("(2,10.0)"));

        t = iter.next();
        assertTrue(t.toString().equals("(3,20.0)"));

        assertFalse(iter.hasNext());
        Util.deleteFile(cluster, output);

    }

    @Test
    public void testScalarErrMultipleRowsInInput() throws Exception{
        pigServer = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        String[] input = {
                "1\t5",
                "2\t10",
                "3\t20"
        };
        String INPUT_FILE = "table_testScalarAliasesMulRows";
        Util.createInputFile(cluster, INPUT_FILE, input);
        pigServer.registerQuery("A = LOAD '" + INPUT_FILE +  "' as (a0: long, a1: double);");
        pigServer.registerQuery("B = LOAD '" + INPUT_FILE +  "' as (b0: long, b1: double);");
        pigServer.registerQuery("C = foreach A generate $0, B.$0;");
        try {
            pigServer.openIterator("C");
            fail("exception expected - scalar input has multiple rows");
        } catch (IOException pe){
            Util.checkStrContainsSubStr(pe.getCause().getMessage(),
                    "Scalar has more than one row in the output"
            );
        }
    }


    // See PIG-1434
    @Test
    public void testScalarAliasesGrammarNegative() throws Exception{
        String[] input = {
                "1\t5",
                "2\t10",
                "3\t20"
        };

        String inputPath = BUILD_TEST_TMP+"table_testScalarAliasesGrammar";
        TestScalarAliases.createLocalInputFile(inputPath, input);

        try {
            pigServer.registerQuery("A = LOAD '" + inputPath + "' as (a0: long, a1: double);");
            pigServer.registerQuery("B = group A all;");
            pigServer.registerQuery("C = foreach B generate COUNT(A);");
            // Only projections of C are supported
            pigServer.registerQuery("Y = foreach A generate C;");
            pigServer.openIterator( "Y" );
            //Control should not reach here
            fail("Scalar projections are only supported");
        } catch (IOException pe){
            assertTrue(pe.getMessage().contains("Invalid scalar projection: C"));
        }
    }

    // See PIG-1636
    @Test
    public void testScalarAliasesLimit() throws Exception{
        String[] input = {
                "a\t1",
                "b\t2",
                "c\t3",
                "a\t4",
                "c\t5"
        };

        // Test the use of scalars in expressions
        String inputPath = BUILD_TEST_TMP+"table_testScalarAliasesLimit";
        TestScalarAliases.createLocalInputFile(inputPath, input);
        // Test in script mode
        pigServer.registerQuery("A = LOAD '" + inputPath + "' as (a0:chararray, a1: int);");
        pigServer.registerQuery("G = group A all;");
        pigServer.registerQuery("C = foreach G generate SUM(A.$1) as total;");
        pigServer.registerQuery("C1 = limit C 1;");
        pigServer.registerQuery("Y = foreach A generate a0, a1 * (double)C1.total;");

        Iterator<Tuple> iter = pigServer.openIterator("Y");

        // Average is 11
        Tuple t = iter.next();
        assertTrue(t.toString().equals("(a,15.0)"));

        t = iter.next();
        assertTrue(t.toString().equals("(b,30.0)"));

        t = iter.next();
        assertTrue(t.toString().equals("(c,45.0)"));

        t = iter.next();
        assertTrue(t.toString().equals("(a,60.0)"));

        t = iter.next();
        assertTrue(t.toString().equals("(c,75.0)"));

        assertFalse(iter.hasNext());
    }

    /**
     * Test that a specific string is included in the error message when an
     * exception is thrown for using a relation in a
     * scalar context without projecting any columns out of it
     */
    // See PIG-1788
    @Test
    public void testScalarWithNoProjection() throws Exception{
        String query =
            "  A = load 'table_testScalarWithNoProjection' as (x, y);" +
            "  B = group A by x;" +
            // B is unintentionally being used as scalar,
            // the user intends it to be COUNT(A)
            "  C = foreach B generate COUNT(B);";

        Util.checkExceptionMessage(query, "C",
                "A column needs to be projected from a relation" +
                " for it to be used as a scalar"
        );
    }
}

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

import static org.apache.pig.ExecType.LOCAL;
import static org.apache.pig.ExecType.MAPREDUCE;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.apache.pig.impl.plan.CompilationMessageCollector;
import org.apache.pig.impl.util.Utils;
import org.apache.pig.newplan.logical.expression.LogicalExpressionPlan;
import org.apache.pig.newplan.logical.relational.LOCogroup;
import org.apache.pig.newplan.logical.relational.LOJoin;
import org.apache.pig.newplan.logical.relational.LOSort;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.newplan.logical.visitor.CastLineageSetter;
import org.apache.pig.newplan.logical.visitor.ColumnAliasConversionVisitor;
import org.apache.pig.newplan.logical.visitor.SchemaAliasVisitor;
import org.apache.pig.newplan.logical.visitor.TypeCheckingRelVisitor;
import org.apache.pig.newplan.logical.visitor.UnionOnSchemaSetter;
import org.apache.pig.parser.ParserException;
import org.apache.pig.parser.ParserTestingUtils;
import org.apache.pig.test.utils.NewLogicalPlanUtil;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestProjectRange  {

    protected final Log log = LogFactory.getLog(getClass());

    protected static ExecType execType = LOCAL;

    private static MiniCluster cluster;
    protected static PigServer pigServer;
    private static final String INP_FILE_5FIELDS = "TestProjectRange_5fields";


    @BeforeClass
    public static void oneTimeSetUp() throws Exception {

        String execTypeString = System.getProperty("test.exectype");

        if(execTypeString!=null && execTypeString.length()>0){
            execType = ExecType.fromString(execTypeString);
        }

        String[] input = {"10\t20\t30\t40\t50", "11\t21\t31\t41\t51"};
        Util.createLocalInputFile(INP_FILE_5FIELDS, input);

        if(execType == MAPREDUCE) {
            cluster = MiniCluster.buildCluster();
            Util.copyFromLocalToCluster(cluster, INP_FILE_5FIELDS, INP_FILE_5FIELDS);
        }
    }

    @Before
    public void setup() throws ExecException{
        if(execType == MAPREDUCE) {
            pigServer = new PigServer(MAPREDUCE, cluster.getProperties());
        } else {
            pigServer = new PigServer(LOCAL);
        }
    }

    @After
    public void tearDown() throws Exception {
        pigServer.shutdown();
    }

    @AfterClass
    public static void oneTimeTearDown() throws Exception {
        new File(INP_FILE_5FIELDS).delete();
        if(cluster != null)
            cluster.shutDown();
    }


    /**
     * Test project-range in foreach with limits on both sides
     * @throws IOException
     * @throws ParserException
     */

    @Test
    public void testFullRangeForeachWSchema() throws IOException, ParserException {

        String query;

        //specifying the new aliases
        query =
            "  l1 = load '" + INP_FILE_5FIELDS + "' as (a : int, b : float, c : int, d : int, e : int);"
            + "f = foreach l1 generate a .. c as (aa, bb, cc);"
            ;
        compileAndCompareSchema("aa : int, bb : float, cc : int", query, "f");

        //specifying the new aliases - refer to column by pos
        query =
            "  l1 = load '" + INP_FILE_5FIELDS + "' as (a : int, b : float, c : int, d : int, e : int);"
            + "f = foreach l1 generate $0 .. $2 as (aa, bb, cc);"
            ;
        compileAndCompareSchema("aa : int, bb : float, cc : int", query, "f");

        //column with pos , name
        query =
            "  l1 = load '" + INP_FILE_5FIELDS + "' as (a : int, b : float, c : int, d : int, e : int);"
            + "f = foreach l1 generate $0 .. c as (aa, bb, cc);"
            ;
        compileAndCompareSchema("aa : int, bb : float, cc : int", query, "f");


        //specifying the new aliases
        query =
            "  l1 = load '" + INP_FILE_5FIELDS + "' as (a : int, b : float, c : int, d : int, e : int);"
            + "f = foreach l1 generate b .. d as (bb, cc, dd);"
            ;
        compileAndCompareSchema("bb : float, cc : int, dd : int", query, "f");

        //begin, end of range is same
        query =
            "  l1 = load '" + INP_FILE_5FIELDS + "' as (a : int, b : float, c : int, d : int, e : int);"
            + "f = foreach l1 generate b .. b as (bb), $2 .. $2;"
            ;
        compileAndCompareSchema("bb : float, c : int", query, "f");

        // without aliases - two projections
        query =
            "  l1 = load '" + INP_FILE_5FIELDS + "' as (a : int, b : int, c : int, d : int, e : int);"
            + "f = foreach l1 generate a .. c, d .. e ;"
            ;
        compileAndCompareSchema("a : int, b : int, c : int, d : int, e : int", query, "f");

        // without aliases
        query =
            "  l1 = load '" + INP_FILE_5FIELDS + "' as (a : int, b : int, c : int, d : int, e : int);"
            + "f = foreach l1 generate a .. c ;"
            ;
        compileAndCompareSchema("a : int, b : int, c : int", query, "f");
        Iterator<Tuple> it = pigServer.openIterator("f");

        List<Tuple> expectedRes =
            Util.getTuplesFromConstantTupleStrings(
                    new String[] {
                            "(10,20,30)",
                            "(11,21,31)",
                    });
        Util.checkQueryOutputsAfterSort(it, expectedRes);

    }


    private void compileAndCompareSchema(String expectedSchStr, String query, String alias)
    throws IOException, ParserException {

        Schema expectedSch = null;

        if(expectedSchStr != null)
            expectedSch = Utils.getSchemaFromString(expectedSchStr);

        compileAndCompareSchema(expectedSch, query, alias);

    }

    private void compileAndCompareSchema(Schema expectedSch, String query,
            String alias) throws IOException {
        Util.registerMultiLineQuery(pigServer, query);

        Schema sch = pigServer.dumpSchema(alias);
        assertEquals("Checking expected schema", expectedSch, sch);
    }

    /**
     * Test project-range in foreach with starting limit
     * @throws IOException
     * @throws ParserException
     */
    @Test
    public void testEndRangeForeachWSchema() throws IOException, ParserException {

        //specifying the new aliases
        String query;
        query =
            "  l1 = load '" + INP_FILE_5FIELDS + "' as (a : int, b : float, c : int, d : int, e : int);"
            + "f = foreach l1 generate  .. c as (aa, bb, cc);"
            ;
        compileAndCompareSchema("aa : int, bb : float, cc : int", query, "f");

        //col position
        query =
            "  l1 = load '" + INP_FILE_5FIELDS + "' as (a : int, b : float, c : int, d : int, e : int);"
            + "f = foreach l1 generate  .. $2 as (aa, bb, cc);"
            ;
        compileAndCompareSchema("aa : int, bb : float, cc : int", query, "f");

        //end is the beginning!
        query =
            "  l1 = load '" + INP_FILE_5FIELDS + "' as (a : int, b : float, c : int, d : int, e : int);"
            + "f = foreach l1 generate  .. $0 as (aa, bb, cc);"
            ;
        compileAndCompareSchema("aa : int", query, "f");


        query =
            "  l1 = load '" + INP_FILE_5FIELDS + "' as (a : int, b : float, c : int, d : int, e : int);"
            + "f = foreach l1 generate  .. c as (aa, bb, cc);"
            ;
        compileAndCompareSchema("aa : int, bb : float, cc : int", query, "f");

        // without aliases
        query =
            "  l1 = load '" + INP_FILE_5FIELDS + "' as (a : int, b : int, c : int, d : long, e : int);"
            + "f = foreach l1 generate  .. $3 ;"
            ;
        compileAndCompareSchema("a : int, b : int, c : int, d : long", query, "f");
        Iterator<Tuple> it = pigServer.openIterator("f");

        List<Tuple> expectedRes =
            Util.getTuplesFromConstantTupleStrings(
                    new String[] {
                            "(10,20,30,40L)",
                            "(11,21,31,41L)",
                    });
        Util.checkQueryOutputsAfterSort(it, expectedRes);

    }


    /**
     * Test project-range in foreach with start limit
     * @throws IOException
     * @throws ParserException
     */
    @Test
    public void testStartRangeForeachWSchema() throws IOException, ParserException {

        //specifying the new aliases
        String query =
            "  l1 = load '" + INP_FILE_5FIELDS + "' " +
            		"as (a : int, b : float, c : int, d : int, e : int);"
            + "f = foreach l1 generate  c ..  as (aa, bb, cc);"
            ;
        compileAndCompareSchema("aa : int, bb : int, cc : int", query, "f");

        // without aliases
        query =
            "  l1 = load '" + INP_FILE_5FIELDS + "' " +
                        "as (a : int, b : int, c : int, d : long, e : int);"
            + "f = foreach l1 generate  $1 ..  ;"
            ;
        compileAndCompareSchema("b : int, c : int, d : long, e : int", query, "f");

        //start with last column - beginning is the end!
        query =
            "  l1 = load '" + INP_FILE_5FIELDS + "' " +
                        "as (a : int, b : int, c : int, d : long, e : int);"
            + "f = foreach l1 generate  e ..  ;"
            ;
        compileAndCompareSchema("e : int", query, "f");

        //specifying the new aliases for one
        query =
            "  l1 = load '" + INP_FILE_5FIELDS + "' " +
            		"as (a : int, b : long, c : int, d : int, e : int);"
            + "f = foreach l1 generate  c ..  as (aa, bb, cc), b .. ;"
            ;
        compileAndCompareSchema(
                "aa : int, bb : int, cc : int, b : long, c : int, d : int, e : int",
                query,
                "f"
        );

        Iterator<Tuple> it = pigServer.openIterator("f");

        List<Tuple> expectedRes =
            Util.getTuplesFromConstantTupleStrings(
                    new String[] {
                            "(30,40,50,20L,30,40,50)",
                            "(31,41,51,21L,31,41,51)",

                    });
        Util.checkQueryOutputsAfterSort(it, expectedRes);

    }


    /**
     * Test multiple different types of range-project with foreach
     * @throws IOException
     * @throws ParserException
     */
   @Test
    public void testMixRangeForeachWSchema() throws IOException, ParserException {

        //specifying the new aliases
        String query;
        query =
            "  l1 = load '" + INP_FILE_5FIELDS + "' as (a : int, b : float, c : int, d : int, e : int);"
            + "f = foreach l1 generate  .. b, c .. d, d .. as (aa, bb);"
            ;
        compileAndCompareSchema("a : int, b : float, c : int, d : int, aa : int, bb : int", query, "f");


        // without aliases
        query =
            "  l1 = load '" + INP_FILE_5FIELDS + "' as (a : int, b : int, c : int, d : long, e : int);"
            + "f = foreach l1 generate ..$0 as (first), e.. as (last), d ..,  .. b ;"
            ;
        compileAndCompareSchema("first : int, last : int, d : long, e : int, a : int, b : int", query, "f");
        Iterator<Tuple> it = pigServer.openIterator("f");

        List<Tuple> expectedRes =
            Util.getTuplesFromConstantTupleStrings(
                    new String[] {
                            "(10,50,40L,50,10,20)",
                            "(11,51,41L,51,11,21)",
                    });
        Util.checkQueryOutputsAfterSort(it, expectedRes);

    }

    /**
     * -ve test cases
     * @throws IOException
     * @throws ParserException
     */
    @Test
    public void testNegativeForeachWSchema() throws IOException, ParserException {
        String query;
        query =
            "  l1 = load '" + INP_FILE_5FIELDS + "' as (a : int, b : float, c : int, d : int, e : int);"
            + "f = foreach l1 generate  $3 .. $1;"
            ;
        Util.checkExceptionMessage(query, "f",
                "start column appears after end column in range projection");

        query =
            "  l1 = load '" + INP_FILE_5FIELDS + "' as (a : int, b : float, c : int, d : int, e : int);"
            + "f = foreach l1 generate  c .. b;"
            ;
        Util.checkExceptionMessage(query, "f",
                "start column appears after end column in range projection");
    }


    /**
     * -ve test cases
     * @throws IOException
     * @throws ParserException
     */
    @Test
    public void testNegativeForeachNOSchema() throws IOException, ParserException {
        String query;
        query =
            "  l1 = load '" + INP_FILE_5FIELDS + "';"
            + "f = foreach l1 generate  $3 .. $1;"
            ;
        Util.checkExceptionMessage(query, "f",
                "start column appears after end column in range projection");

        query =
            "  l1 = load '" + INP_FILE_5FIELDS + "' ;"
            + "f = foreach l1 generate  a .. b;"
            ;
        Util.checkExceptionMessage(query, "f",
                "Invalid field projection. Projected field [a] does not exist.");
    }

    /**
     * Test foreach without schema
     * @throws IOException
     * @throws ParserException
     */
    @Test
    public void testStartRangeForeachNOSchema() throws IOException, ParserException {

        String query;

        // without aliases
        query =
            "  l1 = load '" + INP_FILE_5FIELDS + "';"
            + "f = foreach l1 generate ..$3  as (a,b,c,d);"
            ;
        compileAndCompareSchema("a : bytearray,b : bytearray,c : bytearray,d : bytearray", query, "f");


        Util.registerMultiLineQuery(pigServer, query);

        pigServer.explain("f", System.err);
        Iterator<Tuple> it = pigServer.openIterator("f");

        List<Tuple> expectedRes =
            Util.getTuplesFromConstantTupleStringAsByteArray(
                    new String[] {
                            "('10','20','30','40')",
                            "('11','21','31','41')",
                    });
        Util.checkQueryOutputsAfterSort(it, expectedRes);

    }


    /**
     * Test foreach without schema
     * @throws IOException
     * @throws ParserException
     */
    @Test
    public void testMixRangeForeachNOSchema() throws IOException, ParserException {

        String query;

        // without aliases
        query =
            "  l1 = load '" + INP_FILE_5FIELDS + "';"
            + "f = foreach l1 generate ..$0 as (first), $4.. as (last), $3 ..,  .. $1 ;"
            ;
        compileAndCompareSchema((Schema)null, query, "f");


        Util.registerMultiLineQuery(pigServer, query);

        pigServer.explain("f", System.err);
        Iterator<Tuple> it = pigServer.openIterator("f");

        List<Tuple> expectedRes =
            Util.getTuplesFromConstantTupleStringAsByteArray(
                    new String[] {
                            "('10','50','40','50','10','20')",
                            "('11','51','41','51','11','21')",
                    });
        Util.checkQueryOutputsAfterSort(it, expectedRes);

    }

    /**
     * Test foreach without schema - with some operations after the foreach
     * @throws IOException
     * @throws ParserException
     */
    @Test
    public void testRangeForeachWFilterNOSchema() throws IOException, ParserException {
        String query;

        query =
            "  l1 = load '" + INP_FILE_5FIELDS + "';"
            + "f = foreach l1 generate ..$0 as (first), $4.. as (last), $3 ..,  .. $1 ;"
            + " fil = filter f by $0 > 10;"
            ;

        Util.registerMultiLineQuery(pigServer, query);

        pigServer.explain("fil", System.err);
        Iterator<Tuple> it = pigServer.openIterator("fil");

        List<Tuple> expectedRes =
            Util.getTuplesFromConstantTupleStringAsByteArray(
                    new String[] {
                            "('11','51','41','51','11','21')",
                    });
        Util.checkQueryOutputsAfterSort(it, expectedRes);

    }

    @Test
    public void testRangeOrderByWSchema() throws IOException, ParserException{
        String query;

        {
            query =
                "  l1 = load '" + INP_FILE_5FIELDS +
                        "' as (a : int, b : long, c : int, d : int, e : int);"
                + " o = order l1 by  .. $2 DESC ;"
                ;
            compileAndCompareSchema("a : int, b : long, c : int, d : int, e : int", query, "o");

            //check number of sort expression plans

            LogicalPlan lp = createAndProcessLPlan(query);
            boolean[] isAsc = {false,false,false};
            checkNumExpressionPlansForSort(lp, 3, isAsc);
        }

        {
            query =
                "  l1 = load '" + INP_FILE_5FIELDS +
                        "' as (a : int, b : long, c : int, d : int, e : int);"
                + " o = order l1 by  $3 ..  ;"
                ;
            compileAndCompareSchema("a : int, b : long, c : int, d : int, e : int", query, "o");

            //check number of sort expression plans

            LogicalPlan lp = createAndProcessLPlan(query);
            boolean[] isAsc = {true, true};
            checkNumExpressionPlansForSort(lp, 2, isAsc);
        }

        {
            query =
                "  l1 = load '" + INP_FILE_5FIELDS +
                        "' as (a : int, b : long, c : int, d : int, e : int);"
                + " o = order l1 by  d .. DESC  ;"
                ;
            compileAndCompareSchema("a : int, b : long, c : int, d : int, e : int", query, "o");

            //check number of sort expression plans

            LogicalPlan lp = createAndProcessLPlan(query);
            boolean[] isAsc = {false, false};
            checkNumExpressionPlansForSort(lp, 2, isAsc);
        }

        {
            query =
                "  l1 = load '" + INP_FILE_5FIELDS +
                        "' as (a : int, b : long, c : int, d : int, e : int);"
                + " f = foreach l1 generate *;"
                + " o = order f by  $0 .. c ASC  ;"
                + " lim = limit o 10; ;"
                ;
            compileAndCompareSchema("a : int, b : long, c : int, d : int, e : int", query, "lim");

            //check number of sort expression plans

            LogicalPlan lp = createAndProcessLPlan(query);
            boolean[] isAsc = {true, true, true};
            checkNumExpressionPlansForSort(lp, 3, isAsc);
        }

        query =
            "  l1 = load '" + INP_FILE_5FIELDS +
                    "' as (a : int, b : long, c : int, d : int, e : int);"
            + " o = order l1 by $0 .. $4  ;"
            ;
        compileAndCompareSchema("a : int, b : long, c : int, d : int, e : int", query, "o");

        //check number of sort expression plans

        LogicalPlan lp = createAndProcessLPlan(query);
        boolean[] isAsc = {true,true,true,true,true};
        checkNumExpressionPlansForSort(lp, 5, isAsc);

        Util.registerMultiLineQuery(pigServer, query);

        Iterator<Tuple> it = pigServer.openIterator("o");

        List<Tuple> expectedRes =
            Util.getTuplesFromConstantTupleStrings(
                    new String[] {
                            "(10,20,30,40,50)",
                            "(11,21,31,41,51)",
                    });
        Util.checkQueryOutputs(it, expectedRes);
    }


    /**
     * Test nested order-by with schema
     * @throws IOException
     * @throws ParserException
     */
    @Test
    public void testRangeOrderByNestedWSchema() throws IOException, ParserException{
        String query;

        {
            query =
                "  l1 = load '" + INP_FILE_5FIELDS +
                        "' as (a : int, b : long, c : int, d : int, e : int);"
                + " g = group l1 by a;"
                + " f = foreach g { o = order l1 by  .. $2 DESC; generate group, o;}"
                ;
            String expectedSchStr = "g : int,o: {t : (a: int,b: long,c: int,d: int,e: int)}";
            Schema expectedSch = getCleanedGroupSchema(expectedSchStr);
            compileAndCompareSchema(expectedSch, query, "f");

            //check number of sort expression plans

            LogicalPlan lp = createAndProcessLPlan(query);
            boolean[] isAsc = {false,false,false};
            checkNumExpressionPlansForSort(lp, 3, isAsc);
        }
        {
            query =
                "  l1 = load '" + INP_FILE_5FIELDS +
                        "' as (a : int, b : long, c : int, d : int, e : int);"
                + " g = group l1 by a;"
                + " f = foreach g { o = order l1 by  d .. ; generate group, o;}"
                ;
            String expectedSchStr = "g : int,o: {t : (a: int,b: long,c: int,d: int,e: int)}";
            Schema expectedSch = getCleanedGroupSchema(expectedSchStr);
            compileAndCompareSchema(expectedSch, query, "f");

            //check number of sort expression plans

            LogicalPlan lp = createAndProcessLPlan(query);
            boolean[] isAsc = {true,true};
            checkNumExpressionPlansForSort(lp, 2, isAsc);
        }
        {

            query =
                "  l1 = load '" + INP_FILE_5FIELDS +
                        "' as (a : int, b : long, c : int, d : int, e : int);"
                + " g = group l1 by a;"
                + " f = foreach g { o = order l1 by  $2 .. $3 ASC, $1..c DESC; generate group, o;}"
                ;
            String expectedSchStr = "g : int,o: {t : (a: int,b: long,c: int,d: int,e: int)}";
            Schema expectedSch = getCleanedGroupSchema(expectedSchStr);
            compileAndCompareSchema(expectedSch, query, "f");

            //check number of sort expression plans

            LogicalPlan lp = createAndProcessLPlan(query);
            boolean[] isAsc = {true,true,false,false};
            checkNumExpressionPlansForSort(lp, 4, isAsc);

        }

        query =
            "  l1 = load '" + INP_FILE_5FIELDS +
                    "' as (a : int, b : long, c : int, d : int, e : int);"
            + " g = group l1 by a;"
            + " f = foreach g { o = order l1 by  $2 .. $3 DESC, $1 ASC; generate group, o;}"
            ;

        String expectedSchStr = "g : int,o: {t : (a: int,b: long,c: int,d: int,e: int)}";
        Schema expectedSch = getCleanedGroupSchema(expectedSchStr);
        compileAndCompareSchema(expectedSch, query, "f");

        //check number of sort expression plans
               LogicalPlan lp = createAndProcessLPlan(query);
        boolean[] isAsc = {false,false,true};
        checkNumExpressionPlansForSort(lp, 3, isAsc);

        Util.registerMultiLineQuery(pigServer, query);

        Iterator<Tuple> it = pigServer.openIterator("f");

        List<Tuple> expectedRes =
            Util.getTuplesFromConstantTupleStrings(
                    new String[] {
                            "(10,{(10,20,30,40,50)})",
                            "(11,{(11,21,31,41,51)})",
                    });
        Util.checkQueryOutputs(it, expectedRes);
    }

    /**
     * Test nested order-by without schema
     * @throws IOException
     * @throws ParserException
     */
    @Test
    public void testRangeOrderByNestedNOSchema() throws IOException, ParserException{
        String query;

        {
            query =
                "  l1 = load '" + INP_FILE_5FIELDS + "';"
                + " g = group l1 by $0;"
                + " f = foreach g { o = order l1 by  .. $2 DESC; generate group, o;}"
                ;
            String expectedSchStr = "g : bytearray, o: {t : ()}";
            Schema expectedSch = getCleanedGroupSchema(expectedSchStr);
            compileAndCompareSchema(expectedSch, query, "f");

            //check number of sort expression plans

            LogicalPlan lp = createAndProcessLPlan(query);
            boolean[] isAsc = {false,false,false};
            checkNumExpressionPlansForSort(lp, 3, isAsc);
        }
        {
            query =
                "  l1 = load '" + INP_FILE_5FIELDS + "';"
                + " g = group l1 by $0;"
                + " f = foreach g { o = order l1 by  $3 .. ; generate group, o;}"
                ;
            String expectedSchStr = "g : bytearray, o: {t : ()}";
            Schema expectedSch = getCleanedGroupSchema(expectedSchStr);
            compileAndCompareSchema(expectedSch, query, "f");

            //check number of sort expression plans

            LogicalPlan lp = createAndProcessLPlan(query);
            //project to end can't be expanded
            boolean[] isAsc = {true};
            checkNumExpressionPlansForSort(lp, 1, isAsc);
        }
        {

            query =
                "  l1 = load '" + INP_FILE_5FIELDS + "';"
                + " g = group l1 by $1;"
                + " f = foreach g { o = order l1 by  $2 .. $3 ASC, $1 .. $2 DESC; generate group, o;}"
                ;
            String expectedSchStr = "g : bytearray, o: {t : ()}";
            Schema expectedSch = getCleanedGroupSchema(expectedSchStr);
            compileAndCompareSchema(expectedSch, query, "f");

            //check number of sort expression plans

            LogicalPlan lp = createAndProcessLPlan(query);
            boolean[] isAsc = {true,true,false,false};
            checkNumExpressionPlansForSort(lp, 4, isAsc);

        }

        query =
            "  l1 = load '" + INP_FILE_5FIELDS + "';"
            + " g = group l1 by 1;"
            + " f = foreach g { o = order l1 by  $2 .. $3 desc; generate group, o;}"
            ;
        String expectedSchStr = "g : int, o: {t : ()}";
        Schema expectedSch = getCleanedGroupSchema(expectedSchStr);
        compileAndCompareSchema(expectedSch, query, "f");
        //check number of sort expression plans

        LogicalPlan lp = createAndProcessLPlan(query);
        boolean[] isAsc = {false,false};
        checkNumExpressionPlansForSort(lp, 2, isAsc);

        Util.registerMultiLineQuery(pigServer, query);

        Iterator<Tuple> it = pigServer.openIterator("f");

        List<Tuple> expectedRes =
            Util.getTuplesFromConstantTupleStrings(
                    new String[] {
                            "(1,{(11,21,31,41,51),(10,20,30,40,50)})",
                    });
        Util.checkQueryOutputs(it, expectedRes);
    }

    private LOSort checkNumExpressionPlansForSort(LogicalPlan lp, int numPlans, boolean[] isAsc) {
        Class<?> sortClass = org.apache.pig.newplan.logical.relational.LOSort.class;
        LOSort sort = (LOSort) NewLogicalPlanUtil.getRelOpFromPlan(lp, sortClass);
        assertEquals("number of sort col plans", numPlans, sort.getSortColPlans().size());

        List<Boolean> ascCols = sort.getAscendingCols();
        for(int i = 0; i < ascCols.size(); i++){
            assertEquals("ascending order", isAsc[i], ascCols.get(i));
        }

        return sort;
    }

    private LogicalPlan createAndProcessLPlan(String query) throws FrontendException {
        //TODO: create a common util function for logical plan tests
        LogicalPlan lp = generateLogicalPlan(query);
        new ColumnAliasConversionVisitor( lp ).visit();
        new SchemaAliasVisitor( lp ).visit();

        CompilationMessageCollector collector = new CompilationMessageCollector() ;
        new TypeCheckingRelVisitor( lp, collector).visit();
        new UnionOnSchemaSetter( lp ).visit();
        new CastLineageSetter(lp, collector).visit();

        return lp;

    }

    private LogicalPlan generateLogicalPlan(String query) {
        try {
            return ParserTestingUtils.generateLogicalPlan( query );
        } catch(Exception ex) {
            ex.printStackTrace();
            Assert.fail( "Failed to generate logical plan for query [" + query + "] due to exception: " + ex );
        }
        return null;
    }

    @Test
    public void testRangeOrderByMixWSchema() throws IOException, ParserException{
        String query;

        query =
            "  l1 = load '" + INP_FILE_5FIELDS +
                    "' as (a : int, b : long, c : int, d : int, e : int);"
            + " o = order l1 by  b .. c, d .. DESC,  a DESC;"
            ;
        compileAndCompareSchema("a : int, b : long, c : int, d : int, e : int", query, "o");

        //check number of sort expression plans

        LogicalPlan lp = createAndProcessLPlan(query);
        boolean[] isAsc = {true,true,false,false,false};
        checkNumExpressionPlansForSort(lp, 5, isAsc);

        Util.registerMultiLineQuery(pigServer, query);

        Iterator<Tuple> it = pigServer.openIterator("o");

        List<Tuple> expectedRes =
            Util.getTuplesFromConstantTupleStrings(
                    new String[] {
                            "(10,20,30,40,50)",
                            "(11,21,31,41,51)",
                    });
        Util.checkQueryOutputs(it, expectedRes);
    }


    @Test
    public void testRangeOrderByMixNOSchema() throws IOException, ParserException{
        String query;

        query =
            "  l1 = load '" + INP_FILE_5FIELDS + "';"
            + " o = order l1 by  $1 .. $2 DESC,  $0 , $4 .. DESC;"
            ;
        compileAndCompareSchema((Schema)null, query, "o");

        //check number of sort expression plans

        LogicalPlan lp = createAndProcessLPlan(query);
        boolean[] isAsc = {false, false,true,false};
        checkNumExpressionPlansForSort(lp, 4, isAsc);

        Util.registerMultiLineQuery(pigServer, query);

        pigServer.explain("o", System.err);
        Iterator<Tuple> it = pigServer.openIterator("o");

        List<Tuple> expectedRes =
            Util.getTuplesFromConstantTupleStrings(
                    new String[] {
                            "(11,21,31,41,51)",
                            "(10,20,30,40,50)",
                    });
        Util.checkQueryOutputs(it, expectedRes);
    }

    @Test
    public void testRangeOrderByStartNOSchema() throws IOException, ParserException{
        String query;

        query =
            "  l1 = load '" + INP_FILE_5FIELDS + "';"
            + " o = order l1 by $3 .. DESC;"
            ;
        compileAndCompareSchema((Schema)null, query, "o");

        //check number of sort expression plans

        LogicalPlan lp = createAndProcessLPlan(query);
        boolean[] isAsc = {false};
        checkNumExpressionPlansForSort(lp, 1, isAsc);

        Util.registerMultiLineQuery(pigServer, query);

        pigServer.explain("o", System.err);
        Iterator<Tuple> it = pigServer.openIterator("o");

        List<Tuple> expectedRes =
            Util.getTuplesFromConstantTupleStrings(
                    new String[] {
                            "(11,21,31,41,51)",
                            "(10,20,30,40,50)",
                    });
        Util.checkQueryOutputs(it, expectedRes);
    }

    @Test
    public void testRangeOrderByStartNegNOSchema() throws IOException, ParserException{
        String query;

        query =
            "  l1 = load '" + INP_FILE_5FIELDS + "';"
            + " o = order l1 by $3 .. DESC, $1;"
            ;
        Util.checkExceptionMessage(query, "o","Project-range to end (eg. x..)" +
                " is supported in order-by only as last sort column");
    }

    @Test
    public void testRangeGroupWSchema() throws IOException, ParserException{
        String query;

        {
            query =
                "  l1 = load '" + INP_FILE_5FIELDS +
                "' as (a : int, b : long, c : int, d : int, e : int);" +
                "  l2 = load '" + INP_FILE_5FIELDS +
                "' as (a : int, b : long, c : int, d : int, e : int);" +
                "  g = group l1 by   d ..,  l2 by d ..;"
                ;
            String expectedSchStr = "grp: (d: int,e : int)," +
                            "l1: {t : (a: int,b: long,c: int,d: int,e: int)}," +
                            "l2: {t : (a: int,b: long,c: int,d: int,e: int)}";

            Schema expectedSch = getCleanedGroupSchema(expectedSchStr);
            compileAndCompareSchema(expectedSch, query, "g");
            //check number of group expression plans
            LogicalPlan lp = createAndProcessLPlan(query);
            checkNumExpressionPlansForGroup(lp, 2);
        }

        {
            query =
                "  l1 = load '" + INP_FILE_5FIELDS +
                "' as (a : int, b : long, c : int, d : int, e : int);" +
                "  l2 = load '" + INP_FILE_5FIELDS +
                "' as (a : int, b : long, c : int, d : int, e : int);" +
                "  g = group l1 by   c .. $3,  l2 by $3..$4;"
                ;
            String expectedSchStr = "grp: (c: int,d : int)," +
                            "l1: {t : (a: int,b: long,c: int,d: int,e: int)}," +
                            "l2: {t : (a: int,b: long,c: int,d: int,e: int)}";

            Schema expectedSch = getCleanedGroupSchema(expectedSchStr);
            compileAndCompareSchema(expectedSch, query, "g");
            //check number of group expression plans
            LogicalPlan lp = createAndProcessLPlan(query);
            checkNumExpressionPlansForGroup(lp, 2);
        }

        query =
            "  l1 = load '" + INP_FILE_5FIELDS +
            "' as (a : int, b : long, c : int, d : int, e : int);" +
            "  l2 = load '" + INP_FILE_5FIELDS +
            "' as (a : int, b : long, c : int, d : int, e : int);" +
            "  g = group l1 by   .. c,  l2 by .. c;"
            ;
        String expectedSchStr = "grp: (a: int,b: long,c: int)," +
        		"l1: {t : (a: int,b: long,c: int,d: int,e: int)}," +
        		"l2: {t : (a: int,b: long,c: int,d: int,e: int)}";

        Schema expectedSch = getCleanedGroupSchema(expectedSchStr);
        compileAndCompareSchema(expectedSch, query, "g");


        //check number of group expression plans
        LogicalPlan lp = createAndProcessLPlan(query);
        checkNumExpressionPlansForGroup(lp, 3);

        Util.registerMultiLineQuery(pigServer, query);

        List<Tuple> expectedRes =
            Util.getTuplesFromConstantTupleStrings(
                    new String[] {
                            "((10,20,30),{(10,20,30,40,50)},{(10,20,30,40,50)})",
                            "((11,21,31),{(11,21,31,41,51)},{(11,21,31,41,51)})",
                    });
        Iterator<Tuple> it = pigServer.openIterator("g");
        Util.checkQueryOutputs(it, expectedRes);
    }

    /**
     * some transformations to schema, because the parser does not accept
     * group as a column name in schema, and to add tuple within bag schema
     * @param expectedSchStr
     * @return
     * @throws ParserException
     * @throws FrontendException
     */
    private Schema getCleanedGroupSchema(String expectedSchStr) throws ParserException, FrontendException {
        Schema expectedSch = Utils.getSchemaFromString(expectedSchStr);
        expectedSch.getField(0).alias = "group";
        if(expectedSch.size() > 1)
            expectedSch.getField(1).schema.getField(0).alias = null;
        if(expectedSch.size() > 2)
            expectedSch.getField(2).schema.getField(0).alias = null;
        expectedSch = org.apache.pig.newplan.logical.Util.fixSchemaAddTupleInBag(expectedSch);
        return expectedSch;
    }

    private LOCogroup checkNumExpressionPlansForGroup(LogicalPlan lp, int numPlans) {
        Class<?> groupClass = org.apache.pig.newplan.logical.relational.LOCogroup.class;
        LOCogroup grp = (LOCogroup) NewLogicalPlanUtil.getRelOpFromPlan(lp, groupClass);

        for( int inp : grp.getExpressionPlans().keySet()){
            List<LogicalExpressionPlan> plans = grp.getExpressionPlans().get(inp);
            assertEquals("number of group-by plans", numPlans, plans.size());
        }

        return grp;
    }

    @Test
    public void testRangeCoGroupMixWSchema() throws IOException, ParserException{
        String query;

        query =
            "  l1 = load '" + INP_FILE_5FIELDS +
            "' as (a : int, b : long, c : int, d : int, e : int);" +
            "  l2 = load '" + INP_FILE_5FIELDS +
            "' as (a : int, b : long, c : int, d : int, e : int);" +
            "  g = group l1 by  (a + b, c .. d, e.. ),  l2 by ($0 + $1, c..d, $4..);"
            ;
        String expectedSchStr = "grp: (x : long, c :int , d :int, e : int)," +
                        "l1: {t : (a: int,b: long,c: int,d: int,e: int)}," +
                        "l2: {t : (a: int,b: long,c: int,d: int,e: int)}";

        Schema expectedSch = getCleanedGroupSchema(expectedSchStr);
        expectedSch.getField(0).schema.getField(0).alias = null;
        compileAndCompareSchema(expectedSch, query, "g");


        //check number of group expression plans
        LogicalPlan lp = createAndProcessLPlan(query);
        checkNumExpressionPlansForGroup(lp, 4);

        Util.registerMultiLineQuery(pigServer, query);

        List<Tuple> expectedRes =
            Util.getTuplesFromConstantTupleStrings(
                    new String[] {
                            "((30,30,40,50),{(10,20,30,40,50)},{(10,20,30,40,50)})",
                            "((32,31,41,51),{(11,21,31,41,51)},{(11,21,31,41,51)})",
                    });
        Iterator<Tuple> it = pigServer.openIterator("g");
        Util.checkQueryOutputs(it, expectedRes);
    }

    @Test
    public void testRangeGroupMixWSchema() throws IOException, ParserException{
        String query;

        {
            query =
                "  l1 = load '" + INP_FILE_5FIELDS +
                "' as (a : int, b : long, c : int, d : int, e : int);" +
                "  g = group l1 by  b .. c;"
                ;
            String expectedSchStr = "grp: (b : long, c :int)," +
                            "l1: {t : (a: int,b: long,c: int,d: int,e: int)}";

            Schema expectedSch = getCleanedGroupSchema(expectedSchStr);
            compileAndCompareSchema(expectedSch, query, "g");


            //check number of group expression plans
            LogicalPlan lp = createAndProcessLPlan(query);
            checkNumExpressionPlansForGroup(lp, 2);
        }

        query =
            "  l1 = load '" + INP_FILE_5FIELDS +
            "' as (a : int, b : long, c : int, d : int, e : int);" +
            "  g = group l1 by  $2 .. ;" +
            "  lim = limit g 2;"
            ;
        String expectedSchStr = "grp: (c :int , d :int, e : int)," +
                        "l1: {t : (a: int,b: long,c: int,d: int,e: int)}";

        Schema expectedSch = getCleanedGroupSchema(expectedSchStr);
        compileAndCompareSchema(expectedSch, query, "lim");


        //check number of group expression plans
        LogicalPlan lp = createAndProcessLPlan(query);
        checkNumExpressionPlansForGroup(lp, 3);

        Util.registerMultiLineQuery(pigServer, query);

        List<Tuple> expectedRes =
            Util.getTuplesFromConstantTupleStrings(
                    new String[] {
                            "((30,40,50),{(10,20,30,40,50)})",
                            "((31,41,51),{(11,21,31,41,51)})",
                    });
        Iterator<Tuple> it = pigServer.openIterator("lim");
        Util.checkQueryOutputs(it, expectedRes);
    }


    @Test
    public void testRangeGroupMixNOSchema() throws IOException, ParserException{
        String query;

        {
            query =
                "  l1 = load '" + INP_FILE_5FIELDS + "';" +
                "  g = group l1 by  .. $2;"
                ;
            String expectedSchStr = "g : (duma, dumb, dumc), l1: {t : ()}";
            Schema expectedSch = getCleanedGroupSchema(expectedSchStr);
            setAliasesToNull(expectedSch.getField(0).schema);
            compileAndCompareSchema(expectedSch, query, "g");


            //check number of group expression plans
            LogicalPlan lp = createAndProcessLPlan(query);
            checkNumExpressionPlansForGroup(lp, 3);
        }

        {
            query =
                "  l1 = load '" + INP_FILE_5FIELDS + "';" +
                "  g = group l1 by  $3 .. $3;"
                ;
            String expectedSchStr = "g : bytearray, l1: {t : ()}";
            Schema expectedSch = getCleanedGroupSchema(expectedSchStr);
            compileAndCompareSchema(expectedSch, query, "g");


            //check number of group expression plans
            LogicalPlan lp = createAndProcessLPlan(query);
            checkNumExpressionPlansForGroup(lp, 1);
        }

        query =
            "  l1 = load '" + INP_FILE_5FIELDS + "';" +
            "  g = group l1 by  $2 .. ;"
            ;
        String expectedSchStr = "grp: (), l1: {t : ()}";

        Schema expectedSch = getCleanedGroupSchema(expectedSchStr);
        compileAndCompareSchema(expectedSch, query, "g");


        //check number of group expression plans
        LogicalPlan lp = createAndProcessLPlan(query);
        checkNumExpressionPlansForGroup(lp, 1);

        Util.registerMultiLineQuery(pigServer, query);

        List<Tuple> expectedRes =
            Util.getTuplesFromConstantTupleStrings(
                    new String[] {
                            "((30,40,50),{(10,20,30,40,50)})",
                            "((31,41,51),{(11,21,31,41,51)})",
                    });
        Iterator<Tuple> it = pigServer.openIterator("g");
        Util.checkQueryOutputs(it, expectedRes);
    }

    private void setAliasesToNull(Schema schema) {
       for(FieldSchema fs : schema.getFields()){
           fs.alias = null;
       }
    }

    @Test
    public void testRangeJoinMixWSchema() throws IOException, ParserException{
        String query;

        query =
            "  l1 = load '" + INP_FILE_5FIELDS +
            "' as (a : int, b : long, c : int, d : int, e : int);" +
            "  l2 = load '" + INP_FILE_5FIELDS +
            "' as (a : int, b : long, c : int, d : int, e : int);" +
            "  j = join l1 by  (a + b, c .. d, e.. ),  l2 by ($0 + $1, c..d, $4..);"
            ;
        String expectedSchStr = "l1::a: int, l1::b: long, l1::c: int, l1::d: int, l1::e: int," +
                        "l2::a: int, l2::b: long, l2::c: int, l2::d: int, l2::e: int";

        compileAndCompareSchema(expectedSchStr, query, "j");


        //check number of group expression plans
        LogicalPlan lp = createAndProcessLPlan(query);
        checkNumExpressionPlansForJoin(lp, 4);

        Util.registerMultiLineQuery(pigServer, query);

        List<Tuple> expectedRes =
            Util.getTuplesFromConstantTupleStrings(
                    new String[] {
                            "(10,20,30,40,50,10,20,30,40,50)",
                            "(11,21,31,41,51,11,21,31,41,51)",
                    });
        Iterator<Tuple> it = pigServer.openIterator("j");
        Util.checkQueryOutputs(it, expectedRes);
    }

    @Test
    public void testRangeJoinMixNOSchema() throws IOException, ParserException{
        String query;

        query =
            "  l1 = load '" + INP_FILE_5FIELDS + "';" +
            "  l2 = load '" + INP_FILE_5FIELDS + "';" +
            "  j = join l1 by  $0 .. $3,  l2 by $0 .. $3;"
            ;

        compileAndCompareSchema((Schema)null, query, "j");

        //check number of group expression plans
        LogicalPlan lp = createAndProcessLPlan(query);
        checkNumExpressionPlansForJoin(lp, 4);

        Util.registerMultiLineQuery(pigServer, query);

        List<Tuple> expectedRes =
            Util.getTuplesFromConstantTupleStrings(
                    new String[] {
                            "(10,20,30,40,50,10,20,30,40,50)",
                            "(11,21,31,41,51,11,21,31,41,51)",
                    });
        Iterator<Tuple> it = pigServer.openIterator("j");
        Util.checkQueryOutputs(it, expectedRes);
    }

    @Test
    public void testRangeCoGroupNegNoSchema() throws IOException, ParserException{
        String query;
        //cogroup
        query =
            "  l1 = load '" + INP_FILE_5FIELDS +  "';" +
            "  l2 = load '" + INP_FILE_5FIELDS +  "';" +
            "  g = cogroup l1 by  ($0 ..  ),  l2 by ($0 .. );";
        Util.checkExceptionMessage(query, "g", "Cogroup/Group by '*' or 'x..' " +
        		"(range of columns to the end) " +
                        "is only allowed if the input has a schema");
    }

    private LOJoin checkNumExpressionPlansForJoin(LogicalPlan lp, int numPlans) {
        Class<?> joinClass = org.apache.pig.newplan.logical.relational.LOJoin.class;
        LOJoin join = (LOJoin) NewLogicalPlanUtil.getRelOpFromPlan(lp, joinClass);

        for( int inp : join.getExpressionPlans().keySet()){
            List<LogicalExpressionPlan> plans = join.getExpressionPlans().get(inp);
            assertEquals("number of join exp plans", numPlans, plans.size());
        }

        return join;
    }

}

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
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.Utils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;


/**
 * Test Project-(star/range) expansion when used as udf argument
 */
public class TestProjectStarRangeInUdf  {

    protected final Log log = LogFactory.getLog(getClass());

    protected static PigServer pigServer;
    private static final String INP_FILE_5FIELDS = "TestProjectRange_5fields";


    @BeforeClass
    public static void oneTimeSetUp() throws Exception {
        String[] input = {"10\t20\t30\t40\t50", "11\t21\t31\t41\t51"};
        Util.createLocalInputFile(INP_FILE_5FIELDS, input);
    }

    @Before
    public void setup() throws ExecException{
        pigServer = new PigServer(LOCAL);
    }

    @After
    public void tearDown() throws Exception {
        pigServer.shutdown();
    }

    @AfterClass
    public static void oneTimeTearDown() throws Exception {
        new File(INP_FILE_5FIELDS).delete();
    }

    @Test
    public void testProjStarExpandInForeach1() throws IOException{
        //star expansion lets CONCAT be used if input has two cols
        String query;

        query =
            "  l1 = load '" + INP_FILE_5FIELDS + "' as (a, b);"
            + "f = foreach l1 generate CONCAT(*) as ct;"
            ; 
        compileAndCompareSchema("ct : bytearray", query, "f");
    }

    @Test
    public void testProjStarExpandInForeach1Multi() throws IOException{
        //star expansion gives 3 columns, CONCAT(*) should pass
        String query;

        query =
            "  l1 = load '" + INP_FILE_5FIELDS + "' as (a, b, c);"
            + "f = foreach l1 generate CONCAT(*) as ct;"
            ; 
        compileAndCompareSchema("ct : bytearray", query, "f");
    }
    
    @Test
    public void testProjStarExpandInForeach1NegativeNoSchema() throws IOException{
        
        String query;

        query =
            "  l1 = load '" + INP_FILE_5FIELDS + "' ;"
            + "f = foreach l1 generate CONCAT(*) as ct;"
            ; 
        Util.checkExceptionMessage(query, "f",
                "Could not infer the matching function for " +
                "org.apache.pig.builtin.CONCAT");


        query =
            "  l1 = load '" + INP_FILE_5FIELDS + "' ;"
            + "f = foreach l1 generate SIZE(*) as ct;"
            ; 
        Util.checkExceptionMessage(query, "f",
                "Could not infer the matching function for " +
                "org.apache.pig.builtin.SIZE");
        
    }
    
    @Test
    public void testProjStarExpandInForeach2() throws IOException {

        String query;

        query =
            "  l1 = load '" + INP_FILE_5FIELDS + "' as (a : int, b : int, c : int);"
            + "f = foreach l1 generate TOTUPLE(*) as tb;"
            ; 
        compileAndCompareSchema("tb : (a : int, b : int, c : int)", query, "f");
        Iterator<Tuple> it = pigServer.openIterator("f");

        List<Tuple> expectedRes = 
            Util.getTuplesFromConstantTupleStrings(
                    new String[] {
                            "((10,20,30))",
                            "((11,21,31))",
                    });
        Util.checkQueryOutputsAfterSort(it, expectedRes);

    }

    //PIG-2223 
    // lookup on column name in udf output tuple schema
    @Test
    public void testProjStarExpandInForeachLookup1() throws IOException {

        String query =
            "  l1 = load '" + INP_FILE_5FIELDS + "' as (a : int, b : int, c : int);"
            + "f = foreach l1 generate TOTUPLE(*) as tb;"
            + "f2 = foreach f generate tb.a, tb.b;"                    
            ; 
        compileAndCompareSchema("a : int, b : int", query, "f2");
    }
    
    //PIG-2223 
    // lookup on column name in udf output tuple schema
    @Test
    public void testProjStarExpandInForeachLookup2() throws IOException {

        String query =
            "  l1 = load '" + INP_FILE_5FIELDS + "' as (a : int, b : int, c : int);"
            + "f = foreach l1 generate TOTUPLE(b .. ) as tb;"
            + "f2 = foreach f generate tb.b as b2, tb.c as c2;"     
            + "f3 = foreach f2 generate b2, b2 + c2 as bc2;"     
            ; 
        compileAndCompareSchema("b2 : int, bc2 : int", query, "f3");
    }
    
    @Test
    public void testProjStarExpandInFilter1() throws IOException{
        //TOBAG has * and a bincond expression as argument
        String query;

        query =
            "  l1 = load '" + INP_FILE_5FIELDS + "' as (a : int, b : int);"
            + "f = filter l1 by SUM(TOBAG((a == 10 ? 100 : 0), *)) == 130;"
            ; 
        compileAndCompareSchema("a : int, b : int", query, "f");
        Iterator<Tuple> it = pigServer.openIterator("f");

        List<Tuple> expectedRes = 
            Util.getTuplesFromConstantTupleStrings(
                    new String[] {
                            "(10,20)",
                    });
        Util.checkQueryOutputsAfterSort(it, expectedRes);
    }
    
    @Test
    public void testProjRangeExpandInFilterNoSchema1() throws IOException{
        //star expansion lets CONCAT be used if input has two cols
        String query;

        query =
            "  l1 = load '" + INP_FILE_5FIELDS + "' ;"
            + "f = filter l1 by SUM(TOBAG($0 .. $1)) == 30;"
            ; 
        compileAndCompareSchema((Schema)null, query, "f");
        Iterator<Tuple> it = pigServer.openIterator("f");

        List<Tuple> expectedRes = 
            Util.getTuplesFromConstantTupleStringAsByteArray(
                    new String[] {
                            "('10','20','30','40','50')",
                    });
        Util.checkQueryOutputsAfterSort(it, expectedRes);
    }
    
    /**
     * Test project-range in foreach with limits on both sides
     * @throws IOException
     * @throws ParseException
     */
    @Test
    public void testProjRangeExpandInForeach() throws IOException {

        String query;

        query =
            "  l1 = load '" + INP_FILE_5FIELDS + "' as (a, b : chararray, c : chararray, d);"
            + "f = foreach l1 generate CONCAT($1 .. $2) as ct;"
            ; 
        compileAndCompareSchema("ct : chararray", query, "f");
        Iterator<Tuple> it = pigServer.openIterator("f");

        List<Tuple> expectedRes = 
            Util.getTuplesFromConstantTupleStrings(
                    new String[] {
                            "('2030')",
                            "('2131')",
                    });
        Util.checkQueryOutputsAfterSort(it, expectedRes);

    }
    
    @Test
    public void testProjRangeExpandInJoin() throws IOException {

        String query;

        query =
            "  l1 = load '" + INP_FILE_5FIELDS + "' as (a : chararray, b : chararray, c : chararray, d);"
            + "f1 = foreach l1 generate a, b, c, '1' as num;"
            + "l2 = load '" + INP_FILE_5FIELDS + "' as (a : chararray, b : chararray, c : chararray, d);"
            + "f2 = foreach l1 generate c, a, b, '2' as num;" 
            + "j = join f1 by CONCAT($0 .. $1), f2 by CONCAT(a .. b);"
            ; 
        String schStr =
            "f1::a : chararray, f1::b : chararray, f1::c : chararray, f1::num : chararray," + 
            "f2::c : chararray, f2::a : chararray, f2::b : chararray, f2::num : chararray";
            
        compileAndCompareSchema(schStr, query, "j");
        Iterator<Tuple> it = pigServer.openIterator("j");

        List<Tuple> expectedRes = 
            Util.getTuplesFromConstantTupleStrings(
                    new String[] {
                            "('10', '20', '30', '1', '30', '10', '20', '2')",
                            "('11', '21', '31', '1', '31', '11', '21', '2')",
                    });
        Util.checkQueryOutputsAfterSort(it, expectedRes);

    }

    
    @Test
    public void testProjMixExpand1() throws IOException {

        String query;

        query =
            "  l1 = load '" + INP_FILE_5FIELDS + "' as (a : int, b : int, c : int);"
            + "f = foreach l1 generate TOBAG(*, $0 .. $2) as tt;"
            ; 
     
        compileAndCompareSchema("tt : {(NullAlias : int)}", query, "f");
        Iterator<Tuple> it = pigServer.openIterator("f");

        List<Tuple> expectedRes = 
            Util.getTuplesFromConstantTupleStrings(
                    new String[] {
                            "({(10),(20),(30),(10),(20),(30)})",
                            "({(11),(21),(31),(11),(21),(31)})",
                    });
        Util.checkQueryOutputsAfterSort(it, expectedRes);

    }
    
    @Test
    public void testProjMixExpand1NoSchema() throws IOException {

        String query;

        query =
            "  l1 = load '" + INP_FILE_5FIELDS + "';"
            + "f = foreach l1 generate TOBAG(*, $0 .. $2) as tt;"
            ; 
        Schema sch = Utils.getSchemaFromString("tt : {(NullALias)}");
        sch.getField(0).schema.getField(0).schema.getField(0).alias = null;
        sch.getField(0).schema.getField(0).schema.getField(0).type = DataType.NULL;
        
        compileAndCompareSchema(sch, query, "f");
        Iterator<Tuple> it = pigServer.openIterator("f");

        List<Tuple> expectedRes = 
            Util.getTuplesFromConstantTupleStringAsByteArray(
                    new String[] {
                            "({('10'),('20'),('30'),('40'),('50'),('10'),('20'),('30')})",
                            "({('11'),('21'),('31'),('41'),('51'),('11'),('21'),('31')})",
                    });
        Util.checkQueryOutputsAfterSort(it, expectedRes);

    }
    
    @Test
    public void testProjMixExpand2() throws IOException {

        String query;

        query =
            "  l1 = load '" + INP_FILE_5FIELDS + "' as (a : int, b : int, c : int, d : int);"
            + "f = foreach l1 generate TOTUPLE(1, $0 .. $1, 2+3, $2 .. , d - 1) as tt;"
            ; 
     
        String schStr = "tt : (NullAliasA : int, a : int, b : int," +
            " NullAliasB : int, c : int, d : int, NullAliasC : int)";
        compileAndCompareSchema(schStr, query, "f");
        Iterator<Tuple> it = pigServer.openIterator("f");

        List<Tuple> expectedRes = 
            Util.getTuplesFromConstantTupleStrings(
                    new String[] {
                            "((1,10,20,5,30,40,39))",
                            "((1,11,21,5,31,41,40))",
                    });
        Util.checkQueryOutputsAfterSort(it, expectedRes);

    }
    
    @Test
    public void testProjMixExpand2NoSchema() throws IOException {

        String query;

        query =
            "  l1 = load '" + INP_FILE_5FIELDS + "' ;"
            + "f = foreach l1 generate TOTUPLE(1, $0 .. $1, 2+3, $2 .. , $4 -1) as tt;"
            ; 
     
        compileAndCompareSchema("tt :()", query, "f");
        pigServer.explain("f", System.out);
        Iterator<Tuple> it = pigServer.openIterator("f");

        List<Tuple> expectedRes = 
            Util.getTuplesFromConstantTupleStringAsByteArray(
                    new String[] {
                            "((1,'10','20',5,'30','40','50',49))",
                            "((1,'11','21',5,'31','41','51',50))",
                    });
        Util.checkQueryOutputsAfterSort(it, expectedRes);

    }
    
    @Test
    public void testProjMixExpand3() throws IOException {

        String query;

        query =
            "  l1 = load '" + INP_FILE_5FIELDS + "' as (a : int, b : int, c : chararray, d : chararray);"
            + "f = foreach l1 generate TOTUPLE($0 .. $1, CONCAT($2 .. )) as tt;"
            ; 
     
        String schStr = "tt : (a : int, b : int, NullAlias : chararray)";
        compileAndCompareSchema(schStr, query, "f");
        Iterator<Tuple> it = pigServer.openIterator("f");

        List<Tuple> expectedRes = 
            Util.getTuplesFromConstantTupleStrings(
                    new String[] {
                            "((10,20,'3040'))",
                            "((11,21,'3141'))",
                    });
        Util.checkQueryOutputsAfterSort(it, expectedRes);

    }
    
    
    private void compileAndCompareSchema(String expectedSchStr, String query, String alias)
    throws IOException {

        Schema expectedSch = null;
        
        if(expectedSchStr != null)
            expectedSch = Utils.getSchemaFromString(expectedSchStr);
        Util.schemaReplaceNullAlias(expectedSch);
        compileAndCompareSchema(expectedSch, query, alias);

    }

    private void compileAndCompareSchema(Schema expectedSch, String query,
            String alias) throws IOException {
        Util.registerMultiLineQuery(pigServer, query);

        Schema sch = pigServer.dumpSchema(alias);
        assertEquals("Checking expected schema", expectedSch, sch);
    }

}

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
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.pig.EvalFunc;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.parser.ParseException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;




public class TestUnionOnSchema  {
    static MiniCluster cluster ;
    private static final String EMPTY_DIR = "emptydir";
    private static final String INP_FILE_2NUMS = "TestUnionOnSchemaInput1";
    private static final String INP_FILE_2NUM_1CHAR_1BAG = "TestUnionOnSchemaInput2";
    private static final String INP_FILE_EMPTY= "TestUnionOnSchemaInput3";
    
    @Before
    public void setUp() throws Exception {
        FileLocalizer.setInitialized(false);
    }


    @After
    public void tearDown() throws Exception {
    }
    
    @BeforeClass
    public static void oneTimeSetup() throws IOException, Exception {
        cluster = MiniCluster.buildCluster();
        FileSystem fs = cluster.getFileSystem();
        if (!fs.mkdirs(new Path(EMPTY_DIR))) {
            throw new Exception("failed to create empty dir");
        }
        // first input file
        PrintWriter w = new PrintWriter(new FileWriter(INP_FILE_2NUMS));
        w.println("1\t2");
        w.println("5\t3");
        w.close();
        Util.copyFromLocalToCluster(cluster, INP_FILE_2NUMS, INP_FILE_2NUMS);
        
        // 2nd input file
        w = new PrintWriter(new FileWriter(INP_FILE_2NUM_1CHAR_1BAG));
        w.println("1\tabc\t2\t{(1,a),(1,b)}\t(1,c)");
        w.println("5\tdef\t3\t{(2,a),(2,b)}\t(2,c)");
        w.close();
        Util.copyFromLocalToCluster(cluster, INP_FILE_2NUM_1CHAR_1BAG, INP_FILE_2NUM_1CHAR_1BAG);

        //3rd input - empty file
        w = new PrintWriter(new FileWriter(INP_FILE_EMPTY));
        w.close();
        Util.copyFromLocalToCluster(cluster, INP_FILE_EMPTY, INP_FILE_EMPTY);
        
    }
    
    @AfterClass
    public static void oneTimeTearDown() throws Exception {

        new File(INP_FILE_2NUMS).delete();
        new File(INP_FILE_2NUM_1CHAR_1BAG).delete();
        new File(INP_FILE_EMPTY).delete();
        cluster.shutDown();
    }
 

    /**
     * Test UNION ONSCHEMA on two inputs with same schema
     * @throws IOException
     * @throws ParseException
     */
    @Test
    public void testUnionOnSchemaSameSchema() throws IOException, ParseException {
        PigServer pig = new PigServer(ExecType.LOCAL);
        String query =
            "  l1 = load '" + INP_FILE_2NUMS + "' as (i : int, j : int);"
            + "l2 = load '" + INP_FILE_2NUMS + "' as (i : int, j : int);"
            + "u = union onschema l1, l2;"
        ; 
        Util.registerMultiLineQuery(pig, query);
        Iterator<Tuple> it = pig.openIterator("u");
        
        List<Tuple> expectedRes = 
            Util.getTuplesFromConstantTupleStrings(
                    new String[] {
                            "(1,2)",
                            "(5,3)",
                            "(1,2)",
                            "(5,3)"
                    });
        Util.checkQueryOutputsAfterSort(it, expectedRes);

    }
    
    
    /**
     * Test UNION ONSCHEMA with cast from bytearray to another type
     * @throws IOException
     * @throws ParseException
     */
    @Test
    public void testUnionOnSchemaCastOnByteArray() throws IOException, ParseException {
        PigServer pig = new PigServer(ExecType.LOCAL);
        String query =
            "  l1 = load '" + INP_FILE_2NUMS + "' as (i, j);"
            + " f1 = foreach l1 generate (int)i, (int)j;"
            + "u = union onschema f1, l1;"
        ; 
        Util.registerMultiLineQuery(pig, query);
        Iterator<Tuple> it = pig.openIterator("u");
        
        List<Tuple> expectedRes = 
            Util.getTuplesFromConstantTupleStrings(
                    new String[] {
                            "(1,2)",
                            "(5,3)",
                            "(1,2)",
                            "(5,3)"
                    });
        Util.checkQueryOutputsAfterSort(it, expectedRes);

    }
    
    /**
     * Test UNION ONSCHEMA where a common column has additional 'namespace' part
     *  in the column name in one of the inputs
     * @throws IOException
     * @throws ParseException
     */
    @Test
    public void testUnionOnSchemaScopedColumnName() throws IOException, ParseException {
        PigServer pig = new PigServer(ExecType.LOCAL);
        String query_prefix = 
        "  l1 = load '" + INP_FILE_2NUMS + "' as (i : int, j : int); " 
        + "g = group l1 by i; "
        + "f = foreach g generate flatten(l1); "
        + "l2 = load '" + INP_FILE_2NUMS + "' as (i : int, j : int); ";

        String query = query_prefix + "u = union onschema f, l2; " ; 
        Util.registerMultiLineQuery(pig, query);
        Schema sch = pig.dumpSchema("u");
        Schema expectedSch = Util.getSchemaFromString("i: int, j: int");
        assertEquals("Checking expected schema",sch, expectedSch);
        Iterator<Tuple> it = pig.openIterator("u");

        List<Tuple> expectedRes = 
            Util.getTuplesFromConstantTupleStrings(
                    new String[] {
                            "(1,2)",
                            "(5,3)",
                            "(1,2)",
                            "(5,3)"
                    });
        Util.checkQueryOutputsAfterSort(it, expectedRes);
        
        // now try reversing the order of relation
        query = query_prefix + "u = union onschema l2, f; " ; 
        Util.registerMultiLineQuery(pig, query);
        sch = pig.dumpSchema("u");
        expectedSch = Util.getSchemaFromString("i: int, j: int");
        assertEquals("Checking expected schema",sch, expectedSch);
        it = pig.openIterator("u");
        Util.checkQueryOutputsAfterSort(it, expectedRes);

    }
    
    /**
     * Test UNION ONSCHEMA where a common column has additional 'namespace' part
     *  in the column name in both the inputs
     * @throws IOException
     * @throws ParseException
     */
    @Test
    public void testUnionOnSchemaScopedColumnNameBothInp1() throws IOException, ParseException {
        PigServer pig = new PigServer(ExecType.LOCAL);
        String query = 
        "  l1 = load '" + INP_FILE_2NUMS + "' as (i : int, j : int); " 
        + "g1 = group l1 by i; "
        + "f1 = foreach g1 generate group as gkey, flatten(l1); "
        + "l2 = load '" + INP_FILE_2NUMS + "' as (i : int, x : chararray); " 
        + "g2 = group l2 by i; "
        + "f2 = foreach g2 generate group as gkey, flatten(l2); "
        + "u = union onschema f1, f2; " ; 
        Util.registerMultiLineQuery(pig, query);
        
        Schema sch = pig.dumpSchema("u");
        Schema expectedSch = 
            Util.getSchemaFromString("gkey: int, l1::i: int, l1::j: int, l2::i: int, l2::x: chararray");
        assertEquals("Checking expected schema",sch, expectedSch);

        Iterator<Tuple> it = pig.openIterator("u");
        List<Tuple> expectedRes = 
            Util.getTuplesFromConstantTupleStrings(
                    new String[] {
                            "(1,1,2,null,null)",
                            "(5,5,3,null,null)",
                            "(1,null,null,1,'2')",
                            "(5,null,null,5,'3')"
                    });
        Util.checkQueryOutputsAfterSort(it, expectedRes);
    }
    
    /**
     * Test UNION ONSCHEMA where a common column has additional 'namespace' part
     *  in the column name in both the inputs
     * @throws IOException
     * @throws ParseException
     */
    @Test
    public void testUnionOnSchemaScopedColumnNameBothInp2() throws IOException, ParseException {
        PigServer pig = new PigServer(ExecType.LOCAL);
        String query =
            "   l1 = load '" + INP_FILE_2NUMS + "' as (i : int, j : int); " 
            + " l2 = load '" + INP_FILE_2NUMS + "' as (i : int, x : chararray); " 
            + " cg1 = cogroup l1 by i, l2 by i; "
            + " f1 = foreach cg1 generate group as gkey, flatten(l1), flatten(l2); "
            + " cg2 = cogroup l2 by i, l1 by i; "
            + " f2 = foreach cg1 generate group as gkey, flatten(l2), flatten(l1); "
            + "u = union onschema f1, f2; " ; 
        Util.registerMultiLineQuery(pig, query);
                
        Schema sch = pig.dumpSchema("u");
        Schema expectedSch = 
            Util.getSchemaFromString("gkey: int, l1::i: int, l1::j: int, l2::i: int, l2::x: chararray");
        assertEquals("Checking expected schema",sch, expectedSch);

        Iterator<Tuple> it = pig.openIterator("u");
        List<Tuple> expectedRes = 
            Util.getTuplesFromConstantTupleStrings(
                    new String[] {
                            "(1,1,2,1,'2')",
                            "(5,5,3,5,'3')",
                            "(1,1,2,1,'2')",
                            "(5,5,3,5,'3')",
                    });
        Util.checkQueryOutputsAfterSort(it, expectedRes);
        
    }
    
    /**
     * Test UNION ONSCHEMA where a common column has additional 'namespace' part
     *  in the column name in one of the inputs.
     *  Negative test case
     * @throws IOException
     * @throws ParseException
     */
    @Test
    public void testUnionOnSchemaScopedColumnNameNeg() throws IOException, ParseException {
        
        String expectedErr = "Found more than one match: l1::i, l2::i";
        String query_prefix =
            "  l1 = load '/tmp/fn' as (i : int, j : long); "
            + "l2 = load '/tmp/fn' as (i : int, j : long); "
            + "cg = cogroup l1 by i, l2 by i;"
            + "f = foreach cg generate flatten(l1), flatten(l2); "
            + "l3 = load '/tmp/fn2' as (i : int, j : long); "
            ;
        String query = query_prefix +  "u = union onschema f, l3; ";
        checkSchemaEx(query, expectedErr);

        // now try reversing the order of relation
        query = query_prefix +  "u = union onschema l3, f; ";
        checkSchemaEx(query, expectedErr);

    }

    /**
     * Test UNION ONSCHEMA on two inputs with same column names, but different
     * numeric types - test type promotion
     * @throws IOException
     * @throws ParseException
     */
    @Test
    public void testUnionOnSchemaDiffNumType() throws IOException, ParseException {
        PigServer pig = new PigServer(ExecType.LOCAL);
        String query =
            "  l1 = load '" + INP_FILE_2NUMS + "' as (i : int, j : double);"
            + "l2 = load '" + INP_FILE_2NUMS + "' as (i : long, j : float);"
            + "u = union onschema l1, l2;"
        ; 
        Util.registerMultiLineQuery(pig, query);
        Iterator<Tuple> it = pig.openIterator("u");
        
        List<Tuple> expectedRes = 
            Util.getTuplesFromConstantTupleStrings(
                    new String[] {
                            "(1L,2.0)",
                            "(5L,3.0)",
                            "(1L,2.0)",
                            "(5L,3.0)"
                    });
        Util.checkQueryOutputsAfterSort(it, expectedRes);

    }
    
    

    /**
     * Test UNION ONSCHEMA on two inputs with no common columns
     * @throws IOException
     * @throws ParseException
     */
    @Test
    public void testUnionOnSchemaNoCommonCols() throws IOException, ParseException {
        PigServer pig = new PigServer(ExecType.LOCAL);
        String query =
            "  l1 = load '" + INP_FILE_2NUMS + "' as (i : int, j : int);"
            + "l2 = load '" + INP_FILE_2NUMS + "' as (x : long, y : float);"
            + "u = union onschema l1, l2;"
        ; 
        Util.registerMultiLineQuery(pig, query);
        Iterator<Tuple> it = pig.openIterator("u");
        
        List<Tuple> expectedRes = 
            Util.getTuplesFromConstantTupleStrings(
                    new String[] {
                            "(1,2,null,null)",
                            "(5,3,null,null)",
                            "(null,null,1L,2.0F)",
                            "(null,null,5L,3.0F)"
                    });
        Util.checkQueryOutputsAfterSort(it, expectedRes);

    }
    
    /**
     * Test UNION ONSCHEMA on two inputs , one input with additional columns
     * @throws IOException
     * @throws ParseException
     */
    @Test
    public void testUnionOnSchemaAdditionalColumn() throws IOException, ParseException {
        PigServer pig = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        //PigServer pig = new PigServer(ExecType.LOCAL);
        String query =
            "  l1 = load '" + INP_FILE_2NUMS + "' as (i : int, j : int);"
            + "l2 = load '" + INP_FILE_2NUM_1CHAR_1BAG + "' as " 
            + "  (i : long, c : chararray, j : int " 
            +       ", b : bag { t : tuple (c1 : int, c2 : chararray)}" 
            +       ", t : tuple (tc1 : int, tc2 : chararray) );"
            + "l3 = load '" + INP_FILE_EMPTY + "' as (i : int, x : long);"
            + "u = union onschema l1, l2, l3;"
        ; 
        Util.registerMultiLineQuery(pig, query);
        pig.explain("u", System.out);

        Iterator<Tuple> it = pig.openIterator("u");
        
        List<Tuple> expectedRes = 
            Util.getTuplesFromConstantTupleStrings(
                    new String[] {
                            "(1L,2,null,null,null,null)",
                            "(5L,3,null,null,null,null)",
                            "(1L,2,'abc',{(1,'a'),(1,'b')},(1,'c'),null)",
                            "(5L,3,'def',{(2,'a'),(2,'b')},(2,'c'),null)",

                    });
        Util.checkQueryOutputsAfterSort(it, expectedRes);
    }
    
    
    /**
     * Test UNION ONSCHEMA on 3 inputs 
     * @throws IOException
     * @throws ParseException
     */
    @Test
    public void testUnionOnSchema3Inputs() throws IOException, ParseException {
        PigServer pig = new PigServer(ExecType.LOCAL);
        String query =
            "  l1 = load '" + INP_FILE_2NUMS + "' as (i : int, j : int); "
            + "l2 = load '" + INP_FILE_2NUMS + "' as (i : double, x : int); "            
            + "l3 = load '" + INP_FILE_2NUM_1CHAR_1BAG + "' as " 
            + "  (i : long, c : chararray, j : int " 
            +       ", b : bag { t : tuple (c1 : int, c2 : chararray)} ); "
            + "u = union onschema l1, l2, l3;"
        ; 
        Util.registerMultiLineQuery(pig, query);
        pig.explain("u", System.out);

        Iterator<Tuple> it = pig.openIterator("u");
        
        List<Tuple> expectedRes = 
            Util.getTuplesFromConstantTupleStrings(
                    new String[] {
                            "(1.0,2,null,null,null)",
                            "(5.0,3,null,null,null)",
                            "(1.0,null,2,null,null)",
                            "(5.0,null,3,null,null)",
                            "(1.0,2,null,'abc',{(1,'a'),(1,'b')})",
                            "(5.0,3,null,'def',{(2,'a'),(2,'b')})",

                    });
        Util.checkQueryOutputsAfterSort(it, expectedRes);
    }

    /**
     * Test UNION ONSCHEMA with bytearray type 
     * @throws IOException
     * @throws ParseException
     */
    @Test
    public void testUnionOnSchemaByteArrayConversions() throws IOException, ParseException {
        PigServer pig = new PigServer(ExecType.LOCAL);
        String query =
            " l1 = load '" + INP_FILE_2NUM_1CHAR_1BAG + "' as " 
            + "  (i : bytearray, x : bytearray, j : bytearray " 
            +       ", b : bytearray); "
            + "l2 = load '" + INP_FILE_2NUM_1CHAR_1BAG + "' as " 
            + "  (i : long, c : chararray, j : int " 
            +       ", b : bag { t : tuple (c1 : int, c2 : chararray)} ); "
            + "u = union onSchema l1, l2;"
        ; 
        Util.registerMultiLineQuery(pig, query);
        pig.explain("u", System.out);

        Iterator<Tuple> it = pig.openIterator("u");
        
        List<Tuple> expectedRes = 
            Util.getTuplesFromConstantTupleStrings(
                    new String[] {
                            "(1L,null,2,{(1,'a'),(1,'b')},'abc')",
                            "(1L,'abc',2,{(1,'a'),(1,'b')},null)",
                            "(5L,null,3,{(2,'a'),(2,'b')},'def')",
                            "(5L,'def',3,{(2,'a'),(2,'b')},null)",                            
                    });
        //update expectedRes to use bytearray instead of chararray in 2nd field
        for(Tuple t : expectedRes){
            if(t.get(1) != null){
                t.set(1, new DataByteArray(t.get(1).toString()));
            }
        }
        Util.checkQueryOutputsAfterSort(it, expectedRes);
    }
    
    /**
     * negative test - test error on no schema
     * @throws IOException
     * @throws ParseException
     */
    @Test
    public void testUnionOnSchemaNoSchema() throws IOException, ParseException {
        String expectedErr = "UNION ONSCHEMA cannot be used with " +
        "relations that have null schema";
        String query =
            "  l1 = load '" + INP_FILE_2NUMS + "' ;"
            + "l2 = load '" + INP_FILE_2NUMS + "' as (x : long, y : float);"
            + "u = union onschema l1, l2;"
        ; 
        checkSchemaEx(query, expectedErr);
        
        query =
            "  l1 = load '" + INP_FILE_2NUMS + "' ;"
            + "l2 = load '" + INP_FILE_2NUMS + "' ;"
            + "u = union onschema l1, l2;"
        ; 
       checkSchemaEx(query, expectedErr);

    }
    
    /**
     * negative test - test error on null alias in one of the FieldSchema
     * @throws IOException
     * @throws ParseException
     */
    @Test
    public void testUnionOnSchemaNullAliasInFieldSchema() throws IOException, ParseException {
        String expectedErr = "Schema of relation f has a null fieldschema for " +
        		"column(s). Schema :{long,y: float}";
        String query =
            "  l = load '" + INP_FILE_2NUMS + "' as (x : long, y : float);"
            + "f = foreach l generate x+1, y;"
            + "u = union onschema l, f;"
        ; 
        checkSchemaEx(query, expectedErr);

    }


    private void checkSchemaEx(String query, String expectedErr) throws IOException {
        PigServer pig = new PigServer(ExecType.LOCAL);

        boolean foundEx = false;
        try{
            Util.registerMultiLineQuery(pig, query);
        }catch(FrontendException e){
            foundEx = true;
            if(!e.getMessage().contains(expectedErr)){
                String msg = "Expected exception message matching '" 
                    + expectedErr + "' but got '" + e.getMessage() + "'" ;
                fail(msg);
            }
        }
        
        if(!foundEx)
            fail("No exception thrown. Exception is expected.");
        
       
    }
    

    /**
     * negative test - test error on incompatible types in schema
     * @throws IOException
     * @throws ParseException
     */
    @Test
    public void testUnionOnSchemaIncompatibleTypes() throws IOException, ParseException {
        String expectedErr = "Incompatible types for merging schemas";
        String query =
            "  l1 = load '" + INP_FILE_2NUMS + "' as (x : long, y : chararray);"
            + "l2 = load '" + INP_FILE_2NUMS + "' as (x : long, y : float);"
            + "u = union onschema l1, l2;"
        ; 
        checkSchemaEx(query, expectedErr);
 
        query =
            "  l1 = load '" + INP_FILE_2NUMS + "' as (x : long, y : chararray);"
            + "l2 = load '" + INP_FILE_2NUMS + "' as (x : map[ ], y : chararray);"
            + "u = union onschema l1, l2;"
        ; 
        checkSchemaEx(query, expectedErr);

        // Test error on different inner schemas
        expectedErr = "Incompatible types for merging inner schemas";
        // bag column with different internal column names
        query =
            "  l1 = load '" + INP_FILE_2NUMS 
            + "' as (x : long, b : bag { t : tuple (c1 : int, c2 : chararray)}  );"
            
            + "l2 = load '" + INP_FILE_2NUMS 
            + "' as (x : long, b : bag { t : tuple (c2 : int, c3 : chararray)} );"
            + "u = union onschema l1, l2;"
        ; 
        checkSchemaEx(query, expectedErr);
        
        // bag column with different internal column types
        query =
            "  l1 = load '" + INP_FILE_2NUMS 
            + "' as (x : long, b : bag { t : tuple (c1 : int, c2 : chararray)}  );"
            
            + "l2 = load '" + INP_FILE_2NUMS 
            + "' as (x : long, b : bag { t : tuple (c1 : long, c2 : chararray)} );"
            + "u = union onschema l1, l2;"
        ; 
        checkSchemaEx(query, expectedErr);
        
        // tuple column with different internal column types
        query =
            "  l1 = load '" + INP_FILE_2NUMS 
            + "' as (t : tuple (c1 : int, c2 : chararray)  );"
            
            + "l2 = load '" + INP_FILE_2NUMS 
            + "' as (t : tuple (c1 : long, c2 : chararray) );"
            + "u = union onschema l1, l2;"
        ; 
        checkSchemaEx(query, expectedErr);
    }
    
    
    /**
     * Test UNION ONSCHEMA with input relation having udfs
     * @throws IOException
     * @throws ParseException
     */
    @Test
    public void testUnionOnSchemaInputUdfs() throws IOException, ParseException {
        PigServer pig = new PigServer(ExecType.LOCAL);
        String query =
            "  l1 = load '" + INP_FILE_2NUMS + "' as (i : int, j : chararray);"
            + "l2 = load '" + INP_FILE_2NUMS + "' as (i : int, j : chararray);"
            + "f1 = foreach l1 generate i, CONCAT(j,j) as cj, " +
            		"org.apache.pig.test.TestUnionOnSchema\\$UDFTupleNullSchema(i,j) as uo;"
            + "u = union onschema f1, l2;"
        ; 
        Util.registerMultiLineQuery(pig, query);

        Schema sch = pig.dumpSchema("u");
        Schema expectedSch = 
            Util.getSchemaFromString("i: int, cj: chararray, uo: Tuple(), j: chararray");
        assertEquals("Checking expected schema",sch, expectedSch);
        

        Iterator<Tuple> it = pig.openIterator("u");
        List<Tuple> expectedRes = 
            Util.getTuplesFromConstantTupleStrings(
                    new String[] {
                            "(1,null,null,'2')",
                            "(5,null,null,'3')",
                            "(1,'22',(1,'2'),null)",
                            "(5,'33',(5,'3'),null)"
                    });
        Util.checkQueryOutputsAfterSort(it, expectedRes);

    }
    
    
    /**
     * Test UNION ONSCHEMA with udf whose default type is different from
     * final type
     * @throws IOException
     * @throws ParseException
     */
    @Test
    public void testUnionOnSchemaUdfTypeEvolution() throws IOException, ParseException {
        PigServer pig = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        //PigServer pig = new PigServer(ExecType.LOCAL);
        String query_prefix =
            "  l1 = load '" + INP_FILE_2NUM_1CHAR_1BAG + "' as " 
            + "  (i : int, c : chararray, j : int " 
            +       ", b : bag { t : tuple (c1 : int, c2 : chararray)}" 
            +       ", t : tuple (tc1 : int, tc2 : chararray) );"
            + " l2 = load '" + INP_FILE_2NUM_1CHAR_1BAG + "' as " 
            + "  (i : int, c : chararray, j : int " 
            +       ", b : bag { t : tuple (c1 : int, c2 : chararray)}" 
            +       ", t : tuple (tc1 : int, tc2 : chararray) );"
            + "f1 = foreach l1 generate i, MAX(b.c1) as mx;"
            + "f2 = foreach l2 generate i, COUNT(b.c1) as mx;"

        ; 
        String query = query_prefix  + "u = union onschema f1, f2;";
        Util.registerMultiLineQuery(pig, query);
        Schema sch = pig.dumpSchema("u");
        Schema expectedSch = 
            Util.getSchemaFromString("i: int, mx: long");
        assertEquals("Checking expected schema",sch, expectedSch);
        
        // verify schema for reverse order of relations as well
        query = query_prefix  + "u = union onschema f2, f1;";
        Util.registerMultiLineQuery(pig, query);
        sch = pig.dumpSchema("u");
        expectedSch = 
            Util.getSchemaFromString("i: int, mx: long");
        assertEquals("Checking expected schema",sch, expectedSch);
        
        
        Iterator<Tuple> it = pig.openIterator("u");
        
        List<Tuple> expectedRes = 
            Util.getTuplesFromConstantTupleStrings(
                    new String[] {
                            "(1,1L)",
                            "(5,2L)",
                            "(1,2L)",
                            "(5,2L)"
                    });
        Util.checkQueryOutputsAfterSort(it, expectedRes);
    }

    /**
     * Udf that has schema of tuple column with no inner schema 
     */
    public static class UDFTupleNullSchema extends EvalFunc <Tuple> {
        public Tuple exec(Tuple input) {
            return input;
        }
        
        @Override
        public Schema outputSchema(Schema input) {
            FieldSchema fs =
                new Schema.FieldSchema(getSchemaName("UDFTupleNullSchema", input),
                        DataType.TUPLE);
                return new Schema(fs);
        }

    }
    
    /**
     * Test UNION ONSCHEMA with input relation having column names with multiple
     * level of namespace in their names
     * @throws IOException
     * @throws ParseException
     */
    @Test
    public void testUnionOnSchemaScopeMulti() throws IOException, ParseException {
        PigServer pig = new PigServer(ExecType.LOCAL);
        String query_prefix =
            "  a = load '" + INP_FILE_2NUMS+ "' as (i:int, j:int); "
            + "b = group a by i; "
            + "c = foreach b generate group as gp, flatten(a); "
            + "d = group c by $0; "
            + "e = foreach d generate group as gp, flatten(c); "
            + "f = load  '" + INP_FILE_2NUMS + "' as (i, j); "
            ;
        String query = query_prefix + "u = union onschema e,f;";
        Util.registerMultiLineQuery(pig, query);

        Schema sch = pig.dumpSchema("u");
        Schema expectedSch = 
            Util.getSchemaFromString("gp: int,c::gp: int,i: int,j: int");
        assertEquals("Checking expected schema",sch, expectedSch);
        
        
        query = query_prefix + "u = union onschema f,e;";
        Util.registerMultiLineQuery(pig, query);

        sch = pig.dumpSchema("u");
        expectedSch = 
            Util.getSchemaFromString("i: int,j: int, gp: int,c::gp: int");
        assertEquals("Checking expected schema",sch, expectedSch);
        
        
        Iterator<Tuple> it = pig.openIterator("u");
        List<Tuple> expectedRes = 
            Util.getTuplesFromConstantTupleStrings(
                    new String[] {
                            "(1,2,null,null)",
                            "(5,3,null,null)",
                            "(1,2,1,1)",
                            "(5,3,5,5)",
                    });
        Util.checkQueryOutputsAfterSort(it, expectedRes);

    }
    
    
}

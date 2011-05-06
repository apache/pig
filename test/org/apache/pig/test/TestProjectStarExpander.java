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

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Iterator;
import java.util.List;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.Utils;
import org.apache.pig.parser.ParserException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test project of multiple fields
 */
public class TestProjectStarExpander  {
    private static final String INP_FILE_5FIELDS = "TestProjectStarExpander1";
    
    @Before
    public void setUp() throws Exception {
        FileLocalizer.setInitialized(false);
    }


    @After
    public void tearDown() throws Exception {
    }
    
    @BeforeClass
    public static void oneTimeSetup() throws IOException, Exception {
        // first input file
        PrintWriter w = new PrintWriter(new FileWriter(INP_FILE_5FIELDS));
        w.println("10\t20\t30\t40\t50");
        w.println("11\t21\t31\t41\t51");
        w.close();

    }
    
    @AfterClass
    public static void oneTimeTearDown() throws Exception {

        new File(INP_FILE_5FIELDS).delete();

    }
 


    @Test
    public void testProjectStarForeach() throws IOException, ParserException {
        PigServer pig = new PigServer(ExecType.LOCAL);
        
        //specifying the new aliases only for initial set of fields
        String query =
            "  l1 = load '" + INP_FILE_5FIELDS + "' as (a : int, b : int, c : int, d : int, e : int);"
            + "f = foreach l1 generate * as (aa, bb, cc);"
        ; 

        Util.registerMultiLineQuery(pig, query);
       
        Schema expectedSch = Utils.getSchemaFromString("aa : int, bb : int, cc : int, d : int, e : int");
        Schema sch = pig.dumpSchema("f");
        assertEquals("Checking expected schema", expectedSch, sch);
        
        //specifying aliases for all fields
        query =
            "  l1 = load '" + INP_FILE_5FIELDS + "' as (a : int, b : int, c : int, d : int, e : int);"
            + "f = foreach l1 generate * as (aa, bb, cc, dd, ee);"
        ; 
        Util.registerMultiLineQuery(pig, query);
        
        expectedSch = Utils.getSchemaFromString("aa : int, bb : int, cc : int, dd : int, ee : int");
        sch = pig.dumpSchema("f");
        assertEquals("Checking expected schema", expectedSch, sch);
        Iterator<Tuple> it = pig.openIterator("f");
        
        List<Tuple> expectedRes = 
            Util.getTuplesFromConstantTupleStrings(
                    new String[] {
                            "(10,20,30,40,50)",
                            "(11,21,31,41,51)",
                    });
        Util.checkQueryOutputsAfterSort(it, expectedRes);

    }
    
    /**
     * Test projecting multiple *
     * @throws IOException
     * @throws ParseException
     */
    @Test
    public void testProjectStarMulti() throws IOException, ParserException {
        PigServer pig = new PigServer(ExecType.LOCAL);
        String query =
            "  l1 = load '" + INP_FILE_5FIELDS + "' as (a : int, b : int, c : int);"
            + "f = foreach l1 generate * as (aa, bb, cc), *;"
        ; 

        Util.registerMultiLineQuery(pig, query);
       
        Schema expectedSch = Utils.getSchemaFromString(
                "aa : int, bb : int, cc : int, a : int, b : int, c : int");
        Schema sch = pig.dumpSchema("f");
        assertEquals("Checking expected schema", expectedSch, sch);
        List<Tuple> expectedRes = 
            Util.getTuplesFromConstantTupleStrings(
                    new String[] {
                            "(10,20,30,10,20,30)",
                            "(11,21,31,11,21,31)",
                    });
        Iterator<Tuple> it = pig.openIterator("f");
        Util.checkQueryOutputsAfterSort(it, expectedRes);
    }
   
}
